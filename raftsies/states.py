from abc import ABC, abstractmethod
import json
import logging
import socket
import time
import threading

from typing import Dict, Optional

from raftsies.control import RaftController
from raftsies.events import (
    ClientRequest,
    ClientGetRequest,
    ClientSetRequest,
    ClientDeleteRequest,
    ClientInvalidRequest,
    ClientCloseRequest,
    ClientResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
    Vote,
    RequestVote,
)
from raftsies.log import LogEntry

logger = logging.getLogger(__name__)


class RaftServerState(ABC):
    """Interface for a raft server state.

    Each Raft internal request handler can return another state if processing a
    request results in a server state change.
    """

    def __init__(self, node_num: int, controller: RaftController):
        self.node_num = node_num
        self.controller = controller

        # Latest term each server has seen. Monatonically increasing.
        self.current_term: int = self.controller.get_prev_log_term()
        self.voted_for: Optional[
            int
        ] = None  # Candidate that got vote in current term. Else None.

        # Index of highest log entry that is committed.
        self.commit_index: int = self.controller.get_prev_log_index()

        # Index of highest log entry applied to the state machine.
        self.last_applied: int = self.controller.get_prev_log_index()

        self.majority = len(self.controller.get_network_peers()) // 2 + 1

    @abstractmethod
    def start(self) -> None:
        """Start any background actions needed for this server state."""

    @abstractmethod
    def stop(self) -> None:
        """Stop any background actions needed for this server state."""

    @abstractmethod
    def handle_ElectionTimerExpired(self) -> Optional["RaftServerState"]:
        """Handling election timers can result in a state transition."""

    @abstractmethod
    def handle_ClientRequest(self, conn: socket.socket, req: ClientRequest) -> None:
        """Handling client requests cannot result in a state transition."""

    @abstractmethod
    def handle_AppendEntriesRequest(
        self, conn: socket.socket, req: AppendEntriesRequest
    ) -> Optional["RaftServerState"]:
        """Handling AppendEntriesRequest can result in a state transition."""

    @abstractmethod
    def handle_AppendEntriesResponse(
        self, conn: socket.socket, msg: AppendEntriesResponse
    ) -> Optional["RaftServerState"]:
        """Handling AppendEntriesResponse can result in a state transition."""

    @abstractmethod
    def handle_RequestVote(
        self, conn: socket.socket, msg: RequestVote
    ) -> Optional["RaftServerState"]:
        """Handling RequestVote can result in a state transition."""

    @abstractmethod
    def handle_Vote(
        self, conn: socket.socket, msg: Vote
    ) -> Optional["RaftServerState"]:
        """Handling Vote can result in a state transition."""

    def _revert_to_follower_if_term_is_newer(
        self, new_term: int
    ) -> Optional["RaftServerState"]:
        if new_term > self.current_term:
            state = Follower(self.node_num, self.controller)
            state.current_term = new_term
            return state
        else:
            return None

    def _discard_message_if_lower_term(self, term: int) -> bool:
        if term < self.current_term:
            return True
        else:
            return False


class PendingClient:
    def __init__(
        self, conn: socket.socket, req: ClientRequest, resp: ClientResponse, index: int
    ):
        self.conn = conn
        self.req = req
        self.resp = resp
        self.index = index


class Follower(RaftServerState):
    def __init__(self, node_num: int, controller: RaftController):
        super().__init__(node_num, controller)
        # We'll learn the correct leader_id from the first AppendEntriesRequest we see.
        self.leader_id = 0

        self.heard_from_leader = False
        self.voted = False

        logger.debug("I am now a FOLLOWER!!!!!!")

    def _update_persisted_status(self) -> None:
        current_status = {
            "current_term": self.current_term,
            "heard_from_leader": self.heard_from_leader,
            "voted": self.voted,
        }

        with open(f"follower_status_{self.node_num}.json", "w") as f:
            f.write(json.dumps(current_status))

    def start(self) -> None:
        threading.Thread(target=self.controller.election_timer).start()

    def stop(self) -> None:
        self.controller.stop_timer()

    def handle_ElectionTimerExpired(self) -> Optional["Candidate"]:
        """If we didn't hear from a leader, let's become a candidate."""
        if not self.heard_from_leader:
            return Candidate(self.node_num, self.controller)
        else:
            logger.debug("Leader is still talking to me, continuing as a follower")
            self.heard_from_leader = False
            self._update_persisted_status()
            return None

    def handle_ClientRequest(self, conn: socket.socket, msg: ClientRequest) -> None:
        logger.debug("Not the leader, redirecting client to current leader")
        leader_host = self.controller.NODES[self.leader_id]["host"]
        leader_port = self.controller.NODES[self.leader_id]["port"]
        resp = ClientResponse("REDIRECT", None, leader_host, leader_port)
        self.controller.send_to_client(conn, resp.to_bytes(), True)
        return None

    def handle_AppendEntriesRequest(
        self, conn: socket.socket, req: AppendEntriesRequest
    ) -> Optional[RaftServerState]:
        logger.debug(f"got append entries request: {req}")

        if self._discard_message_if_lower_term(req.term):
            logger.debug("AppendEntriesRequest msg too old")
            return None

        self.heard_from_leader = True
        self._update_persisted_status()

        # Update the current_term and if we do, reset our vote counter.
        if not self.current_term or req.term > self.current_term:
            self.current_term = req.term
            self.voted = False
            self._update_persisted_status()

        # Update Leader ID
        self.leader_id = req.leader_id

        logger.debug(req.uuid)
        if self.controller.add_new_entry(
            req.prev_log_index, req.prev_log_term, req.entries
        ):
            resp = AppendEntriesResponse(
                "OK", self.node_num, self.current_term, req.uuid
            )

            self.controller.apply_entries_to_state_machine(
                self.commit_index, req.leader_commit
            )
            self.commit_index = req.leader_commit
            self.last_applied = self.commit_index
        else:  # Cannot commit
            resp = AppendEntriesResponse(
                "FALSE", self.node_num, self.current_term, req.uuid
            )
        logger.debug("sending response to leader!")
        self.controller.send_to_node(self.leader_id, resp.to_bytes())
        return None

    def handle_AppendEntriesResponse(
        self, conn: socket.socket, resp: AppendEntriesResponse
    ) -> Optional[RaftServerState]:
        """Only the leader handles these messages"""
        return None

    def handle_RequestVote(
        self, conn: socket.socket, msg: RequestVote
    ) -> Optional[RaftServerState]:

        if self._discard_message_if_lower_term(msg.term):
            logger.debug("RequestVote too old")
            return None

        # Check with our persisted data to see if we already voted in this term.
        try:
            with open(f"follower_status_{self.node_num}.json", "r") as f:
                status = json.loads(f.read())
                if status["current_term"] == self.current_term:
                    self.heard_from_leader = status["heard_from_leader"]
                    self.voted = status["voted"]
        except OSError:
            pass

        if self.voted:  # No double voting!
            vote_response = False
        elif msg.last_log_term < self.controller.get_prev_log_term():
            # Candidate is missing entries from this term.
            vote_response = False
        elif (
            msg.last_log_term == self.controller.get_prev_log_term()
            and msg.last_log_index < self.controller.get_prev_log_index()
        ):
            # Candidate and us have entries from the same term, but they don't have as
            # many log entries as us in this term.
            vote_response = False
        else:
            self.voted = True
            vote_response = True
            self._update_persisted_status()

        vote_req = Vote(self.current_term, vote_response)
        self.controller.send_to_node(msg.candidate_id, vote_req.to_bytes())
        return None

    def handle_Vote(self, conn: socket.socket, msg: Vote) -> Optional[RaftServerState]:
        """Followers do not receive votes. Only candidates do. Ignore."""
        return None


# We replicate all commands that modify state in the KV store (SET, DELETE),
# as well as GETs. See section 8 in the paper. This means that we don't need
# to implement additional logic for the heartbeat response prior to responding
# to read requests, at the cost of more bandwidth and larger logs. Since
# we can later compact the logs, this seems to me to be a reasonable
# tradeoff.
CLIENT_COMMANDS = ["GET", "SET", "DELETE"]


class Leader(RaftServerState):
    def __init__(self, node_num: int, controller: RaftController):
        super().__init__(node_num, controller)

        logger.debug("I am now a LEADER!!!!!!")
        self.running = False

        """
        Dict that keeps track of client requests that have come in, and are in the
        "waiting for response from our append entries request" state.
        """
        self.pending_commits: Dict[str, PendingClient] = {}

        """
        Next index is a per-node value that indicates the index of the next log index to
        send to that server. Starts at the leader's last log index + 1. Resets after elections.
        """
        self.next_index = {}

        """
        Match index is a per-node value that indicates the index of the highest log
        entry that has been replicated on a given server. Resets after elections.
        """
        self.match_index = {}
        for peer in self.controller.get_network_peers():
            self.next_index.update({peer: self.controller.get_prev_log_index() + 1})
            self.match_index.update({peer: 0})

    def start(self) -> None:
        self.running = True
        # Section 8: Leaders must commit a no-op into the log at the start
        # of its term.
        self._replicate_nop()
        threading.Thread(target=self.send_heartbeat).start()

    def stop(self) -> None:
        self.running = False

    def _replicate_nop(self) -> None:
        index = self.controller.get_prev_log_index() + 1
        item = LogEntry(index, self.current_term, "NOP")
        for peer in self.controller.get_network_peers():
            append_entries_req = AppendEntriesRequest(
                self.current_term,
                self.node_num,
                self.controller.get_prev_log_index(),
                self.controller.get_prev_log_term(),
                self.commit_index,
                "",
                [item],
            )
            self.controller.send_to_node(peer, append_entries_req.to_bytes())

        self.controller.add_new_entry(
            self.controller.get_prev_log_index(),
            self.controller.get_prev_log_term(),
            [item],
        )

    def send_heartbeat(self) -> None:
        while self.running:
            logger.debug("🖤🖤 sending heartbeat messages to my peers 🖤🖤")
            for peer in self.controller.get_network_peers():
                append_entries_req = AppendEntriesRequest(
                    self.current_term,
                    self.node_num,
                    self.controller.get_prev_log_index(),
                    self.controller.get_prev_log_term(),
                    self.commit_index,
                    "",
                    [],  # Empty because its a heartbeat.
                )
                self.controller.send_to_node(peer, append_entries_req.to_bytes())

            time.sleep(self.controller.HEARTBEAT_TIME)

    def handle_ElectionTimerExpired(self) -> Optional[RaftServerState]:
        """Not relevant for leaders"""
        return None

    def handle_ClientRequest(self, conn: socket.socket, req: ClientRequest) -> None:
        logger.debug(f"got client request {req}")

        if isinstance(req, ClientGetRequest) and req.key:
            requested_value = self.controller.get_key(req.key)
            response = req.handle(requested_value)
            arguments = f"{req.key}"
        elif isinstance(req, ClientSetRequest):
            response = req.handle()
            arguments = f"{req.key} {req.value}"
        elif isinstance(req, ClientDeleteRequest):
            response = req.handle()
            arguments = f"{req.key}"
        elif isinstance(req, ClientInvalidRequest):
            response = req.handle()
            self.controller.send_to_client(conn, response.to_bytes())
            return None
        elif isinstance(req, ClientCloseRequest):
            response = req.handle()
            self.controller.send_to_client(conn, response.to_bytes(), True)
            return None

        index = self.controller.get_prev_log_index() + 1
        item = LogEntry(index, self.current_term, f"{req.op} {arguments}")
        if req.op in CLIENT_COMMANDS:
            for peer in self.controller.get_network_peers():
                append_entries_req = AppendEntriesRequest(
                    self.current_term,
                    self.node_num,
                    self.controller.get_prev_log_index(),
                    self.controller.get_prev_log_term(),
                    self.commit_index,
                    req.uuid,
                    [item],
                )
                self.controller.send_to_node(peer, append_entries_req.to_bytes())

            # Apply to own log but wait for confirmations before applying
            # to state machine.
            self.controller.add_new_entry(
                self.controller.get_prev_log_index(),
                self.controller.get_prev_log_term(),
                [item],
            )

        # Save some deets about this client request. We'll respond when
        # we get enough commits back from our peers.
        self.pending_commits.update(
            {req.uuid: PendingClient(conn, req, response, index)}
        )

    def handle_AppendEntriesRequest(
        self, conn: socket.socket, msg: AppendEntriesRequest
    ) -> Optional[RaftServerState]:
        return self._revert_to_follower_if_term_is_newer(msg.term)

    def handle_AppendEntriesResponse(
        self, conn: socket.socket, msg: AppendEntriesResponse
    ) -> Optional[RaftServerState]:
        if self._discard_message_if_lower_term(msg.term):
            logger.debug("AppendEntriesResponse term too old")
            return None

        new_state = self._revert_to_follower_if_term_is_newer(msg.term)
        if new_state:
            return new_state

        logger.debug(f"got append entries response from a node: {msg}")
        if msg.uuid:
            try:
                pending_change: Optional[PendingClient] = self.pending_commits[msg.uuid]
            except KeyError:
                pending_change = None
        else:
            pending_change = None

        if msg.result == "FALSE":
            # We need to send more entries to get this node up to date.
            self.next_index[msg.node] -= 1

            previous_log_entry = self.controller.log[self.next_index[msg.node]]
            if isinstance(previous_log_entry, LogEntry):
                previous_log_term = previous_log_entry.term
                previous_log_index = previous_log_entry.index - 1
            else:
                raise RuntimeError("cant get previous log entry term")

            backtrack_entries = self.controller.log[self.next_index[msg.node] :]
            # We need backtrack_entries to be a List of LogEntry, so if we have a single
            # LogEntry, we make it a list.
            if isinstance(backtrack_entries, LogEntry):
                backtrack_entries = [backtrack_entries]

            append_entries_req = AppendEntriesRequest(
                self.current_term,
                self.node_num,
                previous_log_index,
                previous_log_term,
                self.commit_index,
                msg.uuid,
                backtrack_entries,
            )
            self.controller.send_to_node(msg.node, append_entries_req.to_bytes())
        elif msg.result == "OK":
            # TODO: Simplify further?
            if pending_change:
                self.match_index[msg.node] = pending_change.index
                self.next_index[msg.node] = pending_change.index + 1
            else:
                logger.debug("Heartbeat response")
                self.match_index[msg.node] = self.controller.get_prev_log_index()
                self.next_index[msg.node] = self.controller.get_prev_log_index() + 1

            logger.debug(
                f"updating node {msg.node} match_index={self.match_index[msg.node]}"
            )
            logger.debug(
                f"updating node {msg.node} next_index={self.next_index[msg.node]}"
            )

            # See if we have enough responses to advance the commit index.
            num_approvals = 0
            for peer in self.controller.get_network_peers():
                if self.match_index[peer] >= self.commit_index + 1:
                    num_approvals += 1

                if num_approvals >= self.majority:
                    logger.debug("got enough approvals!")
                    self.commit_index += 1

            # Respond to the client waiting for that item to be committed.
            if pending_change and self.commit_index >= pending_change.index:
                self.respond_to_client(
                    pending_change.conn, pending_change.req, pending_change.resp,
                )
        return None

    def respond_to_client(
        self, conn: socket.socket, req: ClientRequest, resp: ClientResponse
    ) -> None:
        pending_change = self.pending_commits[req.uuid]

        # Apply the committed change to the state machine.
        if resp.result == "OK" and req.op == "SET" and req.key and req.value:
            self.controller.run_command_on_state_machine(req.op, [req.key, req.value])
        elif resp.result == "OK" and req.op == "DELETE" and req.key:
            self.controller.run_command_on_state_machine(req.op, [req.key])

        # Update state change index since we applied this to the state machine.
        self.last_applied = pending_change.index

        # Respond to the client.
        self.controller.send_to_client(conn, resp.to_bytes())
        logger.debug("done processing client request")

        # Remove pending commit entry since we've completed
        # processing this request.
        del self.pending_commits[req.uuid]

    def handle_RequestVote(
        self, conn: socket.socket, msg: RequestVote
    ) -> Optional[RaftServerState]:
        """If we got a vote request from a newer term, we should step down"""
        return self._revert_to_follower_if_term_is_newer(msg.term)

    def handle_Vote(self, conn: socket.socket, msg: Vote) -> Optional[RaftServerState]:
        """Leaders do not count up votes.
        But if somehow we got a vote from a newer term, we should not be the leader!"""
        return self._revert_to_follower_if_term_is_newer(msg.term)


class Candidate(RaftServerState):
    def __init__(self, node_num: int, controller: RaftController):
        super().__init__(node_num, controller)
        logger.debug("I am now a CANDIDATE!!!!!!")
        self.current_term += 1  # We increment term when starting an election.
        self.votes_for_me = 1  # Vote for oneself when becoming a candidate.

    def start(self) -> None:
        threading.Thread(target=self.controller.election_timer).start()

        logger.debug("starting election...")
        for peer in self.controller.get_network_peers():
            vote_req = RequestVote(
                self.current_term,
                self.node_num,
                self.controller.get_prev_log_index(),
                self.controller.get_prev_log_term(),
            )
            self.controller.send_to_node(peer, vote_req.to_bytes())

    def stop(self) -> None:
        self.controller.stop_timer()

    def handle_ElectionTimerExpired(self) -> Optional[RaftServerState]:
        """Election must have failed. Let's become a candidate again."""
        return Candidate(self.node_num, self.controller)

    def handle_ClientRequest(self, conn: socket.socket, msg: ClientRequest) -> None:
        """We can't handle this as we don't even know who the leader is. Do nothing."""
        return None

    def handle_AppendEntriesRequest(
        self, conn: socket.socket, msg: AppendEntriesRequest
    ) -> Optional[RaftServerState]:
        """We've found a leader! Convert to follower if it's a request from a term newer than ours."""
        if self._discard_message_if_lower_term(msg.term):
            logger.debug("AppendEntriesRequest term too old")
            return None

        # Special case: If we get a request that has the SAME term as us, become a follower.
        # This means that another server was elected leader, and we should no longer be a candidate.
        if msg.term == self.current_term:
            state = Follower(self.node_num, self.controller)
            state.current_term = msg.term
            state.leader_id = msg.leader_id
            return state
        return self._revert_to_follower_if_term_is_newer(msg.term)

    def handle_AppendEntriesResponse(
        self, conn: socket.socket, msg: AppendEntriesResponse
    ) -> Optional[RaftServerState]:
        """We are not a leader, so we don't handle AppendEntriesResponse."""
        return self._revert_to_follower_if_term_is_newer(msg.term)

    def handle_RequestVote(
        self, conn: socket.socket, msg: RequestVote
    ) -> Optional[RaftServerState]:
        """Convert to follower if there is a vote request from a newer term."""
        return self._revert_to_follower_if_term_is_newer(msg.term)

    def handle_Vote(self, conn: socket.socket, msg: Vote) -> Optional[RaftServerState]:
        """Count up votes and convert self to leader if we have enough votes!"""
        new_state = self._revert_to_follower_if_term_is_newer(msg.term)
        if new_state:
            return new_state

        if msg.vote:
            self.votes_for_me += 1

        if self.votes_for_me >= self.majority:
            logger.debug("got enough votes!")
            state = Leader(self.node_num, self.controller)
            state.current_term = self.current_term
            return state

        return None
