#!/usr/bin/env python3

import logging
import threading

from typing import Dict, Type, Optional, NoReturn

from raftsies.events import (
    RaftEvent,
    ElectionTimerExpired,
    ClientRequest,
    AppendEntriesRequest,
    AppendEntriesResponse,
    Vote,
    RequestVote,
)
from raftsies.states import Follower, Leader, Candidate, RaftServerState
from raftsies.control import RaftController

logger = logging.getLogger(__name__)


DISPATCH: Dict[str, Type[RaftServerState]] = {
    "FOLLOWER": Follower,
    "CANDIDATE": Candidate,
    "LEADER": Leader,
}


class RaftNode:
    def __init__(self, node_num: int, state: Optional[str] = None):
        """
        Initial state can be optionally passed in for testing purposes.
        """
        # Immutable state
        self.node_num = node_num  # Index of this node.
        self.controller = RaftController(self.node_num)

        # Volatile and mutable state
        if state and state in DISPATCH.keys():
            self._state = DISPATCH[state](self.node_num, self.controller)
        elif not state:  # We start as followers and then we elect someone.
            self._state = Follower(self.node_num, self.controller)
        else:
            raise RuntimeError(
                f"unknown state {state}, must be FOLLOWER, CANDIDATE, or LEADER"
            )

    def state_transition(self, state: RaftServerState) -> None:
        """Each state gets a reference to the persistent controller"""
        self._state.stop()
        self._state = state
        self._state.start()

    def start(self) -> None:
        self._state.start()
        threading.Thread(target=self.process_events, args=()).start()

    def process_events(self) -> NoReturn:
        logger.debug(
            "starting to handle events, raft cluster messages, and client requests"
        )
        while True:
            conn, event = self.controller.receive()
            logger.debug(f"grabbing event from queue to handle: {str(event)}")

            try:
                msg = RaftEvent.from_bytes(event)
            except RuntimeError:
                logger.debug("Unknown type found - skipping")
                continue

            if isinstance(msg, ClientRequest) and conn:
                new_state = self._state.handle_ClientRequest(conn, msg)
            elif isinstance(msg, AppendEntriesRequest) and conn:
                new_state = self._state.handle_AppendEntriesRequest(conn, msg)
            elif isinstance(msg, AppendEntriesResponse) and conn:
                new_state = self._state.handle_AppendEntriesResponse(conn, msg)
            elif isinstance(msg, RequestVote) and conn:
                new_state = self._state.handle_RequestVote(conn, msg)
            elif isinstance(msg, Vote) and conn:
                new_state = self._state.handle_Vote(conn, msg)
            elif isinstance(msg, ElectionTimerExpired):
                new_state = self._state.handle_ElectionTimerExpired()
            else:
                new_state = None
                logger.debug("Don't know what this event/message is... skipping")

            if new_state:
                self.state_transition(new_state)
