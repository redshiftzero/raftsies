import pytest
import time
from unittest.mock import MagicMock

from raftsies.events import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClientGetRequest,
    ClientSetRequest,
    ClientDeleteRequest,
    ClientCloseRequest,
    ClientInvalidRequest,
    ClientResponse,
    RequestVote,
    Vote,
)
from raftsies.log import LogEntry
from raftsies.states import Follower, Leader, PendingClient


def test_leader_start_begins_heartbeat():
    PEERS = [2, 3, 4]
    fake_controller = MagicMock()
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)

    node = Leader(1, fake_controller)
    node.send_heartbeat = MagicMock()
    node._replicate_nop = MagicMock()
    node.current_term = 1

    node.start()

    assert node.send_heartbeat.call_count == 1
    assert node.running
    assert node._replicate_nop.call_count == 1


def test_leader_send_heartbeat():
    PEERS = [2, 3, 4]
    fake_controller = MagicMock()
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=1)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    node = Leader(1, fake_controller)
    fake_controller.HEARTBEAT_TIME = MagicMock(return_value=0)
    node.running = True
    node.current_term = 1
    node._replicate_nop = MagicMock()

    node.start()  # Runs send_heartbeat which is not mocked.
    time.sleep(0.1)  # Wait a fraction of a second for the heartbeats to go out.
    node.stop()  # Stops heartbeat.

    assert fake_controller.send_to_node.call_count == len(PEERS)
    assert node._replicate_nop.call_count == 1


def test_leader_handle_ElectionTimerExpired_produces_no_state_change():
    fake_controller = MagicMock()
    node = Leader(1, fake_controller)

    new_state = node.handle_ElectionTimerExpired()

    assert not new_state


@pytest.mark.parametrize(
    "client_request",
    [
        (ClientGetRequest("test")),
        (ClientDeleteRequest("test")),
        (ClientSetRequest("foo", "bar")),
    ],
)
def test_leader_handle_ClientRequest_requiring_replication(client_request):
    PEERS = [2, 3, 4]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=1)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    node = Leader(1, fake_controller)
    node.current_term = 1

    result = node.handle_ClientRequest(fake_socket, client_request)

    # We shouldn't change state.
    assert not result

    # We should send an AppendEntries messages to all network peers.
    assert fake_controller.send_to_node.call_count == len(PEERS)

    # We should save the pending client deets so we know that clients
    # need to respond.
    assert node.pending_commits[client_request.uuid]


def test_leader_handle_ClientCloseRequest():
    PEERS = [2, 3, 4]
    fake_socket = MagicMock()
    fake_socket.close = MagicMock()
    fake_controller = MagicMock()
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=1)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()
    fake_controller.send_to_client = MagicMock()

    node = Leader(1, fake_controller)
    node.current_term = 1

    client_request = ClientCloseRequest()

    result = node.handle_ClientRequest(fake_socket, client_request)

    # We shouldn't change state.
    assert not result

    # We don't need to save pending information or send messages to Raft nodes.
    assert fake_controller.send_to_node.call_count == 0
    assert not node.pending_commits
    assert fake_controller.send_to_client.call_count == 1


def test_leader_handle_ClientInvalidRequest():
    PEERS = [2, 3, 4]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=1)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()
    fake_controller.send_to_client = MagicMock()

    node = Leader(1, fake_controller)
    node.current_term = 1

    client_request = ClientInvalidRequest()

    result = node.handle_ClientRequest(fake_socket, client_request)

    # We shouldn't change state.
    assert not result

    # We don't need to save pending information or send messages to Raft nodes.
    assert fake_controller.send_to_node.call_count == 0
    assert not node.pending_commits

    # Connection should still be open though until the client closes.
    assert fake_socket.close.call_count == 0
    assert fake_controller.send_to_client.call_count == 1


def test_leader_handle_AppendEntriesRequest_becomes_follower_if_they_discover_new_leader():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Leader.
    request = AppendEntriesRequest(2, 2, 2, 2, 2)

    node = Leader(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert isinstance(result, Follower)


def test_leader_handle_RequestVote_becomes_follower_if_new_term():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Leader.
    request = RequestVote(2, 2, 2, 2)

    node = Leader(1, fake_controller)
    node.current_term = 1

    result = node.handle_RequestVote(fake_socket, request)

    assert isinstance(result, Follower)


def test_leader_handle_Vote_becomes_follower_if_new_term():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Leader.
    request = Vote(2, True)

    node = Leader(1, fake_controller)
    node.current_term = 1

    result = node.handle_Vote(fake_socket, request)

    assert isinstance(result, Follower)


def test_leader_handle_AppendEntriesResponse_if_resp_from_newer_term():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Candidate.
    request = AppendEntriesResponse("OK", 2, 2)

    node = Leader(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert isinstance(result, Follower)


def test_leader_handle_AppendEntriesResponse_if_resp_is_successful_heartbeat():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.get_prev_log_index = MagicMock(return_value=LOG_INDEX)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    CURRENT_TERM = 1
    FOLLOWER_NODE_NUM = 4
    request = AppendEntriesResponse("OK", FOLLOWER_NODE_NUM, CURRENT_TERM)

    node = Leader(1, fake_controller)
    node.current_term = CURRENT_TERM

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert not result
    assert not node.pending_commits
    assert node.match_index[FOLLOWER_NODE_NUM] == LOG_INDEX
    assert node.next_index[FOLLOWER_NODE_NUM] == LOG_INDEX + 1
    assert fake_controller.send_to_node.call_count == 0


def test_leader_handle_AppendEntriesResponse_if_resp_is_successful_client_change():
    """Successful AppendEntries for a pending client request. First response"""
    PEERS = [2, 3, 4, 5, 6]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=LOG_INDEX)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    CURRENT_TERM = 1
    FOLLOWER_NODE_NUM = 4
    client_request = ClientGetRequest("teehee")
    request = AppendEntriesResponse(
        "OK", FOLLOWER_NODE_NUM, CURRENT_TERM, "client_req_uuid"
    )

    node = Leader(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.commit_index = LOG_INDEX - 1
    node.pending_commits = {
        "client_req_uuid": PendingClient(
            fake_socket, client_request, request, LOG_INDEX
        )
    }
    node.respond_to_client = MagicMock()

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert not result
    assert node.pending_commits  # Entry won't be popped yet
    assert node.respond_to_client.call_count == 0  # We don't respond to client yet.
    assert node.match_index[FOLLOWER_NODE_NUM] == LOG_INDEX
    assert node.next_index[FOLLOWER_NODE_NUM] == LOG_INDEX + 1
    assert fake_controller.send_to_node.call_count == 0


def test_leader_handle_AppendEntriesResponse_if_resp_is_successful_client_change_commit():
    """Successful AppendEntries for a pending client request. Final response, change gets committed."""
    PEERS = [2, 3, 5]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=LOG_INDEX)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    CURRENT_TERM = 1
    FOLLOWER_NODE_NUM = 4
    client_request = ClientGetRequest("teehee")
    request = AppendEntriesResponse(
        "OK", FOLLOWER_NODE_NUM, CURRENT_TERM, "client_req_uuid"
    )

    node = Leader(1, fake_controller)
    node.commit_index = LOG_INDEX - 1
    node.current_term = CURRENT_TERM
    node.pending_commits = {
        "client_req_uuid": PendingClient(
            fake_socket, client_request, request, LOG_INDEX
        )
    }
    node.respond_to_client = MagicMock()

    # Simulate that other nodes have committed.
    for peer in PEERS:
        node.match_index[peer] = LOG_INDEX
        node.next_index[peer] = LOG_INDEX + 1

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert not result
    assert node.respond_to_client.call_count == 1
    assert node.match_index[FOLLOWER_NODE_NUM] == LOG_INDEX
    assert node.next_index[FOLLOWER_NODE_NUM] == LOG_INDEX + 1
    assert fake_controller.send_to_node.call_count == 0


def test_leader_handle_AppendEntriesResponse_if_resp_is_late():
    """Response is late, commit has been handled (meaning that the client response
    has already been sent, and pending_commits is empty)."""
    PEERS = [2, 3, 5]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=LOG_INDEX)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    CURRENT_TERM = 1
    FOLLOWER_NODE_NUM = 4
    request = AppendEntriesResponse(
        "OK", FOLLOWER_NODE_NUM, CURRENT_TERM, "client_req_uuid"
    )

    node = Leader(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.respond_to_client = MagicMock()

    # Simulate that other nodes have committed.
    for peer in PEERS:
        node.match_index[peer] = LOG_INDEX
        node.next_index[peer] = LOG_INDEX + 1

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert not result
    assert (
        node.respond_to_client.call_count == 0
    )  # We don't respond to the client again
    assert node.match_index[FOLLOWER_NODE_NUM] == LOG_INDEX
    assert node.next_index[FOLLOWER_NODE_NUM] == LOG_INDEX + 1
    assert fake_controller.send_to_node.call_count == 0


def test_leader_handle_AppendEntriesResponse_if_resp_is_node_needs_backtracking():
    PEERS = [2, 3, 5]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=LOG_INDEX)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()
    fake_controller.log = MagicMock()
    fake_controller.log.__getitem__ = MagicMock(return_value=LogEntry(1, 1, "test"))

    CURRENT_TERM = 1
    FOLLOWER_NODE_NUM = 4
    client_request = ClientGetRequest("teehee")
    client_response = ClientResponse("OK")
    request = AppendEntriesResponse(
        "FALSE", FOLLOWER_NODE_NUM, CURRENT_TERM, "client_req_uuid"
    )

    node = Leader(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.pending_commits = {
        "client_req_uuid": PendingClient(
            fake_socket, client_request, client_response, LOG_INDEX
        )
    }
    node.respond_to_client = MagicMock()

    # Node is behind and needs backtracking
    node.match_index[FOLLOWER_NODE_NUM] = LOG_INDEX - 1
    node.next_index[FOLLOWER_NODE_NUM] = LOG_INDEX + 1

    # Simulate that other nodes have committed.
    for peer in PEERS:
        node.match_index[peer] = LOG_INDEX
        node.next_index[peer] = LOG_INDEX + 1

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert not result
    assert node.respond_to_client.call_count == 0

    # Next index is reduced by one but match_index is not
    assert node.match_index[FOLLOWER_NODE_NUM] == LOG_INDEX - 1
    assert node.next_index[FOLLOWER_NODE_NUM] == LOG_INDEX

    # Leader sends message to node to catch it up
    assert fake_controller.send_to_node.call_count == 1


def test_leader_handle_AppendEntriesResponse_if_node_needs_backtracking_err_cant_get_prior_log_entry():
    """Testing a runtime error is raised if for whatever reason we cannot get the prior LogEntry
    when attempting to backtrack for a node"""
    PEERS = [2, 3, 5]
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=LOG_INDEX)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()
    fake_controller.log = MagicMock()
    fake_controller.log.__getitem__ = MagicMock(return_value=[])

    CURRENT_TERM = 1
    FOLLOWER_NODE_NUM = 4
    client_request = ClientGetRequest("teehee")
    client_response = ClientResponse("OK")
    request = AppendEntriesResponse(
        "FALSE", FOLLOWER_NODE_NUM, CURRENT_TERM, "client_req_uuid"
    )

    node = Leader(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.pending_commits = {
        "client_req_uuid": PendingClient(
            fake_socket, client_request, client_response, LOG_INDEX
        )
    }
    node.respond_to_client = MagicMock()

    # Node is behind and needs backtracking
    node.match_index[FOLLOWER_NODE_NUM] = LOG_INDEX - 1
    node.next_index[FOLLOWER_NODE_NUM] = LOG_INDEX + 1

    # Simulate that other nodes have committed.
    for peer in PEERS:
        node.match_index[peer] = LOG_INDEX
        node.next_index[peer] = LOG_INDEX + 1

    with pytest.raises(RuntimeError):
        node.handle_AppendEntriesResponse(fake_socket, request)


@pytest.mark.parametrize(
    "client_request", [(ClientSetRequest("foo", "bar")), ClientDeleteRequest("foo")]
)
def test_leader_respond_to_client(client_request):
    """Testing a runtime error is raised if for whatever reason we cannot get the prior LogEntry
    when attempting to backtrack for a node"""
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    LOG_INDEX = 1
    fake_controller.send_to_node = MagicMock()
    fake_controller.send_to_client = MagicMock()
    fake_controller.log = MagicMock()
    fake_controller.log.__getitem__ = MagicMock(return_value=[])
    fake_controller.run_command_on_state_machine = MagicMock()

    CURRENT_TERM = 1
    client_response = ClientResponse("OK")

    node = Leader(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.pending_commits = {
        client_request.uuid: PendingClient(
            fake_socket, client_request, client_response, LOG_INDEX
        )
    }

    result = node.respond_to_client(fake_socket, client_request, client_response)

    assert not result
    assert fake_controller.send_to_client.call_count == 1
    assert fake_controller.send_to_node.call_count == 0
    assert not node.pending_commits  # Pending entry should be deleted


def test_leader_handle_AppendEntriesResponse_discards_msgs_from_old_terms():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()

    CANDIDATE_ID = 2
    OLD_TERM = 0
    msg = AppendEntriesResponse("OK", CANDIDATE_ID, OLD_TERM)
    assert msg.term == 0

    node = Leader(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesResponse(fake_socket, msg)

    assert not result
    assert fake_controller.send_to_node.call_count == 0
