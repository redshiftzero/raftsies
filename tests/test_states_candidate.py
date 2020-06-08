from unittest.mock import MagicMock

from raftsies.events import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClientRequest,
    RequestVote,
    Vote,
)
from raftsies.states import Candidate, Follower, Leader


def test_candidate_handle_AppendEntriesRequest_discards_requests_from_old_terms():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()

    request = AppendEntriesRequest(0, 0, 0, 0, 0)

    node = Candidate(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert not result
    assert fake_controller.send_to_node.call_count == 0


def test_candidate_handle_AppendEntriesRequest_becomes_follower_if_they_discover_leader():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Candidate.
    request = AppendEntriesRequest(2, 2, 2, 2, 2)

    node = Candidate(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert isinstance(result, Follower)


def test_candidate_handle_AppendEntriesRequest_becomes_follower_election_loss():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # In a lost election, AppendEntriesRequest will have the same term as the candidate.
    TERM = 1
    LEADER_ID = 2
    request = AppendEntriesRequest(TERM, LEADER_ID, 2, 2, 2)

    node = Candidate(1, fake_controller)
    node.current_term = TERM

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert isinstance(result, Follower)
    assert result.current_term == TERM
    assert result.leader_id == LEADER_ID


def test_candidate_handle_AppendEntriesResponse_if_resp_from_newer_term():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Candidate.
    request = AppendEntriesResponse(2, 2, 2)

    node = Candidate(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert isinstance(result, Follower)


def test_candidate_handle_ElectionTimerExpired_starts_new_election():
    fake_controller = MagicMock()

    node = Candidate(1, fake_controller)

    result = node.handle_ElectionTimerExpired()

    assert isinstance(result, Candidate)


def test_candidate_handle_RequestVote_becomes_follower_if_req_from_newer_term():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    # This request should have a higher term than the Candidate.
    request = RequestVote(2, 2, 2, 2)

    node = Candidate(1, fake_controller)
    node.current_term = 1

    result = node.handle_RequestVote(fake_socket, request)

    assert isinstance(result, Follower)
    fake_controller.send_to_client.assert_not_called()


def test_candidate_handle_Vote_returns_to_follower_if_vote_from_new_term():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    NEW_TERM = 2
    PREV_TERM = NEW_TERM - 1
    request = Vote(NEW_TERM, True)

    node = Candidate(PREV_TERM, fake_controller)
    node.current_term = PREV_TERM

    result = node.handle_Vote(fake_socket, request)

    assert isinstance(result, Follower)
    assert result.current_term == NEW_TERM


def test_candidate_handle_Vote_does_not_change_state_if_insufficient_votes():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    NEW_TERM = 1
    request = Vote(NEW_TERM, True)

    node = Candidate(NEW_TERM, fake_controller)
    node.current_term = NEW_TERM
    node.votes_for_me = -10  # Can't get elected with negative votes!

    result = node.handle_Vote(fake_socket, request)

    assert not result


def test_candidate_handle_Vote_becomes_leader_with_sufficient_votes():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    NEW_TERM = 1
    request = Vote(NEW_TERM, True)

    node = Candidate(1, fake_controller)
    node.current_term = NEW_TERM
    node.votes_for_me = node.majority

    result = node.handle_Vote(fake_socket, request)

    assert isinstance(result, Leader)
    assert result.current_term == NEW_TERM


def test_candidate_handle_ClientRequest_does_not_respond_to_client_request():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_client = MagicMock()

    request = ClientRequest("GET", "test")

    node = Candidate(1, fake_controller)
    node.current_term = 1

    result = node.handle_ClientRequest(fake_socket, request)

    # We shouldn't change state, and we shouldn't send any messages.
    assert not result
    fake_controller.send_to_client.assert_not_called()


def test_candidate_stop_just_stops_the_timer():
    fake_controller = MagicMock()
    fake_controller.stop_timer = MagicMock()

    node = Candidate(1, fake_controller)

    node.stop()

    assert fake_controller.stop_timer.call_count == 1


def test_candidate_start_begins_election_timer_and_requests_votes_from_peers():
    PEERS = [2, 3, 4]
    fake_controller = MagicMock()
    fake_controller.election_timer = MagicMock()
    fake_controller.get_network_peers = MagicMock(return_value=PEERS)
    fake_controller.get_prev_log_index = MagicMock(return_value=1)
    fake_controller.get_prev_log_term = MagicMock(return_value=0)
    fake_controller.send_to_node = MagicMock()

    node = Candidate(1, fake_controller)
    node.current_term = 1

    node.start()

    assert fake_controller.election_timer.call_count == 1
    assert fake_controller.send_to_node.call_count == len(PEERS)
