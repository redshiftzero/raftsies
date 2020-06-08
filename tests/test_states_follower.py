from unittest.mock import MagicMock

from raftsies.events import (
    ClientRequest,
    AppendEntriesResponse,
    AppendEntriesRequest,
    Vote,
    RequestVote,
)
from raftsies.states import Follower, Candidate


def test_follower_handle_ElectionTimerExpired_if_not_heard_from_leader():
    fake_controller = MagicMock()

    node = Follower(1, fake_controller)
    node.current_term = 1
    node.heard_from_leader = False

    result = node.handle_ElectionTimerExpired()

    assert isinstance(result, Candidate)


def test_follower_handle_ElectionTimerExpired_if_heard_from_leader(tmpdir):
    fake_controller = MagicMock()

    node = Follower(1, fake_controller)
    node.current_term = 1
    node.heard_from_leader = True

    result = node.handle_ElectionTimerExpired()

    assert not result


def test_follower_handle_ClientRequest_redirects_to_leader():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_client = MagicMock()
    fake_controller.NODES = {0: {"host": "localhost", "port": 667}}

    request = ClientRequest("GET", "foo")

    node = Follower(1, fake_controller)
    node.current_term = 1
    node.leader_id = 0
    node.heard_from_leader = True

    result = node.handle_ClientRequest(fake_socket, request)

    assert not result
    assert fake_controller.send_to_client.call_count == 1


def test_follower_handle_AppendEntriesResponse_no_state_change():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    request = AppendEntriesResponse(2, 2, 2)

    node = Follower(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesResponse(fake_socket, request)

    assert not result


def test_follower_stop_just_stops_the_timer():
    fake_controller = MagicMock()
    fake_controller.stop_timer = MagicMock()

    node = Follower(1, fake_controller)

    node.stop()

    assert fake_controller.stop_timer.call_count == 1


def test_follower_start_begins_election_timer():
    fake_controller = MagicMock()
    fake_controller.election_timer = MagicMock()

    node = Follower(1, fake_controller)
    node.current_term = 1

    node.start()

    assert fake_controller.election_timer.call_count == 1


def test_follower_handle_both_failed_AppendEntriesRequest():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()
    fake_controller.add_new_entry = MagicMock(return_value=False)

    node = Follower(1, fake_controller)
    node.current_term = 1
    node.heard_from_leader = False
    node.commit_index = 0
    node.last_applied = 0

    request = AppendEntriesRequest(2, 2, 2, 2, 2)

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert node.current_term == 2
    assert node.heard_from_leader
    assert not result  # no state change
    assert not node.voted
    assert node.leader_id == 2
    assert node.commit_index == 0
    assert node.last_applied == 0
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_both_successful_AppendEntriesRequest():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()
    fake_controller.add_new_entry = MagicMock(return_value=True)

    node = Follower(1, fake_controller)
    node.current_term = 1
    node.heard_from_leader = False
    node.commit_index = 0
    node.last_applied = 0

    request = AppendEntriesRequest(2, 2, 2, 2, 2)

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert node.current_term == 2
    assert node.heard_from_leader
    assert not result  # no state change
    assert not node.voted
    assert node.leader_id == 2
    assert node.commit_index == 2
    assert node.last_applied == 2
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_RequestVote_produces_no_state_change_vote_this_term():
    """No persisted state"""
    CANDIDATE_TERM = 2
    CURRENT_TERM = CANDIDATE_TERM - 1
    CURRENT_INDEX = 2

    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()
    fake_controller.get_prev_log_term = MagicMock(return_value=CURRENT_TERM)
    fake_controller.get_prev_log_index = MagicMock(return_value=CURRENT_INDEX)

    request = RequestVote(CANDIDATE_TERM, 2, CURRENT_INDEX, CURRENT_TERM)

    node = Follower(6666, fake_controller)
    node.current_term = CURRENT_TERM
    node.voted = False
    node.heard_from_leader = False

    result = node.handle_RequestVote(fake_socket, request)

    assert not result
    assert node.voted is True
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_RequestVote_produces_no_state_change_already_voted():
    """No persisted state"""
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()

    request = RequestVote(2, 2, 2, 2)

    node = Follower(6666, fake_controller)
    node.current_term = 1
    node.voted = True

    result = node.handle_RequestVote(fake_socket, request)

    assert not result
    assert node.voted
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_RequestVote_persisted_state_prevents_duplicate_votes():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()

    request = RequestVote(2, 2, 2, 2)

    node = Follower(1, fake_controller)
    node.current_term = 2
    node.voted = True
    node.heard_from_leader = False
    node._update_persisted_status()

    # Now reset voted and heard_from_leader
    node.voted = False
    node.heard_from_leader = True

    result = node.handle_RequestVote(fake_socket, request)

    assert not result
    assert node.voted
    assert not node.heard_from_leader
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_RequestVote_does_not_vote_for_stale_term_candidates():
    CANDIDATE_TERM = 2
    STALE_INDEX = 2
    STALE_TERM = 0

    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()
    fake_controller.get_prev_log_term = MagicMock(return_value=STALE_TERM + 1)

    request = RequestVote(CANDIDATE_TERM, 2, STALE_INDEX, STALE_TERM)

    node = Follower(1, fake_controller)
    node.current_term = STALE_TERM + 1
    node.voted = False
    node.heard_from_leader = False

    result = node.handle_RequestVote(fake_socket, request)

    assert not result
    assert not node.voted
    assert not node.heard_from_leader
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_RequestVote_does_not_vote_for_stale_index_candidates():
    CANDIDATE_TERM = 2
    CURRENT_TERM = CANDIDATE_TERM - 1
    STALE_INDEX = 2

    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()
    fake_controller.get_prev_log_term = MagicMock(return_value=CURRENT_TERM)
    fake_controller.get_prev_log_index = MagicMock(return_value=STALE_INDEX + 2)

    request = RequestVote(CANDIDATE_TERM, 2, STALE_INDEX, CURRENT_TERM)

    node = Follower(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.voted = False
    node.heard_from_leader = False

    result = node.handle_RequestVote(fake_socket, request)

    assert not result
    assert not node.voted
    assert not node.heard_from_leader
    assert fake_controller.send_to_node.call_count == 1


def test_follower_handle_RequestVote_does_not_vote_for_stale_candidates():
    CANDIDATE_TERM = 1
    CURRENT_TERM = 2

    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()

    request = RequestVote(CANDIDATE_TERM, 2, 0, 0)

    node = Follower(1, fake_controller)
    node.current_term = CURRENT_TERM
    node.voted = False
    node.heard_from_leader = False

    result = node.handle_RequestVote(fake_socket, request)

    assert not result
    assert not node.voted
    assert not node.heard_from_leader
    assert fake_controller.send_to_node.call_count == 0


def test_follower_handle_Vote_produces_no_state_change():
    fake_socket = MagicMock()
    fake_controller = MagicMock()

    request = Vote(2, True)

    node = Follower(1, fake_controller)
    node.current_term = 1

    result = node.handle_Vote(fake_socket, request)

    assert not result


def test_follower_handle_AppendEntriesRequest_discards_requests_from_old_terms():
    fake_socket = MagicMock()
    fake_controller = MagicMock()
    fake_controller.send_to_node = MagicMock()

    request = AppendEntriesRequest(0, 0, 0, 0, 0)

    node = Follower(1, fake_controller)
    node.current_term = 1

    result = node.handle_AppendEntriesRequest(fake_socket, request)

    assert not result
    assert fake_controller.send_to_node.call_count == 0
