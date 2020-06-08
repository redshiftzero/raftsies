import pytest
from unittest.mock import patch, MagicMock

from raftsies.server import RaftNode
from raftsies.states import Follower, Candidate, Leader


@patch("raftsies.server.RaftController")
def test_nodes_begin_in_follower_state_by_default(mock_controller):
    node = RaftNode(0)

    assert isinstance(node._state, Follower)


@patch("raftsies.server.RaftController")
def test_nodes_raises_err_for_unknown_state(mock_controller):
    with pytest.raises(RuntimeError):
        RaftNode(0, "CRAZYSTATE")


@pytest.mark.parametrize(
    "input_str,expected_state",
    [("FOLLOWER", Follower), ("LEADER", Leader), ("CANDIDATE", Candidate)],
)
def test_nodes_get_correct_state_for_allowed_state_options(input_str, expected_state):
    with patch("raftsies.server.RaftController"):
        node = RaftNode(0, input_str)

    assert isinstance(node._state, expected_state)


@patch("raftsies.server.RaftController")
def test_nodes_state_transition(mock_controller):
    node = RaftNode(0)

    assert isinstance(node._state, Follower)

    new_state = Leader(1, mock_controller)
    new_state.stop = MagicMock()
    new_state.start = MagicMock()

    old_state = node._state
    old_state.stop = MagicMock()
    node.state_transition(new_state)

    assert old_state.stop.call_count == 1
    assert new_state.stop.call_count == 0
    assert new_state.start.call_count == 1
    assert node._state == new_state


@patch("raftsies.server.RaftController")
def test_nodes_start_node_processing(mock_controller):
    node = RaftNode(0)
    node._state.start = MagicMock()
    node.process_events = MagicMock()

    node.start()

    assert node._state.start.call_count == 1
    assert node.process_events.call_count == 1
