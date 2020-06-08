import uuid

from raftsies.log import LogEntry
from raftsies.events import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClientRequest,
    RequestVote,
    Vote,
    ElectionTimerExpired,
)


def test_append_entries_request_log_entry_serialization_heartbeat():
    current_term = 1
    node_num = 1
    prev_log_index = 1
    prev_log_term = 1
    commit_index = 1

    append_entries_req = AppendEntriesRequest(
        current_term,
        node_num,
        prev_log_index,
        prev_log_term,
        commit_index,
        str(uuid.uuid4()),
        [],  # Empty because its a heartbeat.
    )

    assert len(append_entries_req.entries) == 0
    assert append_entries_req.to_bytes()


def test_append_entries_request_log_entry_serialization_single_entry():
    current_term = 1
    node_num = 1
    prev_log_index = 1
    prev_log_term = 1
    commit_index = 1

    log_entry = LogEntry(1, 1, "blah")
    append_entries_req = AppendEntriesRequest(
        current_term,
        node_num,
        prev_log_index,
        prev_log_term,
        commit_index,
        str(uuid.uuid4()),
        [log_entry],
    )

    assert len(append_entries_req.entries) == 1
    assert str(append_entries_req.entries[0]) == str(log_entry)
    assert append_entries_req.to_bytes()


def test_append_entries_request_log_entry_serialization_multiple_entrys():
    current_term = 1
    node_num = 1
    prev_log_index = 1
    prev_log_term = 1
    commit_index = 1

    log_entry = LogEntry(1, 1, "blah")
    another_one = LogEntry(2, 2, "dj khaled")
    append_entries_req = AppendEntriesRequest(
        current_term,
        node_num,
        prev_log_index,
        prev_log_term,
        commit_index,
        str(uuid.uuid4()),
        [log_entry, another_one],
    )

    assert len(append_entries_req.entries) == 2
    assert str(append_entries_req.entries[0]) == str(log_entry)
    assert str(append_entries_req.entries[1]) == str(another_one)
    assert append_entries_req.to_bytes()


def test_vote_resp_vote_granted():
    TERM = 2
    VOTE_RESP = True

    vote_resp = Vote(TERM, VOTE_RESP)

    assert vote_resp.term == TERM
    assert vote_resp.vote

    data_dict = {"TERM": TERM, "VOTE_GRANTED": VOTE_RESP}
    vote_resp = Vote.from_dict(data_dict)

    assert vote_resp.term == TERM
    assert vote_resp.vote
    assert vote_resp.to_bytes()


def test_vote_request():
    TERM = 2
    CANDIDATE_ID = 1
    LAST_LOG_INDEX = 2
    LAST_LOG_TERM = 1

    vote_req = RequestVote(TERM, CANDIDATE_ID, LAST_LOG_INDEX, LAST_LOG_TERM)

    assert vote_req.term == TERM
    assert vote_req.candidate_id == CANDIDATE_ID
    assert vote_req.last_log_index == LAST_LOG_INDEX
    assert vote_req.last_log_term == LAST_LOG_TERM

    vote_req = RequestVote.from_dict(vote_req._data)

    assert vote_req.term == TERM
    assert vote_req.candidate_id == CANDIDATE_ID
    assert vote_req.last_log_index == LAST_LOG_INDEX
    assert vote_req.last_log_term == LAST_LOG_TERM
    assert vote_req.to_bytes()


def test_append_entries_response_ok_without_uuid():
    RESULT = "OK"
    TERM = 2
    NODE = 1

    msg = AppendEntriesResponse(RESULT, NODE, TERM)

    assert msg.term == TERM
    assert msg.node == NODE
    assert msg.result == RESULT
    assert not msg.uuid

    msg = AppendEntriesResponse.from_dict(msg._data)

    assert msg.term == TERM
    assert msg.node == NODE
    assert msg.result == RESULT
    assert not msg.uuid
    assert msg.to_bytes()


def test_election_timer_expired_can_serialize_to_bytes():
    msg = ElectionTimerExpired()

    assert msg.to_bytes()


def test_client_request_generates_uuid():
    OP = "GET"
    KEY = "foo"

    msg = ClientRequest(OP, KEY)

    generated_uuid = msg.uuid
    assert msg.op == OP
    assert msg.key == KEY
    assert not msg.value
    assert generated_uuid

    msg = ClientRequest.from_dict(msg._data)

    assert msg.op == OP
    assert msg.key == KEY
    assert not msg.value
    assert msg.uuid == generated_uuid


def test_client_set_request_from_dict():
    OP = "SET"
    KEY = "foo"
    VALUE = "bar"

    msg = ClientRequest(OP, KEY, VALUE)

    generated_uuid = msg.uuid
    assert msg.op == OP
    assert msg.key == KEY
    assert msg.value == VALUE
    assert generated_uuid

    msg = ClientRequest.from_dict(msg._data)

    assert msg.op == OP
    assert msg.key == KEY
    assert msg.value == VALUE
    assert msg.uuid == generated_uuid


def test_client_delete_request_from_dict():
    OP = "DELETE"
    KEY = "foo"

    msg = ClientRequest(OP, KEY)

    generated_uuid = msg.uuid
    assert msg.op == OP
    assert msg.key == KEY
    assert not msg.value
    assert generated_uuid

    msg = ClientRequest.from_dict(msg._data)

    assert msg.op == OP
    assert msg.key == KEY
    assert not msg.value
    assert msg.uuid == generated_uuid


def test_client_invalid_request_from_dict():
    OP = "BOO"
    VALUE = "bar"

    msg = ClientRequest(OP, None, VALUE)

    generated_uuid = msg.uuid
    assert msg.op == OP
    assert generated_uuid

    msg = ClientRequest.from_dict(msg._data)

    assert msg.op == "INVALID"


def test_client_invalid_request_from_dict_no_key_for_SET():
    OP = "SET"
    VALUE = "bar"

    msg = ClientRequest(OP, None, VALUE)

    generated_uuid = msg.uuid
    assert msg.op == OP
    assert generated_uuid

    msg = ClientRequest.from_dict(msg._data)

    assert msg.op == "INVALID"


def test_append_entries_request_from_dict():
    TERM = 2
    LEADER_ID = 1
    LAST_LOG_INDEX = 2
    LAST_LOG_TERM = 1
    LEADER_COMMIT = 3

    req = AppendEntriesRequest(
        TERM, LEADER_ID, LAST_LOG_INDEX, LAST_LOG_TERM, LEADER_COMMIT
    )

    assert req.term == TERM
    assert req.leader_id == LEADER_ID
    assert req.prev_log_index == LAST_LOG_INDEX
    assert req.prev_log_term == LAST_LOG_TERM

    req = AppendEntriesRequest.from_dict(req._data)

    assert req.term == TERM
    assert req.leader_id == LEADER_ID
    assert req.prev_log_index == LAST_LOG_INDEX
    assert req.prev_log_term == LAST_LOG_TERM
    assert req.to_bytes()
