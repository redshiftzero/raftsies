import json

from raftsies.log import Log, LogEntry


NODE_NUM = 0
TERM_NUM = 0
TEST_COMMAND = "teehee"


def test_log_entry_from_json():
    json_input = json.dumps({"INDEX": 0, "TERM": TERM_NUM, "COMMAND": TEST_COMMAND})

    log_entry = LogEntry.from_json(json_input)

    assert log_entry.index == 0
    assert log_entry.term == TERM_NUM
    assert log_entry.command == TEST_COMMAND


def test_log_entry_rep():
    test_prev_index = 2
    test_term = 2

    new_item = LogEntry(test_prev_index, test_term, TEST_COMMAND)

    assert new_item.index == test_prev_index
    assert new_item.term == test_term
    assert new_item.command == TEST_COMMAND


def test_log_prev_log_index(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")
    test_prev_index = -1
    test_term = 1
    log_entry = LogEntry(test_prev_index + 1, test_term, TEST_COMMAND)
    log.add_new_entry(test_prev_index, None, [log_entry])

    prev_index = log.prev_log_index()

    assert prev_index == 0


def test_single_first_log_item_always_succeeds(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_prev_index = -1
    test_term = 1
    log_entry = LogEntry(test_prev_index + 1, test_term, TEST_COMMAND)

    assert log.add_new_entry(test_prev_index, None, [log_entry])


def test_log_server_data_location(tmpdir):
    log = Log(666)
    assert log.server_data_location == "log666.txt"


def test_log_state_machine_replay_on_start(tmpdir):
    with open(str(tmpdir) + "hehe.txt", "w") as f:
        f.write("0 SET foo bar\n")
        f.write("0 SET tee hee\n")
        f.write("1 DELETE foo\n")

    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    assert not log.state_machine.get("foo")
    assert log.state_machine.get("tee") == "hee"


def test_prev_item_when_log_empty_returns_none(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    assert not log.prev_item()


def test_prev_item_single_entry_in_log(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_prev_index = -1
    test_term = 1
    log_entry = LogEntry(test_prev_index + 1, test_term, TEST_COMMAND)

    log.add_new_entry(test_prev_index, test_term, [log_entry])

    assert log.prev_log_term() == test_term


def test_prev_log_term_when_log_empty_returns_zero(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    assert log.prev_log_term() == 0


def test_log_string_representation(tmpdir):
    with open(str(tmpdir) + "hehe.txt", "w") as f:
        f.write("0 SET foo bar\n")

    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    assert repr(log) == "[LogEntry(0, 0, SET foo bar)]"


def test_log__getitem__by_int_and_slice(tmpdir):
    with open(str(tmpdir) + "hehe.txt", "w") as f:
        f.write("0 SET foo bar\n")

    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    # Get items by int
    assert log[0].term == 0

    # Get items by slice
    assert log[:][0].term == 0


def test_log_get_key_via_method_on_log_obj(tmpdir):
    with open(str(tmpdir) + "hehe.txt", "w") as f:
        f.write("0 SET foo bar\n")

    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    assert log.get("foo") == "bar"


def test_log_delete_key_already_deleted(tmpdir):
    with open(str(tmpdir) + "hehe.txt", "w") as f:
        f.write("0 SET foo bar\n")

    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    log.apply_to_state_machine("DELETE", ["foo"])
    # Delete again, should result in no Exception
    log.apply_to_state_machine("DELETE", ["foo"])


def test_single_idempotence(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_prev_index = -1
    test_term = 1
    log_entry = LogEntry(test_prev_index + 1, test_term, TEST_COMMAND)

    log.add_new_entry(test_prev_index, test_term, [log_entry])

    assert len(log) == 1

    # Adding same log entry returns True, log is unchanged
    assert log.add_new_entry(test_prev_index, test_term, [log_entry])
    assert len(log) == 1


def test_single_adding_item_after_missing_item_fails(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_prev_index = 5
    test_prev_term = 5
    test_term = 2
    log_entry = LogEntry(test_prev_index + 1, test_term, TEST_COMMAND)

    assert not log.add_new_entry(test_prev_index, test_prev_term, [log_entry])


def test_empty_log_heartbeat_append(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    assert log.add_new_entry(-1, None, [])


def test_populated_log_heartbeat_append_succeed(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
        LogEntry(4, 4, TEST_COMMAND),
        LogEntry(5, 5, TEST_COMMAND),
        LogEntry(6, 5, TEST_COMMAND),
        LogEntry(7, 6, TEST_COMMAND),
        LogEntry(8, 6, TEST_COMMAND),
        LogEntry(9, 6, TEST_COMMAND),
        LogEntry(10, 7, "this would be overwritten"),
    ]
    log.add_new_entry(-1, None, initial_entries)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert log.add_new_entry(9, 6, [])


def test_populated_log_heartbeat_append_fail(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
        LogEntry(4, 4, TEST_COMMAND),
        LogEntry(5, 5, TEST_COMMAND),
        LogEntry(6, 5, TEST_COMMAND),
        LogEntry(7, 6, TEST_COMMAND),
        LogEntry(8, 6, TEST_COMMAND),
        LogEntry(9, 6, TEST_COMMAND),
        LogEntry(10, 7, "this would be overwritten"),
    ]
    log.add_new_entry(-1, None, initial_entries)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert not log.add_new_entry(9000202020, 20000000, [])


def test_single_adding_item_with_previous_entry_having_correct_prev_term_succeeds(
    tmpdir,
):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_prev_index = -1
    test_index = 0
    test_prev_term = 2
    test_term = 2

    # First add the first entry since that will always succeed.
    prev_log_entry = LogEntry(test_index, test_prev_term, TEST_COMMAND)
    log.add_new_entry(test_prev_index, test_prev_term, [prev_log_entry])

    # Now we test by adding a test item. This one has test_prev_term set correctly, so
    # this will succeed.
    log_entry = LogEntry(test_index + 1, test_term, TEST_COMMAND)

    assert log.add_new_entry(test_prev_index + 1, test_prev_term, [log_entry])


def test_single_adding_item_with_previous_entry_having_incorrect_prev_term_fails(
    tmpdir,
):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_prev_index = -1
    test_index = 0
    test_prev_term = 2
    test_term = 3
    incorrect_test_prev_term = test_term

    prev_log_entry = LogEntry(test_index, test_prev_term, TEST_COMMAND)
    log.add_new_entry(test_prev_index, test_prev_term, [prev_log_entry])

    log_entry = LogEntry(test_index + 1, test_term, TEST_COMMAND)

    assert not log.add_new_entry(
        test_prev_index + 1, incorrect_test_prev_term, [log_entry]
    )


def test_multiple_add_entries_at_end_of_log_same_term(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_term = 1
    test_prev_index = -1
    initial_log_entries = [
        LogEntry(0, test_term, TEST_COMMAND),
        LogEntry(1, test_term, TEST_COMMAND),
        LogEntry(2, test_term, TEST_COMMAND),
    ]
    log.add_new_entry(test_prev_index, None, initial_log_entries)

    assert len(log) == 3

    more_log_entries = [
        LogEntry(3, test_term, TEST_COMMAND),
        LogEntry(4, test_term, TEST_COMMAND),
        LogEntry(5, test_term, TEST_COMMAND),
    ]
    assert log.add_new_entry(2, test_term, more_log_entries)

    assert len(log) == 6


def test_multiple_add_entries_at_end_of_log_term_change(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    test_term = 1
    test_prev_index = -1
    initial_log_entries = [
        LogEntry(0, test_term, TEST_COMMAND),
        LogEntry(1, test_term, TEST_COMMAND),
        LogEntry(2, test_term, TEST_COMMAND),
    ]
    log.add_new_entry(test_prev_index, None, initial_log_entries)

    assert len(log) == 3

    test_term = 2
    more_log_entries = [
        LogEntry(3, test_term, TEST_COMMAND),
        LogEntry(4, test_term, TEST_COMMAND),
        LogEntry(5, test_term, TEST_COMMAND),
    ]
    assert log.add_new_entry(2, test_term - 1, more_log_entries)

    assert len(log) == 6


def test_figure_7_a_from_raft_paper(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    # Note! Compared to the paper, the indices are off here because
    # my log is 0-indexed.
    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
        LogEntry(4, 4, TEST_COMMAND),
        LogEntry(5, 5, TEST_COMMAND),
        LogEntry(6, 5, TEST_COMMAND),
        LogEntry(7, 6, TEST_COMMAND),
        LogEntry(8, 6, TEST_COMMAND),
    ]
    log.add_new_entry(-1, None, initial_entries)

    # Try to add entry at index 10. This should fail.
    new_entry = LogEntry(10, 8, TEST_COMMAND)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert not log.add_new_entry(9, 6, [new_entry])


def test_figure_7_b_from_raft_paper(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
    ]
    log.add_new_entry(-1, None, initial_entries)

    new_entry = LogEntry(10, 8, TEST_COMMAND)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert not log.add_new_entry(9, 6, [new_entry])


def test_figure_7_c_from_raft_paper(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
        LogEntry(4, 4, TEST_COMMAND),
        LogEntry(5, 5, TEST_COMMAND),
        LogEntry(6, 5, TEST_COMMAND),
        LogEntry(7, 6, TEST_COMMAND),
        LogEntry(8, 6, TEST_COMMAND),
        LogEntry(9, 6, TEST_COMMAND),
        LogEntry(10, 6, "this will be overwritten"),
    ]
    log.add_new_entry(-1, None, initial_entries)

    # Try to add entry at index 10. This should overwrite a log entry.
    new_entry = LogEntry(10, 8, TEST_COMMAND)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert log.add_new_entry(9, 6, [new_entry])
    assert log.prev_item().command == TEST_COMMAND


def test_figure_7_d_from_raft_paper(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
        LogEntry(4, 4, TEST_COMMAND),
        LogEntry(5, 5, TEST_COMMAND),
        LogEntry(6, 5, TEST_COMMAND),
        LogEntry(7, 6, TEST_COMMAND),
        LogEntry(8, 6, TEST_COMMAND),
        LogEntry(9, 6, TEST_COMMAND),
        LogEntry(10, 7, "this will be overwritten"),
        LogEntry(11, 7, "this one should be gone entirely"),
    ]
    log.add_new_entry(-1, None, initial_entries)
    number_of_initial_entries = len(initial_entries)

    # Try to add entry at index 10. This should overwrite a log entry
    # and truncate the log.
    new_entry = LogEntry(10, 8, TEST_COMMAND)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert log.add_new_entry(9, 6, [new_entry])
    assert len(log) == number_of_initial_entries - 1
    assert log.prev_item().command == TEST_COMMAND


def test_figure_7_e_from_raft_paper(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 4, TEST_COMMAND),
        LogEntry(4, 4, TEST_COMMAND),
        LogEntry(5, 4, TEST_COMMAND),
        LogEntry(6, 4, TEST_COMMAND),
    ]
    log.add_new_entry(-1, None, initial_entries)

    # Try to add entry at index 10. This should fail.
    new_entry = LogEntry(10, 8, TEST_COMMAND)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert not log.add_new_entry(9, 6, [new_entry])


def test_figure_7_f_from_raft_paper(tmpdir):
    log = Log(NODE_NUM, str(tmpdir) + "hehe.txt")

    initial_entries = [
        LogEntry(0, 1, TEST_COMMAND),
        LogEntry(1, 1, TEST_COMMAND),
        LogEntry(2, 1, TEST_COMMAND),
        LogEntry(3, 2, TEST_COMMAND),
        LogEntry(4, 2, TEST_COMMAND),
        LogEntry(5, 2, TEST_COMMAND),
        LogEntry(6, 3, TEST_COMMAND),
        LogEntry(7, 3, TEST_COMMAND),
        LogEntry(8, 3, TEST_COMMAND),
        LogEntry(9, 3, TEST_COMMAND),
        LogEntry(10, 3, TEST_COMMAND),
    ]
    log.add_new_entry(-1, None, initial_entries)

    # Try to add entry at index 10. The terms don't match, so
    # this should also fail.
    new_entry = LogEntry(10, 8, TEST_COMMAND)

    # From the perspective of the leader, the previous index is 9 and
    # previous term is 6.
    assert not log.add_new_entry(9, 6, [new_entry])
