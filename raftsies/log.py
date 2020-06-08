import os
import json
import logging

from typing import Dict, List, Optional, Union


logger = logging.getLogger(__name__)


class LogEntry:
    def __init__(self, index: int, term: int, command: str):
        self.index = index
        self.term = term
        self.command = command

    def __str__(self) -> str:
        return f"{self.term} {self.command}\n"

    def __repr__(self) -> str:
        return f"LogEntry({self.index}, {self.term}, {self.command})"

    def to_json(self) -> str:
        return json.dumps(
            {"INDEX": self.index, "TERM": self.term, "COMMAND": self.command}
        )

    @staticmethod
    def from_json(data: str) -> "LogEntry":
        obj = json.loads(data)
        logger.debug(obj)
        index = obj["INDEX"]
        term = obj["TERM"]
        cmd = obj["COMMAND"]
        logger.debug(f"new LogEntry from json: {index}, {term}, {cmd}")
        return LogEntry(index, term, cmd)


class Log:
    def __init__(self, node_num: int, server_data_location: Optional[str] = None):
        self.node_num = node_num

        # This is a cache of the transaction log. The full log is also
        # persisted to disk.
        self._log: List[LogEntry] = []

        # Location on the server we store the transaction log.
        if not server_data_location:
            self.server_data_location = f"log{node_num}.txt"
        else:
            self.server_data_location = server_data_location
        logger.debug(
            f"transaction log will be stored on disk in: {self.server_data_location}"
        )

        # Create file to store transaction log if it doesn't exist.
        if not os.path.exists(self.server_data_location):
            with open(self.server_data_location, "a"):
                os.utime(self.server_data_location, None)

        self.state_machine = StateMachine()  # Keeps KV State

        with open(self.server_data_location, "r") as f:
            # Expected format is 'term op key value'
            lines = f.readlines()

        # This is replaying previous transactions when we first
        # load the persistent log.
        for index, line in enumerate(lines):
            items = line.rstrip().split()
            term, op, list_args = int(items[0]), items[1], items[2:]
            args = " ".join(list_args)
            command = f"{op} {args}"
            log_item = LogEntry(index, term, command)
            self._log.append(log_item)  # Populate _log cache
            self.apply_to_state_machine(op, list_args)

        logger.debug(f"existing log loaded: {self._log}")

    def apply_to_state_machine(self, op: str, list_args: List[str]) -> None:
        self.state_machine.apply(op, list_args)

    def apply_entries_to_state_machine(self, start_index: int, last_index: int) -> None:
        for index in range(start_index, last_index + 1):
            try:
                log_entry = self._log[index]
            except IndexError:
                continue

            op = log_entry.command.split()[0]
            if op == "SET":
                key = log_entry.command.split()[1]
                value = log_entry.command.split()[2]
                self.apply_to_state_machine(op, [key, value])
            elif op == "DELETE":
                key = log_entry.command.split()[1]
                self.apply_to_state_machine(op, [key])

    def prev_item(self) -> Optional[LogEntry]:
        try:
            return self._log[-1]
        except IndexError:
            return None

    def prev_log_index(self) -> int:
        try:
            return self._log[-1].index
        except IndexError:
            return -1

    def prev_log_term(self) -> int:
        try:
            return self._log[-1].term
        except IndexError:
            return 0

    def __repr__(self) -> str:
        return str(self._log)

    def __getitem__(self, index: Union[slice, int]) -> Union[List[LogEntry], LogEntry]:
        return self._log[index]

    def __len__(self) -> int:
        return len(self._log)

    def add_new_entry(
        self, prev_index: int, prev_term: Optional[int], entries: List[LogEntry]
    ) -> bool:
        # We need to provide a list of entries as when backtracking for log replication
        # we may need to add multiple entries at once.
        # Assumption: all entries provided in the list are ordered.

        # Log is empty
        if self.prev_log_index() == -1 and prev_index == -1:
            logger.debug(f"log currently empty, appending new entries: {entries}")
            for entry in entries:
                logger.debug(f"appending new entry: {entry}")
                self.add_item_to_log_cache_and_persist_to_disk(entry)
            return True

        # Entries are missing in log
        if len(self._log) <= prev_index:
            logger.debug(
                f"entries missing, not appending. current log len: {len(self._log)}"
            )
            return False

        # Previous item term does not match
        if prev_term != self._log[prev_index].term:
            logger.debug(
                f"log: {prev_term} != {self._log[prev_index].term}, not appending"
            )
            return False

        # We can add the entries
        insert_index = 0
        for entry in entries:
            logger.debug(f"appending new entry: {entry}")
            self.add_item_to_log_cache_and_persist_to_disk(entry)
            insert_index = entry.index + 1

        # Remove any other entries left around after the append.
        if insert_index:
            for item_to_remove in self._log[insert_index:]:
                self._log.pop()

        return True

    def add_item_to_log_cache_and_persist_to_disk(self, item: LogEntry) -> None:
        logger.debug(f"item to add is at position (item.index): {item.index}")

        try:  # Replace an item if needed.
            self._log[item.index] = item
        except IndexError:  # The log is empty
            self._log.append(item)

        # TODO: This is horrifically inefficient (rewriting the entire log)
        with open(self.server_data_location, "w") as f:
            f.writelines([str(x) for x in self._log])

    def get(self, key: str) -> Optional[str]:
        return self.state_machine.get(key)


class StateMachine:
    def __init__(self) -> None:
        # This object keeps the current state (i.e. maps keys to values)
        # (can potentially use for checkpointing later?)
        self._data: Dict[str, str] = {}

    def apply(self, op: str, list_args: List[str]) -> None:
        if op == "SET":
            key, value = list_args[0], list_args[1]
            self.set(key, value)
        elif op == "DELETE":
            key = list_args[0]
            self.delete(key)

    def get(self, key: str) -> Optional[str]:
        try:
            return self._data[key]
        except KeyError:
            return None

    def set(self, key: str, value: str) -> None:
        self._data[key] = value
        logger.debug(f"state is now: {self._data}")

    def delete(self, key: str) -> None:
        try:
            del self._data[key]
        except KeyError:
            # Then key is already gone
            pass
        logger.debug(f"state is now: {self._data}")
