import logging
import json
import uuid

from typing import Any, Dict, List, Optional, Union

from raftsies.log import LogEntry

logger = logging.getLogger(__name__)


class RaftEvent:
    """A RaftEvent can be an incoming request or response we need
    to process, or it could be an internal event like a timer expiring.
    """

    type_str = "RaftEvent"

    def __init__(self, data: Optional[Dict[str, Union[str, int]]] = None):
        if data:
            self._data = data
        else:
            self._data = {}

    def __str__(self) -> str:
        return f"{self._data}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    @staticmethod
    def from_bytes(item: bytes) -> "RaftEvent":
        data = json.loads(item.decode("utf-8"))

        try:
            obj_type = data["TYPE"]
        except KeyError:
            logger.error("TYPE key not found, dropping message")

        if obj_type == "Client":
            return ClientRequest.from_dict(data)
        elif obj_type == "ClientResponse":
            return ClientResponse.from_dict(data)
        elif obj_type == "AppendEntries":
            return AppendEntriesRequest.from_dict(data)
        elif obj_type == "AppendEntriesResponse":
            return AppendEntriesResponse.from_dict(data)
        elif obj_type == "RequestVote":
            return RequestVote.from_dict(data)
        elif obj_type == "Vote":
            return Vote.from_dict(data)
        elif obj_type == "ElectionTimerExpired":
            return ElectionTimerExpired()
        else:
            raise RuntimeError("unknown message type found")

    def to_bytes(self) -> bytes:
        if "TYPE" not in self._data.keys():
            self._data.update({"TYPE": self.type_str})
        return json.dumps(self._data).encode("utf-8")


class ElectionTimerExpired(RaftEvent):
    type_str = "ElectionTimerExpired"

    def __init__(self) -> None:
        super().__init__()


class ClientResponse(RaftEvent):
    """
    Returns:
    * 'RESULT' -
        'OK' if state change applied
        'ERROR' if something went awry
        'REDIRECT' if we are not the leader
        'NOTFOUND' if we tried to get a key that was not found
    * 'VALUE' - if the operation was a 'GET' and it was found
    * 'LEADER_HOST' - host of the leader if this is a redirect
    * 'LEADER_PORT' - port of the leader if this is a redirect
    """

    ALLOWED_RESULTS = ["OK", "ERROR", "REDIRECT", "NOTFOUND"]
    type_str = "ClientResponse"

    def __init__(
        self,
        result: str,
        requested_key: Optional[str] = None,
        leader_host: Optional[str] = None,
        leader_port: Optional[int] = None,
    ):
        if result not in self.ALLOWED_RESULTS:
            self.result = "ERROR"

        self.result = result
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.requested_key = requested_key

        self._data: Dict[str, Union[str, int]] = {"RESULT": self.result}
        if self.requested_key:
            self._data.update({"VALUE": self.requested_key})
        if self.leader_host:
            self._data.update({"LEADER_HOST": self.leader_host})
        if self.leader_port:
            self._data.update({"LEADER_PORT": self.leader_port})

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ClientResponse":
        result = data["RESULT"]
        leader_host = data.get("LEADER_HOST", None)
        leader_port = data.get("LEADER_PORT", None)
        requested_key = data.get("VALUE", None)
        obj = ClientResponse(result, requested_key, leader_host, leader_port)
        return obj


class ClientRequest(RaftEvent):
    """
    JSON keys:
    * 'TYPE' must be 'ClientRequest'
    * 'OP' must have value 'GET', 'SET', 'DELETE', or 'CLOSE'
    * 'KEY' allowed for operations 'GET', 'SET', and 'DELETE'
    * 'VALUE' allowed for operation 'SET'
    * 'UUID' generated below, client requests are idempotent

    Returns ClientResponse
    """

    type_str = "Client"
    ALLOWED_OPS = ["GET", "SET", "DELETE", "CLOSE"]

    def __init__(
        self,
        op: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        request_id: Optional[str] = None,
    ):
        if request_id:
            self.uuid = request_id
        else:
            self.uuid = str(uuid.uuid4())  # Must be str so serializes to JSON

        self.op = op
        self.key = key
        self.value = value
        self._data = {"OP": self.op, "UUID": self.uuid}
        if self.key:
            self._data.update({"KEY": self.key})
        if self.value:
            self._data.update({"VALUE": self.value})

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ClientRequest":
        request_id = data.get("UUID", None)
        op = data["OP"]
        key = data.get("KEY", None)
        value = data.get("VALUE", None)
        obj = ClientRequest(op, key, value, request_id)

        if op not in obj.ALLOWED_OPS:
            obj = ClientInvalidRequest(request_id)
        elif op == "GET" and "KEY" in data.keys() and isinstance(data["KEY"], str):
            obj = ClientGetRequest(data["KEY"], request_id)
        elif (
            op == "SET"
            and "KEY" in data.keys()
            and "VALUE" in data.keys()
            and isinstance(data["KEY"], str)
            and isinstance(data["VALUE"], str)
        ):
            obj = ClientSetRequest(data["KEY"], data["VALUE"], request_id)
        elif op == "DELETE" and "KEY" in data.keys() and isinstance(data["KEY"], str):
            obj = ClientDeleteRequest(data["KEY"], request_id)
        elif op == "CLOSE":
            obj = ClientCloseRequest(request_id)
        else:
            obj = ClientInvalidRequest(request_id)
        return obj


class ClientGetRequest(ClientRequest):
    def __init__(self, key: str, request_uuid: Optional[str] = None):
        super().__init__("GET", key, None, request_uuid)

    def handle(self, requested_value: Optional[str]) -> ClientResponse:
        if requested_value:
            return ClientResponse("OK", requested_value)
        else:
            return ClientResponse("NOTFOUND")


class ClientSetRequest(ClientRequest):
    def __init__(self, key: str, value: str, request_uuid: Optional[str] = None):
        super().__init__("SET", key, value, request_uuid)

    def handle(self) -> ClientResponse:
        return ClientResponse("OK")


class ClientDeleteRequest(ClientRequest):
    def __init__(self, key: str, request_uuid: Optional[str] = None):
        super().__init__("DELETE", key, None, request_uuid)

    def handle(self) -> ClientResponse:
        return ClientResponse("OK")


class ClientCloseRequest(ClientRequest):
    def __init__(self, request_uuid: Optional[str] = None) -> None:
        super().__init__("CLOSE", None, None, request_uuid)

    def handle(self) -> ClientResponse:
        return ClientResponse("OK")


class ClientInvalidRequest(ClientRequest):
    def __init__(self, request_uuid: Optional[str] = None) -> None:
        super().__init__("INVALID", None, None, request_uuid)

    def handle(self) -> ClientResponse:
        return ClientResponse("ERROR")


class AppendEntriesRequest(RaftEvent):
    """
    Sent from leader to followers.

    JSON keys:
    * 'TYPE' must be 'AppendEntries'.
    * 'TERM' must be an int (leader's term).
    * 'LEADER_ID' so follower can redirect clients to the leader.
    * 'PREV_LOG_INDEX' is the index of the log entry immediately preceding new ones
    * 'PREV_LOG_TERM' is the term of the previous log index entry
    * 'ENTRIES' are the entries to add (can be empty)
    * 'LEADER_COMMIT' is the leader's commit index
    * 'UUID' links this request to the client request

    Returns AppendEntriesResponse.
    """

    type_str = "AppendEntries"

    def __init__(
        self,
        term: int,
        leader_id: int,
        prev_log_index: int,
        prev_log_term: int,
        leader_commit: int,
        req_uuid: str = "",
        entries: Optional[List[LogEntry]] = None,
    ):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        if entries:
            self.entries = entries
        else:
            self.entries = []
        self.leader_commit = leader_commit
        # Maybe remove?
        self.uuid = req_uuid
        json_entries = json.dumps([x.to_json() for x in self.entries])
        self._data = {
            "TERM": term,
            "LEADER_ID": leader_id,
            "PREV_LOG_INDEX": prev_log_index,
            "PREV_LOG_TERM": prev_log_term,
            "LEADER_COMMIT": leader_commit,
            "ENTRIES": json_entries,
        }
        if self.uuid:
            self._data.update({"UUID": self.uuid})

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "AppendEntriesRequest":
        term = data["TERM"]
        leader_id = data["LEADER_ID"]
        prev_log_index = data["PREV_LOG_INDEX"]
        prev_log_term = data["PREV_LOG_TERM"]
        if data["ENTRIES"]:
            entries = json.loads(data["ENTRIES"])
            entries = [LogEntry.from_json(x) for x in entries]
        else:
            entries = []
        leader_commit = data["LEADER_COMMIT"]
        request_id = data.get("UUID", "")
        obj = AppendEntriesRequest(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            request_id,
            entries,
        )
        obj._data = data
        return obj


class AppendEntriesResponse(RaftEvent):
    """
    Sent from followers to the leader.

    JSON keys:
    * 'TYPE' - must be 'AppendEntriesResponse'
    * 'RESULT' - 'OK' (if state change applied), 'FALSE' if failed or
    'ERROR' if something else went awry
    * 'NODE' - which node id this was from
    * 'TERM' - for leader to update itself

    # TODO: Remove?
    * 'UUID' - links this request to the client request
    """

    ALLOWED_RESULTS = ["OK", "FALSE", "ERROR"]
    type_str = "AppendEntriesResponse"

    def __init__(
        self, result: str, node: int, term: int, req_uuid: Optional[str] = None
    ):
        self.result = result
        if self.result not in self.ALLOWED_RESULTS:
            self.result = "ERROR"

        self.node = node
        self.term = term
        if req_uuid:
            self.uuid = req_uuid
        else:
            self.uuid = ""

        self._data = {
            "TERM": self.term,
            "NODE": self.node,
            "RESULT": self.result,
        }
        if self.uuid:
            self._data.update({"UUID": self.uuid})

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "AppendEntriesResponse":
        result = data["RESULT"]
        node = data["NODE"]
        term = data["TERM"]
        req_uuid = data.get("UUID", "")
        obj = AppendEntriesResponse(result, node, term, req_uuid)
        obj._data = data
        return obj


class RequestVote(RaftEvent):
    """
    Sent from candidates to followers.

    JSON keys:
    * 'TYPE' - must be 'RequestVote'
    * 'TERM' - candidate's term
    * 'CANDIDATE_ID' - node num of the candidate requesting the vote
    * 'LAST_LOG_INDEX' - index of candidate's last log entry
    * 'LAST_LOG_TERM' - term of candidate's last log entry
    """

    type_str = "RequestVote"

    def __init__(
        self, term: int, candidate_id: int, last_log_index: int, last_log_term: int
    ):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term
        self._data = {
            "TERM": self.term,
            "CANDIDATE_ID": self.candidate_id,
            "LAST_LOG_INDEX": self.last_log_index,
            "LAST_LOG_TERM": self.last_log_term,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "RequestVote":
        last_log_index = data["LAST_LOG_INDEX"]
        last_log_term = data["LAST_LOG_TERM"]
        term = data["TERM"]
        candidate_id = data["CANDIDATE_ID"]
        obj = RequestVote(term, candidate_id, last_log_index, last_log_term)
        obj._data = data
        return obj


class Vote(RaftEvent):
    """
    Sent from followers to candidates.

    JSON keys:
    * 'TYPE' - must be 'Vote'
    * 'TERM' - for candidate to update itself
    * 'VOTE_GRANTED' - True or False
    """

    type_str = "Vote"

    def __init__(self, term: int, vote: bool):
        self.term = term
        self.vote = vote
        self._data = {
            "TERM": self.term,
            "VOTE_GRANTED": self.vote,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Vote":
        term = data["TERM"]
        vote = data["VOTE_GRANTED"]
        obj = Vote(term, vote)
        obj._data = data
        return obj
