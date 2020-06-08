import logging
import socket
import time
import random
import queue
import json
import threading

from typing import List, Tuple, Optional, NoReturn

from raftsies.log import Log, LogEntry
from raftsies.events import ElectionTimerExpired


logger = logging.getLogger(__name__)


MSG_PREFIX_SIZE = 4
NODE_CONNECT_TIMEOUT = 1


class RaftController:
    """Raft controller doesn't know the state of the object.

    It keeps track of the persistent log and the configuration.
    """

    def __init__(self, node_num: int, config_location: str = "config.json"):
        # Persistent but mutable state
        self.node_num = node_num
        self.log = Log(self.node_num)
        self.config_location = config_location

        """
        For internal use to stop running state-specific actions.
        """
        self._running = True

        """
        Load configuration and store as constants
        """
        with open(self.config_location, "r") as f:
            cfg = f.read()

        config = json.loads(cfg)
        self.ELECTION_TIME_START = config["election_time_start"]
        self.ELECTION_TIME_STOP = config["election_time_stop"]
        self.HEARTBEAT_TIME = config["heartbeat_time"]
        self.NODES = {int(x): y for x, y in config["nodes"].items()}

        """
        Setup network
        """
        self.receive_q = ReceiveQueue(
            self.NODES[self.node_num]["host"], self.NODES[node_num]["port"]
        )  # Start listening for incoming messages.
        self.client_send_q = ClientSendQueue()

        self.send_qs = {}

        for node_num in self.get_network_peers():
            logger.debug(f"starting send queue on: {self.NODES[node_num]}")
            self.send_qs[node_num] = NodeSendQueue(
                node_num, self.NODES[node_num]["host"], self.NODES[node_num]["port"]
            )

    def stop_timer(self) -> None:
        self._running = False

    def election_timer(self) -> None:
        while self._running:
            wait_time = random.uniform(
                self.ELECTION_TIME_START, self.ELECTION_TIME_STOP
            )
            time.sleep(wait_time)
            e = ElectionTimerExpired()
            self.put_event_on_queue(e.to_bytes())

    def get_network_peers(self) -> List[int]:
        node_indices = list(self.NODES.keys())
        node_indices.pop(self.node_num)
        return node_indices

    def get_prev_log_index(self) -> int:
        return self.log.prev_log_index()

    def get_prev_log_term(self) -> int:
        return self.log.prev_log_term()

    def add_new_entry(
        self, prev_index: int, prev_term: Optional[int], entries: List[LogEntry]
    ) -> bool:
        return self.log.add_new_entry(prev_index, prev_term, entries)

    def get_key(self, key: str) -> Optional[str]:
        return self.log.get(key)

    def run_command_on_state_machine(self, op: str, list_args: List[str]) -> None:
        self.log.apply_to_state_machine(op, list_args)

    def apply_entries_to_state_machine(self, start_index: int, stop_index: int) -> None:
        self.log.apply_entries_to_state_machine(start_index, stop_index)

    def send_to_node(self, node_num: int, msg: bytes) -> None:
        try:
            logger.debug(f"sending to {node_num}: {str(msg)}")
            self.send_qs[node_num].put(msg)
        except RuntimeError:
            logger.debug(f"peer {node_num} seems down, dropping")

    def send_to_client(
        self, conn: socket.socket, msg: bytes, final: bool = False
    ) -> None:
        self.client_send_q.put(conn, msg, final)

    def receive(self) -> Tuple[Optional[socket.socket], bytes]:
        msg_to_process = self.receive_q.get()
        return msg_to_process

    def put_event_on_queue(self, msg: bytes) -> None:
        self.receive_q.put(None, msg)


class ReceiveQueue:
    def __init__(self, host: str, port: int):
        self.queue: queue.Queue[Tuple[Optional[socket.socket], bytes]] = queue.Queue()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, True
        )  # Avoids address in use error
        self.host = host
        self.port = port
        self.s.bind((self.host, self.port))
        self.s.listen()
        logger.debug("starting receiver queue")
        threading.Thread(target=self.run).start()

    def run(self) -> NoReturn:
        logger.debug("listening 👂")
        while True:
            conn, _ = self.s.accept()
            logger.debug("✨ new connection! ✨")
            # Spawn a receiever thread for the new connection.
            # TODO: pool
            threading.Thread(target=self.receiver, args=(conn,)).start()

    def receiver(self, conn: socket.socket) -> None:
        is_connected = True
        while is_connected:
            try:
                is_connected = conn.getpeername()
            except OSError:
                is_connected = False
                logger.debug("✨ Lost connection, stopping receiver thread ✨")

            msg = recv_message(conn)
            if msg:
                logger.debug(f"new msg, adding to queue: {str(msg)}")
                self.put(conn, msg)

    def get(self) -> Tuple[Optional[socket.socket], bytes]:
        return self.queue.get()

    def put(self, conn: Optional[socket.socket], msg: bytes) -> None:
        self.queue.put((conn, msg))


class NodeSendQueue:
    def __init__(self, node_num: int, host: str, port: int):
        self.node_num = node_num
        self.host = host
        self.port = port
        self.queue: queue.Queue[bytes] = queue.Queue()
        self.s: Optional[socket.socket] = None
        threading.Thread(target=self.run).start()

    def connect(self) -> None:
        while True:
            try:
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.s.connect((self.host, self.port))
                logger.debug(f"✅ node connected to {self.node_num} ✅")
                return
            except (OSError, ConnectionRefusedError):
                self.s = None
                # Server is down, wait a few seconds and then try again.
                time.sleep(NODE_CONNECT_TIMEOUT)

    def run(self) -> None:
        self.connect()
        while True:
            # First, ensure node is still connected.
            try:
                if self.s:
                    self.s.getpeername()
            except OSError:
                logger.debug("✨ Lost connection to node, trying to reconnect ✨")
                self.s = None
                self.connect()

            msg = self.queue.get()
            if msg and self.s:
                send_message(self.s, msg)

    def put(self, msg: bytes) -> None:
        self.queue.put(msg)


class ClientSendQueue:
    def __init__(self) -> None:
        self.queue: queue.Queue[Tuple[socket.socket, bytes, bool]] = queue.Queue()
        logger.debug("starting send queue for client responses")
        threading.Thread(target=self.run).start()

    def run(self) -> None:
        while True:
            conn, msg, final = self.queue.get()
            send_message(conn, msg)
            if final:
                logger.debug("closing connection 👋")
                conn.close()

    def put(self, conn: socket.socket, msg: bytes, final: bool) -> None:
        self.queue.put((conn, msg, final))


def send_message(sock: socket.socket, msg: bytes) -> bool:
    try:
        prefix = b"%4d" % len(msg)
        logger.debug(f"sending on wire: len {str(prefix)} with msg {str(msg)}")
        sock.sendall(prefix + msg)
        return True
    except BrokenPipeError:
        return False


def read_n_bytes(sock: socket.socket, n: int) -> bytes:
    data = b""
    while n > 0:
        b = sock.recv(n)
        if not b:
            raise RuntimeError("see ya bye")
        data += b
        n -= len(b)
    return data


def recv_message(sock: socket.socket) -> Optional[bytes]:
    try:
        bytes_msg_len = read_n_bytes(sock, MSG_PREFIX_SIZE)
    except RuntimeError:
        return None

    logger.debug(f"num bytes in incoming message: {str(bytes_msg_len)}")

    try:
        resp = read_n_bytes(sock, int(bytes_msg_len))
    except RuntimeError:
        return None

    logger.debug(f"receiving on wire: len {str(bytes_msg_len)} with msg {str(resp)}")
    return resp
