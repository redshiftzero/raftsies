import socket
import time
import logging

from typing import Optional

from raftsies.control import send_message, recv_message
from raftsies.events import RaftEvent, ClientRequest, ClientResponse

logger = logging.getLogger(__name__)


class RaftClient:
    CLIENT_WAIT_SEC = 0.2

    def __init__(self, server: str, port: int):
        self.server = server
        self.port = port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.server, self.port))
        logger.debug("connected!")

    def _send(self, req: RaftEvent) -> None:
        logger.debug(f"Client is sending: {req}")
        send_message(self.s, req.to_bytes())

    def _send_and_wait_for_response(self, req: RaftEvent) -> RaftEvent:
        self._send(req)
        num_attempts = 5
        while num_attempts > 0:
            try:
                raw_resp = recv_message(self.s)
                if not raw_resp:
                    num_attempts -= 1
                    continue
                resp = ClientResponse.from_bytes(raw_resp)
                logger.debug(f"Client received: {str(resp)}")

                if getattr(resp, "result", None) == "REDIRECT":
                    logger.debug("Was connected to a follower, connecting to leader")
                    self.server = getattr(resp, "leader_host", self.server)
                    self.port = getattr(resp, "leader_port", self.port)
                    self.s.close()
                    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.s.connect((self.server, self.port))
                    self._send(req)
                    continue

                return resp
            except (ConnectionRefusedError, socket.timeout):
                logger.debug("waiting a few seconds and resending...")
                time.sleep(self.CLIENT_WAIT_SEC)
                num_attempts -= 1
            except ConnectionResetError:
                raise ConnectionResetError("lost connection to node")

        # If we get here we've tried multiple times so raise an error for the user.
        # TODO: We could send to another node (but client needs knowledge of cluster
        # membership to do this).
        raise ConnectionError("lost connection to node")

    def get(self, key: str) -> Optional[str]:
        req = ClientRequest("GET", key, None)
        resp = self._send_and_wait_for_response(req)
        if isinstance(resp, ClientResponse) and resp.requested_key:
            return resp.requested_key
        else:
            return None

    def set(self, key: str, value: str) -> bool:
        req = ClientRequest("SET", key, value)
        resp = self._send_and_wait_for_response(req)
        if getattr(resp, "result", None) == "OK":
            return True
        else:
            return False

    def delete(self, key: str) -> bool:
        req = ClientRequest("DELETE", key)
        resp = self._send_and_wait_for_response(req)
        if getattr(resp, "result", None) == "OK":
            return True
        else:
            return False

    def close(self) -> None:
        req = ClientRequest("CLOSE")
        self._send(req)
