import pytest
from unittest.mock import MagicMock, patch

from raftsies.client import RaftClient
from raftsies.events import ClientResponse, ClientRequest


@patch("socket.socket")
def test_close_does_not_wait_for_response(mock_socket):
    client = RaftClient("server", 666)
    assert mock_socket.call_count == 1

    result = client.close()

    assert not result


@patch("socket.socket")
def test_delete_returns_true_if_result_ok(mock_socket):
    client = RaftClient("server", 666)
    client._send_and_wait_for_response = MagicMock(return_value=ClientResponse("OK"))

    result = client.delete("test")

    assert result


@patch("socket.socket")
def test_delete_returns_false_if_result_not_ok(mock_socket):
    client = RaftClient("server", 666)
    client._send_and_wait_for_response = MagicMock(return_value=ClientResponse("ERROR"))

    result = client.delete("test")

    assert not result


@patch("socket.socket")
def test_set_returns_true_if_result_ok(mock_socket):
    client = RaftClient("server", 666)
    client._send_and_wait_for_response = MagicMock(return_value=ClientResponse("OK"))

    result = client.set("foo", "bar")

    assert result


@patch("socket.socket")
def test_set_returns_false_if_result_not_ok(mock_socket):
    client = RaftClient("server", 666)
    client._send_and_wait_for_response = MagicMock(return_value=ClientResponse("ERROR"))

    result = client.set("foo", "bar")

    assert not result


@patch("socket.socket")
def test_get_returns_false_if_result_not_ok(mock_socket):
    client = RaftClient("server", 666)
    client._send_and_wait_for_response = MagicMock(return_value=ClientResponse("ERROR"))

    result = client.get("foo")

    assert not result


@patch("socket.socket")
def test_get_returns_key_if_result_ok(mock_socket):
    client = RaftClient("server", 666)
    client._send_and_wait_for_response = MagicMock(
        return_value=ClientResponse("OK", "bar")
    )

    result = client.get("foo")

    assert result == "bar"


@patch("socket.socket")
@patch("raftsies.client.recv_message", return_value=None)
def test__send_and_wait_for_response_eventually_raises_ConnectionError(
    mock_socket, mock_recv
):
    """If recv_message never returns any data, we should eventually raise ConnectionError"""
    client = RaftClient("server", 666)
    req = ClientRequest("teehee")

    with pytest.raises(ConnectionError):
        client._send_and_wait_for_response(req)


@patch("socket.socket")
@patch("raftsies.client.recv_message", return_value=ClientResponse("OK").to_bytes())
def test__send_and_wait_for_response_success(mock_socket, mock_recv):
    client = RaftClient("server", 666)
    req = ClientRequest("teehee")

    resp = client._send_and_wait_for_response(req)

    assert resp.result == "OK"


@patch("socket.socket")
@patch(
    "raftsies.client.recv_message",
    side_effect=[
        ClientResponse("REDIRECT", None, "redirected_server", 667).to_bytes(),
        ClientResponse("OK").to_bytes(),
    ],
)
def test__send_and_wait_for_response_redirect_to_leader(mock_socket, mock_recv):
    client = RaftClient("server", 666)
    req = ClientRequest("teehee")

    resp = client._send_and_wait_for_response(req)

    assert client.server == "redirected_server"
    assert client.port == 667
    assert resp.result == "OK"


@patch("socket.socket")
@patch(
    "raftsies.client.recv_message",
    side_effect=[
        ConnectionRefusedError("bang"),
        ConnectionRefusedError("bang"),
        ConnectionRefusedError("bang"),
        ClientResponse("OK").to_bytes(),
    ],
)
def test__send_and_wait_for_response_ConnectionRefusedError_multiple_tries(
    mock_socket, mock_recv
):
    client = RaftClient("server", 666)
    req = ClientRequest("teehee")

    resp = client._send_and_wait_for_response(req)

    assert resp.result == "OK"


@patch("socket.socket")
@patch("raftsies.client.recv_message", side_effect=ConnectionResetError("boom"))
def test__send_and_wait_for_response_ConnectionResetError_raises(
    mock_socket, mock_recv
):
    client = RaftClient("server", 666)
    req = ClientRequest("teehee")

    with pytest.raises(ConnectionResetError):
        client._send_and_wait_for_response(req)
