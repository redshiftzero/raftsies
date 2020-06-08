import socket
import threading

from raftsies import control


NUM_REQ = 4


def echo_server(sock):
    num_requests = NUM_REQ
    while num_requests:
        conn, addr = sock.accept()
        print("connected!")
        msg = control.recv_message(conn)
        control.send_message(conn, msg)
        num_requests -= 1


def test_echo():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("localhost", 12345))
    server_sock.listen()
    threading.Thread(target=echo_server, args=(server_sock,), daemon=True).start()

    for n in range(0, NUM_REQ):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(("localhost", 12345))

        msg = b"x" * (10 ** n)
        control.send_message(client_sock, msg)
        response = control.recv_message(client_sock)
        assert msg == response

        client_sock.close()

    server_sock.close()
