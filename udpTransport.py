import json
import threading
from socket import AF_INET, SO_REUSEADDR, SOCK_DGRAM, SOL_SOCKET, socket


class UdpTransport:
    """Socket UDP que entrega cada datagrama recebido (JSON) para on_datagram."""

    def __init__(self, port, on_datagram):
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.on_datagram = on_datagram
        threading.Thread(target=self._receive_loop, daemon=True).start()

    def send(self, addr, payload):
        self.sock.sendto(json.dumps(payload).encode(), addr)

    def _receive_loop(self):
        while True:
            data, _ = self.sock.recvfrom(65535)
            self.on_datagram(json.loads(data))
