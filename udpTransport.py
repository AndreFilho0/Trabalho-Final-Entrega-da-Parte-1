import json
import random
import threading
from socket import AF_INET, SO_REUSEADDR, SOCK_DGRAM, SOL_SOCKET, socket


class UdpTransport:
    """Socket UDP que entrega cada datagrama recebido (JSON) para on_datagram.

    Pode descartar (loss) e atrasar (delay_min..delay_max ms) cada envio, para
    reproduzir numa máquina só a perda e a reordenação de peers em regiões
    diferentes da nuvem.
    """

    def __init__(self, port, on_datagram):
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.on_datagram = on_datagram
        self.lock = threading.Lock()
        self.configure()
        threading.Thread(target=self._receive_loop, daemon=True).start()

    def configure(self, loss=0.0, delay_min=0.0, delay_max=0.0):
        with self.lock:
            self.loss, self.delay_min, self.delay_max = loss, delay_min, delay_max
            self.sent = self.dropped = 0

    def send(self, addr, payload):
        with self.lock:
            self.sent += 1
            if random.random() < self.loss:
                self.dropped += 1
                return
            delay = random.uniform(self.delay_min, self.delay_max) / 1000
        data = json.dumps(payload).encode()
        if delay > 0:
            threading.Timer(delay, self.sock.sendto, (data, addr)).start()
        else:
            self.sock.sendto(data, addr)

    def _receive_loop(self):
        while True:
            data, _ = self.sock.recvfrom(65535)
            self.on_datagram(json.loads(data))
