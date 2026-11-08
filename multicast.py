import threading
import time

from constMP import RTO, TICK
from reliable import ReliableChannels


class Multicast:
    """Multicast de uma rodada sobre UDP.

    Datagramas:
      {"t": "d", round, src, seq, m}  dado do canal confiável
      {"t": "a", round, src, cum}     ACK cumulativo
      {"t": "r", round, src, m}       UDP cru, sem garantia (ORDERING=none)
    """

    def __init__(self, my_id, round_id, members, transport, deliver, ordering="fifo"):
        self.my_id = my_id
        self.round_id = round_id
        self.members = members  # id -> (host, udp_port)
        self.transport = transport
        self.deliver = deliver
        self.ordering = ordering
        self.channels = ReliableChannels()
        self.cond = threading.Condition()
        self.retransmissions = 0
        self.duplicates = 0
        self.closed = False
        threading.Thread(target=self._retransmit_loop, daemon=True).start()

    def multicast(self, msg, reliable=None):
        """No modo none só os dados vão crus: handshake e fim continuam confiáveis
        para o experimento sempre terminar e dar para medir as perdas."""
        if reliable is None:
            reliable = self.ordering != "none"
        for dest, addr in self.members.items():
            if reliable:
                with self.cond:
                    seq = self.channels.send(dest, msg, time.monotonic())
                self._send_data(dest, seq, msg)
            else:
                self._send(addr, {"t": "r", "m": msg})

    def on_datagram(self, d):
        if d["round"] != self.round_id:
            return
        kind, src = d["t"], d["src"]
        if kind == "r":
            self.deliver(src, d["m"])
        elif kind == "a":
            with self.cond:
                self.channels.on_ack(src, d["cum"])
                self.cond.notify_all()
        elif kind == "d":
            with self.cond:
                delivered, cum, duplicate = self.channels.on_data(src, d["seq"], d["m"])
                self.duplicates += duplicate
            # Duplicata também recebe ACK: o ACK anterior pode ter se perdido.
            self._send(self.members[src], {"t": "a", "cum": cum})
            for msg in delivered:
                self.deliver(src, msg)

    def wait_acked(self, timeout):
        """Espera todos os destinatários confirmarem tudo que este peer enviou."""
        with self.cond:
            return self.cond.wait_for(lambda: self.channels.pending() == 0, timeout)

    def close(self):
        self.closed = True

    def _send_data(self, dest, seq, msg):
        self._send(self.members[dest], {"t": "d", "seq": seq, "m": msg})

    def _send(self, addr, payload):
        self.transport.send(addr, {**payload, "round": self.round_id, "src": self.my_id})

    def _retransmit_loop(self):
        while not self.closed:
            time.sleep(TICK)
            with self.cond:
                late = self.channels.retransmit_due(time.monotonic(), RTO)
                self.retransmissions += len(late)
            for dest, seq, msg in late:
                self._send_data(dest, seq, msg)
