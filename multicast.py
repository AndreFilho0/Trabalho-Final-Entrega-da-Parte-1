class Multicast:
    """Multicast de uma rodada: um sendto para cada membro, sem garantia nenhuma."""

    def __init__(self, my_id, round_id, members, transport, deliver):
        self.my_id = my_id
        self.round_id = round_id
        self.members = members  # id -> (host, udp_port)
        self.transport = transport
        self.deliver = deliver

    def multicast(self, msg):
        for addr in self.members.values():
            self.transport.send(addr, {"t": "r", "round": self.round_id, "src": self.my_id, "m": msg})

    def on_datagram(self, d):
        if d["round"] == self.round_id:
            self.deliver(d["src"], d["m"])

    def close(self):
        pass
