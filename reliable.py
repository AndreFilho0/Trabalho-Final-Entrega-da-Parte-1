class ReliableChannels:
    """Canais confiáveis e FIFO, um por par (remetente, destinatário). Não usa rede.

    Remetente: numera cada mensagem por destino (1, 2, 3...) e guarda em
    `unacked` até chegar o ACK; o que passar de `rto` sem ACK é retransmitido.

    Destinatário: só entrega a mensagem `expected` de cada origem. As que chegam
    adiantadas esperam na hold-back queue; as que já foram vistas são duplicatas.
    O ACK é cumulativo ("recebi tudo até N"), então um ACK perdido é coberto
    pelo seguinte.
    """

    def __init__(self):
        self.next_seq = {}
        self.unacked = {}  # destino -> {seq: [mensagem, enviada_em]}
        self.expected = {}
        self.hold_back = {}  # origem -> {seq: mensagem}

    def send(self, dest, msg, now):
        seq = self.next_seq.get(dest, 1)
        self.next_seq[dest] = seq + 1
        self.unacked.setdefault(dest, {})[seq] = [msg, now]
        return seq

    def on_data(self, src, seq, msg):
        """Devolve (mensagens a entregar em ordem, ACK cumulativo, é_duplicata)."""
        expected = self.expected.get(src, 1)
        queue = self.hold_back.setdefault(src, {})
        if seq < expected or seq in queue:
            return [], expected - 1, True

        queue[seq] = msg
        delivered = []
        while expected in queue:
            delivered.append(queue.pop(expected))
            expected += 1
        self.expected[src] = expected
        return delivered, expected - 1, False

    def on_ack(self, dest, cum):
        pending = self.unacked.get(dest, {})
        for seq in [s for s in pending if s <= cum]:
            del pending[seq]

    def retransmit_due(self, now, rto):
        late = []
        for dest, pending in self.unacked.items():
            for seq, entry in sorted(pending.items()):
                if now - entry[1] >= rto:
                    entry[1] = now
                    late.append((dest, seq, entry[0]))
        return late

    def pending(self):
        return sum(len(p) for p in self.unacked.values())

    def held_back(self):
        return sum(len(q) for q in self.hold_back.values())
