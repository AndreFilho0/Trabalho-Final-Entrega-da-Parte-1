import random

from reliable import ReliableChannels


def test_mensagem_adiantada_espera_na_hold_back_queue():
    r = ReliableChannels()
    assert r.on_data("a", 2, "m2") == ([], 0, False)
    assert r.held_back() == 1
    assert r.on_data("a", 1, "m1") == (["m1", "m2"], 2, False)
    assert r.held_back() == 0


def test_duplicata_e_descartada_mas_recebe_ack_de_novo():
    r = ReliableChannels()
    r.on_data("a", 1, "m1")
    assert r.on_data("a", 1, "m1") == ([], 1, True)


def test_ack_cumulativo_libera_tudo_ate_ele():
    r = ReliableChannels()
    for i in range(3):
        r.send("b", i, now=0)
    r.on_ack("b", 2)
    assert r.pending() == 1
    assert r.retransmit_due(now=1, rto=0.5) == [("b", 3, 2)]


def test_rede_com_perda_e_reordenacao_entrega_tudo_uma_vez_e_em_ordem():
    rng = random.Random(42)
    sender, receiver = ReliableChannels(), ReliableChannels()
    in_flight = []
    delivered = []

    def transmit(packet):
        if rng.random() > 0.3:
            in_flight.append(packet)

    for i in range(100):
        transmit(("data", sender.send("B", i, now=0), i))

    now = 0
    while sender.pending():
        now += 1
        rng.shuffle(in_flight)
        batch, in_flight[:] = in_flight[:], []
        for kind, a, b in batch:
            if kind == "data":
                msgs, cum, _ = receiver.on_data("A", a, b)
                delivered.extend(msgs)
                transmit(("ack", cum, None))
            else:
                sender.on_ack("B", a)
        for _, seq, msg in sender.retransmit_due(now, rto=3):
            transmit(("data", seq, msg))
        assert now < 10_000

    assert delivered == list(range(100))
