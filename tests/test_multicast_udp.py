import threading

from metrics import fifo_violations
from multicast import Multicast
from udpTransport import UdpTransport

BASE_PORT = 16789
N_PEERS = 3
N_MSGS = 40


def test_multicast_fifo_sobre_udp_real_com_30_por_cento_de_perda():
    members = {i: ("127.0.0.1", BASE_PORT + i) for i in range(N_PEERS)}
    logs = {i: [] for i in members}
    done = threading.Condition()
    peers = {}

    def deliver_to(i):
        def deliver(src, msg):
            with done:
                logs[i].append((src, msg))
                done.notify_all()
        return deliver

    for i in members:
        transport = UdpTransport(BASE_PORT + i, lambda d, i=i: peers[i].on_datagram(d))
        transport.configure(loss=0.3, delay_min=0, delay_max=20)
        peers[i] = Multicast(i, 1, members, transport, deliver_to(i), ordering="fifo")

    for k in range(N_MSGS):
        for mc in peers.values():
            mc.multicast(k)

    with done:
        assert done.wait_for(lambda: all(len(log) == N_PEERS * N_MSGS for log in logs.values()), 30)
    for mc in peers.values():
        assert mc.wait_acked(10)
        mc.close()

    expected = {(o, k) for o in members for k in range(N_MSGS)}
    for log in logs.values():
        assert set(log) == expected
        assert fifo_violations(log) == 0
