import argparse
import random
import threading
import time

import rpyc

import rpc
from constMP import (GROUPMNGR_ADDR, GROUPMNGR_PORT, HOST, PEER_CTRL_BASE, PEER_UDP_BASE,
                     SERVER_ADDR, SERVER_PORT)
from multicast import Multicast
from udpTransport import UdpTransport

WAIT_TIMEOUT = 60


class Experiment:
    """Estado de uma rodada no peer: quem está pronto, quem terminou e o log de entregas."""

    def __init__(self, n_members):
        self.n_members = n_members
        self.cond = threading.Condition()
        self.ready = set()
        self.ended = set()
        self.log = []

    def deliver(self, src, msg):
        with self.cond:
            if msg[0] == "ready":
                self.ready.add(src)
            elif msg[0] == "end":
                self.ended.add(src)
            else:
                self.log.append((src, msg[1]))
                print(f"Mensagem {msg[1]} do processo {src}", flush=True)
            self.cond.notify_all()

    def wait_all(self, group):
        with self.cond:
            return self.cond.wait_for(lambda: len(group) == self.n_members, WAIT_TIMEOUT)


class Peer(rpyc.Service):
    def __init__(self, index):
        self.udp_port = PEER_UDP_BASE + index
        self.ctrl_port = PEER_CTRL_BASE + index
        self.id = None
        self.server = None
        self.lock = threading.Lock()
        self.current = None
        self.early = []
        self.transport = UdpTransport(self.udp_port, self.on_datagram)

    def register(self):
        with rpc.connect(GROUPMNGR_ADDR, GROUPMNGR_PORT) as conn:
            self.id = conn.root.register(HOST, self.udp_port, self.ctrl_port)
        print(f"Registrado no GroupManager com id {self.id}", flush=True)

    def exposed_start(self, round_id, n_msgs, ordering="fifo", loss=0.0, delay_min=0.0, delay_max=0.0):
        if n_msgs == 0:
            print("Terminando.", flush=True)
            rpc.stop_later(self.server)
            return f"Peer {self.id} terminando."
        self.transport.configure(loss, delay_min, delay_max)
        threading.Thread(target=self.run_round, args=(round_id, n_msgs, ordering, delay_max),
                         daemon=True).start()
        return f"Peer {self.id} iniciou a rodada {round_id}."

    def on_datagram(self, d):
        # Um peer pode receber datagramas de uma rodada que ele ainda não começou.
        with self.lock:
            mc = self.current
            if mc is None or d["round"] > mc.round_id:
                self.early.append(d)
                return
        mc.on_datagram(d)

    def run_round(self, round_id, n_msgs, ordering, delay_max):
        with rpc.connect(GROUPMNGR_ADDR, GROUPMNGR_PORT) as conn:
            members = {peer_id: (host, udp) for peer_id, host, udp, _ in conn.root.list()}

        exp = Experiment(len(members))
        mc = Multicast(self.id, round_id, members, self.transport, exp.deliver, ordering)
        with self.lock:
            if self.current:
                self.current.close()
            self.current = mc
            replay = [d for d in self.early if d["round"] == round_id]
            self.early = [d for d in self.early if d["round"] > round_id]
        for d in replay:
            mc.on_datagram(d)

        mc.multicast(["ready"], reliable=True)
        if not exp.wait_all(exp.ready):
            print("Nem todos os peers responderam ao handshake.", flush=True)
            return

        for k in range(n_msgs):
            time.sleep(random.randrange(10, 100) / 1000)
            mc.multicast(["data", k])

        mc.multicast(["end"], reliable=True)
        if not exp.wait_all(exp.ended):
            print("Faltou o fim de algum peer; enviando o log assim mesmo.", flush=True)
        if not mc.wait_acked(WAIT_TIMEOUT):
            print("Algumas mensagens ficaram sem ACK.", flush=True)
        if ordering == "none":
            # O fim é confiável e pode chegar antes de dados crus ainda atrasados na rede.
            time.sleep(delay_max / 1000 + 0.2)

        with exp.cond:
            log = tuple(exp.log)
        stats = (("retransmissões", mc.retransmissions), ("duplicatas descartadas", mc.duplicates),
                 ("datagramas enviados", self.transport.sent), ("descartados pela falha", self.transport.dropped))
        with rpc.connect(SERVER_ADDR, SERVER_PORT) as conn:
            conn.root.submit_log(self.id, round_id, log, stats)
        print(f"Log da rodada {round_id} enviado ({len(log)} mensagens).", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", type=int, default=0, help="desloca as portas para rodar vários peers na mesma máquina")
    args = parser.parse_args()

    peer = Peer(args.index)
    server = rpc.serve(peer, peer.ctrl_port)
    peer.register()
    print("Aguardando o sinal de início...", flush=True)
    server.start()


if __name__ == "__main__":
    main()
