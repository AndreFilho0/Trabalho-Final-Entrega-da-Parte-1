import argparse
import threading
import time

import rpyc

import rpc
from constMP import DELAY_MAX, DELAY_MIN, GROUPMNGR_ADDR, GROUPMNGR_PORT, LOSS, ORDERING, SERVER_PORT
from metrics import compare

LOG_TIMEOUT = 120


class Comparison(rpyc.Service):
    def __init__(self):
        self.cond = threading.Condition()
        self.round_id = 0
        self.logs = {}
        self.stats = {}
        self.server = None

    def exposed_submit_log(self, peer_id, round_id, log, stats=()):
        with self.cond:
            if round_id != self.round_id:
                return
            self.logs[peer_id] = [tuple(entry) for entry in log]
            self.stats[peer_id] = dict(stats)
            print(f"Log recebido do peer {peer_id}", flush=True)
            self.cond.notify_all()

    def new_round(self):
        with self.cond:
            self.round_id += 1
            self.logs = {}
            self.stats = {}
            return self.round_id

    def wait_logs(self, n_peers):
        with self.cond:
            self.cond.wait_for(lambda: len(self.logs) == n_peers, LOG_TIMEOUT)
            return dict(self.logs), dict(self.stats)


def group_members():
    with rpc.connect(GROUPMNGR_ADDR, GROUPMNGR_PORT) as conn:
        return conn.root.list()


def wait_for_peers(n):
    while len(group_members()) < n:
        time.sleep(0.2)


def start_peers(peers, round_id, n_msgs):
    for _, host, _, ctrl_port in peers:
        with rpc.connect(host, ctrl_port) as conn:
            print(conn.root.start(round_id, n_msgs, ORDERING, LOSS, DELAY_MIN, DELAY_MAX), flush=True)


def run_round(service, n_msgs):
    peers = group_members()
    print(f"Peers: {[p[0] for p in peers]}  ORDERING={ORDERING} LOSS={LOSS} "
          f"DELAY={DELAY_MIN:g}..{DELAY_MAX:g} ms", flush=True)
    round_id = service.new_round()
    start_peers(peers, round_id, n_msgs)
    print("Aguardando os logs dos peers...", flush=True)
    logs, stats = service.wait_logs(len(peers))
    print_report(compare(logs, [p[0] for p in peers], n_msgs), stats, len(peers), n_msgs)


def print_report(result, stats, n_peers, n_msgs):
    columns = ["entregues", "faltando", "duplicadas", "violações FIFO", "retransmissões", "datagramas enviados"]
    print(f"\n{n_peers} peers x {n_msgs} mensagens = {result['esperadas']} esperadas por peer")
    print("peer" + "".join(f"{c:>22}" for c in columns))
    for peer, row in result["per_peer"].items():
        row = {**row, **stats.get(peer, {})}
        print(f"{peer:>4}" + "".join(f"{row.get(c, '-'):>22}" for c in columns))
    print(f"Posições divergentes entre peers: {result['divergentes']} de {result['posições']}"
          f" (primeira: {result['primeira divergência']})\n", flush=True)


def shutdown(service):
    start_peers(group_members(), 0, 0)
    with rpc.connect(GROUPMNGR_ADDR, GROUPMNGR_PORT) as conn:
        conn.root.stop()
    service.server.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--msgs", type=int, help="roda uma única rodada com esse número de mensagens por peer e encerra")
    parser.add_argument("--wait-peers", type=int, default=0, help="espera esse número de peers registrados")
    args = parser.parse_args()

    service = Comparison()
    server = rpc.serve(service, SERVER_PORT)
    threading.Thread(target=server.start, daemon=True).start()
    wait_for_peers(args.wait_peers)

    while True:
        n_msgs = args.msgs if args.msgs is not None else int(
            input("Número de mensagens por peer (0 para terminar) => "))
        if n_msgs == 0:
            break
        run_round(service, n_msgs)
        if args.msgs is not None:
            break
    shutdown(service)
    print("Encerrado.")


if __name__ == "__main__":
    main()
