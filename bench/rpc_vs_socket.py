"""Latência da operação "list" do GroupManager: socket TCP + pickle (como no MPComm) x RPyC.

Uso: python bench/rpc_vs_socket.py [chamadas=500]
"""
import pickle
import statistics
import sys
import threading
import time
from pathlib import Path
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import rpc  # noqa: E402
from groupManager import GroupManager  # noqa: E402

SOCKET_PORT = 15680
RPC_PORT = 15681
PEERS = [(f"10.0.0.{i}", 6789, 5700) for i in range(8)]


def socket_server():
    """Mesmo laço do GroupMngr.py original: uma conexão por pedido."""
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", SOCKET_PORT))
    sock.listen(64)
    while True:
        conn, _ = sock.accept()
        req = pickle.loads(conn.recv(2048))
        if req["op"] == "list":
            conn.send(pickle.dumps([p[0] for p in PEERS]))
        conn.close()


def socket_list():
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(("127.0.0.1", SOCKET_PORT))
    sock.send(pickle.dumps({"op": "list"}))
    peers = pickle.loads(sock.recv(2048))
    sock.close()
    return peers


def rpc_list_new_connection():
    with rpc.connect("127.0.0.1", RPC_PORT) as conn:
        return conn.root.list()


def measure(fn, calls):
    for _ in range(20):
        fn()
    samples = []
    for _ in range(calls):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1e6)
    samples.sort()
    return statistics.mean(samples), statistics.median(samples), samples[int(len(samples) * 0.99) - 1]


def main():
    calls = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    threading.Thread(target=socket_server, daemon=True).start()

    gm = GroupManager()
    for host, udp, ctrl in PEERS:
        gm.exposed_register(host, udp, ctrl)
    threading.Thread(target=rpc.serve(gm, RPC_PORT).start, daemon=True).start()
    time.sleep(0.3)

    persistent = rpc.connect("127.0.0.1", RPC_PORT)
    cases = [
        ("Socket TCP + pickle (1 conexão por pedido, como o original)", socket_list),
        ("RPyC (1 conexão por pedido)", rpc_list_new_connection),
        ("RPyC (conexão persistente)", lambda: persistent.root.list()),
    ]
    print(f"{calls} chamadas de list() com {len(PEERS)} peers registrados\n")
    print("| Transporte | Média (µs) | Mediana (µs) | p99 (µs) |")
    print("|---|---|---|---|")
    for name, fn in cases:
        mean, median, p99 = measure(fn, calls)
        print(f"| {name} | {mean:.0f} | {median:.0f} | {p99:.0f} |")
    persistent.close()


if __name__ == "__main__":
    main()
