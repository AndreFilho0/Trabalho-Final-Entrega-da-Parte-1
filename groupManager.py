import threading

import rpyc

import rpc
from constMP import GROUPMNGR_PORT


class GroupManager(rpyc.Service):
    def __init__(self):
        self.lock = threading.Lock()
        self.members = {}  # id -> (host, udp_port, ctrl_port)
        self.next_id = 0
        self.server = None

    def exposed_register(self, host, udp_port, ctrl_port):
        with self.lock:
            peer_id = self.next_id
            self.next_id += 1
            self.members[peer_id] = (host, udp_port, ctrl_port)
        print(f"Peer {peer_id} registrado: {host} udp={udp_port} ctrl={ctrl_port}", flush=True)
        return peer_id

    def exposed_list(self):
        # Tupla é enviada por valor; uma lista viraria referência remota (netref).
        with self.lock:
            return tuple((peer_id, *addr) for peer_id, addr in sorted(self.members.items()))

    def exposed_stop(self):
        print("Encerrando.", flush=True)
        rpc.stop_later(self.server)


if __name__ == "__main__":
    server = rpc.serve(GroupManager(), GROUPMNGR_PORT)
    print(f"GroupManager ouvindo RPC na porta {GROUPMNGR_PORT}", flush=True)
    server.start()
