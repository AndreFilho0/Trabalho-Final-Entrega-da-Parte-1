import threading
import time

import rpyc
from rpyc.utils.server import ThreadedServer

from constMP import RPC_TIMEOUT

# Só métodos exposed_* ficam acessíveis; nada de modo "classic", que deixaria
# o cliente executar qualquer código no servidor.
CONFIG = {"allow_public_attrs": False, "sync_request_timeout": RPC_TIMEOUT}


def serve(service, port):
    server = ThreadedServer(service, port=port, protocol_config=CONFIG, reuse_addr=True)
    service.server = server
    return server


def stop_later(server, delay=0.2):
    """Fecha o servidor depois que a resposta da chamada atual já saiu."""
    threading.Timer(delay, server.close).start()


def connect(host, port, retry_for=10.0):
    deadline = time.monotonic() + retry_for
    while True:
        try:
            return rpyc.connect(host, port, config=CONFIG)
        except ConnectionRefusedError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
