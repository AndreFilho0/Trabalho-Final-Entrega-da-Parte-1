import os


def env(name, default, cast=str):
    return cast(os.environ.get(name, default))


# Endereço que este processo anuncia para os outros (IP público na nuvem).
HOST = env("HOST", "127.0.0.1")

GROUPMNGR_ADDR = env("GROUPMNGR_ADDR", "127.0.0.1")
GROUPMNGR_PORT = env("GROUPMNGR_PORT", 5680, int)
SERVER_ADDR = env("SERVER_ADDR", "127.0.0.1")
SERVER_PORT = env("SERVER_PORT", 5678, int)

# Cada peer usa BASE + índice, para vários peers caberem na mesma máquina.
PEER_UDP_BASE = env("PEER_UDP_BASE", 6789, int)
PEER_CTRL_BASE = env("PEER_CTRL_BASE", 5700, int)

RPC_TIMEOUT = env("RPC_TIMEOUT", 120, float)

# Parâmetros do experimento, lidos só pelo comparisonServer e repassados aos peers.
ORDERING = env("ORDERING", "fifo")  # none | fifo
LOSS = env("LOSS", 0.0, float)
DELAY_MIN = env("DELAY_MIN", 0, float)  # ms
DELAY_MAX = env("DELAY_MAX", 0, float)  # ms

RTO = env("RTO", 0.15, float)  # s sem ACK até retransmitir
TICK = env("TICK", 0.025, float)  # s entre verificações de retransmissão
