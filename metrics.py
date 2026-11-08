def fifo_violations(log):
    """Quantas vezes uma mensagem de uma origem foi entregue depois de outra posterior dela."""
    violations = 0
    highest = {}
    for origin, k in log:
        if k < highest.get(origin, -1):
            violations += 1
        else:
            highest[origin] = k
    return violations


def divergent_positions(logs):
    """Posições da sequência de entregas em que algum peer difere dos demais.

    É a métrica de "rodadas fora de ordem" do comparisonServer original, mas
    olhando todas as posições e todos os peers (o original pulava os últimos).
    """
    sequences = list(logs.values())
    length = max((len(s) for s in sequences), default=0)
    padded = [s + [None] * (length - len(s)) for s in sequences]
    return [j for j, column in enumerate(zip(*padded)) if len(set(column)) > 1], length


def compare(logs, origins, n_msgs):
    expected = {(o, k) for o in origins for k in range(n_msgs)}
    per_peer = {}
    for peer, log in sorted(logs.items()):
        received = set(log)
        per_peer[peer] = {
            "entregues": len(log),
            "faltando": len(expected - received),
            "duplicadas": len(log) - len(received),
            "violações FIFO": fifo_violations(log),
        }
    divergent, length = divergent_positions(logs)
    return {
        "per_peer": per_peer,
        "esperadas": len(expected),
        "posições": length,
        "divergentes": len(divergent),
        "primeira divergência": divergent[0] if divergent else None,
    }
