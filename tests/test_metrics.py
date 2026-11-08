from metrics import compare, divergent_positions, fifo_violations


def test_fifo_violations_conta_mensagem_que_chega_depois_de_uma_posterior():
    assert fifo_violations([(0, 0), (1, 0), (0, 1)]) == 0
    assert fifo_violations([(0, 1), (0, 0), (0, 2)]) == 1


def test_divergencia_no_ultimo_peer_e_na_ultima_posicao_e_detectada():
    # O original usava range(1, N-1) e range(0, N_MSGS-1) e não via este caso.
    logs = {0: [(0, 0), (1, 0)], 1: [(0, 0), (1, 0)], 2: [(0, 0), (9, 9)]}
    divergent, length = divergent_positions(logs)
    assert divergent == [1]
    assert length == 2


def test_compare_conta_faltas_e_duplicatas():
    logs = {0: [(0, 0), (0, 1), (1, 0), (1, 1)], 1: [(0, 0), (0, 0), (1, 1)]}
    result = compare(logs, origins=[0, 1], n_msgs=2)
    assert result["per_peer"][0]["faltando"] == 0
    assert result["per_peer"][1]["faltando"] == 2
    assert result["per_peer"][1]["duplicadas"] == 1
