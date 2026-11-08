# MPComm — Parte 1: RPC e multicast confiável com ordem FIFO

Trabalho final de **Sistemas Distribuídos (INF0344)** — Ciência da Computação, UFG.
Base: [MPComm](https://github.com/professorfabio/MPComm), do Prof. Fábio M. Costa (cópia intacta em [`original/`](original/)).

## Integrantes

| Nome | Matrícula | E-mail |
|---|---|---|
| André de Oliveira Machado Filho | 202406913 | andrefilho@discente.ufg.br |
| Ryan Gabryel Barbosa de Lima | 202301485 | ryanlima@discente.ufg.br |

## Testar localmente em 3 passos

Precisa só de Python 3.10+.

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

scripts/run_local.sh 4 30                  # 4 peers, 30 mensagens cada, multicast confiável FIFO
ORDERING=none scripts/run_local.sh 4 30    # mesmo experimento com o UDP cru do original
```

O script sobe o GroupManager, os peers e o comparisonServer na sua máquina, roda uma rodada, imprime a tabela
abaixo e encerra tudo. Por padrão ele simula **10% de perda e 0–60 ms de atraso** em cada datagrama
(mude com `LOSS=0.3 DELAY_MAX=100 scripts/run_local.sh`). A saída de cada processo fica em `logs/`.

```
4 peers x 30 mensagens = 120 esperadas por peer
peer   entregues   faltando   duplicadas   violações FIFO   retransmissões   datagramas enviados
   0         120          0            0                0               47                   314
   ...
Posições divergentes entre peers: 116 de 120 (primeira: 0)
```

Outros comandos:

```bash
pytest                                   # testes (inclui rede simulada e UDP real com 30% de perda)
python bench/rpc_vs_socket.py 1000       # latência socket x RPC
```

## O que o sistema faz

Um grupo de **peers** troca mensagens por multicast: cada peer envia `n` mensagens numeradas para todos (inclusive
para si mesmo) em intervalos aleatórios e registra a ordem em que recebeu as mensagens. Dois servidores coordenam o
experimento:

- **GroupManager** (`groupManager.py`): registro e descoberta dos peers.
- **comparisonServer** (`comparisonServer.py`): dá o sinal de início, recebe o log de cada peer e compara.

O objetivo do MPComm é mostrar que, sem coordenação, cada peer recebe as mensagens numa ordem diferente. Nesta parte
melhoramos os protocolos de comunicação:

- **a)** as interações cliente-servidor passam a ser **RPC** (RPyC) em vez de sockets TCP montados à mão;
- **b)** o multicast continua em **sockets UDP**, agora com **tratamento de perda** e **ordem FIFO**.

## Arquitetura

```
                 ┌────────────────────┐        ┌──────────────────────┐
                 │   GroupManager     │        │  comparisonServer    │
                 │ register / list /  │        │ start dos peers,     │
                 │ stop               │        │ submit_log, métricas │
                 └─────────▲──────────┘        └───▲───────────┬──────┘
               RPC (RPyC)  │                       │ RPC       │ RPC: start(rodada, n, ordering, falhas)
          register / list  │         submit_log    │           │
        ┌──────────────────┴───────────────────────┴───────────▼─────────────┐
        │  peer 0               peer 1               peer 2          ...      │
        │  ┌───────────────┐                                                  │
        │  │ Experiment    │  log de entregas, handshake, fim                 │
        │  │ Multicast     │  canal confiável FIFO (seq, ACK, retransmissão)  │
        │  │ UdpTransport  │  socket UDP + injeção de perda/atraso            │
        │  └───────┬───────┘                                                  │
        └──────────┼──────────────────────────────────────────────────────────┘
                   └──── UDP ────► todos os peers (dados e ACKs)
```

| Arquivo | Papel |
|---|---|
| `constMP.py` | endereços, portas e parâmetros (todos podem vir de variáveis de ambiente) |
| `rpc.py` | cria servidores e conexões RPyC com a mesma configuração |
| `groupManager.py` | serviço RPC de membros do grupo |
| `comparisonServer.py` | serviço RPC que coordena a rodada e imprime as métricas |
| `peerCommunicator.py` | peer: serviço RPC de controle + rodada de multicast |
| `multicast.py` | multicast de uma rodada (modo cru ou confiável) e thread de retransmissão |
| `reliable.py` | canais confiáveis FIFO, sem rede (testável isoladamente) |
| `udpTransport.py` | socket UDP, JSON e injeção de falhas |
| `metrics.py` | comparação dos logs (perdas, duplicatas, FIFO, divergência) |

## Parte 1a — de sockets para RPC

### O que mudou

No original, cada interação cliente-servidor abre um socket TCP, serializa um dicionário com `pickle`, faz
`send`/`recv` com tamanho fixo de buffer e fecha. Agora cada servidor é um `rpyc.Service` e o cliente chama métodos:

| Interação | Original (socket TCP + pickle) | Agora (RPyC) |
|---|---|---|
| registro | `send(pickle.dumps({"op": "register", ...}))` | `conn.root.register(host, udp, ctrl)` → id do peer |
| lista de peers | `send({"op": "list"})` + `recv(2048)` | `conn.root.list()` |
| sinal de início | `send((peerNumber, nMsgs))` + `recv(512)` | `conn.root.start(rodada, n, ordering, loss, ...)` |
| envio do log | `send(pickle.dumps(logList))`, servidor faz `recv(32768)` | `conn.root.submit_log(id, rodada, log, stats)` |
| parada | `send({"op": "stop"})` | `conn.root.stop()` |

```python
# antes (peerCommunicatorUDP.py)
clientSock = socket(AF_INET, SOCK_STREAM)
clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
clientSock.send(pickle.dumps({"op": "list"}))
PEERS = pickle.loads(clientSock.recv(2048))
clientSock.close()

# depois (peerCommunicator.py)
with rpc.connect(GROUPMNGR_ADDR, GROUPMNGR_PORT) as conn:
    members = conn.root.list()
```

Detalhes de implementação:

- Os serviços expõem **só** métodos `exposed_*` e usam `allow_public_attrs=False`. Não usamos o modo *classic* do
  RPyC, que deixaria qualquer cliente executar código arbitrário no servidor.
- **Passagem de parâmetros**: o RPyC copia tipos imutáveis (int, str, tupla) e passa objetos mutáveis (lista, dict)
  **por referência**, como um proxy remoto (*netref*): cada acesso ao objeto vira outra chamada pela rede. Por isso
  `list()` devolve uma tupla de tuplas, e o log e as estatísticas vão como tuplas.
- `sync_request_timeout` limita quanto tempo uma chamada espera; `rpc.connect` tenta de novo por alguns segundos se
  o servidor ainda não subiu.
- O `start` só dispara a rodada numa thread e volta na hora; quem termina avisa chamando `submit_log`.

### Comparação

Medição (`python bench/rpc_vs_socket.py 1000`, mesma máquina, 1000 chamadas de `list()` com 8 peers):

| Transporte | Média (µs) | Mediana (µs) | p99 (µs) |
|---|---|---|---|
| Socket TCP + pickle (1 conexão por pedido, como o original) | 91 | 70 | 259 |
| RPyC (1 conexão por pedido) | 569 | 555 | 769 |
| RPyC (conexão persistente) | 181 | 173 | 299 |

O RPC ficou mais lento. Ao conectar, o RPyC negocia o protocolo e troca o nome do serviço; em cada chamada ele
monta um pedido com identificador, serializa com o formato próprio (*brine*) e espera a resposta numa máquina de
estados. O socket do original só manda bytes e lê bytes. Reaproveitar a conexão tira a maior parte do custo, mas
ainda fica uma camada a mais. Para o MPComm isso não pesa: são poucas chamadas de controle por rodada, e o tráfego
pesado (o multicast) continua em UDP.

**Vantagens do RPC**

- **Transparência de acesso**: chamar `conn.root.list()` parece chamar uma função local. Some do código a
  montagem do pedido, o `op` em string, o tamanho do buffer e o fechamento do socket.
- **Enquadramento e serialização resolvidos**: o original lê com `recv(2048)` e `recv(32768)`; um log maior que
  isso chega truncado e o `pickle.loads` falha. Com RPC a mensagem chega inteira, de qualquer tamanho.
- **Erros voltam para quem chamou**: no original, um `op` desconhecido não recebe resposta e o cliente fica
  esperando para sempre. No RPyC, método inexistente, exceção no servidor ou timeout viram exceção no cliente.
- **Interface explícita**: os métodos `exposed_*` são o contrato do serviço; fica fácil ver o que cada servidor
  oferece.
- **Mais seguro que o original**: `pickle.loads` sobre dados da rede permite execução de código arbitrário. O
  RPyC só aceita chamar os métodos expostos.

**Desvantagens do RPC**

- **Desempenho**: a camada a mais custa latência (tabela acima).
- **Acoplamento de linguagem**: RPyC só funciona entre programas Python. Um socket com formato definido (ou gRPC,
  com `.proto`) funciona com qualquer linguagem.
- **A transparência engana**: a chamada parece local, mas pode falhar por motivos que uma chamada local não tem
  (rede caída, servidor lento, timeout). O código continua tendo que tratar isso.
- **Referências remotas escondidas**: passar uma lista "por engano" cria um *netref*, e cada acesso ao item vira
  uma ida e volta pela rede, sem nada no código que deixe isso visível.
- **Menos controle**: com socket o programador decide formato, tempo de vida da conexão e o que aceitar; com RPC
  isso fica dentro da biblioteca, o que dificulta depurar o que realmente passou pela rede.

**Quando o socket ainda é melhor**: tráfego intenso e de baixa latência (por isso o multicast continua em UDP),
comunicação entre linguagens diferentes sem um IDL, ou quando é preciso controlar cada byte do protocolo.

## Parte 1b — perda de mensagens e ordem FIFO no multicast

### O problema

No original cada peer faz um `sendto` UDP para cada membro e entrega o que chegar, na ordem em que chegar. UDP pode
**perder**, **duplicar** e **reordenar** datagramas. Até o handshake `READY` e a mensagem de fim `(-1, -1)` podem se
perder; nesse caso o peer fica parado esperando para sempre.

### A solução: um canal confiável e FIFO por par de processos

`reliable.py` implementa, para cada par (remetente → destinatário):

**No remetente**
- cada mensagem para aquele destino recebe um número de sequência `seq` (1, 2, 3, …);
- a mensagem fica em `unacked` até o ACK chegar;
- a cada 25 ms (`TICK`), o que estiver há mais de 150 ms (`RTO`) sem ACK é **retransmitido**.

**No destinatário**
- guarda `expected`, o próximo número esperado daquela origem;
- `seq < expected` ou já guardada: **duplicata**, descarta, mas manda o ACK de novo (o ACK anterior pode ter se
  perdido);
- `seq > expected`: chegou adiantada, espera na **hold-back queue**;
- quando chega `expected`, entrega essa e as seguintes que já estavam na fila: **ordem FIFO por remetente**;
- responde com **ACK cumulativo** `expected - 1` ("recebi tudo até aqui"), então um ACK perdido é coberto pelo
  próximo.

```
remetente                                    destinatário (expected = 1)
  d(1) ─────────────────────────X  perdido
  d(2) ─────────────────────────►  hold-back {2}            ◄── ack(0)
  d(3) ─────────────────────────►  hold-back {2,3}          ◄── ack(0)
  ... 150 ms sem ACK de 1 ...
  d(1) retransmitida ───────────►  entrega 1, 2, 3          ◄── ack(3)
```

### Datagramas (JSON, não mais pickle)

```
{"t": "d", "round": r, "src": i, "seq": s, "m": mensagem}   dado do canal confiável
{"t": "a", "round": r, "src": i, "cum": c}                  ACK cumulativo
{"t": "r", "round": r, "src": i, "m": mensagem}             UDP cru (ORDERING=none)
```

- `round` separa as rodadas: um datagrama atrasado da rodada anterior é ignorado, e um da rodada seguinte que chega
  antes de o peer começar fica guardado até ele começar.
- O handshake e o fim também vão pelo canal confiável. O peer só envia o log quando recebeu o fim de todos **e**
  todos os destinatários confirmaram o que ele enviou. Depois disso o socket continua aberto respondendo ACKs, para
  quem ainda está retransmitindo não ficar sem resposta.
- Com `ORDERING=none` só as mensagens de dados vão cruas, como no original; handshake e fim continuam confiáveis
  para o experimento sempre terminar e dar para contar o que se perdeu.

### Injeção de falhas

Para ver o efeito de peers em regiões diferentes numa máquina só, `UdpTransport.send` descarta cada datagrama com
probabilidade `LOSS` e atrasa entre `DELAY_MIN` e `DELAY_MAX` ms. Atrasos diferentes por datagrama geram
reordenação, como na Internet. Os valores são definidos no comparisonServer e enviados aos peers no `start`, então
basta mudar num lugar só.

### Resultados

4 peers × 30 mensagens, 10% de perda, 0–60 ms de atraso (`scripts/run_local.sh 4 30`):

| `ORDERING` | Faltando por peer | Duplicadas | Violações FIFO por peer | Datagramas por peer | Posições divergentes |
|---|---|---|---|---|---|
| `none` (original) | 9 a 15 | 0 | 5 a 9 | ~137 | 111 de 111 |
| `fifo` (Parte 1b) | **0** | **0** | **0** | ~300 | 116 de 120 |

Com 30% de perda e 5 peers todos continuam recebendo as 150 mensagens, sem violação FIFO, em menos de 4 segundos.

O custo é tráfego: cada dado tem um ACK e há retransmissões, então cada peer envia mais que o dobro de datagramas.

**O que FIFO não resolve**: FIFO só ordena mensagens **do mesmo remetente**. Mensagens de remetentes diferentes
continuam chegando em ordens diferentes em cada peer ("posições divergentes" continua alto). Isso é o que as
Partes 3 (ordem total) e 4 vão tratar.

## Outros problemas do código-base corrigidos

- `comparisonServer` original: `range(0, N_MSGS-1)` e `range(1, N-1)` deixavam de fora a última posição e o último
  peer, e só olhava `N_MSGS` posições quando cada peer recebe `N × N_MSGS`. `metrics.py` compara todas as posições e
  todos os peers, e mede também faltas, duplicatas e violações de FIFO.
- Espera ativa (`while handShakeCount < N: pass`) trocada por `threading.Condition`.
- Portas fixas impediam vários peers na mesma máquina; agora cada peer usa porta base + `--index`.
- `myself` era usado como variável global implícita dentro da thread.
- Dependência de `api.ipify.org` para descobrir o IP: agora o endereço vem de `HOST`.

## Rodar em máquinas diferentes (nuvem)

Como no MPComm, coloque os servidores numa instância e os peers em outras (de preferência em regiões diferentes).

```bash
# instância dos servidores (IP 1.2.3.4)
python groupManager.py
ORDERING=fifo python comparisonServer.py --wait-peers 6     # pergunta o número de mensagens

# cada instância de peer
HOST=<ip-publico-deste-peer> GROUPMNGR_ADDR=1.2.3.4 SERVER_ADDR=1.2.3.4 python peerCommunicator.py
```

Portas a liberar: 5678 e 5680 (TCP) nos servidores; 5700 (TCP) e 6789 (UDP) nos peers.

## Roteiro do vídeo

1. Visão geral do MPComm e do que pede a Parte 1.
2. Parte 1a: mostrar o código antes e depois, rodar `bench/rpc_vs_socket.py` e discutir vantagens e desvantagens.
3. Parte 1b: `ORDERING=none scripts/run_local.sh 4 30` (perdas e violações FIFO) e depois
   `scripts/run_local.sh 4 30` (zero perdas e zero violações), explicando seq, ACK cumulativo, retransmissão e
   hold-back queue.
4. Mostrar que FIFO não basta para a mesma ordem em todos os peers (gancho para a Parte 3).
