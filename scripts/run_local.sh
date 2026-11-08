#!/usr/bin/env bash
# Uso: scripts/run_local.sh [peers=4] [mensagens=30]
# Sobe GroupManager, N peers e comparisonServer em localhost, roda uma rodada e encerra tudo.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-4}
MSGS=${2:-30}
PY=${PYTHON:-.venv/bin/python}
[ -x "$PY" ] || PY=python3

mkdir -p logs
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT

"$PY" -u groupManager.py > logs/groupManager.log 2>&1 &
pids+=($!)
for i in $(seq 0 $((N - 1))); do
  "$PY" -u peerCommunicator.py --index "$i" > "logs/peer$i.log" 2>&1 &
  pids+=($!)
done

"$PY" -u comparisonServer.py --msgs "$MSGS" --wait-peers "$N"
wait "${pids[@]}" 2>/dev/null || true
echo "Logs de cada processo em logs/"
