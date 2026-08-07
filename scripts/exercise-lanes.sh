#!/usr/bin/env bash
# exercise-lanes.sh — drive every push format at one edge endpoint at once:
#
#   tx      subtx-gen -tcp        -> TX_PORT      (default 8725) continuous stream
#   subtree send-subtree-push     -> SUBTREE_PORT (default 8726) one per interval
#   block   send-block-push       -> BLOCK_PORT   (default 8727) one per interval
#
# Defaults: 256-byte transactions at 100 pps, one subtree/s, one block/min,
# run until Ctrl-C (all three senders stop together; each prints its totals).
#
# Usage:
#   scripts/exercise-lanes.sh [flags]
#
#   -host <h>              edge host (default ::1)
#   -port <p>              send ALL lanes to this one port (lab sink or the
#                          tunnel-sink submit relay; a REAL edge admits
#                          subtree/block only on its per-class 8726/8727)
#   -tx-port <p>           tx ingress port          (default 8725)
#   -subtree-port <p>      subtree push ingress     (default 8726)
#   -block-port <p>        block push ingress       (default 8727)
#   -tx-pps <n>            transaction rate         (default 100)
#   -tx-size <bytes>       transaction payload size (default 256)
#   -subtree-interval <d>  subtree cadence          (default 1s)
#   -subtree-nodes <n>     nodes per subtree        (default 16)
#   -block-interval <d>    block cadence            (default 60s)
#   -block-subtrees <n>    subtree roots per block  (default 4)
#   -duration <d>          total runtime            (default 0 = until Ctrl-C)
#
# Binaries are taken from the repo root (make build) when present, else PATH.
set -euo pipefail

HOST="::1"
TX_PORT=8725
SUBTREE_PORT=8726
BLOCK_PORT=8727
TX_PPS=100
TX_SIZE=256
SUBTREE_INTERVAL=1s
SUBTREE_NODES=16
BLOCK_INTERVAL=60s
BLOCK_SUBTREES=4
DURATION=0

usage() { sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -host)             HOST=$2; shift 2 ;;
    -port)             TX_PORT=$2; SUBTREE_PORT=$2; BLOCK_PORT=$2; shift 2 ;;
    -tx-port)          TX_PORT=$2; shift 2 ;;
    -subtree-port)     SUBTREE_PORT=$2; shift 2 ;;
    -block-port)       BLOCK_PORT=$2; shift 2 ;;
    -tx-pps)           TX_PPS=$2; shift 2 ;;
    -tx-size)          TX_SIZE=$2; shift 2 ;;
    -subtree-interval) SUBTREE_INTERVAL=$2; shift 2 ;;
    -subtree-nodes)    SUBTREE_NODES=$2; shift 2 ;;
    -block-interval)   BLOCK_INTERVAL=$2; shift 2 ;;
    -block-subtrees)   BLOCK_SUBTREES=$2; shift 2 ;;
    -duration)         DURATION=$2; shift 2 ;;
    -h|--help)         usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage 1 ;;
  esac
done

# Bracket IPv6 literals for host:port concatenation.
case "$HOST" in
  *:*) ADDR="[$HOST]" ;;
  *)   ADDR="$HOST" ;;
esac

# Prefer freshly built binaries in the repo root over PATH.
root=$(cd "$(dirname "$0")/.." && pwd)
bin() {
  if [ -x "$root/$1" ]; then echo "$root/$1"; else command -v "$1" || {
    echo "error: $1 not found (run 'make build' or add it to PATH)" >&2; exit 1; }
  fi
}
SUBTX_GEN=$(bin subtx-gen)
SUBTREE_PUSH=$(bin send-subtree-push)
BLOCK_PUSH=$(bin send-block-push)

dur_args=()
[ "$DURATION" != "0" ] && dur_args=(-duration "$DURATION")

echo "exercise-lanes: edge=$HOST duration=${DURATION}"
echo "  tx      -> ${ADDR}:${TX_PORT}       ${TX_PPS} pps, ${TX_SIZE} B payloads (TCP)"
echo "  subtree -> ${ADDR}:${SUBTREE_PORT}  one per ${SUBTREE_INTERVAL}, ${SUBTREE_NODES} nodes"
echo "  block   -> ${ADDR}:${BLOCK_PORT}    one per ${BLOCK_INTERVAL}, ${BLOCK_SUBTREES} subtrees"

pids=()
cleanup() {
  trap - INT TERM
  kill -INT "${pids[@]}" 2>/dev/null || true
  wait "${pids[@]}" 2>/dev/null || true
}
trap cleanup INT TERM

"$SUBTX_GEN" -addr "${ADDR}:${TX_PORT}" -tcp -frame-version 2 \
  -pps "$TX_PPS" -payload-size "$TX_SIZE" "${dur_args[@]}" &
pids+=($!)

"$SUBTREE_PUSH" -addr "${ADDR}:${SUBTREE_PORT}" \
  -interval "$SUBTREE_INTERVAL" -nodes "$SUBTREE_NODES" "${dur_args[@]}" &
pids+=($!)

"$BLOCK_PUSH" -addr "${ADDR}:${BLOCK_PORT}" \
  -interval "$BLOCK_INTERVAL" -subtrees "$BLOCK_SUBTREES" "${dur_args[@]}" &
pids+=($!)

# Propagate the first failure; with -duration all three end on their own.
rc=0
for p in "${pids[@]}"; do
  wait "$p" || rc=$?
done
exit "$rc"
