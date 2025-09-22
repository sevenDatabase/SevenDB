#!/usr/bin/env bash
set -euo pipefail

# run-local-raft-cluster.sh
# Spins up a 3-node SevenDB raft cluster on localhost using distinct ports.
# Each node runs in foreground in its own terminal if you manually split, or
# you can run this script which backgrounds them and tails a combined log.
#
# Requirements:
#  - ./sevendb binary built (run `make build` first)
#  - jq (optional) for pretty status
#
# Ports:
#  Client (redis-like) port base: 7379 -> 7379, 7380, 7381
#  Raft gRPC port base: 7091 -> 7091, 7092, 7093
#
# Data dirs placed under .local-cluster/

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/sevendb"
if [ ! -x "$BIN" ]; then
  echo "[ERROR] Binary $BIN not found or not executable. Run 'make build' first." >&2
  exit 1
fi

BASE_DIR="$REPO_ROOT/.local-cluster"
mkdir -p "$BASE_DIR/logs"

SNAP_THRESHOLD=${SNAP_THRESHOLD:-200}
LOG_LEVEL=${LOG_LEVEL:-info}
WATCH_INTERVAL=${WATCH_INTERVAL:-1}
COLOR=${COLOR:-1}
NO_WATCH=${NO_WATCH:-0}
NO_PREFIX_LOGS=${NO_PREFIX_LOGS:-0}

# Clean previous run if CLEAN=1
if [ "${CLEAN:-0}" = "1" ]; then
  echo "[INFO] Cleaning previous cluster data..."
  rm -rf "$BASE_DIR"/*
  mkdir -p "$BASE_DIR/logs"
fi

# Node definitions
# id  client_port raft_port
NODES=( \
  "1 7379 7091" \
  "2 7380 7092" \
  "3 7381 7093" \
)

PEERS=("1@127.0.0.1:7091" "2@127.0.0.1:7092" "3@127.0.0.1:7093")

join_peers_flags() {
  local out=""
  for p in "${PEERS[@]}"; do
    out+=" --raft-nodes=${p}"
  done
  echo "$out"
}

start_node() {
  local id=$1
  local cport=$2
  local rport=$3
  local datadir="$BASE_DIR/node${id}"
  mkdir -p "$datadir" "$BASE_DIR/logs"
  local log="$BASE_DIR/logs/node${id}.log"
  local peers_flags
  peers_flags=$(join_peers_flags)
  echo "[INFO] Starting node ${id} (client ${cport}, raft ${rport})"
  # Launch process and capture PID explicitly; run from its data dir for isolated metadata/config.
  (
    cd "$datadir" || exit 1
    "$BIN" \
      --port="${cport}" \
      --raft-enabled=true \
      --raft-engine=etcd \
      --raft-node-id="${id}" \
      --raft-listen-addr=":${rport}" \
      --raft-advertise-addr="127.0.0.1:${rport}" \
      --raft-snapshot-threshold-entries="${SNAP_THRESHOLD}" \
      --log-level="${LOG_LEVEL}" \
      ${peers_flags} \
      >"${log}" 2>&1 &
    echo $! > "$datadir/pid"
  )
}

stop_cluster() {
  echo "[INFO] Stopping cluster..."
  for n in "${NODES[@]}"; do
    read -r id cport rport <<<"$n"
    pidfile="$BASE_DIR/node${id}/pid"
    if [ -f "$pidfile" ]; then
      pid=$(cat "$pidfile")
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" || true
      fi
    fi
  done
}

status_sample() {
  echo "[INFO] Sampling raft status from node1 (if running)"
  local nodeDir="$BASE_DIR/node1"
  if [ ! -d "$nodeDir" ]; then
    echo "[WARN] node1 directory missing"
    return
  fi
  if command -v jq >/dev/null 2>&1; then
    ( cd "$nodeDir" && "$BIN" raft-status | jq ) || echo "[WARN] Could not fetch status"
  else
    ( cd "$nodeDir" && "$BIN" raft-status ) || echo "[WARN] Could not fetch status"
  fi
}

trap stop_cluster EXIT

# Start nodes
for entry in "${NODES[@]}"; do
  read -r id cport rport <<<"$entry"
  start_node "$id" "$cport" "$rport"
  # tiny stagger to reduce simultaneous elections
  sleep 0.4
done

echo "[INFO] All nodes started. Logs dir: $BASE_DIR/logs"
sleep 1.2
status_sample

echo "[INFO] Example write load:"
echo "  for i in $(seq 1 100); do redis-cli -p 7379 SET k\$i v\$i >/dev/null; done"

# --- Real-time log streaming with prefixes ---
prefix_color() {
  local node=$1
  if [ "$COLOR" != "1" ]; then echo "[node${node}]"; return; fi
  case $node in
    1) echo $'\033[1;34m[node1]\033[0m';;
    2) echo $'\033[1;32m[node2]\033[0m';;
    3) echo $'\033[1;35m[node3]\033[0m';;
    *) echo "[node${node}]";;
  esac
}

stream_logs() {
  if [ "$NO_PREFIX_LOGS" = "1" ]; then
    # Fallback to simple tail of all logs
    tail -F "$BASE_DIR"/logs/node*.log &
    LOG_TAIL_PID=$!
    return
  fi
  for n in 1 2 3; do
    log="$BASE_DIR/logs/node${n}.log"
    ( tail -F "$log" | while IFS= read -r line; do printf "%s %s\n" "$(prefix_color $n)" "$line"; done ) &
  done
}

# --- Leader watch loop ---
leader_watch() {
  echo "[INFO] Leader watch started (interval ${WATCH_INTERVAL}s). Ctrl+C to exit."
  local lastApplied1=0 lastSnap1=0
  local retries=0
  local fallbackTried=0
  while true; do
    local statusFile="$BASE_DIR/node1/status.json"
    if [ ! -f "$statusFile" ]; then
      # Try alternate (metadata default) path
      local altFile="/etc/dicedb/status.json"
      if [ -f "$altFile" ]; then
        statusFile="$altFile"
      else
        retries=$((retries+1))
        if [ $retries -eq 5 ]; then
          echo "[WATCH] still waiting (5 attempts). Checked: $statusFile and $altFile"
        elif [ $retries -eq 15 ]; then
          echo "[WATCH] 15 attempts without status file. Possible causes: (1) binary not rebuilt, (2) raft not enabled, (3) write permissions denied." 
        elif [ $retries -eq 30 ]; then
          echo "[WATCH] 30 attempts. Running one-time 'raft-status' fallback for visibility." 
          if [ $fallbackTried -eq 0 ]; then
            fallbackTried=1
            ( cd "$BASE_DIR/node1" && "$BIN" raft-status 2>&1 | sed 's/^/[RAFT-STATUS-FALLBACK] /' ) || true
          fi
        fi
        printf "[WATCH] waiting-for-status-file (looking at %s)\n" "$statusFile"
        sleep "$WATCH_INTERVAL"; continue
      fi
    fi
    local json
    if ! json=$(cat "$statusFile" 2>/dev/null); then
      echo "[WATCH] read-error"; sleep "$WATCH_INTERVAL"; continue
    fi
    # Structure: { "ts_unix": <int>, "nodes": [ { shard_id,... } ] }
    # Extract first node object fields.
    local nodeJson=$(echo "$json" | sed -n '/"nodes"/,$p' )
    # Grep patterns from first occurrence only.
    local leader=$(echo "$nodeJson" | grep -o '"leader_id"[^"]*"[^"]*"' | head -n1 | sed 's/.*"leader_id":"\([^"]*\)".*/\1/')
    local isLeader=$(echo "$nodeJson" | grep -o '"is_leader"[^"]*' | head -n1 | grep -o 'true\|false')
    local applied=$(echo "$nodeJson" | grep -o '"last_applied_index"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
    local snap=$(echo "$nodeJson" | grep -o '"last_snapshot_index"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
    local peersConn=$(echo "$nodeJson" | grep -o '"transport_peers_connected"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
    local peersTot=$(echo "$nodeJson" | grep -o '"transport_peers_total"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
    local appliedDelta=$(( applied - lastApplied1 ))
    local snapDelta=$(( snap - lastSnap1 ))
    printf "[WATCH] leader=%s node1_isLeader=%s applied=%s(Δ%+d) snapshot=%s(Δ%+d) peers=%s/%s\n" \
      "$leader" "$isLeader" "$applied" "$appliedDelta" "$snap" "$snapDelta" "$peersConn" "$peersTot"
    lastApplied1=$applied; lastSnap1=$snap
    sleep "$WATCH_INTERVAL"
  done
}

stream_logs

if [ "$NO_WATCH" != "1" ]; then
  leader_watch &
  WATCH_PID=$!
fi

# Wait on background tails (and watch if running)
wait
