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
      --status-file-path="${datadir}/status.json" \
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

# Sample one-shot raft status via CLI (same-process only) – helpful initial sanity check.
status_sample() {
  echo "[INFO] Sampling raft status from node1 (if running)"
  local nodeDir="$BASE_DIR/node1"
  if [ ! -d "$nodeDir" ]; then
    echo "[WARN] node1 directory missing"; return
  fi
  if command -v jq >/dev/null 2>&1; then
    ( cd "$nodeDir" && "$BIN" raft-status | jq ) || echo "[WARN] Could not fetch status"
  else
    ( cd "$nodeDir" && "$BIN" raft-status ) || echo "[WARN] Could not fetch status"
  fi
}
leader_watch() {
  echo "[INFO] Leader watch started (interval ${WATCH_INTERVAL}s). Ctrl+C to exit."
  local statusFile="$BASE_DIR/node1/status.json"
  local lastApplied=0 lastSnap=0
  local attempts=0
  # Preflight diagnostics (once)
  if [ ! -d "$BASE_DIR/node1" ]; then
    echo "[DIAG] node1 directory missing at start: $BASE_DIR/node1"
  else
    echo "[DIAG] node1 dir present"
  fi
  if [ -f "$BASE_DIR/node1/pid" ]; then
    pid=$(cat "$BASE_DIR/node1/pid" 2>/dev/null || echo "")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "[DIAG] node1 process running (pid $pid)"
    else
      echo "[DIAG] node1 pid file exists but process not running"
    fi
  else
    echo "[DIAG] node1 pid file missing"
  fi
  while true; do
    if [ ! -f "$statusFile" ]; then
      attempts=$((attempts+1))
      if [ $attempts -eq 1 ]; then
        echo "[DIAG] Listing node1 directory contents:"; ls -al "$BASE_DIR/node1" || true
      fi
      if [ $attempts -eq 5 ]; then
        echo "[DIAG] tail last 30 lines of node1 log:"; tail -n 30 "$BASE_DIR/logs/node1.log" | sed 's/^/[node1-log] /'
      fi
      if [ $attempts -eq 10 ]; then
        echo "[DIAG] Checking for any status.json under base dir:"; find "$BASE_DIR" -maxdepth 4 -name status.json -printf '%p\n' 2>/dev/null || true
      fi
      if [ $attempts -eq 15 ]; then
        echo "[DIAG] Reprinting node1 command line (from /proc if available):"
        if [ -f "$BASE_DIR/node1/pid" ]; then pid=$(cat "$BASE_DIR/node1/pid" 2>/dev/null || echo ""); fi
        if [ -n "${pid:-}" ] && [ -r "/proc/$pid/cmdline" ]; then tr '\0' ' ' < "/proc/$pid/cmdline" | sed 's/^/[proc-cmd] /'; else echo "[proc-cmd] unavailable"; fi
      fi
      if [ $attempts -eq 20 ]; then
        echo "[DIAG] Giving up after 20 attempts; status file not found at $statusFile"
        echo "[DIAG] Possible causes: early panic, wrong working directory, binary mismatch, raft not enabled, or status writer not triggered."
      fi
      printf "[WATCH] waiting-for-status-file (attempt %d)\n" "$attempts"
      sleep "$WATCH_INTERVAL"; continue
    fi
    # Use jq if available for robust extraction; otherwise grep/sed fallback.
    if command -v jq >/dev/null 2>&1; then
      leader=$(jq -r '.nodes[0].leader_id // empty' "$statusFile" 2>/dev/null || true)
      isLeader=$(jq -r '.nodes[0].is_leader // empty' "$statusFile" 2>/dev/null || true)
      applied=$(jq -r '.nodes[0].last_applied_index // 0' "$statusFile" 2>/dev/null || echo 0)
      snap=$(jq -r '.nodes[0].last_snapshot_index // 0' "$statusFile" 2>/dev/null || echo 0)
      peersConn=$(jq -r '.nodes[0].transport_peers_connected // 0' "$statusFile" 2>/dev/null || echo 0)
      peersTot=$(jq -r '.nodes[0].transport_peers_total // 0' "$statusFile" 2>/dev/null || echo 0)
    else
      json=$(cat "$statusFile" 2>/dev/null || echo '{}')
      leader=$(echo "$json" | grep -o '"leader_id"[^"]*"[^"]*"' | head -n1 | sed 's/.*"leader_id":"\([^"]*\)".*/\1/')
      isLeader=$(echo "$json" | grep -o '"is_leader"[^"]*' | head -n1 | grep -o 'true\|false')
      applied=$(echo "$json" | grep -o '"last_applied_index"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
      snap=$(echo "$json" | grep -o '"last_snapshot_index"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
      peersConn=$(echo "$json" | grep -o '"transport_peers_connected"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
      peersTot=$(echo "$json" | grep -o '"transport_peers_total"[^"]*[0-9]*' | head -n1 | grep -o '[0-9]*$')
    fi
    applied=${applied:-0}; snap=${snap:-0}; peersConn=${peersConn:-0}; peersTot=${peersTot:-0}
    local aDelta=$(( applied - lastApplied ))
    local sDelta=$(( snap - lastSnap ))
    printf "[WATCH] leader=%s node1_isLeader=%s applied=%s(Δ%+d) snapshot=%s(Δ%+d) peers=%s/%s\n" \
      "${leader:-?}" "${isLeader:-?}" "$applied" "$aDelta" "$snap" "$sDelta" "$peersConn" "$peersTot"
    lastApplied=$applied; lastSnap=$snap
    sleep "$WATCH_INTERVAL"
  done
}

trap stop_cluster EXIT

# Start nodes
for entry in "${NODES[@]}"; do
  read -r id cport rport <<<"$entry"
  start_node "$id" "$cport" "$rport"
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
    tail -F "$BASE_DIR"/logs/node*.log &
    LOG_TAIL_PID=$!
    return
  fi
  for n in 1 2 3; do
    log="$BASE_DIR/logs/node${n}.log"
    ( tail -F "$log" | while IFS= read -r line; do printf "%s %s\n" "$(prefix_color $n)" "$line"; done ) &
  done
}

stream_logs

if [ "$NO_WATCH" != "1" ]; then
  leader_watch &
  WATCH_PID=$!
fi

wait
