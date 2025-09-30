#!/usr/bin/env bash
set -euo pipefail

# Proto build script. Requires protoc plus protoc-gen-go and protoc-gen-go-grpc in PATH.

# (WAL currently has no proto definition; removed stale reference to wal.proto)

PROTO_FILES=(
  ./internal/raft/transport.proto
  ./internal/raftwal/raftwal.proto
)

for pf in "${PROTO_FILES[@]}"; do
  protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    "$pf"
done

echo "Protobuf generation complete"