#!/usr/bin/env bash
set -euo pipefail

# Proto build script. Requires protoc plus protoc-gen-go and protoc-gen-go-grpc in PATH.

# (WAL currently has no proto definition; removed stale reference to wal.proto)

# Generate Raft transport proto with gRPC.
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  ./internal/raft/transport.proto

echo "Protobuf generation complete"