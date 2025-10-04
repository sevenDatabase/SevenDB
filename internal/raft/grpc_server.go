package raft

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"

	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// RaftGRPCServer implements the server side of the RaftTransport gRPC service.
// It accepts RaftEnvelope streams, unmarshals raftpb.Messages, and forwards
// them into the appropriate shard's ShardRaftNode via Step().
type RaftGRPCServer struct {
	UnimplementedRaftTransportServer
	localID uint64

	mu     sync.RWMutex
	shards map[string]*ShardRaftNode // shardID -> node
}

// NewRaftGRPCServer creates a new transport server for the given local raft node ID.
func NewRaftGRPCServer(localID uint64) *RaftGRPCServer {
	return &RaftGRPCServer{localID: localID, shards: make(map[string]*ShardRaftNode)}
}

// RegisterShard associates a shardID with its raft node so incoming messages can be routed.
func (s *RaftGRPCServer) RegisterShard(shardID string, node *ShardRaftNode) {
	if node == nil || shardID == "" {
		return
	}
	s.mu.Lock()
	s.shards[shardID] = node
	s.mu.Unlock()
}

// getShard is a helper to safely retrieve a shard node.
func (s *RaftGRPCServer) getShard(shardID string) *ShardRaftNode {
	s.mu.RLock()
	n := s.shards[shardID]
	s.mu.RUnlock()
	return n
}

// MessageStream handles the bidirectional stream. For now we only consume
// envelopes from peers (no responses / acks are sent). Future enhancements
// may use the return stream for flow control or snapshots.
func (s *RaftGRPCServer) MessageStream(stream RaftTransport_MessageStreamServer) error {
	ctx := stream.Context()
	for {
		env, err := stream.Recv()
		if err != nil {
			// gRPC wraps EOF & cancellations; treat both as normal termination.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			// Transport-level EOF (io.EOF) also appears here but we just return nil.
			slog.Debug("raft stream closed", slog.Any("error", err))
			return nil
		}
		if env == nil {
			continue
		}
		if env.ShardId == "" {
			slog.Warn("raft envelope missing shard id")
			continue
		}
		// Unmarshal raft message
		var m raftpb.Message
		if err := gogoproto.Unmarshal(env.RaftMessage, &m); err != nil {
			slog.Warn("raft envelope unmarshal failed", slog.String("shard", env.ShardId), slog.Int("len", len(env.RaftMessage)), slog.Any("error", err))
			continue
		}
		// Only process messages addressed to us (To == localID). Some peers may broadcast.
		if m.To != 0 && m.To != s.localID {
			continue
		}
		n := s.getShard(env.ShardId)
		if n == nil {
			slog.Warn("raft envelope for unknown shard", slog.String("shard", env.ShardId))
			continue
		}
		if err := n.Step(ctx, m); err != nil {
			slog.Warn("raft step failed", slog.String("shard", env.ShardId), slog.Uint64("from", m.From), slog.Uint64("to", m.To), slog.Any("error", err))
			continue
		}
	}
}

// Serve starts a standalone gRPC server for Raft transport at the given address.
// This is optional; you may also register RaftGRPCServer with an existing gRPC Server.
func (s *RaftGRPCServer) Serve(addr string, opts ...grpc.ServerOption) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcSrv := grpc.NewServer(opts...)
	RegisterRaftTransportServer(grpcSrv, s)
	slog.Info("raft grpc server listening", slog.String("addr", addr))
	return grpcSrv.Serve(lis)
}
