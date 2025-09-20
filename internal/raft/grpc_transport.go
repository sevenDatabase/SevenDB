package raft

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/sevenDatabase/SevenDB/internal/raft/pb"
)

// GRPCTransport implements Transport using a persistent bidirectional stream per peer.
// For MVP we open a stream to each peer (excluding self) and send messages best-effort.
// Reconnection logic is minimal; future work: exponential backoff, health checks, metrics.
type GRPCTransport struct {
	mu       sync.RWMutex
	selfID   uint64
	shardID  string
	peers    map[uint64]*peerConn
	resolver PeerResolver // resolves peerID -> address
}

type peerConn struct {
	id     uint64
	addr   string
	conn   *grpc.ClientConn
	stream pb.RaftTransport_MessageStreamClient
	cancel context.CancelFunc
}

// PeerResolver abstracts how we map raft IDs to network addresses.
// Initial implementation will be a static map built from config.
type PeerResolver interface {
	Resolve(id uint64) (string, bool)
	Self() uint64
}

// StaticPeerResolver implements PeerResolver over a fixed map.
type StaticPeerResolver struct {
	self  uint64
	addrs map[uint64]string
}

func (s *StaticPeerResolver) Resolve(id uint64) (string, bool) { v, ok := s.addrs[id]; return v, ok }
func (s *StaticPeerResolver) Self() uint64                  { return s.self }

func NewGRPCTransport(selfID uint64, shardID string, r PeerResolver) *GRPCTransport {
    return &GRPCTransport{selfID: selfID, shardID: shardID, peers: make(map[uint64]*peerConn), resolver: r}
}

// Start establishes outbound streams to all known peers (except self). Idempotent.
func (t *GRPCTransport) Start(ctx context.Context) {
	for id := range t.peerIDSet() {
		if id == t.selfID {
			continue
		}
		go t.ensurePeerStream(id)
	}
}

func (t *GRPCTransport) peerIDSet() map[uint64]struct{} {
	res := make(map[uint64]struct{})
	// Extract from resolver (static implementation only for now)
	if sr, ok := t.resolver.(*StaticPeerResolver); ok {
		for id := range sr.addrs {
			res[id] = struct{}{}
		}
	}
	return res
}

func (t *GRPCTransport) ensurePeerStream(id uint64) {
	addr, ok := t.resolver.Resolve(id)
	if !ok {
		return
	}
	for {
		// Fast path: already connected
		if t.getPeer(id) != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err != nil {
			slog.Warn("raft grpc dial failed", slog.Uint64("peer", id), slog.String("addr", addr), slog.Any("error", err))
			time.Sleep(2 * time.Second)
			continue
		}
		client := pb.NewRaftTransportClient(conn)
		streamCtx, streamCancel := context.WithCancel(context.Background())
		stream, err := client.MessageStream(streamCtx)
		if err != nil {
			slog.Warn("raft open stream failed", slog.Uint64("peer", id), slog.Any("error", err))
			streamCancel()
			_ = conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		pc := &peerConn{id: id, addr: addr, conn: conn, stream: stream, cancel: streamCancel}
		// Install
		if !t.setPeerIfAbsent(pc) {
			streamCancel()
			_ = conn.Close()
			return
		}
		// Reader goroutine to consume any responses (currently ignored)
		go func(p *peerConn) {
			for {
				m, err := p.stream.Recv()
				if err != nil {
					slog.Warn("raft stream recv closed", slog.Uint64("peer", p.id), slog.Any("error", err))
					t.removePeer(p.id)
					return
				}
				_ = m // future: handle responses or flow control acks
			}
		}(pc)
		return
	}
}

func (t *GRPCTransport) getPeer(id uint64) *peerConn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}
func (t *GRPCTransport) setPeerIfAbsent(pc *peerConn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.peers[pc.id]; exists {
		return false
	}
	t.peers[pc.id] = pc
	return true
}
func (t *GRPCTransport) removePeer(id uint64) {
	t.mu.Lock()
	pc := t.peers[id]
	delete(t.peers, id)
	t.mu.Unlock()
	if pc != nil {
		pc.cancel()
		_ = pc.conn.Close()
	}
}

// Send implements the Transport interface.
func (t *GRPCTransport) Send(ctx context.Context, msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 || m.To == t.selfID {
			continue
		}
		pc := t.getPeer(m.To)
		if pc == nil {
			go t.ensurePeerStream(m.To)
			continue
		}
		data, err := proto.Marshal(&m)
		if err != nil {
			slog.Warn("raft marshal failed", slog.Uint64("to", m.To), slog.Any("error", err))
			continue
		}
		env := &pb.RaftEnvelope{ShardId: t.shardID, RaftMessage: data}
		if err := pc.stream.Send(env); err != nil {
			slog.Warn("raft send failed", slog.Uint64("to", m.To), slog.Any("error", err))
			t.removePeer(m.To)
			go t.ensurePeerStream(m.To)
		}
	}
}

// Close all connections.
func (t *GRPCTransport) Close() {
	ids := []uint64{}
	{
		t.mu.RLock()
		for id := range t.peers {
			ids = append(ids, id)
		}
		t.mu.RUnlock()
	}
	for _, id := range ids {
		t.removePeer(id)
	}
}
