package bucket

// Raft-backed BucketLog implementation (MVP stub).
// For initial integration we leverage the ShardRaftNode stub which currently
// behaves like a single-node immediate apply raft. This lets us wire the
// abstraction without blocking on full replication wiring. Once the raft
// library is integrated the semantics of CommitIndex assignment remain the
// same (monotonic per bucket) but durability and ordering will be driven by
// raft log indices.

import (
	"context"
	"sync"
	"time"

	raftimpl "github.com/sevenDatabase/SevenDB/internal/raft"
)

// RaftBucketLog converts raft committed records into WALEntry instances.
type RaftBucketLog struct {
	shard *raftimpl.ShardRaftNode
	// Legacy tracking removed; commit indices now come from raft apply pipeline.
	mu sync.Mutex
}

func NewRaftBucketLog(node *raftimpl.ShardRaftNode) *RaftBucketLog { return &RaftBucketLog{shard: node} }

// Append proposes a new logical entry to the shard raft group. It returns the
// per-bucket commit index (synthetic in stub) and uses raft log index as FileOffset.
func (r *RaftBucketLog) Append(ctx context.Context, entry *WALEntry) (uint64, uint64, error) {
	if entry == nil { return 0, 0, ErrNilEntry }
	rec := &raftimpl.RaftLogRecord{BucketID: string(entry.BucketID), Type: int(entry.Type), Payload: entry.Payload}
	commitIdx, raftIdx, err := r.shard.ProposeAndWait(ctx, rec)
	if err != nil { return 0, 0, err }
	entry.CommitIndex = commitIdx
	entry.FileOffset = raftIdx
	entry.Timestamp = time.Now().UnixNano()
	entry.Epoch = EpochID{Bucket: entry.BucketID, Counter: 0}
	return commitIdx, raftIdx, nil
}

// Read streams committed entries for a given bucket starting at fromCommitIndex.
// Because the underlying raft stub only offers a global committed stream, this
// implementation multiplexes on the fly. A production implementation would
// maintain per-bucket queues to avoid scanning.
func (r *RaftBucketLog) Read(ctx context.Context, bucket BucketID, fromCommitIndex uint64) (<-chan *WALEntry, error) {
	out := make(chan *WALEntry, 64)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case cr, ok := <-r.shard.Committed():
				if !ok { return }
				if cr.Record.BucketID != string(bucket) { continue }
				if cr.CommitIndex < fromCommitIndex { continue }
				out <- &WALEntry{BucketID: bucket, CommitIndex: cr.CommitIndex, FileOffset: cr.RaftIndex, Timestamp: time.Now().UnixNano(), Epoch: EpochID{Bucket: bucket}, Type: WALRecordType(cr.Record.Type), Payload: cr.Record.Payload}
			}
		}
	}()
	return out, nil
}

func (r *RaftBucketLog) Snapshot(ctx context.Context, bucket BucketID) (string, error) { return "raft-snapshot-stub", nil }
func (r *RaftBucketLog) Compact(ctx context.Context, bucket BucketID, beforeCommitIndex uint64) error { return nil }
func (r *RaftBucketLog) Close() error { return nil }

// ErrNilEntry is returned for a nil append attempt.
var ErrNilEntry = ErrBucket("nil wal entry")

type ErrBucket string
func (e ErrBucket) Error() string { return string(e) }
