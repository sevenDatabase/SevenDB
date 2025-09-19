package bucket

import "context"

// BucketID is a logical identifier for a bucket. For the MVP we use string.
// (Can be switched to UUID in the future without changing on-disk layout because
// on-disk we just persist the string form.)
type BucketID string

// EpochID represents a (BucketID, Counter) pair. Epoch counter increments when
// a bucket is recreated/migrated. For the MVP only Counter==0 is used.
type EpochID struct {
    Bucket   BucketID `json:"bucket"`
    Counter  uint64   `json:"counter"`
}

// WALRecordType identifies the kind of logical entry inside a bucket log.
// We only use RecDataUpdate for now; the rest are placeholders for future
// subscription/outbox functionality.
type WALRecordType int

const (
    RecDataUpdate WALRecordType = iota
    RecSubscribe
    RecUnsubscribe
    RecOutboxWrite
    RecOutboxAck
    RecEpochCreate
    RecNotifierLease
)

// WALEntry is the canonical bucket-scoped log entry persisted to disk.
// GlobalOffset is assigned by the underlying FileBucketLog (byte offset in the
// bucket file). CommitIndex is monotonically increasing per bucket.
// Payload is an opaque byte slice (caller is free to use JSON / protobuf etc.).
type WALEntry struct {
    GlobalOffset uint64        `json:"global_offset"`
    Timestamp    int64         `json:"ts"`
    BucketID     BucketID      `json:"bucket_id"`
    Epoch        EpochID       `json:"epoch"`
    CommitIndex  uint64        `json:"commit_index"`
    Type         WALRecordType `json:"type"`
    Payload      []byte        `json:"payload"`
}

// BucketLog abstracts a per-bucket logical log. Future implementations (e.g.
// Raft) should satisfy this same interface.
type BucketLog interface {
    Append(ctx context.Context, entry *WALEntry) (commitIndex uint64, globalOffset uint64, err error)
    Read(ctx context.Context, bucket BucketID, fromCommitIndex uint64) (<-chan *WALEntry, error)
    Snapshot(ctx context.Context, bucket BucketID) (snapshotID string, err error)
    Compact(ctx context.Context, bucket BucketID, beforeCommitIndex uint64) error
    Close() error
}
