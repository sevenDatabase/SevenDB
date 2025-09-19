package bucket

import (
    "context"
    "log/slog"
)

// Bucket represents a single-threaded execution unit over a logical log.
// For the MVP it only replays entries and invokes an onApply callback.
type Bucket struct {
    ID          BucketID
    log         BucketLog
    lastApplied uint64

    // onApply is invoked for every applied entry after lastApplied is updated.
    // Used in tests & future wiring to update in-memory state, subscriptions, outbox, etc.
    onApply func(*WALEntry)
}

// NewBucket constructs a bucket bound to a BucketLog.
func NewBucket(id BucketID, l BucketLog) *Bucket {
    return &Bucket{ID: id, log: l}
}

// run executes a sequential apply loop for the bucket until ctx is cancelled.
// It re-reads from (lastApplied+1) each time the stream ends (e.g., reaching EOF)
// which is acceptable for the simple file log (new appends require a new Read).
func (b *Bucket) run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }
        ch, err := b.log.Read(ctx, b.ID, b.lastApplied+1)
        if err != nil {
            slog.Error("bucket run read error", slog.String("bucket", string(b.ID)), slog.Any("error", err))
            return
        }
        drained := false
        for e := range ch {
            b.lastApplied = e.CommitIndex
            if b.onApply != nil {
                b.onApply(e)
            }
        }
        if !drained { // channel closed (EOF). Sleep / backoff could be added; loop to pick new entries.
            // For MVP immediate loop; future: blocking watch or FS notify.
        }
    }
}
