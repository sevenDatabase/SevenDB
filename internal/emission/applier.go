package emission

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	raftimpl "github.com/sevenDatabase/SevenDB/internal/raft"
)

// RaftProposer is defined in raft_integration.go

// Applier consumes raft committed records and applies emission semantics.
type Applier struct {
	node  *raftimpl.ShardRaftNode
	mgr   *Manager
	epoch EpochID // MVP: fixed epoch per bucket (Counter=0)
}

func NewApplier(node *raftimpl.ShardRaftNode, mgr *Manager, bucketUUID string) *Applier {
	// Use current timestamp as epoch counter to ensure monotonicity across restarts
	// This fixes the issue where a restart resets the raft index but the client
	// still has a high watermark from the previous run.
	epoch := EpochID{BucketUUID: bucketUUID, EpochCounter: uint64(time.Now().UnixNano())}
	mgr.SetCurrentEpoch(epoch)
	return &Applier{node: node, mgr: mgr, epoch: epoch}
}

func (a *Applier) Start(ctx context.Context) {
	go func() {
		ch := a.node.Committed()
		for {
			select {
			case <-ctx.Done():
				return
			case cr, ok := <-ch:
				if !ok {
					return
				}
				if cr.Record == nil || cr.Record.Type != raftimpl.RaftRecordTypeAppCommand || len(cr.Record.Payload) == 0 {
					continue
				}
				var pl raftimpl.ReplicationPayload
				if err := json.Unmarshal(cr.Record.Payload, &pl); err != nil {
					slog.Warn("applier: decode payload", slog.Any("error", err))
					continue
				}
				a.applyCommand(ctx, cr, &pl)
			}
		}
	}()
}

func (a *Applier) applyCommand(ctx context.Context, cr *raftimpl.CommittedRecord, pl *raftimpl.ReplicationPayload) {
	switch pl.Cmd {
	case "SUBSCRIBE":
		// Args: sub_id
		if len(pl.Args) < 1 {
			return
		}
		sub := pl.Args[0]
		// start_emit_seq is (epoch, cr.CommitIndex); store no-op here; Notifier will send on subsequent DATA_EVENTs.
		slog.Debug("SUBSCRIBE committed", slog.String("sub_id", sub), slog.Uint64("start_commit", cr.CommitIndex))
		// No durable state yet; subscription table would be separate.
	case "DATA_EVENT":
		// Args: sub_id, delta (raw string)
		if len(pl.Args) < 2 {
			return
		}
		sub := pl.Args[0]
		delta := []byte(pl.Args[1])
		// seq := EmitSeq{Epoch: a.epoch, CommitIndex: cr.CommitIndex}
		// Only the leader should propose the OUTBOX_WRITE; followers will apply it via the handler
		if a.node.IsLeader() {
			// Use the commit index of the DATA_EVENT itself as the sequence number for the emission.
			// This ensures that the emission sequence is tied to the original event's position in the log.
			out := &raftimpl.ReplicationPayload{Version: 1, Cmd: "OUTBOX_WRITE", Args: []string{sub, a.epoch.BucketUUID, formatUint(a.epoch.EpochCounter), formatUint(cr.CommitIndex), string(delta)}}
			b, _ := json.Marshal(out)
			if _, _, err := a.node.ProposeAndWait(ctx, &raftimpl.RaftLogRecord{BucketID: a.epoch.BucketUUID, Type: raftimpl.RaftRecordTypeAppCommand, Payload: b}); err != nil {
				slog.Error("propose OUTBOX_WRITE failed", slog.Any("error", err))
			} else {
				slog.Info("emission-applier: proposed OUTBOX_WRITE", slog.String("sub_id", sub))
			}
		}
	case "OUTBOX_WRITE":
		// Args: sub_id, bucket_uuid, epoch_counter, commit_index, delta
		if len(pl.Args) < 5 {
			return
		}
		sub := pl.Args[0]
		seq := EmitSeq{Epoch: EpochID{BucketUUID: pl.Args[1], EpochCounter: parseUint(pl.Args[2])}, CommitIndex: parseUint(pl.Args[3])}
		delta := []byte(pl.Args[4])
		a.mgr.ApplyOutboxWrite(ctx, sub, seq, delta)
		slog.Info("emission-applier: applied OUTBOX_WRITE", slog.String("sub_id", sub), slog.String("seq", seq.String()))
	case "ACK":
		// Args: sub_id, bucket_uuid, epoch_counter, commit_index
		if len(pl.Args) < 4 {
			return
		}
		sub := pl.Args[0]
		seq := EmitSeq{Epoch: EpochID{BucketUUID: pl.Args[1], EpochCounter: parseUint(pl.Args[2])}, CommitIndex: parseUint(pl.Args[3])}
		if ok := a.mgr.ValidateAck(sub, seq); !ok {
			slog.Warn("ACK regression/duplicate in apply", slog.String("sub_id", sub), slog.String("emit_seq", seq.String()))
			return
		}
		// Only leader proposes purge; followers observe via handler
		if a.node.IsLeader() {
			out := &raftimpl.ReplicationPayload{Version: 1, Cmd: "OUTBOX_PURGE", Args: []string{sub, seq.Epoch.BucketUUID, formatUint(seq.Epoch.EpochCounter), formatUint(seq.CommitIndex)}}
			b, _ := json.Marshal(out)
			if _, _, err := a.node.ProposeAndWait(ctx, &raftimpl.RaftLogRecord{BucketID: seq.Epoch.BucketUUID, Type: raftimpl.RaftRecordTypeAppCommand, Payload: b}); err != nil {
				slog.Error("propose OUTBOX_PURGE failed", slog.Any("error", err))
			}
		}
	case "OUTBOX_PURGE":
		// Args: sub_id, bucket_uuid, epoch_counter, commit_index
		if len(pl.Args) < 4 {
			return
		}
		sub := pl.Args[0]
		seq := EmitSeq{Epoch: EpochID{BucketUUID: pl.Args[1], EpochCounter: parseUint(pl.Args[2])}, CommitIndex: parseUint(pl.Args[3])}
		a.mgr.ApplyOutboxPurge(ctx, sub, seq)
	default:
		// ignore other application commands
	}
}

// basic numeric helpers using decimal strings to keep payloads human-readable for tests
func formatUint(v uint64) string { return strconv.FormatUint(v, 10) }

func parseUint(s string) uint64 { n, _ := strconv.ParseUint(s, 10, 64); return n }
