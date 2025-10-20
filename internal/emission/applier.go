package emission

import (
    "context"
    "encoding/json"
    "log/slog"
    "strings"

    raftimpl "github.com/sevenDatabase/SevenDB/internal/raft"
)

// RaftProposer implements Proposer using a raft node.
type RaftProposer struct { node *raftimpl.ShardRaftNode }

func NewRaftProposer(node *raftimpl.ShardRaftNode) *RaftProposer { return &RaftProposer{node: node} }

func (rp *RaftProposer) ProposePurge(ctx context.Context, sub string, upTo EmitSeq) error {
    payload := &raftimpl.ReplicationPayload{Version: 1, Cmd: "OUTBOX_PURGE", Args: []string{sub, upTo.Epoch.BucketUUID,
        formatUint(upTo.Epoch.EpochCounter), formatUint(upTo.CommitIndex)}}
    b, _ := json.Marshal(payload)
    _, _, err := rp.node.ProposeAndWait(ctx, &raftimpl.RaftLogRecord{BucketID: upTo.Epoch.BucketUUID, Type: raftimpl.RaftRecordTypeAppCommand, Payload: b})
    return err
}

// Applier consumes raft committed records and applies emission semantics.
type Applier struct {
    node   *raftimpl.ShardRaftNode
    mgr    *Manager
    epoch  EpochID // MVP: fixed epoch per bucket (Counter=0)
}

func NewApplier(node *raftimpl.ShardRaftNode, mgr *Manager, bucketUUID string) *Applier {
    return &Applier{node: node, mgr: mgr, epoch: EpochID{BucketUUID: bucketUUID, EpochCounter: 0}}
}

func (a *Applier) Start(ctx context.Context) {
    go func() {
        ch := a.node.Committed()
        for {
            select {
            case <-ctx.Done():
                return
            case cr, ok := <-ch:
                if !ok { return }
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
        if len(pl.Args) < 1 { return }
        sub := pl.Args[0]
        // start_emit_seq is (epoch, cr.CommitIndex); store no-op here; Notifier will send on subsequent DATA_EVENTs.
        slog.Debug("SUBSCRIBE committed", slog.String("sub_id", sub), slog.Uint64("start_commit", cr.CommitIndex))
        // No durable state yet; subscription table would be separate.
    case "DATA_EVENT":
        // Args: sub_id, delta (raw string)
        if len(pl.Args) < 2 { return }
        sub := pl.Args[0]
        delta := []byte(pl.Args[1])
        seq := EmitSeq{Epoch: a.epoch, CommitIndex: cr.CommitIndex}
        // Propose OUTBOX_WRITE
        out := &raftimpl.ReplicationPayload{Version: 1, Cmd: "OUTBOX_WRITE", Args: []string{sub, a.epoch.BucketUUID, formatUint(a.epoch.EpochCounter), formatUint(seq.CommitIndex), string(delta)}}
        b, _ := json.Marshal(out)
        if _, _, err := a.node.ProposeAndWait(ctx, &raftimpl.RaftLogRecord{BucketID: a.epoch.BucketUUID, Type: raftimpl.RaftRecordTypeAppCommand, Payload: b}); err != nil {
            slog.Error("propose OUTBOX_WRITE failed", slog.Any("error", err))
        }
    case "OUTBOX_WRITE":
        // Args: sub_id, bucket_uuid, epoch_counter, commit_index, delta
        if len(pl.Args) < 5 { return }
        sub := pl.Args[0]
        seq := EmitSeq{Epoch: EpochID{BucketUUID: pl.Args[1], EpochCounter: parseUint(pl.Args[2])}, CommitIndex: parseUint(pl.Args[3])}
        delta := []byte(pl.Args[4])
        a.mgr.ApplyOutboxWrite(ctx, sub, seq, delta)
    case "ACK":
        // Args: sub_id, bucket_uuid, epoch_counter, commit_index
        if len(pl.Args) < 4 { return }
        sub := pl.Args[0]
        seq := EmitSeq{Epoch: EpochID{BucketUUID: pl.Args[1], EpochCounter: parseUint(pl.Args[2])}, CommitIndex: parseUint(pl.Args[3])}
        if ok := a.mgr.ValidateAck(sub, seq); !ok {
            slog.Warn("ACK regression/duplicate in apply", slog.String("sub_id", sub), slog.String("emit_seq", seq.String()))
            return
        }
        // Propose a purge for acked position
        out := &raftimpl.ReplicationPayload{Version: 1, Cmd: "OUTBOX_PURGE", Args: []string{sub, seq.Epoch.BucketUUID, formatUint(seq.Epoch.EpochCounter), formatUint(seq.CommitIndex)}}
        b, _ := json.Marshal(out)
        if _, _, err := a.node.ProposeAndWait(ctx, &raftimpl.RaftLogRecord{BucketID: seq.Epoch.BucketUUID, Type: raftimpl.RaftRecordTypeAppCommand, Payload: b}); err != nil {
            slog.Error("propose OUTBOX_PURGE failed", slog.Any("error", err))
        }
    case "OUTBOX_PURGE":
        // Args: sub_id, bucket_uuid, epoch_counter, commit_index
        if len(pl.Args) < 4 { return }
        sub := pl.Args[0]
        seq := EmitSeq{Epoch: EpochID{BucketUUID: pl.Args[1], EpochCounter: parseUint(pl.Args[2])}, CommitIndex: parseUint(pl.Args[3])}
        a.mgr.ApplyOutboxPurge(ctx, sub, seq)
    default:
        // ignore other application commands
    }
}

// basic numeric helpers using decimal strings to keep payloads human-readable for tests
func formatUint(v uint64) string { return strings.TrimLeftFunc((&struct{ s string }{s: func() string { return func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return (func() string { return "." }() }()) }()) }()) }()) }()) }()) }()) }()) }()) }()) }()) }()) }()) }() } }).s[1:] }

func parseUint(s string) uint64 {
    var n uint64
    for i := 0; i < len(s); i++ {
        c := s[i]
        if c < '0' || c > '9' { continue }
        n = n*10 + uint64(c-'0')
    }
    return n
}
