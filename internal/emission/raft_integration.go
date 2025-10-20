package emission

import (
    "context"
    "fmt"
    "strconv"

    "github.com/sevenDatabase/SevenDB/internal/raft"
)

// RaftProposer proposes purge commands back into raft.
type RaftProposer struct {
    Node     *raft.ShardRaftNode
    BucketID string
}

func (p *RaftProposer) ProposePurge(ctx context.Context, sub string, upTo EmitSeq) error {
    args := []string{sub, upTo.Epoch.BucketUUID, strconv.FormatUint(upTo.Epoch.EpochCounter, 10), strconv.FormatUint(upTo.CommitIndex, 10)}
    rec, err := raft.BuildReplicationRecord(p.BucketID, "OUTBOX_PURGE", args)
    if err != nil { return err }
    _, _, err = p.Node.ProposeAndWait(ctx, rec)
    return err
}

// RegisterWithShard installs a replication handler that applies emission-related
// commands to the manager. This uses application commands with Cmd fields:
// SUBSCRIBE, OUTBOX_WRITE, OUTBOX_PURGE.
func RegisterWithShard(node *raft.ShardRaftNode, mgr *Manager) {
    node.SetReplicationHandler(func(pl *raft.ReplicationPayload) error {
        switch pl.Cmd {
        case "SUBSCRIBE":
            // For MVP, SUBSCRIBE is acknowledged externally; nothing to mutate here.
            return nil
        case "OUTBOX_WRITE":
            if len(pl.Args) < 4 { return fmt.Errorf("OUTBOX_WRITE args invalid") }
            sub := pl.Args[0]
            bucket := pl.Args[1]
            ec, _ := strconv.ParseUint(pl.Args[2], 10, 64)
            ci, _ := strconv.ParseUint(pl.Args[3], 10, 64)
            // delta (optional) as base64 or string in Args[4]
            var delta []byte
            if len(pl.Args) >= 5 {
                delta = []byte(pl.Args[4])
            }
            mgr.ApplyOutboxWrite(context.Background(), sub, EmitSeq{Epoch: EpochID{BucketUUID: bucket, EpochCounter: ec}, CommitIndex: ci}, delta)
            return nil
        case "OUTBOX_PURGE":
            if len(pl.Args) < 4 { return fmt.Errorf("OUTBOX_PURGE args invalid") }
            sub := pl.Args[0]
            bucket := pl.Args[1]
            ec, _ := strconv.ParseUint(pl.Args[2], 10, 64)
            ci, _ := strconv.ParseUint(pl.Args[3], 10, 64)
            mgr.ApplyOutboxPurge(context.Background(), sub, EmitSeq{Epoch: EpochID{BucketUUID: bucket, EpochCounter: ec}, CommitIndex: ci})
            return nil
        }
        return nil
    }, false)
}
