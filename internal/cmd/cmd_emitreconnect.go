package cmd

import (
    "strconv"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/internal/emission"
    "github.com/sevenDatabase/SevenDB/internal/shardmanager"
)

var cEMITRECONNECT = &CommandMeta{
    Name:        "EMITRECONNECT",
    HelpShort:   "Resume emission stream after reconnect (MVP)",
    Syntax:      "EMITRECONNECT key sub_id last_commit_index",
    Examples:    "EMITRECONNECT mykey client123:987654321 42",
    IsWatchable: false,
    Execute: func(c *Cmd, sm *shardmanager.ShardManager) (*CmdRes, error) {
        res := &CmdRes{Rs: &wire.Result{}}
        if len(c.C.Args) < 3 {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "usage: EMITRECONNECT key sub_id last_commit_index"
            return res, nil
        }
        key := c.C.Args[0]
        subID := c.C.Args[1]
        lastStr := c.C.Args[2]
        lastIdx, err := strconv.ParseUint(lastStr, 10, 64)
        if err != nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "invalid last_commit_index"
            return res, nil
        }
        rn := sm.GetRaftNodeForKey(key)
        if rn == nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "raft not enabled for shard"
            return res, nil
        }
        // MVP epoch: 0, bucket uuid is shard ID
        req := emission.ReconnectRequest{SubID: subID, LastProcessedEmitSeq: emission.EmitSeq{Epoch: emission.EpochID{BucketUUID: rn.ShardID(), EpochCounter: 0}, CommitIndex: lastIdx}}
        // Ask the Manager via the shard's notifier/manager; we don't expose manager directly, but notifier holds it internally.
        n := sm.NotifierForKey(key)
        if n == nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "emission not enabled for shard"
            return res, nil
        }
        // We need Manager to evaluate reconnect status. Expose via a tiny helper in emission or reach into shard manager; for MVP, duplicate logic through manager on shard's side.
        // Since Notifier exposes no getter, add a small helper here by using shard manager's emission manager accessor.
        // Workaround: the shard manager doesn't expose manager; instead we can rely on reconnect not mutating state and use ACK watermark stored in manager.
        // Provide a minimal pathway: add a helper on shard manager to run reconnect (implemented there) and set resume on notifier when OK.
        ack := sm.EmissionReconnectForKey(key, req)
        switch ack.Status {
        case emission.ReconnectOK:
            if n != nil {
                n.SetResumeFrom(subID, ack.NextCommitIndex)
            }
            res.Rs.Status = wire.Status_OK
            res.Rs.Message = "OK " + strconv.FormatUint(ack.NextCommitIndex, 10)
        case emission.ReconnectStaleSequence:
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "STALE_SEQUENCE"
        case emission.ReconnectInvalidSequence:
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "INVALID_SEQUENCE"
        default:
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "SUBSCRIPTION_NOT_FOUND"
        }
        return res, nil
    },
}

func init() {
    CommandRegistry.AddCommand(cEMITRECONNECT)
}
