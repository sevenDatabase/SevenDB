package cmd

import (
    "context"
    "strconv"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/internal/raft"
    "github.com/sevenDatabase/SevenDB/internal/shardmanager"
)

var cEMITACK = &CommandMeta{
    Name:        "EMITACK",
    HelpShort:   "Acknowledge an emitted event (MVP)",
    Syntax:      "EMITACK key sub_id commit_index",
    Examples:    "EMITACK mykey client123:987654321 42",
    IsWatchable: false,
    Execute: func(c *Cmd, sm *shardmanager.ShardManager) (*CmdRes, error) {
        res := &CmdRes{Rs: &wire.Result{}}
        if len(c.C.Args) < 3 {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "usage: EMITACK key sub_id commit_index"
            return res, nil
        }
        key := c.C.Args[0]
        subID := c.C.Args[1]
        ciStr := c.C.Args[2]
        if _, err := strconv.ParseUint(ciStr, 10, 64); err != nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "invalid commit_index"
            return res, nil
        }
        rn := sm.GetRaftNodeForKey(key)
        if rn == nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = "raft not enabled for shard"
            return res, nil
        }
        // MVP epoch counter is 0; bucket uuid is shardID
        args := []string{subID, rn.ShardID(), "0", ciStr}
        rec, err := raft.BuildReplicationRecord(rn.ShardID(), "ACK", args)
        if err != nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = err.Error()
            return res, nil
        }
        if _, _, err := rn.ProposeAndWait(context.Background(), rec); err != nil {
            res.Rs.Status = wire.Status_ERR
            res.Rs.Message = err.Error()
            return res, nil
        }
        res.Rs.Status = wire.Status_OK
        res.Rs.Message = "ACKED"
        return res, nil
    },
}

func init() {
    CommandRegistry.AddCommand(cEMITACK)
}
