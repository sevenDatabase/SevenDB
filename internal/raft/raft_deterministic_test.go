package raft

import (
    "context" // Unused import to be removed
    "strconv" // Unused import to be removed
    "testing" // Unused import to be removed
    "time"    // Unused import to be removed
)

// TestDeterministicManualMode validates Manual=true operation without background goroutines.
func TestDeterministicManualMode(t *testing.T) {
    cfg := RaftConfig{ShardID: "det-shard", NodeID: "1", Engine: "etcd", ForwardProposals: true, Manual: true, DisablePersistence: true, HeartbeatMillis: 50, ElectionTimeoutMillis: 300}
    n, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new node: %v", err) }
    defer n.Close()

    // Process any initial Ready (bootstrap conf change). Some raft versions may emit none until first tick.
    if !n.ManualProcessReady() { // non-blocking first
        // fall back to blocking to guarantee we process at least one cycle
        n.ManualTick()
        n.ManualProcessNextReady()
    }
    for n.ManualProcessReady() {} // drain any additional
    if !n.IsLeader() { n.ManualForceCampaign() }
    for i := 0; i < 50 && !n.IsLeader(); i++ { n.ManualTick(); for n.ManualProcessReady() {} }
    if !n.IsLeader() {
        t.Fatalf("node never became leader in manual mode (lastApplied=%d)", n.Status().LastAppliedIndex)
    }

    const proposals = 5
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    // Track relative committed count by draining committed channel.
    committed := 0
    bucket := "b"
    for i := 0; i < proposals; i++ {
        if _, err := n.Propose(ctx, &RaftLogRecord{BucketID: bucket, Type: 1, Payload: []byte(strconv.Itoa(i))}); err != nil { t.Fatalf("propose %d: %v", i, err) }
        // Drive until we observe this proposal committed.
        startTime := time.Now()
        for {
            // Process any Ready first.
            for n.ManualProcessReady() {}
            // Drain committed channel.
        drain:
            for {
                select {
                case rec := <-n.Committed():
                    if rec != nil && rec.Record != nil && rec.Record.BucketID == bucket { committed++ }
                default:
                    break drain
                }
            }
            if committed >= i+1 { break }
            if time.Since(startTime) > 500*time.Millisecond { t.Fatalf("timeout waiting commit %d (have=%d)", i+1, committed) }
            n.ManualTick()
        }
    }
    if committed != proposals { t.Fatalf("unexpected committed=%d want=%d", committed, proposals) }
}
