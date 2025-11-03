package raft

import (
    "context"
    "testing"
)

func TestReplicationHandlerInvoked(t *testing.T) {
    n, err := NewShardRaftNode(RaftConfig{ShardID: "h1", Engine: "stub"})
    if err != nil { t.Fatalf("new node: %v", err) }
    defer n.Close()

    called := false
    var gotCmd string
    n.SetReplicationHandler(func(pl *ReplicationPayload) error {
        called = true
        gotCmd = pl.Cmd
        return nil
    }, false)

    rec, _ := BuildReplicationRecord("h1", "PING", []string{"arg1"})
    if _, _, err := n.ProposeAndWait(context.Background(), rec); err != nil {
        t.Fatalf("propose: %v", err)
    }
    if !called || gotCmd != "PING" {
        t.Fatalf("handler not invoked or wrong cmd: called=%v cmd=%s", called, gotCmd)
    }
}

func TestReplicationHandlerStrictPanicsOnError(t *testing.T) {
    n, err := NewShardRaftNode(RaftConfig{ShardID: "h2", Engine: "stub"})
    if err != nil { t.Fatalf("new node: %v", err) }
    defer n.Close()

    n.SetReplicationHandler(func(pl *ReplicationPayload) error {
        return assertError("boom")
    }, true)

    defer func() {
        if r := recover(); r == nil {
            t.Fatalf("expected panic in strict mode")
        }
    }()
    rec, _ := BuildReplicationRecord("h2", "X", nil)
    _, _, _ = n.ProposeAndWait(context.Background(), rec)
}

// assertError implements error for test clarity.
type assertError string

func (e assertError) Error() string { return string(e) }
