package ironhawk

import (
    "testing"
    "github.com/sevenDatabase/SevenDB/config"
)

// Restart-resume test placeholder: requires a persistent Raft engine to validate state across process restarts.
// With the default in-memory stub engine, commit indexes and outbox do not persist, so this test is skipped.
func TestEmissionContract_Restart_Reconnect_Resume(t *testing.T) {
    if config.Config.RaftEngine != "etcd" {
        t.Skip("skipping: requires persistent raft engine (set --raft-engine=etcd) to validate restart resume semantics")
    }
    // Outline (when etcd raft is wired in CI):
    // 1) Start server with emission-contract enabled and etcd raft.
    // 2) SUBSCRIBE (GET.WATCH), SET a value, verify emission.
    // 3) Stop server cleanly, then start it again with same data dirs.
    // 4) EMITRECONNECT with last commit index, produce another SET, verify no gaps (exact next emission arrives).
}
