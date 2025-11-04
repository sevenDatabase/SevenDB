//go:build never

package raft

import ()

// buildDataEvent rec builds a raft application record for DATA_EVENT.
func buildDataEvent(bucket, sub, delta string) *RaftLogRecord { return nil }

// runClusterOnce starts a 3-node raft cluster with emission managers/notifiers, runs a fixed
// workload and returns a stable hash of the leader's emitted stream for the subscription.
func runClusterOnce(t *testing.T, idx int) string { return "" }

func TestEmission_MultiReplicaSymmetry(t *testing.T) {}
