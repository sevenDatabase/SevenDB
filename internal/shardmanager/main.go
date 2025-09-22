// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package shardmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/bucket"
	"github.com/sevenDatabase/SevenDB/internal/raft"
	"github.com/sevenDatabase/SevenDB/internal/shard"
	"github.com/sevenDatabase/SevenDB/internal/shardthread"
	"github.com/sevenDatabase/SevenDB/internal/store"
)

type ShardManager struct {
	shards      []*shard.Shard
	sigChan     chan os.Signal // sigChan is the signal channel for the shard manager
	// raftNodes (optional) holds one ShardRaftNode per shard when raft is enabled.
	raftNodes   []*raft.ShardRaftNode
	raftSrv     *raft.RaftGRPCServer
	localRaftID uint64
}

var globalManager *ShardManager

// GetGlobal returns the process-wide shard manager instance (nil if server not started in this process).
func GetGlobal() *ShardManager { return globalManager }

// NewShardManager creates a new ShardManager instance with the given number of Shards and a parent context.
func NewShardManager(shardCount int, globalErrorChan chan error) *ShardManager {
	shards := make([]*shard.Shard, shardCount)
	maxKeysPerShard := config.DefaultKeysLimit / shardCount
	for i := 0; i < shardCount; i++ {
		shards[i] = &shard.Shard{
			ID:     i,
			Thread: shardthread.NewShardThread(i, globalErrorChan, store.NewPrimitiveEvictionStrategy(maxKeysPerShard)),
		}
	}
	sm := &ShardManager{shards: shards, sigChan: make(chan os.Signal, 1)}
	globalManager = sm
	if config.Config != nil && config.Config.RaftEnabled {
		var peerMap map[uint64]string
		var canonical []string
		var err error
		if len(config.Config.RaftNodes) > 0 {
			peerMap, canonical, err = raft.ParsePeerSpecs(config.Config.RaftNodes)
			if err != nil {
				slog.Error("raft peer parse failed", slog.Any("error", err))
			} else {
				if config.Config.RaftAdvertiseAddr == "" { config.Config.RaftAdvertiseAddr = config.Config.RaftListenAddr }
				localID, derr := raft.DetermineLocalID(config.Config.RaftNodeID, config.Config.RaftAdvertiseAddr, peerMap)
				if derr != nil {
					slog.Error("raft local id determination failed", slog.Any("error", derr))
				} else {
					sm.localRaftID = localID
				}
			}
		} else {
			// Single-node implicit cluster
			peerMap = map[uint64]string{1: config.Config.RaftAdvertiseAddr}
			canonical = []string{"1@" + config.Config.RaftAdvertiseAddr}
			sm.localRaftID = 1
		}
		sm.raftNodes = make([]*raft.ShardRaftNode, shardCount)
		for i := 0; i < shardCount; i++ {
			cfg := raft.RaftConfig{ShardID: string(rune('a' + i)), Engine: config.Config.RaftEngine, HeartbeatMillis: config.Config.RaftHeartbeatMillis, ElectionTimeoutMillis: config.Config.RaftElectionTimeoutMillis, NodeID: fmt.Sprintf("%d", sm.localRaftID), Peers: canonical, DataDir: config.Config.RaftPersistentDir}
			rn, err := raft.NewShardRaftNode(cfg)
			if err != nil {
				slog.Error("failed to start raft for shard", slog.Int("shard", i), slog.Any("error", err))
				continue
			}
			sm.raftNodes[i] = rn
		}
		slog.Info("raft enabled for shards", slog.Int("count", shardCount), slog.Uint64("local_id", sm.localRaftID))
	}
	return sm
}

// Run starts the ShardManager, manages its lifecycle, and listens for errors.
func (manager *ShardManager) Run(ctx context.Context) {
	signal.Notify(manager.sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	shardCtx, cancelShard := context.WithCancel(ctx)
	defer cancelShard()

	// If raft multi-node & etcd engine enabled, start gRPC server and attach transports.
	if manager.raftNodes != nil && config.Config.RaftEngine == "etcd" && manager.localRaftID != 0 && config.Config.RaftListenAddr != "" {
		resolverAddrs := make(map[uint64]string)
		if len(config.Config.RaftNodes) > 0 {
			pm, _, err := raft.ParsePeerSpecs(config.Config.RaftNodes)
			if err == nil { resolverAddrs = pm }
		}
		if len(resolverAddrs) == 0 { // single node fallback
			resolverAddrs[manager.localRaftID] = config.Config.RaftAdvertiseAddr
		}
		resolver := &raft.StaticPeerResolver{SelfID: manager.localRaftID, Addrs: resolverAddrs}
		// Register shards in server
		if len(resolverAddrs) > 1 { // only run server if multi-node
			manager.raftSrv = raft.NewRaftGRPCServer(manager.localRaftID)
			for _, rn := range manager.raftNodes { if rn != nil { manager.raftSrv.RegisterShard(rn.ShardID(), rn) } }
			go func() {
				if err := manager.raftSrv.Serve(config.Config.RaftListenAddr); err != nil {
					slog.Error("raft grpc server exited", slog.Any("error", err))
				}
			}()
		}
		// Attach transports
		for _, rn := range manager.raftNodes {
			if rn == nil { continue }
			gt := raft.NewGRPCTransport(manager.localRaftID, rn.ShardID(), resolver)
			rn.SetTransport(gt)
			go gt.Start(shardCtx)
		}
	}

	manager.start(shardCtx, &wg)

	// Periodic status writer (local development / external observation). Writes status.json in metadata dir.
	if manager.raftNodes != nil && config.Config != nil && config.Config.RaftEnabled {
		statusPath := filepath.Join(config.MetadataDir, "status.json")
		// secondary path: CWD (process working directory) for tooling/scripts expecting local file
		cwd, _ := os.Getwd()
		altPath := filepath.Join(cwd, "status.json")
		slog.Info("status writer active", slog.String("primary", statusPath), slog.String("alt", altPath), slog.Duration("interval", time.Second))
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-shardCtx.Done():
					return
				case <-ticker.C:
					// Collect snapshots
					snaps := manager.RaftStatusSnapshots()
					payload := map[string]interface{}{"ts_unix": time.Now().Unix(), "nodes": snaps}
					b, err := json.MarshalIndent(payload, "", "  ")
					if err != nil { slog.Debug("status marshal failed", slog.Any("error", err)); continue }
					// write primary
					tmp := statusPath + ".tmp"
					if err := os.WriteFile(tmp, b, 0o600); err != nil {
						slog.Debug("status write failed", slog.String("path", statusPath), slog.Any("error", err))
					} else {
						_ = os.Rename(tmp, statusPath)
					}
					// write alternate (best-effort, no atomic rename needed but keep similar pattern)
					tmpAlt := altPath + ".tmp"
					if err := os.WriteFile(tmpAlt, b, 0o600); err != nil {
						slog.Debug("status alt write failed", slog.String("path", altPath), slog.Any("error", err))
					} else { _ = os.Rename(tmpAlt, altPath) }
				}
			}
		}()
	}

	select {
	case <-ctx.Done():
		// Parent context was canceled, trigger shutdown
	case <-manager.sigChan:
		// OS signal received, trigger shutdown
	}

	wg.Wait() // Wait for all shard goroutines to exit.
}

// start initializes and starts the shard threads.
func (manager *ShardManager) start(ctx context.Context, wg *sync.WaitGroup) {
	for _, sh := range manager.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sh.Thread.Start(ctx)
		}()
	}
}

func (manager *ShardManager) GetShardForKey(key string) *shard.Shard {
	return manager.shards[xxhash.Sum64String(key)%uint64(manager.ShardCount())]
}

// GetShardCount returns the number of shards managed by this ShardManager.
func (manager *ShardManager) ShardCount() int8 {
	return int8(len(manager.shards))
}

func (manager *ShardManager) Shards() []*shard.Shard {
	return manager.shards
}

// BucketLogFor returns a bucket log implementation for the shard index. When raft is enabled
// it returns a RaftBucketLog; otherwise a file-backed log. Errors are logged and a nil may be returned.
func (manager *ShardManager) BucketLogFor(shardIdx int) bucket.BucketLog {
	if shardIdx < 0 || shardIdx >= len(manager.shards) {
		return nil
	}
	if manager.raftNodes != nil && manager.raftNodes[shardIdx] != nil {
		return bucket.NewRaftBucketLog(manager.raftNodes[shardIdx])
	}
	fbl, err := bucket.NewFileBucketLog()
	if err != nil {
		slog.Error("failed to create file bucket log", slog.Int("shard", shardIdx), slog.Any("error", err))
		return nil
	}
	return fbl
}

// RaftStatusSnapshots returns a slice of raft.StatusSnapshot for each shard raft node (if enabled).
func (manager *ShardManager) RaftStatusSnapshots() []interface{} { // using interface{} to avoid import cycle; CLI marshals raw
    if manager == nil || manager.raftNodes == nil {
        return nil
    }
    out := make([]interface{}, 0, len(manager.raftNodes))
    for _, rn := range manager.raftNodes {
        if rn == nil { continue }
        snap := rn.Status() // StatusSnapshot is a plain struct
        out = append(out, snap)
    }
    return out
}
