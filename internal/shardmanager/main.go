// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package shardmanager

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/cespare/xxhash/v2"
	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/bucket"
	"github.com/sevenDatabase/SevenDB/internal/raft"
	"github.com/sevenDatabase/SevenDB/internal/shard"
	"github.com/sevenDatabase/SevenDB/internal/shardthread"
	"github.com/sevenDatabase/SevenDB/internal/store"
)

type ShardManager struct {
	shards  []*shard.Shard
	sigChan chan os.Signal // sigChan is the signal channel for the shard manager
	// raftNodes (optional) holds one ShardRaftNode per shard when raft is enabled.
	raftNodes []*raft.ShardRaftNode
}

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
	if config.Config != nil && config.Config.RaftEnabled {
		sm.raftNodes = make([]*raft.ShardRaftNode, shardCount)
		for i := 0; i < shardCount; i++ {
			rn, err := raft.NewShardRaftNode(raft.RaftConfig{ShardID:  string(rune('a'+i))})
			if err != nil {
				slog.Error("failed to start raft for shard", slog.Int("shard", i), slog.Any("error", err))
				continue
			}
			sm.raftNodes[i] = rn
		}
		slog.Info("raft enabled for shards", slog.Int("count", shardCount))
	}
	return sm
}

// Run starts the ShardManager, manages its lifecycle, and listens for errors.
func (manager *ShardManager) Run(ctx context.Context) {
	signal.Notify(manager.sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	shardCtx, cancelShard := context.WithCancel(ctx)
	defer cancelShard()

	manager.start(shardCtx, &wg)

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
	if shardIdx < 0 || shardIdx >= len(manager.shards) { return nil }
	if manager.raftNodes != nil && manager.raftNodes[shardIdx] != nil {
		return bucket.NewRaftBucketLog(manager.raftNodes[shardIdx])
	}
	fbl, err := bucket.NewFileBucketLog()
	if err != nil { slog.Error("failed to create file bucket log", slog.Int("shard", shardIdx), slog.Any("error", err)); return nil }
	return fbl
}
