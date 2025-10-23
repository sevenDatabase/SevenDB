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
	"github.com/sevenDatabase/SevenDB/internal/emission"
	"github.com/sevenDatabase/SevenDB/internal/raft"
	"github.com/sevenDatabase/SevenDB/internal/shard"
	"github.com/sevenDatabase/SevenDB/internal/shardthread"
	"github.com/sevenDatabase/SevenDB/internal/store"
)

// noopSender implements emission.Sender with logging only; replaced later by a real bridge.
type noopSender struct{}

func (n *noopSender) Send(ctx context.Context, ev *emission.DataEvent) error {
	slog.Debug("noop sender emit", slog.String("sub_id", ev.SubID), slog.String("emit_seq", ev.EmitSeq.String()), slog.Int("delta_bytes", len(ev.Delta)))
	return nil
}

type ShardManager struct {
	shards  []*shard.Shard
	sigChan chan os.Signal // sigChan is the signal channel for the shard manager
	// raftNodes (optional) holds one ShardRaftNode per shard when raft is enabled.
	raftNodes   []*raft.ShardRaftNode
	raftSrv     *raft.RaftGRPCServer
	localRaftID uint64
	// emission (optional) per-shard manager/notifier when EmissionContractEnabled
	emissionMgrs      []*emission.Manager
	emissionNotifiers []*emission.Notifier
	emissionAppliers  []*emission.Applier
	// gating and controllers for leader-as-notifier
	emissionGates       []*gatedSender
	notifierControllers []*leaderNotifierController
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
				if config.Config.RaftAdvertiseAddr == "" {
					config.Config.RaftAdvertiseAddr = config.Config.RaftListenAddr
				}
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
		if config.Config.EmissionContractEnabled {
			sm.emissionMgrs = make([]*emission.Manager, shardCount)
			sm.emissionNotifiers = make([]*emission.Notifier, shardCount)
			sm.emissionAppliers = make([]*emission.Applier, shardCount)
			sm.emissionGates = make([]*gatedSender, shardCount)
			sm.notifierControllers = make([]*leaderNotifierController, shardCount)
		}
		for i := 0; i < shardCount; i++ {
			cfg := raft.RaftConfig{ShardID: string(rune('a' + i)), Engine: config.Config.RaftEngine, HeartbeatMillis: config.Config.RaftHeartbeatMillis, ElectionTimeoutMillis: config.Config.RaftElectionTimeoutMillis, NodeID: fmt.Sprintf("%d", sm.localRaftID), Peers: canonical, DataDir: config.Config.RaftPersistentDir}
			// If Forge WAL is enabled at the server level, route raft writes through the unified Forge WAL
			if config.Config.EnableWAL && config.Config.WALVariant == "forge" {
				cfg.WALFactory = raft.ForgeWALFactory
			}
			rn, err := raft.NewShardRaftNode(cfg)
			if err != nil {
				slog.Error("failed to start raft for shard", slog.Int("shard", i), slog.Any("error", err))
				continue
			}
			sm.raftNodes[i] = rn

			// Emission contract wiring (single-notifier per shard) behind flag
			if config.Config.EmissionContractEnabled {
				mgr := emission.NewManager(cfg.ShardID)
				// Register replication handler to apply OUTBOX_* directly on commit (idempotent with applier)
				emission.RegisterWithShard(rn, mgr)
				// Start applier to translate DATA_EVENT/ACK to OUTBOX_* and apply
				applier := emission.NewApplier(rn, mgr, cfg.ShardID)
				applier.Start(context.Background())
				// Sender bridge is wired later after IOThreads available; wrap with a gate
				gate := newGatedSender(nil)
				proposer := &emission.RaftProposer{Node: rn, BucketID: cfg.ShardID}
				notifier := emission.NewNotifier(mgr, gate, proposer, cfg.ShardID)
				notifier.Start(context.Background())
				sm.emissionMgrs[i] = mgr
				sm.emissionNotifiers[i] = notifier
				sm.emissionAppliers[i] = applier
				sm.emissionGates[i] = gate
				sm.notifierControllers[i] = newLeaderNotifierController(cfg.ShardID, rn, gate)
				slog.Info("emission contract enabled for shard", slog.Int("shard", i), slog.String("bucket_uuid", cfg.ShardID))
			}
		}
		slog.Info("raft enabled for shards", slog.Int("count", shardCount), slog.Uint64("local_id", sm.localRaftID))
		// Early best-effort initial status write (honors explicit status-file-path if provided)
		func() {
			defer func() { _ = recover() }()
			snaps := sm.RaftStatusSnapshots()
			payload := map[string]interface{}{"ts_unix": time.Now().Unix(), "nodes": snaps}
			b, err := json.MarshalIndent(payload, "", "  ")
			if err != nil {
				return
			}
			var primary string
			if config.Config.StatusFilePath != "" {
				primary = config.Config.StatusFilePath
			} else {
				primary = filepath.Join(config.MetadataDir, "status.json")
			}
			tmp := primary + ".tmp"
			if err := os.WriteFile(tmp, b, 0o600); err == nil {
				if err := os.Rename(tmp, primary); err != nil {
					slog.Error("initial status file rename failed", slog.String("tmp", tmp), slog.String("dest", primary), slog.Any("error", err))
				} else {
					slog.Info("initial status file written", slog.String("path", primary), slog.Int("bytes", len(b)))
				}
			} else {
				slog.Error("initial status file write failed", slog.String("path", primary), slog.Any("error", err))
			}
			if config.Config.StatusFilePath == "" { // also write alt only when not explicit
				cwd, _ := os.Getwd()
				alt := filepath.Join(cwd, "status.json")
				tmpAlt := alt + ".tmp"
				if err := os.WriteFile(tmpAlt, b, 0o600); err == nil {
					_ = os.Rename(tmpAlt, alt)
				}
			}
		}()
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
			if err == nil {
				resolverAddrs = pm
			}
		}
		if len(resolverAddrs) == 0 { // single node fallback
			resolverAddrs[manager.localRaftID] = config.Config.RaftAdvertiseAddr
		}
		resolver := &raft.StaticPeerResolver{SelfID: manager.localRaftID, Addrs: resolverAddrs}
		// Register shards in server
		if len(resolverAddrs) > 1 { // only run server if multi-node
			manager.raftSrv = raft.NewRaftGRPCServer(manager.localRaftID)
			for _, rn := range manager.raftNodes {
				if rn != nil {
					manager.raftSrv.RegisterShard(rn.ShardID(), rn)
				}
			}
			go func() {
				if err := manager.raftSrv.Serve(config.Config.RaftListenAddr); err != nil {
					slog.Error("raft grpc server exited", slog.Any("error", err))
				}
			}()
		}
		// Attach transports
		for _, rn := range manager.raftNodes {
			if rn == nil {
				continue
			}
			gt := raft.NewGRPCTransport(manager.localRaftID, rn.ShardID(), resolver)
			rn.SetTransport(gt)
			go gt.Start(shardCtx)
		}
	}

	// Start leader-as-notifier controllers (only when emission is enabled)
	if manager.emissionGates != nil && manager.notifierControllers != nil {
		for _, ctl := range manager.notifierControllers {
			if ctl != nil {
				ctl.Start(shardCtx)
			}
		}
	}

	manager.start(shardCtx, &wg)

	// Periodic status writer (local development / external observation). Writes status.json in metadata dir.
	if manager.raftNodes != nil && config.Config != nil && config.Config.RaftEnabled {
		var statusPath string
		var altPath string
		if config.Config.StatusFilePath != "" {
			statusPath = config.Config.StatusFilePath
			altPath = "" // disable alt when explicit path provided
		} else {
			statusPath = filepath.Join(config.MetadataDir, "status.json")
			cwd, _ := os.Getwd()
			altPath = filepath.Join(cwd, "status.json")
		}
		if altPath != "" {
			slog.Info("status writer active", slog.String("primary", statusPath), slog.String("alt", altPath), slog.Duration("interval", time.Second))
		} else {
			slog.Info("status writer active", slog.String("path", statusPath), slog.Duration("interval", time.Second))
		}
		go func() {
			writeOnce := func() {
				snaps := manager.RaftStatusSnapshots()
				payload := map[string]interface{}{"ts_unix": time.Now().Unix(), "nodes": snaps}
				// Attach emission metrics snapshot if emission is enabled.
				if config.Config != nil && config.Config.EmissionContractEnabled {
					// import cycle safe: shardmanager already depends on emission
					payload["emission_metrics"] = emission.Metrics.Snapshot()
				}
				b, err := json.MarshalIndent(payload, "", "  ")
				if err != nil {
					slog.Debug("status marshal failed", slog.Any("error", err))
					return
				}
				// primary (explicit or metadata)
				tmp := statusPath + ".tmp"
				if err := os.WriteFile(tmp, b, 0o600); err != nil {
					slog.Error("status write failed", slog.String("path", statusPath), slog.Any("error", err))
				} else {
					if err := os.Rename(tmp, statusPath); err != nil {
						slog.Error("status rename failed", slog.String("tmp", tmp), slog.String("dest", statusPath), slog.Any("error", err))
					} else {
						// success log includes first shard indexes for quick debugging
						if len(snaps) > 0 {
							if m, ok := snaps[0].(map[string]interface{}); ok {
								la := m["last_applied_index"]
								ls := m["last_snapshot_index"]
								slog.Debug("status write ok", slog.String("path", statusPath), slog.Int("bytes", len(b)), slog.Any("applied", la), slog.Any("snapshot", ls))
							}
						}
					}
				}
				// alt only if enabled
				if altPath != "" {
					tmpAlt := altPath + ".tmp"
					if err := os.WriteFile(tmpAlt, b, 0o600); err != nil {
						slog.Error("status alt write failed", slog.String("path", altPath), slog.Any("error", err))
					} else {
						if err := os.Rename(tmpAlt, altPath); err != nil {
							slog.Error("status alt rename failed", slog.String("tmp", tmpAlt), slog.String("dest", altPath), slog.Any("error", err))
						}
					}
				}
			}
			// Immediate first write
			writeOnce()
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-shardCtx.Done():
					return
				case <-ticker.C:
					writeOnce()
					// Also emit a compact single-line status for log-based observation fallback.
					func() {
						defer func() { _ = recover() }()
						snaps := manager.RaftStatusSnapshots()
						if len(snaps) == 0 {
							return
						}
						// use only first shard snapshot for concise leader/apply view
						if first, ok := snaps[0].(interface{}); ok {
							b, err := json.Marshal(first)
							if err == nil {
								slog.Info("raft_status", slog.String("node", fmt.Sprintf("%d", manager.localRaftID)), slog.String("snapshot", string(b)))
							}
						}
					}()
					// Optional compact emission metrics log every N seconds
					if config.Config != nil && config.Config.EmissionContractEnabled && config.Config.MetricsLogIntervalSec > 0 {
						now := time.Now().Unix()
						if now%int64(config.Config.MetricsLogIntervalSec) == 0 {
							m := emission.Metrics.Snapshot()
							getI64 := func(k string) int64 {
								if v, ok := m[k]; ok {
									switch t := v.(type) {
									case int64:
										return t
									case int:
										return int64(t)
									case uint64:
										return int64(t)
									}
								}
								return 0
							}
							getF := func(k string) float64 {
								if v, ok := m[k]; ok {
									switch t := v.(type) {
									case float64:
										return t
									case int64:
										return float64(t)
									case int:
										return float64(t)
									}
								}
								return 0
							}
							slog.Info("emission_metrics",
								slog.Int64("pending", getI64("pending_entries")),
								slog.Int64("subs", getI64("subs_with_pending")),
								slog.Int64("sends_per_sec", getI64("sends_per_sec")),
								slog.Int64("acks_per_sec", getI64("acks_per_sec")),
								slog.Float64("lat_ms_avg", getF("send_latency_ms_avg")),
								slog.Int64("reconnect_ok", getI64("reconnects_ok_total")),
								slog.Int64("reconnect_stale", getI64("reconnects_stale_total")),
							)
						}
					}
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

// GetRaftNodeForKey returns the raft node responsible for the shard that owns the given key.
// Returns nil if raft is not enabled or index out of range.
func (manager *ShardManager) GetRaftNodeForKey(key string) *raft.ShardRaftNode {
	if manager == nil || manager.raftNodes == nil {
		return nil
	}
	idx := int(xxhash.Sum64String(key) % uint64(manager.ShardCount()))
	if idx < 0 || idx >= len(manager.raftNodes) {
		return nil
	}
	return manager.raftNodes[idx]
}

// SetEmissionSender wires a Sender into the notifier for the shard owning the key.
// No-op if emission is disabled or components are missing.
func (manager *ShardManager) SetEmissionSenderForKey(key string, sender emission.Sender) {
	if manager == nil || manager.emissionNotifiers == nil {
		return
	}
	idx := int(xxhash.Sum64String(key) % uint64(manager.ShardCount()))
	if idx < 0 || idx >= len(manager.emissionNotifiers) {
		return
	}
	// Prefer updating gate if present to preserve leader-only semantics
	if manager.emissionGates != nil && idx < len(manager.emissionGates) {
		if g := manager.emissionGates[idx]; g != nil {
			g.SetUnderlying(sender)
			return
		}
	}
	if n := manager.emissionNotifiers[idx]; n != nil {
		n.SetSender(sender)
	}
}

// NotifierForKey returns the Notifier for the shard owning the key (for ACK path).
func (manager *ShardManager) NotifierForKey(key string) *emission.Notifier {
	if manager == nil || manager.emissionNotifiers == nil {
		return nil
	}
	idx := int(xxhash.Sum64String(key) % uint64(manager.ShardCount()))
	if idx < 0 || idx >= len(manager.emissionNotifiers) {
		return nil
	}
	return manager.emissionNotifiers[idx]
}

// EmissionReconnectForKey runs Manager.Reconnect for the shard owning the key.
// Returns a ReconnectAck indicating whether the client can resume and the next commit index.
func (manager *ShardManager) EmissionReconnectForKey(key string, req emission.ReconnectRequest) emission.ReconnectAck {
	var zero emission.ReconnectAck
	if manager == nil || manager.emissionMgrs == nil {
		return zero
	}
	idx := int(xxhash.Sum64String(key) % uint64(manager.ShardCount()))
	if idx < 0 || idx >= len(manager.emissionMgrs) {
		return zero
	}
	mgr := manager.emissionMgrs[idx]
	if mgr == nil {
		return zero
	}
	return mgr.Reconnect(req)
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
		if rn == nil {
			continue
		}
		snap := rn.Status() // StatusSnapshot is a plain struct
		out = append(out, snap)
	}
	return out
}

// SetEmissionSender wires a sender implementation for a given shard index, if emission is enabled.
// Safe to call after server/watch manager initialization to swap from noop to a real bridge.
func (manager *ShardManager) SetEmissionSender(shardIdx int, sender emission.Sender) {
	if manager == nil || shardIdx < 0 || shardIdx >= len(manager.emissionNotifiers) {
		return
	}
	if manager.emissionGates != nil && shardIdx < len(manager.emissionGates) {
		if g := manager.emissionGates[shardIdx]; g != nil {
			g.SetUnderlying(sender)
			return
		}
	}
	if n := manager.emissionNotifiers[shardIdx]; n != nil {
		n.SetSender(sender)
	}
}

// ProposeDataEvent proposes a DATA_EVENT for a subscription to the shard's raft group.
// The applier will convert it to an OUTBOX_WRITE with the assigned commit index.
func (manager *ShardManager) ProposeDataEvent(ctx context.Context, shardIdx int, subID string, delta []byte) error {
	if manager == nil || shardIdx < 0 || shardIdx >= len(manager.raftNodes) {
		return fmt.Errorf("invalid shard idx")
	}
	rn := manager.raftNodes[shardIdx]
	if rn == nil {
		return fmt.Errorf("raft not enabled for shard")
	}
	rec, err := raft.BuildReplicationRecord(rn.ShardID(), "DATA_EVENT", []string{subID, string(delta)})
	if err != nil {
		return err
	}
	_, _, err = rn.ProposeAndWait(ctx, rec)
	return err
}

// ProposeDataEventForKey selects shard by key and proposes a DATA_EVENT.
func (manager *ShardManager) ProposeDataEventForKey(ctx context.Context, key string, subID string, delta []byte) error {
	rn := manager.GetRaftNodeForKey(key)
	if rn == nil {
		return fmt.Errorf("raft not enabled for key shard")
	}
	rec, err := raft.BuildReplicationRecord(rn.ShardID(), "DATA_EVENT", []string{subID, string(delta)})
	if err != nil {
		return err
	}
	_, _, err = rn.ProposeAndWait(ctx, rec)
	return err
}
