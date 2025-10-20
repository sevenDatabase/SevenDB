// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package ironhawk

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/cmd"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	"github.com/sevenDatabase/SevenDB/internal/wal"
)

type WatchManager struct {
	mu                   sync.RWMutex
	clientWatchThreadMap map[string]*IOThread

	keyFPMap    map[string]map[uint64]bool
	fpClientMap map[uint64]map[string]bool
	fpCmdMap    map[uint64]*cmd.Cmd
}

func NewWatchManager() *WatchManager {
	return &WatchManager{
		clientWatchThreadMap: map[string]*IOThread{},

		keyFPMap:    map[string]map[uint64]bool{},
		fpClientMap: map[uint64]map[string]bool{},
		fpCmdMap:    map[uint64]*cmd.Cmd{},
	}
}

func (w *WatchManager) RegisterThread(t *IOThread) {
	// Register (or refresh) the IOThread for the client irrespective of its mode.
	//
	// Rationale: Durable watch acks are sent via NotifyWatchers which resolves
	// client threads from this map. Some tests (and legitimate clients) perform
	// subscriptions from a connection that initially handshakes in "command"
	// mode. Restricting registration only to explicit "watch" mode caused the
	// initial watch response never to be delivered, leading to blocked clients
	// (and test hangs) waiting on the GET.WATCH ack. Allowing all modes here is
	// safe; CleanupThreadWatchSubscriptions will prune on disconnect.
	w.mu.Lock()
	defer w.mu.Unlock()
	w.clientWatchThreadMap[t.ClientID] = t
}

func (w *WatchManager) HandleWatch(c *cmd.Cmd, t *IOThread) error {
	// Compute fingerprint using the base command (without appending .WATCH),
	// matching what we return to clients and persist in WAL.
	fp, key := c.Fingerprint(), c.Key()
	slog.Debug("creating a new subscription",
		slog.String("key", key),
		slog.String("cmd", c.String()),
		slog.Any("fingerprint", fp),
		slog.String("client_id", t.ClientID))

	// First, log SUBSCRIBE event to WAL; only proceed on success
	// Log SUBSCRIBE event to WAL and force durability before ack/registration
	if wal.DefaultWAL != nil && !c.IsReplay {
		subCmd := &wire.Command{
			Cmd:  "SUBSCRIBE",
			Args: []string{t.ClientID, c.String(), strconv.FormatUint(fp, 10)},
		}
		if err := wal.DefaultWAL.LogCommand(subCmd); err != nil {
			slog.Error("failed to log SUBSCRIBE to WAL", slog.Any("error", err))
			return err // do not register subscription if not durably logged
		}
		if err := wal.DefaultWAL.Sync(); err != nil {
			slog.Error("failed to sync SUBSCRIBE WAL entry", slog.Any("error", err))
			return err
		}
	}

	// Now update in-memory subscription state atomically
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.keyFPMap[key]; !ok {
		w.keyFPMap[key] = make(map[uint64]bool)
	}
	w.keyFPMap[key][fp] = true

	if _, ok := w.fpClientMap[fp]; !ok {
		w.fpClientMap[fp] = make(map[string]bool)
	}
	w.fpClientMap[fp][t.ClientID] = true

	w.fpCmdMap[fp] = c
	w.clientWatchThreadMap[t.ClientID] = t
	return nil
}

func (w *WatchManager) HandleUnwatch(c *cmd.Cmd, t *IOThread) error {
	if len(c.C.Args) != 1 {
		return nil
	}

	// Parse the fingerprint from the command
	fp, err := strconv.ParseUint(c.C.Args[0], 10, 64)
	if err != nil {
		return err
	}

	// First, log UNSUBSCRIBE to WAL; only proceed on success
	// Log UNSUBSCRIBE event to WAL and force durability before removing
	if wal.DefaultWAL != nil && !c.IsReplay {
		unsubCmd := &wire.Command{
			Cmd:  "UNSUBSCRIBE",
			Args: []string{t.ClientID, strconv.FormatUint(fp, 10)},
		}
		if err := wal.DefaultWAL.LogCommand(unsubCmd); err != nil {
			slog.Error("failed to log UNSUBSCRIBE to WAL", slog.Any("error", err))
			return err
		}
		if err := wal.DefaultWAL.Sync(); err != nil {
			slog.Error("failed to sync UNSUBSCRIBE WAL entry", slog.Any("error", err))
			return err
		}
	}

	// Update in-memory state
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.fpClientMap[fp], t.ClientID)
	if len(w.fpClientMap[fp]) == 0 {
		delete(w.fpClientMap, fp)
		delete(w.fpCmdMap, fp)
		// Note: keyFPMap cleanup is lazy
	}

	return nil
}

// RestoreSubscription rebuilds an in-memory subscription from WAL replay.
// It does not attempt to send any messages; it only updates state so future updates can notify.
func (w *WatchManager) RestoreSubscription(clientID, commandStr string, fp uint64) error {
	parts := strings.Fields(commandStr)
	if len(parts) == 0 {
		return nil
	}

	wc := &wire.Command{Cmd: parts[0]}
	if len(parts) > 1 {
		wc.Args = parts[1:]
	}
	c := &cmd.Cmd{C: wc, IsReplay: true}

	key := c.Key()

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.keyFPMap[key]; !ok {
		w.keyFPMap[key] = make(map[uint64]bool)
	}
	w.keyFPMap[key][fp] = true

	if _, ok := w.fpClientMap[fp]; !ok {
		w.fpClientMap[fp] = make(map[string]bool)
	}
	w.fpClientMap[fp][clientID] = true

	w.fpCmdMap[fp] = c
	return nil
}

// RemoveSubscription removes a subscription mapping during WAL replay.
func (w *WatchManager) RemoveSubscription(clientID string, fp uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.fpClientMap[fp], clientID)
	if len(w.fpClientMap[fp]) == 0 {
		delete(w.fpClientMap, fp)
		delete(w.fpCmdMap, fp)
		// keyFPMap cleanup remains lazy
	}
	return nil
}

func (w *WatchManager) CleanupThreadWatchSubscriptions(t *IOThread) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Delete the mapping of Watch thread to client id
	delete(w.clientWatchThreadMap, t.ClientID)

	// Delete all the subscriptions of the client from the fingerprint maps
	// Note: this is an O(n) operation and hence if there are large number of clients, this might be expensive.
	// We can do a lazy deletion of the fingerprint map if this becomes a problem.
	for fp := range w.fpClientMap {
		delete(w.fpClientMap[fp], t.ClientID)
		if len(w.fpClientMap[fp]) == 0 {
			delete(w.fpClientMap, fp)
		}
	}
}

func (w *WatchManager) NotifyWatchers(c *cmd.Cmd, shardManager *shardmanager.ShardManager, t *IOThread) {
	// If emission-contract is enabled, do not send directly.
	// The emission path will propose OUTBOX_WRITE via raft and the Notifier will deliver.
	if config.Config != nil && config.Config.EmissionContractEnabled {
		// Execute the watch command to compute the delta result without sending to clients yet.
		res, err := c.Execute(shardManager)
		if err != nil {
			// WRONGTYPE and similar are common; log at debug and return.
			slog.Debug("emission-contract: watch compute returned error", slog.Any("cmd", c.String()), slog.Any("error", err))
			return
		}
		// Use the wire.Result.Message as the delta payload for MVP (string encoding).
		var deltaBytes []byte
		if res != nil && res.Rs != nil && res.Rs.Message != "" {
			deltaBytes = []byte(res.Rs.Message)
		} else {
			// Empty delta is allowed; still record an event for ordering.
			deltaBytes = nil
		}
		// Determine shard by key to map to the correct raft node
		shard := shardManager.GetShardForKey(c.Key())
		shardIdx := 0
		if shard != nil {
			shardIdx = shard.ID
		}
		// For each subscribed client to this key's fingerprint, propose a DATA_EVENT with sub-id as clientID:fp
		fp := c.Fingerprint()
		w.mu.RLock()
		subs := make([]string, 0, len(w.fpClientMap[fp]))
		for clientID := range w.fpClientMap[fp] {
			subs = append(subs, clientID)
		}
		w.mu.RUnlock()
		for _, clientID := range subs {
			subID := fmt.Sprintf("%s:%d", clientID, fp)
			if err := shardManager.ProposeDataEvent(context.Background(), shardIdx, subID, deltaBytes); err != nil {
				slog.Warn("emission-contract: propose DATA_EVENT failed", slog.String("sub_id", subID), slog.Any("error", err))
			}
		}
		return
	}
	// Use RLock instead as we are not really modifying any shared maps here.
	w.mu.RLock()
	defer w.mu.RUnlock()

	key := c.Key()
	for fp := range w.keyFPMap[key] {
		_c := w.fpCmdMap[fp]
		if _c == nil {
			// TODO: Not having a command for a fingerprint is a bug.
			continue
		}

		r, err := _c.Execute(shardManager)
		if err != nil {
			// During watch notifications, it is common to hit WRONGTYPE errors if the
			// watched key has changed to an incompatible type. These are not fatal and
			// should not spam error logs. Log at debug level instead.
			slog.Debug("watch notify command returned error",
				slog.Any("cmd", _c.String()),
				slog.Any("error", err))
			continue
		}

		for clientID := range w.fpClientMap[fp] {
			thread := w.clientWatchThreadMap[clientID]
			if thread == nil {
				// if there is no thread against the client, delete the client from the map
				delete(w.clientWatchThreadMap, clientID)
				continue
			}

			// If this is first time a client is connecting it'd be sending a .WATCH command
			// in that case we don't need to notify all other clients subscribed to the key
			if strings.HasSuffix(c.C.Cmd, ".WATCH") && t.ClientID != clientID {
				continue
			}

			err := thread.serverWire.Send(context.Background(), r.Rs)
			if err != nil {
				slog.Error("failed to write response to thread",
					slog.Any("client_id", thread.ClientID),
					slog.String("mode", thread.Mode),
					slog.Any("error", err))
			}
		}

		slog.Debug("notifying watchers for key", slog.String("key", key), slog.Int("watchers", len(w.fpClientMap[fp])))
	}
}
