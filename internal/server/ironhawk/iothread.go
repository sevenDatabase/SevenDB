// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package ironhawk

import (
	"context"
	"fmt"
	"log/slog"
    "github.com/sevenDatabase/SevenDB/internal/logging"
	"strconv"
	"strings"

	"github.com/dicedb/dicedb-go"

	"sync/atomic"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/auth"
	"github.com/sevenDatabase/SevenDB/internal/cmd"
	"github.com/sevenDatabase/SevenDB/internal/observability"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	"github.com/sevenDatabase/SevenDB/internal/types"
	"github.com/sevenDatabase/SevenDB/internal/wal"
)

type IOThread struct {
	ClientID   string
	Mode       string
	Session    *auth.Session
	serverWire *dicedb.ServerWire
}

var durableSetSyncCount uint64 // atomically incremented each time a DURABLE/SYNC SET forces WAL.Sync()
var bufferedSetCount uint64    // atomically incremented for SET without durability request

func init() {
	// Register lightweight metrics for durable vs buffered SET writes
	observability.RegisterCustomCollector(func() []string {
		ds := atomic.LoadUint64(&durableSetSyncCount)
		bs := atomic.LoadUint64(&bufferedSetCount)
		return []string{
			fmt.Sprintf("sevendb_set_durable_sync_total %d", ds),
			fmt.Sprintf("sevendb_set_buffered_total %d", bs),
		}
	})
}

func NewIOThread(clientFD int) (*IOThread, error) {
	w, err := dicedb.NewServerWire(config.MaxRequestSize, config.KeepAlive, clientFD)
	if err != nil {
		if err.Kind == wire.NotEstablished {
			slog.Error("failed to establish connection to client", slog.Int("client-fd", clientFD), slog.Any("error", err))

			return nil, err.Unwrap()
		}
		slog.Error("unexpected error during client connection establishment, this should be reported to DiceDB maintainers", slog.Int("client-fd", clientFD))
		return nil, err.Unwrap()
	}

	return &IOThread{
		serverWire: w,
		Session:    auth.NewSession(),
	}, nil
}

func (t *IOThread) Start(ctx context.Context, shardManager *shardmanager.ShardManager, watchManager *WatchManager) error {
	for {
		var c *wire.Command
		recvCh := make(chan *wire.Command, 1)
		errCh := make(chan error, 1)

		go func() {
			tmpC, err := t.serverWire.Receive()
			if err != nil {
				errCh <- err.Unwrap()
				return
			}
			recvCh <- tmpC
		}()

		select {
		case <-ctx.Done():
			slog.Debug("io-thread context canceled, shutting down receive loop")
			return ctx.Err()
		case err := <-errCh:
			return err
		case tmp := <-recvCh:
			c = tmp
			if c != nil {
				logging.VInfo("verbose", "iothread: received command", slog.String("cmd", c.Cmd), slog.Any("args", c.Args))
			}
		}

		_c := &cmd.Cmd{
			C:        c,
			ClientID: t.ClientID,
			Mode:     t.Mode,
		}

		res, err := _c.Execute(shardManager)
		if err != nil {
			res = &cmd.CmdRes{
				Rs: &wire.Result{
					Status:  wire.Status_ERR,
					Message: err.Error(),
				},
			}
			if sendErr := t.serverWire.Send(ctx, res.Rs); sendErr != nil {
				return sendErr.Unwrap()
			}
			// Continue in case of error
			continue
		}

		// Preserve command-chosen status. Only default the message if status is OK and message is empty.
		if res != nil && res.Rs != nil && res.Rs.Status == wire.Status_OK && res.Rs.Message == "" {
			res.Rs.Message = "OK"
		}

		// Determine if this is a watch command. Require the explicit ".WATCH" suffix
		// so that commands like "UNWATCH" are not misclassified as watch commands.
		isWatchCmd := strings.HasSuffix(c.Cmd, ".WATCH")

		// Log command to WAL if enabled and not a replay and not a watch/unwatch op
		// Watch/Unwatch are logged via WatchManager as SUBSCRIBE/UNSUBSCRIBE and must be atomic with ack
		if wal.DefaultWAL != nil && !_c.IsReplay && !isWatchCmd && c.Cmd != "UNWATCH" {
			if err := wal.DefaultWAL.LogCommand(_c.C); err != nil {
				slog.Error("failed to log command to WAL", slog.Any("error", err))
			} else {
				// If the SET command includes the DURABLE or SYNC flag, force a synchronous WAL fsync before responding.
				// We inspect args for the literal token; command layer has already validated syntax.
				// This behavior is gated by config.Config.WALEnableDurableSet to allow opt-in per deployment.
				if c.Cmd == "SET" && config.Config != nil && config.Config.WALEnableDurableSet {
					if durableRequested(c.Args) {
						if syncErr := wal.DefaultWAL.Sync(); syncErr != nil {
							// Surface the sync error to the client instead of acknowledging success.
							res = &cmd.CmdRes{Rs: &wire.Result{Status: wire.Status_ERR, Message: fmt.Sprintf("wal sync failed: %v", syncErr)}}
						} else {
							atomic.AddUint64(&durableSetSyncCount, 1)
						}
					} else {
						// Non-durable SET (no DURABLE/SYNC flag requested)
						atomic.AddUint64(&bufferedSetCount, 1)
					}
				} else if c.Cmd == "SET" { // durable feature disabled but SET executed
					atomic.AddUint64(&bufferedSetCount, 1)
				}
			}
		}

		// TODO: Optimize this. We are doing this for all command execution
		// Also, we are allowing people to override the client ID.
		// Also, CLientID is duplicated in command and io-thread.
		// Also, we shouldn't allow execution/registration incase of invalid commands
		// like for B.WATCH cmd since it'll err out we shall return and not create subscription
		if err == nil {
			t.ClientID = _c.ClientID
		}

		if _c.Meta != nil && _c.Meta.IsWatchable {
			// Use the base command fingerprint for watch subscriptions.
			// This ensures the fingerprint returned to the client matches the one
			// used internally for registration and persisted to WAL, allowing
			// UNWATCH to correctly identify and remove the subscription across restarts.
			res.Rs.Fingerprint64 = _c.Fingerprint()
		}

		if c.Cmd == "HANDSHAKE" && err == nil {
			t.ClientID = _c.C.Args[0]
			t.Mode = _c.C.Args[1]
			slog.Info("HANDSHAKE processed", slog.String("client_id", t.ClientID), slog.String("mode", t.Mode))
		}

		var watchRegistered bool
		if isWatchCmd {
			if err := watchManager.HandleWatch(_c, t); err != nil {
				// WAL write (inside HandleWatch) failed; send error and continue
				errRes := &wire.Result{Status: wire.Status_ERR, Message: err.Error()}
				if sendErr := t.serverWire.Send(ctx, errRes); sendErr != nil {
					return sendErr.Unwrap()
				}
				continue
			}
			watchRegistered = true
			// Ensure this fingerprint routes to the current connection's clientID.
			// This addresses the case where, after a restart, WAL-replayed subscriptions
			// still point at a stale clientID. Rebind to this connection so future
			// emissions are delivered here.
			watchManager.RebindClientForFP(_c.Fingerprint(), t.ClientID)

			// Also rebind any pending outbox entries from a previous connection (if any)
			// to this new connection. This handles the case where a client reconnects
			// and re-issues the WATCH command but does not send EMITRECONNECT.
			// We pass empty oldClientID to let the manager search by fingerprint.
			if shardManager != nil {
				moved := shardManager.EmissionRebindForKey(_c.Key(), _c.Fingerprint(), "", t.ClientID)
				if moved {
					logging.VInfo("verbose", "iothread: implicit rebind on WATCH", slog.String("key", _c.Key()), slog.Uint64("fp", _c.Fingerprint()), slog.String("newClient", t.ClientID))
				}
			}
		} else if strings.HasSuffix(c.Cmd, "UNWATCH") {
			if err := watchManager.HandleUnwatch(_c, t); err != nil {
				errRes := &wire.Result{Status: wire.Status_ERR, Message: err.Error()}
				if sendErr := t.serverWire.Send(ctx, errRes); sendErr != nil {
					return sendErr.Unwrap()
				}
				continue
			}
		}

		watchManager.RegisterThread(t)
		logging.VInfo("verbose", "iothread: registered thread", slog.String("client_id", t.ClientID))

		// On successful EMITRECONNECT, rebind the subscription fingerprint to this connection's clientID
		if strings.EqualFold(c.Cmd, "EMITRECONNECT") {
			if res != nil && res.Rs != nil && res.Rs.Status == wire.Status_OK && len(c.Args) >= 3 {
				// c.Args[1] is sub_id => clientId:fp
				var fp uint64
				var oldClientID string
				parts := strings.SplitN(c.Args[1], ":", 2)
				if len(parts) == 2 {
					oldClientID = parts[0]
					if v, perr := strconv.ParseUint(parts[1], 10, 64); perr == nil {
						fp = v
					}
				}
				
					if fp != 0 {
					logging.VInfo("verbose", "iothread: rebinding client for fp", slog.Uint64("fp", fp), slog.String("old_client", oldClientID), slog.String("client_id", t.ClientID))
					watchManager.RebindClientForFP(fp, t.ClientID)
				} else if oldClientID != "" {
					// Fallback: if fp is 0 (unknown), try to find the subscription by key and oldClientID
					key := c.Args[0]
					logging.VInfo("verbose", "iothread: rebinding client for key", slog.String("key", key), slog.String("old_client", oldClientID), slog.String("client_id", t.ClientID))
					watchManager.RebindClientForKey(key, oldClientID, t.ClientID)
				}

				// Trigger resume on Notifier (moved from cmd_emitreconnect to avoid race)
				if strings.HasPrefix(res.Rs.Message, "OK ") {
							if nextIdx, err := strconv.ParseUint(strings.TrimPrefix(res.Rs.Message, "OK "), 10, 64); err == nil {
								key := c.Args[0]
								if n := shardManager.NotifierForKey(key); n != nil {
									subID := c.Args[1]
									var newSub string
									// If fp is 0, we might have rebound by key, so we need to find the real fp to construct newSub
									if fp == 0 {
										foundFP := watchManager.GetFingerprintForClient(key, t.ClientID)
										if foundFP != 0 {
											fp = foundFP
											newSub = t.ClientID + ":" + strconv.FormatUint(fp, 10)
										}
									} else {
										newSub = t.ClientID + ":" + strconv.FormatUint(fp, 10)
									}
									
									if newSub != "" {
										n.SetResumeFrom(subID, 0)
										n.SetResumeFrom(newSub, nextIdx)
										logging.VInfo("verbose", "iothread: triggered resume", slog.String("old_sub", subID), slog.String("new_sub", newSub), slog.Uint64("next_idx", nextIdx))
									}
								} else {
									slog.Error("iothread: notifier not found for key", slog.String("key", key))
								}
							}
						}
			} else {
				slog.Warn("iothread: EMITRECONNECT skipped rebind",
					slog.Bool("res_ok", res != nil && res.Rs != nil && res.Rs.Status == wire.Status_OK),
					slog.Int("args_len", len(c.Args)))
			}
		}

		// Send responses:
		// - Non-watch commands: always send directly (legacy behavior)
		// - Watch commands: under emission-contract, send the initial ack (with Fingerprint)
		//   directly here because the emission path defers delivery to the notifier and no
		//   immediate NotifyWatchers send occurs. In legacy path, NotifyWatchers will send.
		if !isWatchCmd {
			if sendErr := t.serverWire.Send(ctx, res.Rs); sendErr != nil {
				return sendErr.Unwrap()
			}
		} else if config.Config != nil && config.Config.EmissionContractEnabled {
			if sendErr := t.serverWire.Send(ctx, res.Rs); sendErr != nil {
				return sendErr.Unwrap()
			}
		}

		// TODO: Streamline this because we need ordering of updates
		// that are being sent to watchers.
		if err == nil {
			// Only notify watchers (which also sends the initial watch response) after successful registration
			if !isWatchCmd || (isWatchCmd && watchRegistered) {
				// Avoid spurious emissions for non-mutating commands (e.g., EMITACK/EMITRECONNECT/HANDSHAKE).
				// Notify for:
				//   - initial GET.WATCH registration (isWatchCmd && watchRegistered)
				//   - known mutating commands that can affect watched values (SET/DEL/RENAME/GETSET)
				cmdName := c.Cmd
				shouldNotify := false
				if isWatchCmd && watchRegistered {
					shouldNotify = true
				} else {
					switch cmdName {
					case "SET", "GETSET", "DEL", "RENAME":
						shouldNotify = true
					}
				}
				if shouldNotify {
					watchManager.NotifyWatchers(_c, shardManager, t)
				} else {
					slog.Debug("skip notify for non-mutating command", slog.String("cmd", cmdName))
				}
			}
		}
	}
}

func (t *IOThread) Stop() error {
	t.serverWire.Close()
	t.Session.Expire()
	return nil
}

// durableRequested reports whether the SET command's args include a request for
// synchronous durability via the DURABLE or SYNC tokens.
func durableRequested(args []string) bool {
	for _, a := range args {
		if strings.EqualFold(a, string(types.DURABLE)) || strings.EqualFold(a, string(types.SYNC)) {
			return true
		}
	}
	return false
}
