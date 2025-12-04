package ironhawk

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/internal/emission"
)

// BridgeSender routes emission DataEvents to existing IOThreads using the WatchManager maps.
// subID format assumed: clientID:fp (fingerprint in base10). This can be adjusted later.
type BridgeSender struct {
	wm *WatchManager
}

func NewBridgeSender(wm *WatchManager) *BridgeSender { return &BridgeSender{wm: wm} }

func (b *BridgeSender) Send(ctx context.Context, ev *emission.DataEvent) error {
	if b.wm == nil || ev == nil {
		return nil
	}
	b.wm.mu.RLock()
	defer b.wm.mu.RUnlock()

	// Try to parse sub_id into clientID:fp
	parts := strings.SplitN(ev.SubID, ":", 2)
	if len(parts) != 2 {
		// Fallback: broadcast (development only)
		delivered := 0
		for _, thread := range b.wm.clientWatchThreadMap {
			if thread == nil {
				continue
			}
			epochStr := ev.EmitSeq.Epoch.BucketUUID + ":" + strconv.FormatUint(uint64(ev.EmitSeq.Epoch.EpochCounter), 10)
			prefixedMsg := "[emit_epoch=" + epochStr + ", emit_commit_index=" + strconv.FormatUint(ev.EmitSeq.CommitIndex, 10) + "] " + string(ev.Delta)
			rs := &wire.Result{Status: wire.Status_OK, Message: prefixedMsg}
			if err := thread.serverWire.Send(ctx, rs); err != nil {
				msg := err.Error()
				if strings.Contains(msg, "closed wire") || strings.Contains(msg, "closed") {
					slog.Debug("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
				} else {
					slog.Warn("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
				}
				continue
			}
			delivered++
		}
		slog.Info("emission-bridge delivered (broadcast)", slog.Int("count", delivered), slog.String("sub_id", ev.SubID), slog.String("emit_seq", ev.EmitSeq.String()))
		if delivered == 0 {
			return fmt.Errorf("bridge: no recipients for sub_id %s", ev.SubID)
		}
		return nil
	}

	originalClientID := parts[0]
	fpStr := parts[1]

	// Parse fingerprint
	var fp uint64
	for i := 0; i < len(fpStr); i++ {
		c := fpStr[i]
		if c < '0' || c > '9' {
			fp = 0
			break
		}
		fp = fp*10 + uint64(c-'0')
	}

	// Determine target clients.
	// Prefer explicit binding via fpClientMap (handles rebinds after restarts).
	// IMPORTANT: do NOT fall back to the encoded original client ID when a
	// fingerprint exists but has no active subscribers. Falling back silently
	// caused emissions to be delivered to the wrong client in some cases
	// (e.g. stale or mis-bound subIDs). Instead, treat that as "no recipients"
	// and return an error so callers can retry/rebind as appropriate.
	var targetIDs []string
	usedFPMap := false
	if fp != 0 {
		if clients, ok := b.wm.fpClientMap[fp]; ok && len(clients) > 0 {
			usedFPMap = true
			for cid := range clients {
				targetIDs = append(targetIDs, cid)
			}
		} else {
			// Fingerprint present but no active subscribers; don't silently deliver
			// to the original encoded clientID (which might be stale/wrong).
			slog.Info("emission-bridge: no active subscribers for fp; aborting delivery", slog.Uint64("fp", fp), slog.String("sub_id", ev.SubID))
			return fmt.Errorf("bridge: no active subscribers for fp %d", fp)
		}
	} else {
		// No fingerprint parsed: fall back to the original client encoded in subID
		// (this is rare / development-only behaviour).
		targetIDs = []string{originalClientID}
	}
	// Diagnostic log: show how targets resolved for this emission
	slog.Info("emission-bridge: resolved targets",
		slog.String("sub_id", ev.SubID),
		slog.Uint64("fp", fp),
		slog.Bool("used_fp_map", usedFPMap),
		slog.String("targets", fmt.Sprintf("%v", targetIDs)))
	// slog.Info("emission-bridge: resolved targets", slog.String("sub_id", ev.SubID), slog.Uint64("fp", fp), slog.Any("targets", targetIDs))

	delivered := 0
	for _, clientID := range targetIDs {
		thread, ok := b.wm.clientWatchThreadMap[clientID]
		if !ok || thread == nil {
			slog.Info("emission-bridge: no active thread for resolved client",
				slog.String("client_id", clientID),
				slog.String("sub_id", ev.SubID))
			continue
		}

		// Construct response with fingerprint so client can identify this as an emission update
		epochStr := ev.EmitSeq.Epoch.BucketUUID + ":" + strconv.FormatUint(uint64(ev.EmitSeq.Epoch.EpochCounter), 10)
		prefixedMsg := "[emit_epoch=" + epochStr + ", emit_commit_index=" + strconv.FormatUint(ev.EmitSeq.CommitIndex, 10) + "] " + string(ev.Delta)
		rs := &wire.Result{Status: wire.Status_OK, Message: prefixedMsg, Fingerprint64: fp}

		// Best-effort: embed command-specific response (e.g., GET) when known
		if fp != 0 {
			if c := b.wm.fpCmdMap[fp]; c != nil && c.C != nil {
				base := c.C.Cmd
				if strings.HasSuffix(base, ".WATCH") {
					base = strings.TrimSuffix(base, ".WATCH")
				}
				switch base {
				case "GET":
					rs.Response = &wire.Result_GETRes{GETRes: &wire.GETRes{Value: string(ev.Delta)}}
				}
			}
		}

		if err := thread.serverWire.Send(ctx, rs); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "closed wire") || strings.Contains(msg, "closed") {
				slog.Debug("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
			} else {
				slog.Warn("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
			}
		} else {
			slog.Info("emission-bridge: delivered to target", slog.String("client_id", clientID), slog.String("sub_id", ev.SubID))
			delivered++
		}
	}

	// Fallback: if no thread found for resolved clients, try to find ANY client with an active
	// thread that might be interested in this fingerprint. This handles the case where a client
	// reconnected but didn't re-issue WATCH (e.g., CLI disconnect/reconnect scenario).
	if delivered == 0 && fp != 0 {
		slog.Info("emission-bridge: attempting fallback delivery by scanning all threads",
			slog.Uint64("fp", fp),
			slog.String("sub_id", ev.SubID))

		// Look for any thread that might be watching this fingerprint
		for clientID, thread := range b.wm.clientWatchThreadMap {
			if thread == nil {
				continue
			}
			// Check if this client is subscribed to the fingerprint
			if clients, ok := b.wm.fpClientMap[fp]; ok && clients[clientID] {
				epochStr := ev.EmitSeq.Epoch.BucketUUID + ":" + strconv.FormatUint(uint64(ev.EmitSeq.Epoch.EpochCounter), 10)
				prefixedMsg := "[emit_epoch=" + epochStr + ", emit_commit_index=" + strconv.FormatUint(ev.EmitSeq.CommitIndex, 10) + "] " + string(ev.Delta)
				rs := &wire.Result{Status: wire.Status_OK, Message: prefixedMsg, Fingerprint64: fp}

				if c := b.wm.fpCmdMap[fp]; c != nil && c.C != nil {
					base := c.C.Cmd
					if strings.HasSuffix(base, ".WATCH") {
						base = strings.TrimSuffix(base, ".WATCH")
					}
					if base == "GET" {
						rs.Response = &wire.Result_GETRes{GETRes: &wire.GETRes{Value: string(ev.Delta)}}
					}
				}

				if err := thread.serverWire.Send(ctx, rs); err == nil {
					slog.Info("emission-bridge: fallback delivery succeeded",
						slog.String("client_id", clientID),
						slog.Uint64("fp", fp))
					delivered++
				}
			}
		}
	}

	if delivered == 0 {
		// No active thread for the target client(s); signal back so caller can retry later or rebind.
		keys := make([]string, 0, len(b.wm.clientWatchThreadMap))
		for k := range b.wm.clientWatchThreadMap {
			keys = append(keys, k)
		}
		slog.Warn("bridge: no active thread for client(s)",
			slog.Any("targets", targetIDs),
			slog.String("sub_id", ev.SubID),
			slog.Any("available_clients", keys),
			slog.String("wm_ptr", fmt.Sprintf("%p", b.wm)))
		return fmt.Errorf("bridge: no active thread for client(s) %v", targetIDs)
	}

	slog.Info("emission-bridge delivered", slog.Int("count", delivered), slog.String("sub_id", ev.SubID), slog.String("emit_seq", ev.EmitSeq.String()))
	return nil
}

// No additional types; we use wire.Result directly for now.
