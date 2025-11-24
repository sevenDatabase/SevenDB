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
	// If the fingerprint is bound to active clients (via RebindClientForFP), use them.
	// This handles the case where the subscription's original clientID (in SubID)
	// differs from the current connection's clientID.
	var targetIDs []string
	if fp != 0 && len(b.wm.fpClientMap[fp]) > 0 {
		for cid := range b.wm.fpClientMap[fp] {
			targetIDs = append(targetIDs, cid)
		}
	} else {
		targetIDs = []string{originalClientID}
	}
	// slog.Info("emission-bridge: resolved targets", slog.String("sub_id", ev.SubID), slog.Uint64("fp", fp), slog.Any("targets", targetIDs))

	delivered := 0
	for _, clientID := range targetIDs {
		thread, ok := b.wm.clientWatchThreadMap[clientID]
		if !ok || thread == nil {
			continue
		}

		// Construct response
		epochStr := ev.EmitSeq.Epoch.BucketUUID + ":" + strconv.FormatUint(uint64(ev.EmitSeq.Epoch.EpochCounter), 10)
		prefixedMsg := "[emit_epoch=" + epochStr + ", emit_commit_index=" + strconv.FormatUint(ev.EmitSeq.CommitIndex, 10) + "] " + string(ev.Delta)
		rs := &wire.Result{Status: wire.Status_OK, Message: prefixedMsg}

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
			delivered++
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
