package ironhawk

import (
	"context"
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
	delivered := 0
	if parts := strings.SplitN(ev.SubID, ":", 2); len(parts) == 2 {
		clientID := parts[0]
		if thread, ok := b.wm.clientWatchThreadMap[clientID]; ok && thread != nil {
			// If we can parse fingerprint and locate the original command, craft
			// a structured response to mirror legacy NotifyWatchers behavior.
			// Prefix Message with emit commit index so clients can auto-ACK reliably;
			// retain raw delta in typed responses where applicable.
			prefixedMsg := "[emit_commit_index=" + strconv.FormatUint(ev.EmitSeq.CommitIndex, 10) + "] " + string(ev.Delta)
			rs := &wire.Result{Status: wire.Status_OK, Message: prefixedMsg}
			// Best-effort: embed command-specific response (e.g., GET) when known
			if fpStr := parts[1]; fpStr != "" {
				// parse base-10 fingerprint
				var fp uint64
				for i := 0; i < len(fpStr); i++ { // fast parse without strconv to avoid import churn
					c := fpStr[i]
					if c < '0' || c > '9' {
						fp = 0
						break
					}
					fp = fp*10 + uint64(c-'0')
				}
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
			}
			if err := thread.serverWire.Send(ctx, rs); err != nil {
				// Closed client wires are expected during disconnects; avoid spamming WARN.
				msg := err.Error()
				if strings.Contains(msg, "closed wire") || strings.Contains(msg, "closed") {
					slog.Debug("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
				} else {
					slog.Warn("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
				}
			} else {
				delivered = 1
			}
		}
	} else {
		// Fallback: broadcast (development only)
		for _, thread := range b.wm.clientWatchThreadMap {
			if thread == nil {
				continue
			}
			prefixedMsg := "[emit_commit_index=" + strconv.FormatUint(ev.EmitSeq.CommitIndex, 10) + "] " + string(ev.Delta)
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
	}
	slog.Debug("bridge delivered", slog.Int("count", delivered), slog.String("sub_id", ev.SubID), slog.String("emit_seq", ev.EmitSeq.String()))
	return nil
}

// No additional types; we use wire.Result directly for now.
