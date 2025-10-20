package ironhawk

import (
	"context"
	"log/slog"
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
		if thread, ok := b.wm.clientWatchThreadMap[parts[0]]; ok && thread != nil {
			rs := &wire.Result{Status: wire.Status_OK, Message: string(ev.Delta)}
			if err := thread.serverWire.Send(ctx, rs); err != nil {
				slog.Warn("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
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
			rs := &wire.Result{Status: wire.Status_OK, Message: string(ev.Delta)}
			if err := thread.serverWire.Send(ctx, rs); err != nil {
				slog.Warn("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
				continue
			}
			delivered++
		}
	}
	slog.Debug("bridge delivered", slog.Int("count", delivered), slog.String("sub_id", ev.SubID), slog.String("emit_seq", ev.EmitSeq.String()))
	return nil
}

// No additional types; we use wire.Result directly for now.
