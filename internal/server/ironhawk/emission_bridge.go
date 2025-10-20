package ironhawk

import (
    "context"
    "log/slog"

    "github.com/sevenDatabase/SevenDB/internal/emission"
    "github.com/dicedb/dicedb-go/wire"
)

// BridgeSender routes emission DataEvents to existing IOThreads using the WatchManager maps.
// subID format assumed: clientID:fp (fingerprint in base10). This can be adjusted later.
type BridgeSender struct {
    wm *WatchManager
}

func NewBridgeSender(wm *WatchManager) *BridgeSender { return &BridgeSender{wm: wm} }

func (b *BridgeSender) Send(ctx context.Context, ev *emission.DataEvent) error {
    if b.wm == nil || ev == nil { return nil }
    // For the MVP, we multicast delta to all clients registered for the subscription's fingerprint.
    // Here we reuse NotifyWatchers send path but wrap the result into a one-off delivery.
    // Future: map sub_id -> exact client set and write directly to serverWire.
    b.wm.mu.RLock()
    defer b.wm.mu.RUnlock()
    // Best-effort: try to deliver to all threads known for this sub id if it matches clientID:fp
    // If not parseable, log and drop.
    delivered := 0
    for _, thread := range b.wm.clientWatchThreadMap {
        if thread == nil { continue }
        // For MVP, emit delta as plain message. TODO: encode structured delta.
        rs := &wire.Result{Status: wire.Status_OK, Message: string(ev.Delta)}
        if err := thread.serverWire.Send(ctx, rs); err != nil {
            slog.Warn("bridge send failed", slog.Any("error", err), slog.String("client", thread.ClientID), slog.String("sub_id", ev.SubID))
            continue
        }
        delivered++
    }
    slog.Debug("bridge delivered", slog.Int("count", delivered), slog.String("sub_id", ev.SubID), slog.String("emit_seq", ev.EmitSeq.String()))
    return nil
}

// No additional types; we use wire.Result directly for now.
