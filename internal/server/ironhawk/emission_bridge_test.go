package ironhawk

import (
    "context"
    "strings"
    "testing"

    "github.com/sevenDatabase/SevenDB/internal/emission"
)

func TestBridge_NoActiveThreadError(t *testing.T) {
    wm := NewWatchManager()
    b := NewBridgeSender(wm)
    // SubID encodes clientID:fp; but no thread registered for client
    err := b.Send(context.Background(), &emission.DataEvent{SubID: "missingClient:123", EmitSeq: emission.EmitSeq{CommitIndex: 1}, Delta: []byte("x")})
    if err == nil || !strings.Contains(err.Error(), "no active thread") {
        t.Fatalf("expected no active thread error, got %v", err)
    }
}

func TestBridge_NoRecipientsErrorOnBroadcast(t *testing.T) {
    wm := NewWatchManager()
    b := NewBridgeSender(wm)
    // No colon in SubID triggers broadcast path; with no clients registered, expect error
    err := b.Send(context.Background(), &emission.DataEvent{SubID: "broadcast", EmitSeq: emission.EmitSeq{CommitIndex: 2}, Delta: []byte("y")})
    if err == nil || !strings.Contains(err.Error(), "no recipients") {
        t.Fatalf("expected no recipients error, got %v", err)
    }
}
