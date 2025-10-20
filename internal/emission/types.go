package emission

import "fmt"

// EpochID identifies the logical epoch of a bucket.
// For MVP we derive BucketUUID from the provided bucket/shard id string.
type EpochID struct {
    BucketUUID   string
    EpochCounter uint64
}

// EmitSeq provides a globally unique, totally ordered position for an emission.
type EmitSeq struct {
    Epoch       EpochID
    CommitIndex uint64
}

func (e EmitSeq) String() string {
    return fmt.Sprintf("(%s,%d,%d)", e.Epoch.BucketUUID, e.Epoch.EpochCounter, e.CommitIndex)
}

// DataEvent is the message sent to clients.
type DataEvent struct {
    SubID   string
    EmitSeq EmitSeq
    Delta   []byte
}

// ClientAck represents an acknowledgment from client side.
type ClientAck struct {
    SubID   string
    EmitSeq EmitSeq
}

// ReconnectRequest/Response are server-side types mirroring planned protobufs.
type ReconnectRequest struct {
    SubID                 string
    LastProcessedEmitSeq  EmitSeq
}

type ReconnectStatus int

const (
    ReconnectOK ReconnectStatus = iota
    ReconnectStaleSequence
    ReconnectInvalidSequence
    ReconnectSubscriptionNotFound
)

type ReconnectAck struct {
    Status           ReconnectStatus
    CurrentEpoch     EpochID
    NextCommitIndex  uint64
}
