package determinism

import (
    "sort"
    "strconv"
    "strings"

    "github.com/cespare/xxhash/v2"
)

// Emission describes a single canonicalized emission/event used for determinism checks.
// Keep this narrow and stable across tests.
type Emission struct {
    Fingerprint uint64            // stable subscription identity
    EmitSeq     uint64            // per-subscription monotonic sequence (if available)
    Event       string            // e.g., WATCH_ACK, DATA, SNAPSHOT
    Fields      map[string]string // domain payload fields (sorted when encoded)
}

// CanonicalLine returns a stable, byte-identical representation for hashing.
func CanonicalLine(e Emission) []byte {
    keys := make([]string, 0, len(e.Fields))
    for k := range e.Fields {
        keys = append(keys, k)
    }
    sort.Strings(keys)

    var b strings.Builder
    // Avoid spaces/locale differences; keep ASCII and explicit separators.
    b.WriteString(strconv.FormatUint(e.Fingerprint, 10))
    b.WriteByte('|')
    b.WriteString(strconv.FormatUint(e.EmitSeq, 10))
    b.WriteByte('|')
    b.WriteString(e.Event)
    for _, k := range keys {
        b.WriteByte('|')
        b.WriteString(k)
        b.WriteByte('=')
        b.WriteString(e.Fields[k])
    }
    return []byte(b.String())
}

// Hash64 computes a stable 64-bit hash of the canonical line.
func Hash64(e Emission) uint64 { return xxhash.Sum64(CanonicalLine(e)) }
