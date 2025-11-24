package logging

import (
    "log/slog"
    "os"
    "strings"
    "sync"
)

var (
    mu sync.RWMutex
    tags map[string]bool
)

func init() {
    tags = make(map[string]bool)
    if v := os.Getenv("LOG_TAGS"); v != "" {
        for _, t := range strings.Split(v, ",") {
            t = strings.TrimSpace(t)
            if t != "" {
                tags[t] = true
            }
        }
    }
}

// VerboseEnabled returns true if the given tag is enabled via LOG_TAGS.
func VerboseEnabled(tag string) bool {
    mu.RLock()
    defer mu.RUnlock()
    return tags[tag]
}

// Enable turns on a tag at runtime.
func Enable(tag string) {
    if tag == "" {
        return
    }
    mu.Lock()
    tags[tag] = true
    mu.Unlock()
}

// EnableMany enables a comma-separated list of tags at runtime.
func EnableMany(csv string) {
    for _, t := range strings.Split(csv, ",") {
        Enable(strings.TrimSpace(t))
    }
}

// VInfo logs an Info message only when the tag is enabled.
// It forwards to slog.Info so callers can pass structured attributes.
func VInfo(tag string, msg string, attrs ...slog.Attr) {
    if !VerboseEnabled(tag) {
        return
    }
    // convert slog.Attr to any pairs accepted by slog.Info
    if len(attrs) == 0 {
        slog.Info(msg)
        return
    }
    pairs := make([]any, 0, len(attrs)*2)
    for _, a := range attrs {
        pairs = append(pairs, a.Key)
        pairs = append(pairs, a.Value.Any())
    }
    slog.Info(msg, pairs...)
}
