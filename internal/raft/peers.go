package raft

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ParsePeerSpecs parses a slice of peer specifications into a map[id]addr and returns
// the ordered slice of canonical specs (id@addr). Accepted formats:
//
//	id@host:port   (preferred explicit form)
//	host:port      (implicit; ids assigned deterministically by sorted order starting at 1)
//
// Mixing explicit and implicit forms is allowed; explicit IDs take precedence; remaining
// unassigned addresses are given the next available ascending IDs.
func ParsePeerSpecs(specs []string) (map[uint64]string, []string, error) {
	addrs := make(map[uint64]string)
	explicit := make(map[string]uint64)
	var implicit []string
	used := map[uint64]struct{}{}
	for _, s := range specs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if at := strings.IndexByte(s, '@'); at != -1 {
			idPart := s[:at]
			addr := s[at+1:]
			id, err := strconv.ParseUint(idPart, 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid peer id '%s' in '%s'", idPart, s)
			}
			if addr == "" {
				return nil, nil, fmt.Errorf("empty addr in peer spec '%s'", s)
			}
			if _, exists := addrs[id]; exists {
				return nil, nil, fmt.Errorf("duplicate peer id %d", id)
			}
			addrs[id] = addr
			explicit[addr] = id
			used[id] = struct{}{}
		} else {
			implicit = append(implicit, s)
		}
	}
	sort.Strings(implicit)
	// Assign IDs to implicit addresses in order.
	nextID := uint64(1)
	for _, addr := range implicit {
		if _, dup := explicit[addr]; dup {
			continue
		}
		for {
			if _, taken := used[nextID]; !taken {
				break
			}
			nextID++
		}
		addrs[nextID] = addr
		used[nextID] = struct{}{}
	}
	// Produce canonical specs id@addr
	ids := make([]uint64, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	canonical := make([]string, 0, len(ids))
	for _, id := range ids {
		canonical = append(canonical, fmt.Sprintf("%d@%s", id, addrs[id]))
	}
	return addrs, canonical, nil
}

// DetermineLocalID resolves the local raft node ID.
// Precedence:
//  1. If cfgNodeID (string) is provided and parses to a known ID, use it.
//  2. If advertiseAddr matches one of the peer addrs, use that ID.
//  3. If only one peer exists, use its ID.
//  4. Otherwise error.
func DetermineLocalID(cfgNodeID string, advertiseAddr string, peers map[uint64]string) (uint64, error) {
	if cfgNodeID != "" {
		v, err := strconv.ParseUint(cfgNodeID, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid raft-node-id '%s'", cfgNodeID)
		}
		if _, ok := peers[v]; ok {
			return v, nil
		}
		return 0, fmt.Errorf("raft-node-id %d not present in peer set", v)
	}
	if advertiseAddr != "" {
		for id, addr := range peers {
			if addr == advertiseAddr {
				return id, nil
			}
		}
	}
	if len(peers) == 1 {
		for id := range peers {
			return id, nil
		}
	}
	return 0, fmt.Errorf("could not determine local raft node id; specify raft-node-id")
}
