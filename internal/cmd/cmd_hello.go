// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/errors"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	dstore "github.com/sevenDatabase/SevenDB/internal/store"
)

// HELLO returns server and connection properties for the ironhawk command path.
// This mirrors the legacy eval HELLO response and adds client_id for the current connection.
var cHELLO = &CommandMeta{
	Name:      "HELLO",
	Syntax:    "HELLO [proto]",
	HelpShort: "HELLO returns server and connection properties (proto, id, role, modules, client_id)",
	HelpLong: `HELLO replies with a JSON object describing the server and connection.
Fields: proto, id (host:port), role, modules (array), client_id.
The optional argument is currently ignored and kept for compatibility.`,
	Examples: `HELLO\n{"proto":2,"id":"127.0.0.1:7379","role":"master","modules":[],"client_id":"abcd-1234"}`,
	Eval:     evalHELLOCmd,
	Execute:  executeHELLOCmd,
}

func init() {
	CommandRegistry.AddCommand(cHELLO)
}

func evalHELLOCmd(c *Cmd, s *dstore.Store) (*CmdRes, error) {
	// Accept 0 or 1 argument for compatibility; error otherwise
	if len(c.C.Args) > 1 {
		return &CmdRes{Rs: &wire.Result{Status: wire.Status_ERR, Message: errors.ErrWrongArgumentCount("HELLO").Error()}}, nil
	}
	host := "127.0.0.1"
	port := 7379
	if config.Config != nil {
		if config.Config.Host != "" {
			host = config.Config.Host
		}
		if config.Config.Port != 0 {
			port = config.Config.Port
		}
	}
	// If no client id is registered yet for this connection, assign one now.
	if c.ClientID == "" {
		c.ClientID = genUUID()
	}
	// Include multiple synonymous keys for client id to maximize compatibility with clients:
	// - "clientId" (preferred by modern clients)
	// - "client_id" and "client" as fallbacks
	resp := map[string]interface{}{
		"proto":         2,
		"id":            fmt.Sprintf("%s:%d", host, port),
		"role":          "master",
		"modules":       []interface{}{},
		"clientId":      c.ClientID,
		"client_id":     c.ClientID,
		"client":        c.ClientID,
		"serverVersion": config.DiceDBVersion,
		"version":       config.DiceDBVersion,
	}
	b, _ := json.Marshal(resp)
	return &CmdRes{Rs: &wire.Result{Status: wire.Status_OK, Message: string(b)}}, nil
}

func executeHELLOCmd(c *Cmd, sm *shardmanager.ShardManager) (*CmdRes, error) {
	shard := sm.GetShardForKey("-")
	return evalHELLOCmd(c, shard.Thread.Store())
}

func genUUID() string {
	// Generate 16 random bytes and format as UUID v4-like string
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		// Fallback to hex timestamp-like if random fails
		return hex.EncodeToString([]byte("fallback"))
	}
	// Set version (4) and variant (RFC 4122) bits
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	// Format 8-4-4-4-12
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}
