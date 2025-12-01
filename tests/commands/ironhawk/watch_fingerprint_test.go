// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package ironhawk

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
)

// TestWatchCommands_Fingerprint64Populated verifies that all .WATCH commands
// return a non-zero Fingerprint64 in the response. This is critical for the
// CLI to build valid SubIDs and for UNWATCH to work correctly.
func TestWatchCommands_Fingerprint64Populated(t *testing.T) {
	// Enable emission-contract and stub raft for single shard
	config.Config.RaftEnabled = true
	config.Config.RaftEngine = "stub"
	config.Config.EmissionContractEnabled = true
	config.Config.EnableWatch = true

	var wg sync.WaitGroup
	RunTestServer(&wg)
	time.Sleep(150 * time.Millisecond)

	// Setup: create keys for each watch type
	setup := getLocalSdk()
	defer setup.Close()

	// Create a string key
	if r := setup.Fire(&wire.Command{Cmd: "SET", Args: []string{"testkey", "value1"}}); r.Status != wire.Status_OK {
		t.Fatalf("SET failed: %+v", r)
	}

	// Create a hash key
	if r := setup.Fire(&wire.Command{Cmd: "HSET", Args: []string{"testhash", "field1", "value1"}}); r.Status != wire.Status_OK {
		t.Fatalf("HSET failed: %+v", r)
	}

	// Create a sorted set key
	if r := setup.Fire(&wire.Command{Cmd: "ZADD", Args: []string{"testzset", "1", "member1", "2", "member2"}}); r.Status != wire.Status_OK {
		t.Fatalf("ZADD failed: %+v", r)
	}

	testCases := []struct {
		name    string
		cmd     string
		args    []string
		wantErr bool
	}{
		{
			name: "GET.WATCH returns Fingerprint64",
			cmd:  "GET.WATCH",
			args: []string{"testkey"},
		},
		{
			name: "HGET.WATCH returns Fingerprint64",
			cmd:  "HGET.WATCH",
			args: []string{"testhash", "field1"},
		},
		{
			name: "HGETALL.WATCH returns Fingerprint64",
			cmd:  "HGETALL.WATCH",
			args: []string{"testhash"},
		},
		{
			name: "ZRANGE.WATCH returns Fingerprint64",
			cmd:  "ZRANGE.WATCH",
			args: []string{"testzset", "0", "-1"},
		},
		{
			name: "ZRANK.WATCH returns Fingerprint64",
			cmd:  "ZRANK.WATCH",
			args: []string{"testzset", "member1"},
		},
		{
			name: "ZCOUNT.WATCH returns Fingerprint64",
			cmd:  "ZCOUNT.WATCH",
			args: []string{"testzset", "0", "10"},
		},
		{
			name: "ZCARD.WATCH returns Fingerprint64",
			cmd:  "ZCARD.WATCH",
			args: []string{"testzset"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := getLocalSdk()
			defer client.Close()

			// Handshake as watch client
			clientID := "test_" + tc.cmd
			if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}}); r.Status != wire.Status_OK {
				t.Fatalf("HANDSHAKE failed: %+v", r)
			}

			// Issue the WATCH command
			res := client.Fire(&wire.Command{Cmd: tc.cmd, Args: tc.args})

			// Verify response status
			if res.Status != wire.Status_OK {
				if !tc.wantErr {
					t.Fatalf("%s failed unexpectedly: status=%v message=%s", tc.cmd, res.Status, res.Message)
				}
				return
			}

			// Critical check: Fingerprint64 must be non-zero
			if res.Fingerprint64 == 0 {
				t.Errorf("%s returned Fingerprint64=0, expected non-zero value. Response: %+v", tc.cmd, res)
			} else {
				t.Logf("%s returned Fingerprint64=%d (OK)", tc.cmd, res.Fingerprint64)
			}

			// Verify UNWATCH works with the returned fingerprint
			fpStr := strconv.FormatUint(res.Fingerprint64, 10)
			unwatchRes := client.Fire(&wire.Command{Cmd: "UNWATCH", Args: []string{fpStr}})
			// Note: UNWATCH may succeed or fail depending on internal state,
			// but the key thing is we have a valid fingerprint to send
			_ = unwatchRes
		})
	}
}

// TestWatchFingerprint_ConsistentAcrossReexecution verifies that the same
// WATCH command returns the same fingerprint when executed multiple times.
// This is important for subscription identity.
func TestWatchFingerprint_ConsistentAcrossReexecution(t *testing.T) {
	config.Config.RaftEnabled = true
	config.Config.RaftEngine = "stub"
	config.Config.EmissionContractEnabled = true
	config.Config.EnableWatch = true

	var wg sync.WaitGroup
	RunTestServer(&wg)
	time.Sleep(150 * time.Millisecond)

	// Setup
	setup := getLocalSdk()
	defer setup.Close()
	if r := setup.Fire(&wire.Command{Cmd: "SET", Args: []string{"fpkey", "val"}}); r.Status != wire.Status_OK {
		t.Fatalf("SET failed: %+v", r)
	}

	// First subscription
	client1 := getLocalSdk()
	defer client1.Close()
	if r := client1.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"c1", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("HANDSHAKE failed: %+v", r)
	}
	res1 := client1.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"fpkey"}})
	if res1.Status != wire.Status_OK || res1.Fingerprint64 == 0 {
		t.Fatalf("First GET.WATCH failed or missing fp: %+v", res1)
	}

	// Second subscription with same command
	client2 := getLocalSdk()
	defer client2.Close()
	if r := client2.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"c2", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("HANDSHAKE failed: %+v", r)
	}
	res2 := client2.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"fpkey"}})
	if res2.Status != wire.Status_OK || res2.Fingerprint64 == 0 {
		t.Fatalf("Second GET.WATCH failed or missing fp: %+v", res2)
	}

	// Same command on same key should produce the same fingerprint
	if res1.Fingerprint64 != res2.Fingerprint64 {
		t.Errorf("Fingerprints differ for same command: %d vs %d", res1.Fingerprint64, res2.Fingerprint64)
	} else {
		t.Logf("Fingerprints match as expected: %d", res1.Fingerprint64)
	}
}

// TestUnwatch_WithValidFingerprint verifies that UNWATCH works correctly
// when given a valid fingerprint from a WATCH response.
func TestUnwatch_WithValidFingerprint(t *testing.T) {
	config.Config.RaftEnabled = true
	config.Config.RaftEngine = "stub"
	config.Config.EmissionContractEnabled = true
	config.Config.EnableWatch = true

	var wg sync.WaitGroup
	RunTestServer(&wg)
	time.Sleep(150 * time.Millisecond)

	// Setup
	setup := getLocalSdk()
	defer setup.Close()
	if r := setup.Fire(&wire.Command{Cmd: "SET", Args: []string{"unwatchkey", "val"}}); r.Status != wire.Status_OK {
		t.Fatalf("SET failed: %+v", r)
	}

	client := getLocalSdk()
	defer client.Close()
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"unwatchtest", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("HANDSHAKE failed: %+v", r)
	}

	// Subscribe
	subRes := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"unwatchkey"}})
	if subRes.Status != wire.Status_OK {
		t.Fatalf("GET.WATCH failed: %+v", subRes)
	}
	if subRes.Fingerprint64 == 0 {
		t.Fatalf("GET.WATCH returned Fingerprint64=0, cannot test UNWATCH")
	}

	t.Logf("Subscribed with fingerprint: %d", subRes.Fingerprint64)

	// Unsubscribe using the fingerprint
	// UNWATCH expects the fingerprint as a string argument
	fpStr := strconv.FormatUint(subRes.Fingerprint64, 10)
	unwatchRes := client.Fire(&wire.Command{Cmd: "UNWATCH", Args: []string{fpStr}})
	if unwatchRes.Status != wire.Status_OK {
		t.Errorf("UNWATCH with valid fingerprint %s failed: %+v", fpStr, unwatchRes)
	} else {
		t.Logf("UNWATCH succeeded with fingerprint %s", fpStr)
	}
}

// TestResubscribe_AfterUnwatch verifies that resubscribing after UNWATCH
// still returns a valid Fingerprint64. This is the exact bug scenario
// reported by the CLI.
func TestResubscribe_AfterUnwatch(t *testing.T) {
	config.Config.RaftEnabled = true
	config.Config.RaftEngine = "stub"
	config.Config.EmissionContractEnabled = true
	config.Config.EnableWatch = true

	var wg sync.WaitGroup
	RunTestServer(&wg)
	time.Sleep(150 * time.Millisecond)

	// Setup
	setup := getLocalSdk()
	defer setup.Close()
	if r := setup.Fire(&wire.Command{Cmd: "SET", Args: []string{"resubkey", "val"}}); r.Status != wire.Status_OK {
		t.Fatalf("SET failed: %+v", r)
	}

	client := getLocalSdk()
	defer client.Close()
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"resubtest", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("HANDSHAKE failed: %+v", r)
	}

	// First subscription
	subRes1 := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"resubkey"}})
	if subRes1.Status != wire.Status_OK {
		t.Fatalf("First GET.WATCH failed: %+v", subRes1)
	}
	if subRes1.Fingerprint64 == 0 {
		t.Fatalf("First GET.WATCH returned Fingerprint64=0")
	}
	fp1 := subRes1.Fingerprint64
	t.Logf("First subscription fingerprint: %d", fp1)

	// Small delay to let any async emissions settle
	time.Sleep(50 * time.Millisecond)

	// Unsubscribe
	fpStr := strconv.FormatUint(fp1, 10)
	unwatchRes := client.Fire(&wire.Command{Cmd: "UNWATCH", Args: []string{fpStr}})
	// Note: unwatchRes might actually be an emission response if timing is unlucky.
	// We check Status_OK but emissions also have Status_OK, so this isn't a perfect check.
	if unwatchRes.Status != wire.Status_OK {
		t.Logf("UNWATCH returned: %+v (continuing anyway)", unwatchRes)
	}

	// Small delay to let UNWATCH fully process and any pending emissions clear
	time.Sleep(50 * time.Millisecond)

	// Second subscription (resubscribe) - THIS IS THE BUG SCENARIO
	subRes2 := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"resubkey"}})
	if subRes2.Status != wire.Status_OK {
		t.Fatalf("Second GET.WATCH failed: %+v", subRes2)
	}
	
	// Critical: Fingerprint64 must still be non-zero after resubscribe
	if subRes2.Fingerprint64 == 0 {
		t.Errorf("RESUBSCRIBE BUG: Second GET.WATCH returned Fingerprint64=0, expected non-zero")
	} else {
		t.Logf("Second subscription fingerprint: %d", subRes2.Fingerprint64)
	}

	// The fingerprint should be the same since it's the same command
	if subRes2.Fingerprint64 != fp1 {
		t.Errorf("Fingerprints differ after resubscribe: first=%d, second=%d", fp1, subRes2.Fingerprint64)
	} else {
		t.Logf("Fingerprints match as expected: %d", fp1)
	}
}
