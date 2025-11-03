package ironhawk

import (
	"strconv"
	"testing"
	"time"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
)

// Two clients watching the same key should receive identical emission sequences.
func TestEmissionContract_TwoClients_SameKey_IdenticalSequence(t *testing.T) {
	// Enable emission-contract and stub raft for single shard
	config.Config.RaftEnabled = true
	config.Config.RaftEngine = "stub"
	config.Config.EmissionContractEnabled = true
	config.Config.EnableWatch = true
	config.Config.EmissionNotifierPollMs = 2

	RunTestServer(nil)
	time.Sleep(150 * time.Millisecond)

	pub := getLocalSdk()
	defer pub.Close()

	// Watcher A
	wa := getLocalSdk()
	defer wa.Close()
	if r := wa.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"wa", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watcher A handshake failed: %+v", r)
	}
	subResA := wa.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"mc"}})
	if subResA.Status != wire.Status_OK || subResA.Fingerprint64 == 0 {
		t.Fatalf("watcher A GET.WATCH failed or missing fp: %+v", subResA)
	}
	subIDA := "wa:" + strconv.FormatUint(subResA.Fingerprint64, 10)

	// Watcher B
	wb := getLocalSdk()
	defer wb.Close()
	if r := wb.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"wb", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watcher B handshake failed: %+v", r)
	}
	subResB := wb.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"mc"}})
	if subResB.Status != wire.Status_OK || subResB.Fingerprint64 == 0 {
		t.Fatalf("watcher B GET.WATCH failed or missing fp: %+v", subResB)
	}
	subIDB := "wb:" + strconv.FormatUint(subResB.Fingerprint64, 10)

	// Produce two updates
	if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"mc", "v1"}}); r.Status != wire.Status_OK {
		t.Fatalf("SET v1 failed: %+v", r)
	}
	if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"mc", "v2"}}); r.Status != wire.Status_OK {
		t.Fatalf("SET v2 failed: %+v", r)
	}

	// Collect sequences from both watchers
	seqA := make([]string, 0, 2)
	seqB := make([]string, 0, 2)
	deadline := time.Now().Add(2 * time.Second)
	for (len(seqA) < 2 || len(seqB) < 2) && time.Now().Before(deadline) {
		if len(seqA) < 2 {
			res := wa.Fire(&wire.Command{Cmd: "PING"})
			if res.Message == "v1" || res.Message == "v2" {
				// de-dup consecutive same message just in case
				if len(seqA) == 0 || seqA[len(seqA)-1] != res.Message {
					seqA = append(seqA, res.Message)
				}
			} else {
				// small backoff to yield when no emission pending
				time.Sleep(10 * time.Millisecond)
			}
		}
		if len(seqB) < 2 {
			res := wb.Fire(&wire.Command{Cmd: "PING"})
			if res.Message == "v1" || res.Message == "v2" {
				if len(seqB) == 0 || seqB[len(seqB)-1] != res.Message {
					seqB = append(seqB, res.Message)
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	if len(seqA) != 2 || len(seqB) != 2 {
		t.Fatalf("did not receive two emissions on both watchers. A=%v B=%v", seqA, seqB)
	}
	if seqA[0] != seqB[0] || seqA[1] != seqB[1] {
		t.Fatalf("watchers received different sequences. A=%v B=%v", seqA, seqB)
	}

	// ACK up to 2 for both (best-effort; commit indexes align to order under stub raft)
	_ = wa.Fire(&wire.Command{Cmd: "EMITACK", Args: []string{"mc", subIDA, "2"}})
	_ = wb.Fire(&wire.Command{Cmd: "EMITACK", Args: []string{"mc", subIDB, "2"}})
}
