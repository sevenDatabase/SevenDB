package ironhawk

import (
	"testing"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/internal/cmd"
)

// TestFingerprintStability ensures watch fingerprints depend only on the command text
// and remain stable across client/session differences and restarts.
func TestFingerprintStability(t *testing.T) {
	makeCmd := func(args ...string) *cmd.Cmd {
		return &cmd.Cmd{C: &wire.Command{Cmd: "GET.WATCH", Args: args}}
	}
	a := makeCmd("k1")
	b := makeCmd("k1")
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("fingerprint mismatch for identical commands: %d vs %d", a.Fingerprint(), b.Fingerprint())
	}

	// Changing client metadata must not affect fingerprint
	c := makeCmd("k1")
	c.ClientID = "client-xyz"
	c.Mode = "watch"
	if a.Fingerprint() != c.Fingerprint() {
		t.Fatalf("fingerprint changed with client metadata: %d vs %d", a.Fingerprint(), c.Fingerprint())
	}

	// Different args -> different fingerprint
	d := makeCmd("k2")
	if a.Fingerprint() == d.Fingerprint() {
		t.Fatalf("fingerprint collision for different commands: %d", a.Fingerprint())
	}
}
