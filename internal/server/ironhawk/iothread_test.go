package ironhawk

import "testing"

func TestDurableRequested(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"no_flags", []string{"k", "v"}, false},
		{"durable_upper", []string{"k", "v", "DURABLE"}, true},
		{"durable_mixed", []string{"k", "v", "durAble"}, true},
		{"sync_upper", []string{"k", "v", "SYNC"}, true},
		{"sync_mixed", []string{"k", "v", "SyNc"}, true},
		{"other_tokens", []string{"k", "v", "XX"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := durableRequested(tc.args); got != tc.want {
				t.Fatalf("durableRequested(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}
