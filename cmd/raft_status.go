package cmd

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	"github.com/spf13/cobra"
)

var raftStatusCmd = &cobra.Command{
	Use:   "raft-status",
	Short: "Print raft status for all shards (JSON) including transport peer connectivity (run against a running sevendb instance).",
	RunE: func(cmd *cobra.Command, args []string) error {
		if config.Config == nil { // load flags to populate config for uniform behavior
			config.Load(cmd.Flags())
		}
		if config.Config == nil || !config.Config.RaftEnabled {
			fmt.Println(`{"raft_enabled":false}`)
			return nil
		}
		sm := shardmanager.GetGlobal()
		if sm == nil { // Separate process invocation; we cannot access live state.
			fmt.Println(`{"error":"no running server process; start sevendb normally to populate live status"}`)
			return nil
		}
		snapshots := sm.RaftStatusSnapshots()
		b, err := json.MarshalIndent(snapshots, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(raftStatusCmd)
	slog.Debug("raft-status command registered")
}
