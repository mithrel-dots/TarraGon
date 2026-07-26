package cli

import (
	"github.com/mithrel-dots/tarragon/internal/daemon"
	"github.com/spf13/cobra"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run the Tarragon daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		return daemon.RunDaemon()
	},
}

func init() {
	rootCmd.AddCommand(daemonCmd)
}
