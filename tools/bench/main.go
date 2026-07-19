// Command bench runs TarraGon's development benchmarks from a checkout.
package main

import (
	"os"
	"time"

	"github.com/mithrel-dots/tarragon/internal/bench"
	"github.com/mithrel-dots/tarragon/internal/bench/ipc"
	"github.com/mithrel-dots/tarragon/internal/wire"
	"github.com/spf13/cobra"
)

func main() {
	var plugins bench.Options
	benchCmd := &cobra.Command{
		Use:          "bench",
		Short:        "Benchmark installed plugins through the daemon UI socket",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return bench.Run(plugins, cmd.OutOrStdout())
		},
	}
	benchCmd.CompletionOptions.DisableDefaultCmd = true
	benchCmd.Flags().IntVar(&plugins.RandomInputs, "random", 100, "Number of random inputs")
	benchCmd.Flags().IntVar(&plugins.Iterations, "iterations", 1, "Runs per input")
	benchCmd.Flags().DurationVar(&plugins.Timeout, "timeout", 2*time.Second, "Timeout per query")
	benchCmd.Flags().Int64Var(&plugins.Seed, "seed", 0, "Random seed (default: time-based)")
	benchCmd.Flags().Float64Var(&plugins.WorstPct, "worst-pct", 99, "Percentile for latency reporting")
	benchCmd.Flags().StringVar(&plugins.Socket, "socket", wire.ResolveUISocketPath(), "Daemon UI Unix socket path")
	var opts ipc.Options
	ipcCmd := &cobra.Command{
		Use:   "ipc",
		Short: "Measure plugin-to-core and core-to-UI suggestion delivery on isolated local transports",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return ipc.Run(cmd.Context(), opts, cmd.OutOrStdout())
		},
	}
	ipcCmd.Flags().IntVar(&opts.Samples, "samples", 2000, "Measured queries per route per round (one suggestion per plugin per query)")
	ipcCmd.Flags().IntVar(&opts.Warmup, "warmup", 200, "Unmeasured queries before each route")
	ipcCmd.Flags().IntVar(&opts.Plugins, "plugins", 1, "Number of persistent fixture plugin processes responding to every query")
	ipcCmd.Flags().IntVar(&opts.Burst, "burst", 1, "Queries sent back-to-back before waiting for all results (1-64)")
	ipcCmd.Flags().IntVar(&opts.Rounds, "rounds", 3, "Number of rounds, rotating the route order")
	ipcCmd.Flags().DurationVar(&opts.Timeout, "timeout", 3*time.Second, "Deadline per fixture I/O operation")
	ipcCmd.Flags().BoolVar(&opts.Matrix, "matrix", false, "Also exercise all combinations of native and UDS-adapted broker clients")
	ipcCmd.AddCommand(&cobra.Command{
		Use:    "fixture",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return ipc.RunFixture(cmd.InOrStdin(), cmd.OutOrStdout())
		},
	})
	benchCmd.AddCommand(ipcCmd)
	if err := benchCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
