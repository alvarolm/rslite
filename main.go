// main.go
package main

import (
	"fmt"
	"os"

	"github.com/alvarolm/rslite/sync"
	"github.com/spf13/cobra"
)

const ExampleUsage = `
  # Sync all tables from source to target
  rslite source.db target.db

  # Sync only specific tables
  rslite source.db target.db -t users,orders

  # Sync using "Primary Key" filters (sync records with "Primary Key" > 100)
  rslite source.db target.db -f gt -p 100

  # Sync specific tables without deleting existing records
  rslite source.db target.db -t users,orders -n

  # Complex sync with filters and specific tables
  rslite source.db target.db -t users,orders -f gte -p 1000 -n`

func main() {
	var cfg sync.Config

	rootCmd := &cobra.Command{
		Version: "v0.0.1",
		Use:     `syncs [source db] [target db]`,
		Short:   "sqlite row based synchronization for local dbs",
		Long:    "sqlite row based synchronization for local dbs",
		Example: ExampleUsage,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.SrcDbPath = args[0]
			cfg.DstDbPath = args[1]
			return sync.Sync(cfg)
		},
	}

	flags := rootCmd.Flags()
	flags.StringVarP(&cfg.Filter, "filter", "f", "", "filter type: gt, lt, gte, or lte")
	flags.StringVarP(&cfg.Value, "value", "v", "", "filter value")
	flags.BoolVarP(&cfg.NoDelete, "nodelete", "n", false, "don't delete records from target")
	flags.StringSliceVarP(&cfg.Tables, "tables", "t", nil, "tables to sync (comma-separated)")

	// Custom error handling
	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, rootCmd.Short)
		fmt.Fprintln(os.Stderr)
		rootCmd.Usage()
		os.Exit(1)
	}

}
