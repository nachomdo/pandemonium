package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var rootCommand = &cobra.Command{
	Use:   "kvs [options] [commands]",
	Short: "Operates over a KV store",
	Run: func(cmd *cobra.Command, args []string) {
		if b, _ := cmd.Flags().GetBool("version"); b {
			fmt.Println("version 1.0.0")
		}
	},
}

func init() {
	var verbose bool
	rootCommand.AddCommand(getCommand)
	rootCommand.AddCommand(setCommand)
	rootCommand.AddCommand(rmCommand)
	rootCommand.Flags().BoolVarP(&verbose, "version", "V", false, "version")
}

//Execute root command entrypoint
func Execute() error {

	return rootCommand.Execute()
}
