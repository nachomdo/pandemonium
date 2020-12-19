package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var rmCommand = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("rm command with %v \n", args)
	},
	Use:   "rm [key]",
	Short: "Remove a given key",
}
