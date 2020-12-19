package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var getCommand = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("get command with %v \n", args)
	},
	Use:   `get <KEY>`,
	Short: "Get the value of a string key to a string",
}
