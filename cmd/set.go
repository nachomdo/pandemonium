package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var setCommand = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("set command with %v \n", args)
	},
	Use:   "set [key] [value]",
	Short: "Set the value of a string key to a string",
}
