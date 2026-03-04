// Package app implements the server command.
package app

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewServerCommand creates the `outpostnet-server` command.
func NewServerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "outpostnet-server",
		Short: "OutpostNet node-level server",
		Long:  "OutpostNet Server runs on each edge node, managing sidecar registration and service routing via Unix domain socket.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer()
		},
	}

	cmd.AddCommand(newVersionCommand())

	return cmd
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.Get())
		},
	}
}

func runServer() error {
	v := version.Get()
	fmt.Printf("outpostnet-server %s starting...\n", v.GitVersion)
	fmt.Println("Server not yet implemented.")
	return nil
}
