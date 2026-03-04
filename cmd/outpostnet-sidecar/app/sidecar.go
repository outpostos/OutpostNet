// Package app implements the sidecar command.
package app

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewSidecarCommand creates the `outpostnet-sidecar` command.
func NewSidecarCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "outpostnet-sidecar",
		Short: "OutpostNet pod-level sidecar",
		Long:  "OutpostNet Sidecar runs alongside application containers, exporting local services and importing external services via Unix domain socket.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSidecar()
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

func runSidecar() error {
	v := version.Get()
	fmt.Printf("outpostnet-sidecar %s starting...\n", v.GitVersion)
	fmt.Println("Sidecar not yet implemented.")
	return nil
}
