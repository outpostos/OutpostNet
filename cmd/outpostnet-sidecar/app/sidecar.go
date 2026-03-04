// Package app implements the sidecar command.
package app

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/internal/sidecar"
	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewSidecarCommand creates the `outpostnet-sidecar` command.
func NewSidecarCommand() *cobra.Command {
	cfg := sidecar.DefaultConfig()

	cmd := &cobra.Command{
		Use:   "outpostnet-sidecar",
		Short: "OutpostNet pod-level sidecar",
		Long:  "OutpostNet Sidecar runs alongside application containers, exporting local services and importing external services via Unix domain socket.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSidecar(cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.ServerSocket, "server-socket", cfg.ServerSocket, "Server Unix socket path")
	cmd.Flags().StringVar(&cfg.SidecarID, "sidecar-id", cfg.SidecarID, "Unique sidecar identifier")
	cmd.Flags().DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", cfg.HeartbeatInterval, "Heartbeat send interval")
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

func runSidecar(cfg sidecar.Config) error {
	v := version.Get()
	log.Printf("outpostnet-sidecar %s starting...", v.GitVersion)
	log.Printf("server-socket: %s, sidecar-id: %s", cfg.ServerSocket, cfg.SidecarID)

	if cfg.SidecarID == "" {
		hostname, _ := os.Hostname()
		cfg.SidecarID = hostname
		log.Printf("using hostname as sidecar-id: %s", cfg.SidecarID)
	}

	sc := sidecar.New(cfg)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sc.Run()
	}()

	select {
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down...", sig)
		sc.Stop()
		time.Sleep(100 * time.Millisecond)
		return nil
	case err := <-errCh:
		return err
	}
}
