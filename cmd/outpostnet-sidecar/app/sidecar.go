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
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewSidecarCommand creates the `outpostnet-sidecar` command.
func NewSidecarCommand() *cobra.Command {
	var configFile string
	cfg := sidecar.DefaultConfig()

	cmd := &cobra.Command{
		Use:   "outpostnet-sidecar",
		Short: "OutpostNet pod-level sidecar",
		Long:  "OutpostNet Sidecar runs alongside application containers, exporting local services and importing external services via Unix domain socket.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configFile != "" {
				fileCfg, err := config.LoadSidecarConfig(configFile)
				if err != nil {
					return fmt.Errorf("load config: %w", err)
				}
				cfg.ServerSocket = fileCfg.Server
				if fileCfg.SidecarID != "" {
					cfg.SidecarID = fileCfg.SidecarID
				}
				if fileCfg.HeartbeatInterval != "" {
					d, err := time.ParseDuration(fileCfg.HeartbeatInterval)
					if err != nil {
						return fmt.Errorf("parse heartbeat_interval: %w", err)
					}
					cfg.HeartbeatInterval = d
				}
				for _, exp := range fileCfg.Exports {
					cfg.Exports = append(cfg.Exports, sidecar.ExportEntry{
						Name:     exp.Name,
						Protocol: protocol.ServiceProtocol(config.ParseProtocol(exp.Protocol)),
						Port:     exp.LocalPort,
					})
				}
				for _, imp := range fileCfg.Imports {
					cfg.Imports = append(cfg.Imports, sidecar.ImportEntry{
						Name:       imp.Name,
						Protocol:   protocol.ServiceProtocol(config.ParseProtocol(imp.Protocol)),
						ListenPort: imp.ListenPort,
					})
				}
			}
			return runSidecar(cfg)
		},
	}

	cmd.Version = version.Get().String()
	cmd.Flags().StringVar(&configFile, "config", "", "Path to YAML config file")
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
	log.Printf("server-socket: %s, sidecar-id: %s, exports: %d, imports: %d",
		cfg.ServerSocket, cfg.SidecarID, len(cfg.Exports), len(cfg.Imports))

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
