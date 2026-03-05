// Package app implements the server command.
package app

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/internal/server"
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewServerCommand creates the `outpostnet-server` command.
func NewServerCommand() *cobra.Command {
	var configFile string
	cfg := server.DefaultConfig()

	cmd := &cobra.Command{
		Use:   "outpostnet-server",
		Short: "OutpostNet node-level server",
		Long:  "OutpostNet Server runs on each edge node, managing sidecar registration and service routing via Unix domain socket.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configFile != "" {
				fileCfg, err := config.LoadServerConfig(configFile)
				if err != nil {
					return fmt.Errorf("load config: %w", err)
				}
				cfg.SocketPath = fileCfg.Listen
				if fileCfg.HeartbeatTimeout != "" {
					d, err := time.ParseDuration(fileCfg.HeartbeatTimeout)
					if err != nil {
						return fmt.Errorf("parse heartbeat_timeout: %w", err)
					}
					cfg.HeartbeatTimeout = d
				}
			}
			return runServer(cfg)
		},
	}

	cmd.Flags().StringVar(&configFile, "config", "", "Path to YAML config file")
	cmd.Flags().StringVar(&cfg.SocketPath, "socket", cfg.SocketPath, "Unix socket path")
	cmd.Flags().DurationVar(&cfg.HeartbeatTimeout, "heartbeat-timeout", cfg.HeartbeatTimeout, "Heartbeat timeout duration")
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

func runServer(cfg server.Config) error {
	v := version.Get()
	log.Printf("outpostnet-server %s starting...", v.GitVersion)
	log.Printf("socket: %s, heartbeat-timeout: %s", cfg.SocketPath, cfg.HeartbeatTimeout)

	srv := server.New(cfg)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run()
	}()

	select {
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down...", sig)
		srv.Stop()
		time.Sleep(100 * time.Millisecond)
		return nil
	case err := <-errCh:
		return err
	}
}
