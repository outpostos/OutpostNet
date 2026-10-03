// Package app implements the outpostnet-service command: the OutpostService
// stable service-access entry point (see docs/service-layer.md).
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/internal/servicehost"
	"github.com/outpostos/OutpostNet/pkg/service"
	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewServiceCommand creates the `outpostnet-service` command.
func NewServiceCommand() *cobra.Command {
	var (
		configFile    string
		nodeID        string
		healthAddr    string
		watchInterval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "outpostnet-service",
		Short: "OutpostService stable service access",
		Long: "outpostnet-service gives applications stable local entries for service identities " +
			"(<service>@<node_id>) and exports local instances, on top of the OutpostNet QUIC tunnels, " +
			"peer directory and management API. Cross-node traffic is TCP only.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if configFile == "" {
				return fmt.Errorf("--config is required")
			}
			cfg, err := service.LoadConfig(configFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			// Flag overrides; the config watcher re-applies them to every
			// reloaded file so reloads compare against the running config.
			override := func(c *service.Config) {
				if nodeID != "" {
					c.NodeID = nodeID
				}
				if healthAddr != "" {
					c.HealthAddr = healthAddr
				}
			}
			override(cfg)
			if err := service.ValidateConfig(cfg); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}
			return run(cfg, configFile, watchInterval, override)
		},
	}
	cmd.Version = version.Get().String()
	cmd.Flags().StringVar(&configFile, "config", "", "Path to the Service YAML config file (required)")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "Node ID override")
	cmd.Flags().StringVar(&healthAddr, "health-addr", "", "Health endpoint address override (default :8083)")
	cmd.Flags().DurationVar(&watchInterval, "watch-interval", 5*time.Second, "Config file poll interval for in-place export changes (0 disables)")
	return cmd
}

func run(cfg *service.Config, configFile string, watchInterval time.Duration, override func(*service.Config)) error {
	logger := slog.Default()
	logger.Info("outpostnet-service starting", "version", version.Get().GitVersion, "node_id", cfg.NodeID,
		"health", cfg.HealthAddr, "management", cfg.Management.SocketPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h := servicehost.New(cfg, logger)
	if watchInterval > 0 {
		go func() {
			// Start watching once the host owns its tunnel servers, so an
			// early file change cannot be overwritten by startup.
			for !h.Ready() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
			if err := h.WatchConfig(ctx, configFile, watchInterval, override); err != nil {
				logger.Error("config watcher stopped", "error", err)
			}
		}()
	}
	return h.Run(ctx)
}
