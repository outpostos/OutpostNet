// Package app implements the agent command.
package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/internal/agent"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/version"
)

// NewAgentCommand creates the `outpostnet-agent` command.
func NewAgentCommand() *cobra.Command {
	var (
		configFile     string
		region         string
		metricsAddr    string
		healthAddr     string
		persistenceDir string
	)

	cmd := &cobra.Command{
		Use:   "outpostnet-agent",
		Short: "OutpostNet cross-region agent",
		Long:  "OutpostNet Agent manages QUIC tunnels between edge regions, enabling cross-region service access with automatic reconnection and config persistence.",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadAgentConfig(configFile, region, metricsAddr, healthAddr, persistenceDir)
			if err != nil {
				return err
			}
			return runAgent(cfg)
		},
	}

	cmd.Version = version.Get().String()
	cmd.Flags().StringVar(&configFile, "config", "", "Path to YAML config file")
	cmd.Flags().StringVar(&region, "region", "", "Region name (overrides config file)")
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "Metrics endpoint address (overrides config file)")
	cmd.Flags().StringVar(&healthAddr, "health-addr", "", "Health check endpoint address (overrides config file)")
	cmd.Flags().StringVar(&persistenceDir, "persistence-dir", "", "Directory for config persistence (overrides config file)")
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

func loadAgentConfig(configFile, region, metricsAddr, healthAddr, persistenceDir string) (*agentconfig.Config, error) {
	var cfg *agentconfig.Config

	if configFile != "" {
		var err error
		cfg, err = agentconfig.LoadConfig(configFile)
		if err != nil {
			return nil, fmt.Errorf("load config: %w", err)
		}
	} else {
		// Minimal config from flags.
		cfg = &agentconfig.Config{
			Region:      region,
			HealthAddr:  ":8082",
			Metrics:     agentconfig.MetricsConfig{Addr: ":9090"},
			Persistence: agentconfig.PersistenceConfig{Dir: "/var/lib/outpostnet"},
		}
	}

	// CLI flags override config file values.
	if region != "" {
		cfg.Region = region
	}
	if metricsAddr != "" {
		cfg.Metrics.Addr = metricsAddr
	}
	if healthAddr != "" {
		cfg.HealthAddr = healthAddr
	}
	if persistenceDir != "" {
		cfg.Persistence.Dir = persistenceDir
	}

	if cfg.Region == "" {
		return nil, fmt.Errorf("region is required (via --region flag or config file)")
	}

	return cfg, nil
}

func runAgent(cfg *agentconfig.Config) error {
	v := version.Get()
	log.Printf("outpostnet-agent %s starting...", v.GitVersion)
	log.Printf("region: %s, health: %s, metrics: %s", cfg.Region, cfg.HealthAddr, cfg.Metrics.Addr)

	a := agent.New(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(ctx)
	}()

	select {
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down...", sig)
		a.Stop()
		time.Sleep(100 * time.Millisecond)
		return nil
	case err := <-errCh:
		return err
	}
}
