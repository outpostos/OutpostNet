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
		configFile      string
		region          string
		nodeID          string
		metricsAddr     string
		healthAddr      string
		persistenceDir  string
		observationAddr string
	)

	cmd := &cobra.Command{
		Use:   "outpostnet-agent",
		Short: "OutpostNet cross-region agent",
		Long:  "OutpostNet Agent manages QUIC tunnels between edge regions, enabling cross-region service access with automatic reconnection and config persistence.",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadAgentConfig(configFile, region, nodeID, metricsAddr, healthAddr, persistenceDir)
			if err != nil {
				return err
			}
			return runAgent(cfg, observationAddr)
		},
	}

	cmd.Version = version.Get().String()
	cmd.Flags().StringVar(&configFile, "config", "", "Path to YAML config file")
	cmd.Flags().StringVar(&region, "region", "", "Optional region name (overrides config file; used for relay routing)")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "Optional node ID (overrides config file; used for peer-directory role arbitration)")
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "Metrics endpoint address (overrides config file)")
	cmd.Flags().StringVar(&healthAddr, "health-addr", "", "Health check endpoint address (overrides config file)")
	cmd.Flags().StringVar(&persistenceDir, "persistence-dir", "", "Directory for config persistence (overrides config file)")
	cmd.Flags().StringVar(&observationAddr, "observation-addr", DefaultObservationAddr,
		"Loopback address of the OutpostRange observation API (component OUTPOSTNET, requires node_id); empty disables it. "+
			"Observation failures are logged and never stop the agent")
	cmd.AddCommand(newVersionCommand(), newCertificateCommand())

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

func loadAgentConfig(configFile, region, nodeID, metricsAddr, healthAddr, persistenceDir string) (*agentconfig.Config, error) {
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
			NodeID:      nodeID,
			HealthAddr:  ":8082",
			Metrics:     agentconfig.MetricsConfig{Addr: ":9090"},
			Persistence: agentconfig.PersistenceConfig{Dir: "/var/lib/outpostnet"},
			Management:  agentconfig.ManagementConfig{SocketPath: agentconfig.DefaultManagementSocketPath},
		}
	}

	// CLI flags override config file values.
	if region != "" {
		cfg.Region = region
	}
	if nodeID != "" {
		cfg.NodeID = nodeID
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

	return cfg, nil
}

// DefaultObservationAddr is the default OutpostRange observation endpoint of
// outpostnet-agent (outpostnet-service uses 127.0.0.1:17961).
const DefaultObservationAddr = "127.0.0.1:17960"

func runAgent(cfg *agentconfig.Config, observationAddr string) error {
	v := version.Get()
	log.Printf("outpostnet-agent %s starting...", v.GitVersion)
	log.Printf("health: %s, metrics: %s", cfg.HealthAddr, cfg.Metrics.Addr)

	a := agent.New(cfg, nil)
	a.EnableObservation(observationAddr)

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
