// Package app implements the controller command.
package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/outpostos/OutpostNet/internal/controller/reconciler"
	"github.com/outpostos/OutpostNet/internal/controller/webhook"
	"github.com/outpostos/OutpostNet/pkg/version"
)

// Options holds the controller configuration.
type Options struct {
	MetricsAddr          string
	HealthProbeAddr      string
	EnableLeaderElection bool
	SidecarImage         string
	WebhookPort          int
}

// NewControllerCommand creates the `outpostnet-controller` command.
func NewControllerCommand() *cobra.Command {
	opts := Options{
		MetricsAddr:     ":8080",
		HealthProbeAddr: ":8081",
		SidecarImage:    "outpostnet-sidecar:latest",
		WebhookPort:     9443,
	}

	cmd := &cobra.Command{
		Use:   "outpostnet-controller",
		Short: "OutpostNet controller manager",
		Long:  "OutpostNet Controller manages sidecar injection via Mutating Webhook, auto-generates ConfigMaps, and manages EndpointSlices for service discovery.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runController(opts)
		},
	}

	cmd.Flags().StringVar(&opts.MetricsAddr, "metrics-addr", opts.MetricsAddr, "Metrics endpoint address")
	cmd.Flags().StringVar(&opts.HealthProbeAddr, "health-probe-addr", opts.HealthProbeAddr, "Health probe endpoint address")
	cmd.Flags().BoolVar(&opts.EnableLeaderElection, "leader-elect", opts.EnableLeaderElection, "Enable leader election for HA")
	cmd.Flags().StringVar(&opts.SidecarImage, "sidecar-image", opts.SidecarImage, "Sidecar container image")
	cmd.Flags().IntVar(&opts.WebhookPort, "webhook-port", opts.WebhookPort, "Webhook server port")
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

func runController(opts Options) error {
	v := version.Get()
	setupLog := ctrl.Log.WithName("setup")
	ctrl.SetLogger(zap.New())

	setupLog.Info("starting outpostnet-controller", "version", v.GitVersion)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		LeaderElection:         opts.EnableLeaderElection,
		LeaderElectionID:       "outpostnet-controller-leader",
		HealthProbeBindAddress: opts.HealthProbeAddr,
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			Port: opts.WebhookPort,
		}),
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	// Health checks.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Register sidecar injection webhook.
	mgr.GetWebhookServer().Register("/mutate-pods", &ctrlwebhook.Admission{
		Handler: &webhook.SidecarInjector{SidecarImage: opts.SidecarImage},
	})
	setupLog.Info("registered sidecar injection webhook", "path", "/mutate-pods")

	// Register Pod reconciler for ConfigMap generation.
	if err := (&reconciler.PodReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up pod reconciler")
		os.Exit(1)
	}
	setupLog.Info("registered pod reconciler")

	// Register EndpointSlice reconciler for service discovery.
	if err := (&reconciler.EndpointSliceReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up endpointslice reconciler")
		os.Exit(1)
	}
	setupLog.Info("registered endpointslice reconciler")

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}

	return nil
}
