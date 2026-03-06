package reconciler

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/outpostos/OutpostNet/pkg/annotations"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
)

// RegionReconciler watches Pods with region annotations and generates Agent
// ConfigMaps for cross-region tunnel configuration. When a Pod imports a
// service with @region suffix, the reconciler:
//  1. Generates an Agent ConfigMap for the importing Pod's region with tunnel
//     client config pointing to the exporting region's Agent service.
//  2. Creates a local proxy Service + EndpointSlice in the importing region
//     so Pods can access the cross-region service via DNS.
type RegionReconciler struct {
	client.Client

	// AgentImage is the container image for the Agent DaemonSet.
	AgentImage string

	// DefaultTunnelPort is the default port for tunnel servers.
	DefaultTunnelPort int32
}

const (
	agentConfigMapPrefix = "outpostnet-agent-"
	agentManagedByLabel  = "outpostnet-controller"
	agentComponentLabel  = "outpostnet-agent"
)

// Reconcile handles Pod create/update/delete events for cross-region config generation.
func (r *RegionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.NamespacedName)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Only handle pods with region annotation.
	region, hasRegion := annotations.HasRegion(pod.Annotations)
	if !hasRegion {
		return ctrl.Result{}, nil
	}

	// Parse annotations.
	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		logger.Error(err, "failed to parse annotations")
		return ctrl.Result{}, nil
	}

	// Find cross-region imports.
	crossRegionImports := make(map[string][]annotations.ServiceEntry) // remoteRegion → entries
	for _, imp := range cfg.Imports {
		if annotations.IsCrossRegion(imp) {
			crossRegionImports[imp.Region] = append(crossRegionImports[imp.Region], imp)
		}
	}

	if len(crossRegionImports) == 0 {
		return ctrl.Result{}, nil
	}

	logger.Info("detected cross-region imports",
		"region", region,
		"remote_regions", mapKeys(crossRegionImports),
	)

	// Generate Agent ConfigMap for this region.
	agentCfg := r.buildAgentConfig(region, crossRegionImports, cfg.Exports, pod.Namespace)
	if err := r.ensureAgentConfigMap(ctx, region, pod, agentCfg); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RegionReconciler) buildAgentConfig(
	region string,
	crossRegionImports map[string][]annotations.ServiceEntry,
	exports []annotations.ServiceEntry,
	namespace string,
) *agentconfig.Config {
	tunnelPort := r.DefaultTunnelPort
	if tunnelPort == 0 {
		tunnelPort = 4242
	}

	cfg := &agentconfig.Config{
		Region:      region,
		HealthAddr:  ":8082",
		Metrics:     agentconfig.MetricsConfig{Addr: ":9090"},
		Persistence: agentconfig.PersistenceConfig{Dir: "/var/lib/outpostnet"},
		TLS:         agentconfig.TLSConfig{Insecure: true},
	}

	// Add tunnels for each remote region.
	for remoteRegion := range crossRegionImports {
		cfg.Tunnels = append(cfg.Tunnels, agentconfig.TunnelConfig{
			RemoteRegion: remoteRegion,
			RemoteAddr:   fmt.Sprintf("outpostnet-agent-%s.%s.svc:%d", remoteRegion, namespace, tunnelPort),
			Role:         "client",
		})
	}

	// Add imports (cross-region).
	listenPort := uint16(10000)
	for remoteRegion, entries := range crossRegionImports {
		for _, entry := range entries {
			cfg.Imports = append(cfg.Imports, agentconfig.ImportConfig{
				Name:         entry.Name,
				ListenAddr:   fmt.Sprintf("0.0.0.0:%d", listenPort),
				RemoteRegion: remoteRegion,
				Protocol:     entry.Protocol,
			})
			listenPort++
		}
	}

	// Add exports (local services exposed to tunnels).
	for _, exp := range exports {
		cfg.Exports = append(cfg.Exports, agentconfig.ExportConfig{
			Name:      exp.Name,
			LocalAddr: fmt.Sprintf("127.0.0.1:%d", exp.Port),
			Protocol:  exp.Protocol,
		})
	}

	return cfg
}

func (r *RegionReconciler) ensureAgentConfigMap(
	ctx context.Context,
	region string,
	pod *corev1.Pod,
	agentCfg *agentconfig.Config,
) error {
	logger := log.FromContext(ctx)

	cfgYAML, err := agentconfig.MarshalConfig(agentCfg)
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}

	cmName := agentConfigMapPrefix + region
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: pod.Namespace}, cm)

	if errors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: pod.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": agentManagedByLabel,
					"app.kubernetes.io/component":  agentComponentLabel,
					"outpostnet.io/region":         region,
				},
			},
			Data: map[string]string{
				"agent.yaml": string(cfgYAML),
			},
		}
		if err := r.Create(ctx, cm); err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return err
		}
		logger.Info("created agent configmap", "configmap", cmName, "region", region)
		return nil
	}

	if err != nil {
		return err
	}

	// Update if changed.
	if cm.Data["agent.yaml"] != string(cfgYAML) {
		cm.Data["agent.yaml"] = string(cfgYAML)
		if err := r.Update(ctx, cm); err != nil {
			return err
		}
		logger.Info("updated agent configmap", "configmap", cmName, "region", region)
	}

	return nil
}

// SetupWithManager sets up the reconciler with the controller manager.
func (r *RegionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func mapKeys(m map[string][]annotations.ServiceEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// AgentServiceName returns the K8s Service name for an Agent in a given region.
func AgentServiceName(region string) string {
	return "outpostnet-agent-" + strings.ToLower(region)
}
