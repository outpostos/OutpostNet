// Package reconciler implements OutpostNet controller reconcilers.
package reconciler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/outpostos/OutpostNet/pkg/annotations"
	"github.com/outpostos/OutpostNet/pkg/config"
)

// PodReconciler watches OutpostNet-injected Pods and manages their ConfigMaps.
type PodReconciler struct {
	client.Client
}

// Reconcile handles Pod create/update/delete events.
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.NamespacedName)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if errors.IsNotFound(err) {
			// Pod deleted — ConfigMap cleanup is handled by OwnerReference GC.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Only handle pods with the injected label.
	if pod.Labels == nil || pod.Labels[annotations.InjectedLabel] != "true" {
		return ctrl.Result{}, nil
	}

	// Parse annotations.
	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		logger.Error(err, "failed to parse annotations")
		return ctrl.Result{}, nil // Don't retry on invalid annotations.
	}

	// Generate sidecar config YAML.
	sidecarYAML, err := generateSidecarConfig(pod, cfg)
	if err != nil {
		logger.Error(err, "failed to generate sidecar config")
		return ctrl.Result{}, err
	}

	// Create or update ConfigMap.
	cmName := fmt.Sprintf("outpostnet-sidecar-%s", pod.Name)
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: pod.Namespace}, cm)

	if errors.IsNotFound(err) {
		// Create new ConfigMap.
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: pod.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "outpostnet-controller",
					"outpostnet.io/pod":            pod.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(pod, corev1.SchemeGroupVersion.WithKind("Pod")),
				},
			},
			Data: map[string]string{
				"sidecar.yaml": sidecarYAML,
			},
		}
		if err := r.Create(ctx, cm); err != nil {
			if errors.IsAlreadyExists(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		logger.Info("created configmap", "configmap", cmName)
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	// Update existing ConfigMap if config changed.
	if cm.Data["sidecar.yaml"] != sidecarYAML {
		cm.Data["sidecar.yaml"] = sidecarYAML
		if err := r.Update(ctx, cm); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("updated configmap", "configmap", cmName)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the reconciler with the controller manager.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func generateSidecarConfig(pod *corev1.Pod, cfg *annotations.PodConfig) (string, error) {
	sidecarID := cfg.SidecarID
	if sidecarID == "" {
		sidecarID = pod.Name
	}

	sc := config.SidecarConfig{
		Server:            "/var/run/outpostnet/server.sock",
		SidecarID:         sidecarID,
		HeartbeatInterval: "10s",
	}

	for _, e := range cfg.Exports {
		sc.Exports = append(sc.Exports, config.ServiceExport{
			Name:      e.Name,
			LocalPort: e.Port,
			Protocol:  e.Protocol,
		})
	}

	for _, imp := range cfg.Imports {
		sc.Imports = append(sc.Imports, config.ServiceImport{
			Name:       imp.Name,
			ListenPort: imp.Port,
			Protocol:   imp.Protocol,
		})
	}

	return config.MarshalSidecarConfig(&sc)
}
