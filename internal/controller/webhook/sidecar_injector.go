// Package webhook implements the OutpostNet Mutating Webhook for sidecar injection.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/outpostos/OutpostNet/pkg/annotations"
)

// SidecarInjector handles mutating webhook requests for Pod creation.
type SidecarInjector struct {
	SidecarImage string
}

// Handle processes admission requests and injects sidecar containers.
func (si *SidecarInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("namespace", req.Namespace, "name", req.Name)

	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		logger.Error(err, "failed to unmarshal pod")
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Skip if not annotated for injection.
	if !annotations.HasOutpostNet(pod.Annotations) {
		return admission.Allowed("no outpostnet injection requested")
	}

	// Skip if already injected.
	if pod.Labels != nil && pod.Labels[annotations.InjectedLabel] == "true" {
		return admission.Allowed("already injected")
	}

	// Parse annotations.
	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		logger.Error(err, "failed to parse annotations")
		return admission.Denied(fmt.Sprintf("invalid outpostnet annotations: %v", err))
	}

	if err := annotations.Validate(cfg); err != nil {
		logger.Error(err, "annotation validation failed")
		return admission.Denied(fmt.Sprintf("outpostnet annotation validation: %v", err))
	}

	// Inject sidecar.
	si.injectSidecar(pod, cfg)

	logger.Info("injected outpostnet sidecar",
		"exports", len(cfg.Exports),
		"imports", len(cfg.Imports))

	marshaled, err := json.Marshal(pod)
	if err != nil {
		logger.Error(err, "failed to marshal mutated pod")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

func (si *SidecarInjector) injectSidecar(pod *corev1.Pod, cfg *annotations.PodConfig) {
	// Add injected label.
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[annotations.InjectedLabel] = "true"

	// Determine the configmap name. Use pod name if available, otherwise generateName.
	podIdentifier := pod.Name
	if podIdentifier == "" {
		podIdentifier = pod.GenerateName
	}
	configMapName := fmt.Sprintf("outpostnet-sidecar-%s", podIdentifier)

	// Add volumes if not already present.
	si.ensureVolumes(pod, configMapName)

	// Build sidecar container.
	sidecar := corev1.Container{
		Name:  "outpostnet-sidecar",
		Image: si.SidecarImage,
		Args:  []string{"--config=/etc/outpostnet/sidecar.yaml"},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "outpostnet-config",
				MountPath: "/etc/outpostnet",
				ReadOnly:  true,
			},
			{
				Name:      "outpostnet-socket",
				MountPath: "/var/run/outpostnet",
			},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
		},
	}

	pod.Spec.Containers = append(pod.Spec.Containers, sidecar)
}

func (si *SidecarInjector) ensureVolumes(pod *corev1.Pod, configMapName string) {
	hasConfig := false
	hasSocket := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "outpostnet-config" {
			hasConfig = true
		}
		if v.Name == "outpostnet-socket" {
			hasSocket = true
		}
	}

	if !hasConfig {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "outpostnet-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName,
					},
					Optional: boolPtr(true),
				},
			},
		})
	}

	if !hasSocket {
		hostPathType := corev1.HostPathDirectoryOrCreate
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "outpostnet-socket",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/run/outpostnet",
					Type: &hostPathType,
				},
			},
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}

