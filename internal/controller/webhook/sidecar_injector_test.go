package webhook

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/outpostos/OutpostNet/pkg/annotations"
)

func newAdmissionRequest(pod *corev1.Pod) admission.Request {
	raw, _ := json.Marshal(pod)
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Object:    runtime.RawExtension{Raw: raw},
			Namespace: "default",
			Name:      pod.Name,
		},
	}
}

func TestInjectSidecar(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:test"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
			},
		},
	}

	resp := injector.Handle(context.Background(), newAdmissionRequest(pod))
	if !resp.Allowed {
		t.Fatalf("expected allowed, got denied: %s", resp.Result.Message)
	}
	if resp.PatchType == nil {
		t.Fatal("expected patch response")
	}
}

func TestSkipNoAnnotation(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:test"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plain-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
			},
		},
	}

	resp := injector.Handle(context.Background(), newAdmissionRequest(pod))
	if !resp.Allowed {
		t.Fatalf("expected allowed for non-annotated pod")
	}
	if resp.PatchType != nil {
		t.Error("expected no patches for non-annotated pod")
	}
}

func TestSkipAlreadyInjected(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:test"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "injected-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
			},
		},
	}

	resp := injector.Handle(context.Background(), newAdmissionRequest(pod))
	if !resp.Allowed {
		t.Fatal("expected allowed for already-injected pod")
	}
	if resp.PatchType != nil {
		t.Error("expected no patches for already-injected pod")
	}
}

func TestDenyInvalidAnnotation(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:test"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bad-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "invalid-format",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
			},
		},
	}

	resp := injector.Handle(context.Background(), newAdmissionRequest(pod))
	if resp.Allowed {
		t.Fatal("expected denied for invalid annotation")
	}
}

func TestDenyNoExportsOrImports(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:test"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
			},
		},
	}

	resp := injector.Handle(context.Background(), newAdmissionRequest(pod))
	if resp.Allowed {
		t.Fatal("expected denied for no exports or imports")
	}
}

func TestInjectAddsVolumesAndContainer(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:v1"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-app",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
				annotations.ImportsAnnotation: "db:3306:tcp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "myapp:latest"},
			},
		},
	}

	cfg, _ := annotations.Parse(pod.Annotations)
	injector.injectSidecar(pod, cfg)

	// Check label.
	if pod.Labels[annotations.InjectedLabel] != "true" {
		t.Error("expected injected label")
	}

	// Check containers.
	if len(pod.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(pod.Spec.Containers))
	}
	sidecar := pod.Spec.Containers[1]
	if sidecar.Name != "outpostnet-sidecar" {
		t.Errorf("sidecar name: %s", sidecar.Name)
	}
	if sidecar.Image != "outpostnet-sidecar:v1" {
		t.Errorf("sidecar image: %s", sidecar.Image)
	}

	// Check volumes.
	if len(pod.Spec.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(pod.Spec.Volumes))
	}
	foundConfig := false
	foundSocket := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "outpostnet-config" {
			foundConfig = true
			if v.ConfigMap == nil {
				t.Error("config volume should be ConfigMap type")
			}
		}
		if v.Name == "outpostnet-socket" {
			foundSocket = true
			if v.HostPath == nil || v.HostPath.Path != "/var/run/outpostnet" {
				t.Error("socket volume should be HostPath /var/run/outpostnet")
			}
		}
	}
	if !foundConfig || !foundSocket {
		t.Errorf("missing volumes: config=%v, socket=%v", foundConfig, foundSocket)
	}

	// Check volume mounts on sidecar.
	if len(sidecar.VolumeMounts) != 2 {
		t.Fatalf("expected 2 volume mounts, got %d", len(sidecar.VolumeMounts))
	}
}

func TestInjectWithExistingVolumes(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:v1"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-app",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation: "true",
				annotations.ExportsAnnotation: "api:8000:tcp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "myapp:latest"},
			},
			// Pre-existing volumes with the same names.
			Volumes: []corev1.Volume{
				{
					Name: "outpostnet-config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "custom-config"},
						},
					},
				},
				{
					Name: "outpostnet-socket",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/custom/socket/path",
						},
					},
				},
			},
		},
	}

	cfg, _ := annotations.Parse(pod.Annotations)
	injector.injectSidecar(pod, cfg)

	// Should NOT add duplicate volumes.
	if len(pod.Spec.Volumes) != 2 {
		t.Errorf("expected 2 volumes (no duplicates), got %d", len(pod.Spec.Volumes))
	}
	// ConfigMap name should remain the original one.
	if pod.Spec.Volumes[0].ConfigMap.Name != "custom-config" {
		t.Errorf("config volume should preserve original ConfigMap name, got %q",
			pod.Spec.Volumes[0].ConfigMap.Name)
	}
}

func TestInjectWithGenerateName(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:v1"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "my-app-", // No Name, using GenerateName
			Namespace:    "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation: "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "myapp:latest"},
			},
		},
	}

	cfg, _ := annotations.Parse(pod.Annotations)
	injector.injectSidecar(pod, cfg)

	// ConfigMap name should use GenerateName when Name is empty.
	if len(pod.Spec.Volumes) == 0 {
		t.Fatal("expected volumes to be added")
	}
	configVol := pod.Spec.Volumes[0]
	if configVol.Name != "outpostnet-config" {
		t.Errorf("expected outpostnet-config volume, got %q", configVol.Name)
	}
	if configVol.ConfigMap == nil {
		t.Fatal("expected ConfigMap volume source")
	}
	expectedName := "outpostnet-sidecar-my-app-"
	if configVol.ConfigMap.LocalObjectReference.Name != expectedName {
		t.Errorf("expected ConfigMap name %q, got %q", expectedName, configVol.ConfigMap.LocalObjectReference.Name)
	}
}

func TestInjectSidecarResources(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:v1"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-app",
			Annotations: map[string]string{
				annotations.InjectAnnotation: "true",
				annotations.ExportsAnnotation: "svc:8080:tcp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "app"}},
		},
	}

	cfg, _ := annotations.Parse(pod.Annotations)
	injector.injectSidecar(pod, cfg)

	if len(pod.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(pod.Spec.Containers))
	}
	sidecar := pod.Spec.Containers[1]

	// Check CPU requests/limits.
	cpuReq := sidecar.Resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "10m" {
		t.Errorf("expected CPU request 10m, got %q", cpuReq.String())
	}
	cpuLim := sidecar.Resources.Limits[corev1.ResourceCPU]
	if cpuLim.String() != "100m" {
		t.Errorf("expected CPU limit 100m, got %q", cpuLim.String())
	}

	// Check memory requests/limits.
	memReq := sidecar.Resources.Requests[corev1.ResourceMemory]
	if memReq.String() != "16Mi" {
		t.Errorf("expected memory request 16Mi, got %q", memReq.String())
	}
	memLim := sidecar.Resources.Limits[corev1.ResourceMemory]
	if memLim.String() != "64Mi" {
		t.Errorf("expected memory limit 64Mi, got %q", memLim.String())
	}
}

func TestInjectSidecarVolumeMounts(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:v1"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "mount-test",
			Annotations: map[string]string{
				annotations.InjectAnnotation: "true",
				annotations.ExportsAnnotation: "test:9090:udp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "app"}},
		},
	}

	cfg, _ := annotations.Parse(pod.Annotations)
	injector.injectSidecar(pod, cfg)

	sidecar := pod.Spec.Containers[1]
	if len(sidecar.VolumeMounts) != 2 {
		t.Fatalf("expected 2 volume mounts, got %d", len(sidecar.VolumeMounts))
	}

	// Find each mount and verify properties.
	configMount, socketMount := false, false
	for _, m := range sidecar.VolumeMounts {
		switch m.Name {
		case "outpostnet-config":
			configMount = true
			if m.MountPath != "/etc/outpostnet" {
				t.Errorf("config mount path: expected /etc/outpostnet, got %q", m.MountPath)
			}
			if !m.ReadOnly {
				t.Error("config mount should be read-only")
			}
		case "outpostnet-socket":
			socketMount = true
			if m.MountPath != "/var/run/outpostnet" {
				t.Errorf("socket mount path: expected /var/run/outpostnet, got %q", m.MountPath)
			}
		}
	}
	if !configMount {
		t.Error("config volume mount not found")
	}
	if !socketMount {
		t.Error("socket volume mount not found")
	}
}

func TestInjectSidecarArgs(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:latest"}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "args-test",
			Annotations: map[string]string{
				annotations.InjectAnnotation: "true",
				annotations.ExportsAnnotation: "svc:80:tcp",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "app"}},
		},
	}

	cfg, _ := annotations.Parse(pod.Annotations)
	injector.injectSidecar(pod, cfg)

	sidecar := pod.Spec.Containers[1]
	if len(sidecar.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(sidecar.Args))
	}
	expectedArg := "--config=/etc/outpostnet/sidecar.yaml"
	if sidecar.Args[0] != expectedArg {
		t.Errorf("expected arg %q, got %q", expectedArg, sidecar.Args[0])
	}
}

func TestHandleUnmarshalError(t *testing.T) {
	injector := &SidecarInjector{SidecarImage: "outpostnet-sidecar:v1"}

	// Invalid JSON in Object.Raw.
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Object: runtime.RawExtension{Raw: []byte("invalid json")},
		},
	}

	resp := injector.Handle(context.Background(), req)
	if resp.Allowed {
		t.Error("expected denied for invalid JSON")
	}
	if resp.Result.Code != 400 { // BadRequest
		t.Errorf("expected status code 400, got %d", resp.Result.Code)
	}
}
