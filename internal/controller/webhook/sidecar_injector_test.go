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
