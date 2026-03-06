package reconciler

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/outpostos/OutpostNet/pkg/annotations"
)

func TestGenerateSidecarConfig(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-app-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
				annotations.ImportsAnnotation: "db:3306:tcp,cache:6379:tcp",
			},
		},
	}

	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	yamlStr, err := generateSidecarConfig(pod, cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	// Verify key fields are present.
	checks := []string{
		"server: /var/run/outpostnet/server.sock",
		"sidecar_id: my-app-pod",
		"heartbeat_interval: 10s",
		"name: web",
		"local_port: 8080",
		"protocol: tcp",
		"name: db",
		"listen_port: 3306",
		"name: cache",
		"listen_port: 6379",
	}
	for _, check := range checks {
		if !strings.Contains(yamlStr, check) {
			t.Errorf("expected %q in YAML output:\n%s", check, yamlStr)
		}
	}
}

func TestGenerateSidecarConfigCustomID(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-123",
			Annotations: map[string]string{
				annotations.InjectAnnotation:    "true",
				annotations.SidecarIDAnnotation: "custom-sidecar-id",
				annotations.ExportsAnnotation:   "svc:80:tcp",
			},
		},
	}

	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	yamlStr, err := generateSidecarConfig(pod, cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	if !strings.Contains(yamlStr, "sidecar_id: custom-sidecar-id") {
		t.Errorf("expected custom sidecar_id in YAML:\n%s", yamlStr)
	}
}

func TestGenerateSidecarConfigExportOnly(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "exporter",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "api:3000:tcp",
			},
		},
	}

	cfg, err := annotations.Parse(pod.Annotations)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	yamlStr, err := generateSidecarConfig(pod, cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	if !strings.Contains(yamlStr, "name: api") {
		t.Errorf("expected export in YAML:\n%s", yamlStr)
	}
	// Empty imports should either be absent or be "imports: []"
	if strings.Contains(yamlStr, "listen_port:") {
		t.Errorf("expected no import entries in YAML:\n%s", yamlStr)
	}
}

// newScheme creates a runtime.Scheme with core types registered.
func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	return s
}

func TestPodReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &PodReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonexistent-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}
}

func TestPodReconciler_Reconcile_NotInjected(t *testing.T) {
	scheme := newScheme()

	// Pod exists but does not have the injected label.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plain-pod",
			Namespace: "default",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &PodReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "plain-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify no ConfigMap was created.
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-sidecar-plain-pod",
		Namespace: "default",
	}, cm)
	if cmErr == nil {
		t.Error("expected configmap to not exist, but it was found")
	}
}

func TestPodReconciler_Reconcile_CreatesConfigMap(t *testing.T) {
	scheme := newScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
				annotations.ImportsAnnotation: "db:3306:tcp",
			},
			UID: "test-uid-123",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &PodReconciler{Client: fakeClient}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "web-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify ConfigMap was created.
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-sidecar-web-pod",
		Namespace: "default",
	}, cm)
	if cmErr != nil {
		t.Fatalf("expected configmap to exist, got error: %v", cmErr)
	}

	// Verify ConfigMap contents.
	sidecarYAML, ok := cm.Data["sidecar.yaml"]
	if !ok {
		t.Fatal("expected sidecar.yaml key in configmap data")
	}
	if !strings.Contains(sidecarYAML, "sidecar_id: web-pod") {
		t.Errorf("expected sidecar_id in YAML:\n%s", sidecarYAML)
	}
	if !strings.Contains(sidecarYAML, "name: web") {
		t.Errorf("expected export name 'web' in YAML:\n%s", sidecarYAML)
	}
	if !strings.Contains(sidecarYAML, "name: db") {
		t.Errorf("expected import name 'db' in YAML:\n%s", sidecarYAML)
	}

	// Verify labels on ConfigMap.
	if cm.Labels["app.kubernetes.io/managed-by"] != "outpostnet-controller" {
		t.Errorf("expected managed-by label, got %q", cm.Labels["app.kubernetes.io/managed-by"])
	}
	if cm.Labels["outpostnet.io/pod"] != "web-pod" {
		t.Errorf("expected pod label, got %q", cm.Labels["outpostnet.io/pod"])
	}

	// Verify owner reference.
	if len(cm.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(cm.OwnerReferences))
	}
	if cm.OwnerReferences[0].Name != "web-pod" {
		t.Errorf("expected owner ref name 'web-pod', got %q", cm.OwnerReferences[0].Name)
	}
}

func TestPodReconciler_Reconcile_UpdatesConfigMap(t *testing.T) {
	scheme := newScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-pod",
			Namespace: "default",
			Labels: map[string]string{
				annotations.InjectedLabel: "true",
			},
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
			UID: "test-uid-123",
		},
	}

	// Pre-create a ConfigMap with old data.
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "outpostnet-sidecar-web-pod",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "outpostnet-controller",
				"outpostnet.io/pod":            "web-pod",
			},
		},
		Data: map[string]string{
			"sidecar.yaml": "old-config-data",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, existingCM).Build()
	r := &PodReconciler{Client: fakeClient}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "web-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify ConfigMap was updated (not still "old-config-data").
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-sidecar-web-pod",
		Namespace: "default",
	}, cm)
	if cmErr != nil {
		t.Fatalf("expected configmap to exist, got error: %v", cmErr)
	}

	sidecarYAML := cm.Data["sidecar.yaml"]
	if sidecarYAML == "old-config-data" {
		t.Error("expected configmap to be updated, but it still has old data")
	}
	if !strings.Contains(sidecarYAML, "name: web") {
		t.Errorf("expected export 'web' in updated YAML:\n%s", sidecarYAML)
	}
	if !strings.Contains(sidecarYAML, "local_port: 8080") {
		t.Errorf("expected local_port 8080 in updated YAML:\n%s", sidecarYAML)
	}
}
