package reconciler

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
