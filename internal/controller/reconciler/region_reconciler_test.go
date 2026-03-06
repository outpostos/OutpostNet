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

func TestBuildAgentConfig(t *testing.T) {
	r := &RegionReconciler{
		DefaultTunnelPort: 4242,
	}

	crossRegionImports := map[string][]annotations.ServiceEntry{
		"cloud": {
			{Name: "cloud-db", Port: 3306, Protocol: "tcp", Region: "cloud"},
			{Name: "cloud-api", Port: 8080, Protocol: "tcp", Region: "cloud"},
		},
	}
	exports := []annotations.ServiceEntry{
		{Name: "edge-sensor", Port: 9000, Protocol: "tcp"},
	}

	cfg := r.buildAgentConfig("edge-a", crossRegionImports, exports, "default")

	if cfg.Region != "edge-a" {
		t.Errorf("region = %q, want edge-a", cfg.Region)
	}

	// Check tunnels.
	if len(cfg.Tunnels) != 1 {
		t.Fatalf("tunnels = %d, want 1", len(cfg.Tunnels))
	}
	if cfg.Tunnels[0].RemoteRegion != "cloud" {
		t.Errorf("tunnel remote_region = %q, want cloud", cfg.Tunnels[0].RemoteRegion)
	}
	if cfg.Tunnels[0].Role != "client" {
		t.Errorf("tunnel role = %q, want client", cfg.Tunnels[0].Role)
	}
	expectedAddr := "outpostnet-agent-cloud.default.svc:4242"
	if cfg.Tunnels[0].RemoteAddr != expectedAddr {
		t.Errorf("tunnel remote_addr = %q, want %q", cfg.Tunnels[0].RemoteAddr, expectedAddr)
	}

	// Check imports.
	if len(cfg.Imports) != 2 {
		t.Fatalf("imports = %d, want 2", len(cfg.Imports))
	}
	if cfg.Imports[0].Name != "cloud-db" {
		t.Errorf("import[0].Name = %q, want cloud-db", cfg.Imports[0].Name)
	}
	if cfg.Imports[0].RemoteRegion != "cloud" {
		t.Errorf("import[0].RemoteRegion = %q, want cloud", cfg.Imports[0].RemoteRegion)
	}

	// Check exports.
	if len(cfg.Exports) != 1 {
		t.Fatalf("exports = %d, want 1", len(cfg.Exports))
	}
	if cfg.Exports[0].Name != "edge-sensor" {
		t.Errorf("export[0].Name = %q, want edge-sensor", cfg.Exports[0].Name)
	}
	if cfg.Exports[0].LocalAddr != "127.0.0.1:9000" {
		t.Errorf("export[0].LocalAddr = %q, want 127.0.0.1:9000", cfg.Exports[0].LocalAddr)
	}
}

func TestBuildAgentConfigMultiRegion(t *testing.T) {
	r := &RegionReconciler{
		DefaultTunnelPort: 4242,
	}

	crossRegionImports := map[string][]annotations.ServiceEntry{
		"cloud": {
			{Name: "cloud-db", Port: 3306, Protocol: "tcp", Region: "cloud"},
		},
		"edge-b": {
			{Name: "sensor-b", Port: 9001, Protocol: "tcp", Region: "edge-b"},
		},
	}

	cfg := r.buildAgentConfig("edge-a", crossRegionImports, nil, "outpostnet")

	if len(cfg.Tunnels) != 2 {
		t.Fatalf("tunnels = %d, want 2", len(cfg.Tunnels))
	}
	if len(cfg.Imports) != 2 {
		t.Fatalf("imports = %d, want 2", len(cfg.Imports))
	}
}

func TestBuildAgentConfigDefaults(t *testing.T) {
	r := &RegionReconciler{} // DefaultTunnelPort = 0, should default to 4242

	crossRegionImports := map[string][]annotations.ServiceEntry{
		"cloud": {
			{Name: "svc", Port: 80, Protocol: "tcp", Region: "cloud"},
		},
	}

	cfg := r.buildAgentConfig("edge", crossRegionImports, nil, "default")

	if cfg.HealthAddr != ":8082" {
		t.Errorf("health_addr = %q, want :8082", cfg.HealthAddr)
	}
	if cfg.Metrics.Addr != ":9090" {
		t.Errorf("metrics addr = %q, want :9090", cfg.Metrics.Addr)
	}

	// Verify default port used.
	expectedAddr := "outpostnet-agent-cloud.default.svc:4242"
	if cfg.Tunnels[0].RemoteAddr != expectedAddr {
		t.Errorf("tunnel remote_addr = %q, want %q", cfg.Tunnels[0].RemoteAddr, expectedAddr)
	}
}

func TestAgentServiceName(t *testing.T) {
	if got := AgentServiceName("edge-a"); got != "outpostnet-agent-edge-a" {
		t.Errorf("AgentServiceName(edge-a) = %q", got)
	}
	if got := AgentServiceName("Cloud"); got != "outpostnet-agent-cloud" {
		t.Errorf("AgentServiceName(Cloud) = %q", got)
	}
}

func TestMapKeys(t *testing.T) {
	m := map[string][]annotations.ServiceEntry{
		"a": nil,
		"b": nil,
	}
	keys := mapKeys(m)
	if len(keys) != 2 {
		t.Errorf("mapKeys returned %d keys, want 2", len(keys))
	}
}

func TestRegionGraphDirectPath(t *testing.T) {
	g := NewRegionGraph(map[string]string{
		"edge-a": "cloud",
		"cloud":  "edge-a",
	})

	path := g.FindPath("edge-a", "cloud")
	if len(path) != 2 {
		t.Fatalf("path len = %d, want 2", len(path))
	}
	if path[0] != "edge-a" || path[1] != "cloud" {
		t.Errorf("path = %v, want [edge-a cloud]", path)
	}
}

func TestRegionGraphSingleHopRelay(t *testing.T) {
	// A and B can't reach each other directly, but both can reach C.
	g := NewRegionGraph(map[string]string{
		"edge-a":  "relay-c",
		"cloud-b": "relay-c",
	})

	path := g.FindPath("edge-a", "cloud-b")
	if len(path) != 3 {
		t.Fatalf("path len = %d, want 3: %v", len(path), path)
	}
	if path[0] != "edge-a" || path[1] != "relay-c" || path[2] != "cloud-b" {
		t.Errorf("path = %v, want [edge-a relay-c cloud-b]", path)
	}
}

func TestRegionGraphMultiHopRelay(t *testing.T) {
	// A → C → D → B
	g := NewRegionGraph(map[string]string{
		"A": "C",
		"C": "D",
		"D": "B",
	})

	path := g.FindPath("A", "B")
	if len(path) != 4 {
		t.Fatalf("path len = %d, want 4: %v", len(path), path)
	}
	if path[0] != "A" || path[3] != "B" {
		t.Errorf("path = %v, want [A C D B]", path)
	}
}

func TestRegionGraphUnreachable(t *testing.T) {
	g := NewRegionGraph(map[string]string{
		"A": "B",
		"C": "D",
	})

	path := g.FindPath("A", "C")
	if path != nil {
		t.Errorf("expected nil path, got %v", path)
	}
}

func TestComputeRelayConfigsDirect(t *testing.T) {
	g := NewRegionGraph(map[string]string{
		"edge-a": "cloud",
	})

	relays := ComputeRelayConfigs(g, "edge-a", "cloud", "svc-b")
	if relays != nil {
		t.Errorf("expected nil relays for direct path, got %v", relays)
	}
}

func TestComputeRelayConfigsSingleHop(t *testing.T) {
	g := NewRegionGraph(map[string]string{
		"edge-a":  "relay-c",
		"cloud-b": "relay-c",
	})

	relays := ComputeRelayConfigs(g, "edge-a", "cloud-b", "svc-b")
	if relays == nil {
		t.Fatal("expected relay configs, got nil")
	}

	rc, ok := relays["relay-c"]
	if !ok {
		t.Fatal("expected relay config for relay-c")
	}
	if len(rc) != 1 {
		t.Fatalf("relay-c rules = %d, want 1", len(rc))
	}
	if rc[0].Service != "svc-b" {
		t.Errorf("service = %q, want svc-b", rc[0].Service)
	}
	if rc[0].FromRegion != "edge-a" {
		t.Errorf("from_region = %q, want edge-a", rc[0].FromRegion)
	}
	if rc[0].ToRegion != "cloud-b" {
		t.Errorf("to_region = %q, want cloud-b", rc[0].ToRegion)
	}
}

func TestComputeRelayConfigsMultiHop(t *testing.T) {
	g := NewRegionGraph(map[string]string{
		"A": "C",
		"C": "D",
		"D": "B",
	})

	relays := ComputeRelayConfigs(g, "A", "B", "svc")
	if relays == nil {
		t.Fatal("expected relay configs, got nil")
	}

	// Should have relay rules for C and D.
	if len(relays) != 2 {
		t.Fatalf("relay regions = %d, want 2", len(relays))
	}

	rcC := relays["C"]
	if len(rcC) != 1 || rcC[0].FromRegion != "A" || rcC[0].ToRegion != "D" {
		t.Errorf("C relay = %+v, want A→D", rcC)
	}

	rcD := relays["D"]
	if len(rcD) != 1 || rcD[0].FromRegion != "C" || rcD[0].ToRegion != "B" {
		t.Errorf("D relay = %+v, want C→B", rcD)
	}
}

func TestComputeRelayConfigsUnreachable(t *testing.T) {
	g := NewRegionGraph(map[string]string{
		"A": "B",
		"C": "D",
	})

	relays := ComputeRelayConfigs(g, "A", "C", "svc")
	if relays != nil {
		t.Errorf("expected nil relays for unreachable, got %v", relays)
	}
}

// newRegionScheme creates a runtime.Scheme with core types for RegionReconciler tests.
func newRegionScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	return s
}

func TestRegionReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newRegionScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &RegionReconciler{
		Client:            fakeClient,
		DefaultTunnelPort: 4242,
	}
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

func TestRegionReconciler_Reconcile_NoRegion(t *testing.T) {
	scheme := newRegionScheme()

	// Pod without region annotation — should be a no-op.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plain-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.ExportsAnnotation: "web:8080:tcp",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &RegionReconciler{
		Client:            fakeClient,
		DefaultTunnelPort: 4242,
	}
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

	// Verify no agent ConfigMap was created.
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-agent-edge-a",
		Namespace: "default",
	}, cm)
	if cmErr == nil {
		t.Error("expected no agent configmap to exist, but it was found")
	}
}

func TestRegionReconciler_Reconcile_NoCrossRegionImports(t *testing.T) {
	scheme := newRegionScheme()

	// Pod with region but only local imports (no @region suffix) — should be a no-op.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "local-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.RegionAnnotation:  "edge-a",
				annotations.ImportsAnnotation: "db:3306:tcp",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &RegionReconciler{
		Client:            fakeClient,
		DefaultTunnelPort: 4242,
	}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "local-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify no agent ConfigMap was created.
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-agent-edge-a",
		Namespace: "default",
	}, cm)
	if cmErr == nil {
		t.Error("expected no agent configmap for local-only imports")
	}
}

func TestRegionReconciler_Reconcile_CreatesAgentConfigMap(t *testing.T) {
	scheme := newRegionScheme()

	// Pod with region annotation and cross-region import (name@region format).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "edge-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.RegionAnnotation:  "edge-a",
				annotations.ImportsAnnotation: "cloud-db@cloud:3306:tcp",
				annotations.ExportsAnnotation: "sensor:9000:tcp",
			},
			UID: "uid-edge-123",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	r := &RegionReconciler{
		Client:            fakeClient,
		DefaultTunnelPort: 4242,
	}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "edge-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify agent ConfigMap was created.
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-agent-edge-a",
		Namespace: "default",
	}, cm)
	if cmErr != nil {
		t.Fatalf("expected agent configmap to exist, got error: %v", cmErr)
	}

	// Verify ConfigMap labels.
	if cm.Labels["app.kubernetes.io/managed-by"] != agentManagedByLabel {
		t.Errorf("expected managed-by label %q, got %q", agentManagedByLabel, cm.Labels["app.kubernetes.io/managed-by"])
	}
	if cm.Labels["app.kubernetes.io/component"] != agentComponentLabel {
		t.Errorf("expected component label %q, got %q", agentComponentLabel, cm.Labels["app.kubernetes.io/component"])
	}
	if cm.Labels["outpostnet.io/region"] != "edge-a" {
		t.Errorf("expected region label 'edge-a', got %q", cm.Labels["outpostnet.io/region"])
	}

	// Verify ConfigMap data contains agent config YAML.
	agentYAML, ok := cm.Data["agent.yaml"]
	if !ok {
		t.Fatal("expected agent.yaml key in configmap data")
	}
	if !strings.Contains(agentYAML, "region: edge-a") {
		t.Errorf("expected 'region: edge-a' in agent config YAML:\n%s", agentYAML)
	}
	if !strings.Contains(agentYAML, "remote_region: cloud") {
		t.Errorf("expected tunnel remote_region 'cloud' in agent config YAML:\n%s", agentYAML)
	}
	if !strings.Contains(agentYAML, "outpostnet-agent-cloud.default.svc:4242") {
		t.Errorf("expected remote_addr in agent config YAML:\n%s", agentYAML)
	}
	if !strings.Contains(agentYAML, "name: cloud-db") {
		t.Errorf("expected import 'cloud-db' in agent config YAML:\n%s", agentYAML)
	}
	if !strings.Contains(agentYAML, "name: sensor") {
		t.Errorf("expected export 'sensor' in agent config YAML:\n%s", agentYAML)
	}
}

func TestRegionReconciler_Reconcile_UpdatesAgentConfigMap(t *testing.T) {
	scheme := newRegionScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "edge-pod",
			Namespace: "default",
			Annotations: map[string]string{
				annotations.InjectAnnotation:  "true",
				annotations.RegionAnnotation:  "edge-a",
				annotations.ImportsAnnotation: "cloud-db@cloud:3306:tcp",
			},
			UID: "uid-edge-456",
		},
	}

	// Pre-create a ConfigMap with old data.
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "outpostnet-agent-edge-a",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": agentManagedByLabel,
				"app.kubernetes.io/component":  agentComponentLabel,
				"outpostnet.io/region":         "edge-a",
			},
		},
		Data: map[string]string{
			"agent.yaml": "old-agent-config",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, existingCM).Build()
	r := &RegionReconciler{
		Client:            fakeClient,
		DefaultTunnelPort: 4242,
	}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "edge-pod",
			Namespace: "default",
		},
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue")
	}

	// Verify ConfigMap was updated.
	cm := &corev1.ConfigMap{}
	cmErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "outpostnet-agent-edge-a",
		Namespace: "default",
	}, cm)
	if cmErr != nil {
		t.Fatalf("expected agent configmap to exist, got error: %v", cmErr)
	}

	agentYAML := cm.Data["agent.yaml"]
	if agentYAML == "old-agent-config" {
		t.Error("expected configmap to be updated, but it still has old data")
	}
	if !strings.Contains(agentYAML, "region: edge-a") {
		t.Errorf("expected 'region: edge-a' in updated agent config YAML:\n%s", agentYAML)
	}
	if !strings.Contains(agentYAML, "name: cloud-db") {
		t.Errorf("expected import 'cloud-db' in updated agent config YAML:\n%s", agentYAML)
	}
}
