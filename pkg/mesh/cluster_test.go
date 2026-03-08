package mesh

import (
	"fmt"
	"testing"
	"time"
)

func TestNewClusterState(t *testing.T) {
	cs := NewClusterState("node-1")
	if cs.LocalNodeID() != "node-1" {
		t.Errorf("expected local node ID 'node-1', got %q", cs.LocalNodeID())
	}
	if cs.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", cs.NodeCount())
	}
}

func TestClusterState_AddRemoveNodes(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddNode(NodeInfo{NodeID: "peer-1", Address: "10.0.0.1:7630", Status: NodeStatusHealthy})
	cs.AddNode(NodeInfo{NodeID: "peer-2", Address: "10.0.0.2:7630", Status: NodeStatusHealthy})

	if cs.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes, got %d", cs.NodeCount())
	}

	n, ok := cs.GetNode("peer-1")
	if !ok {
		t.Fatal("expected peer-1 to exist")
	}
	if n.Address != "10.0.0.1:7630" {
		t.Errorf("expected address '10.0.0.1:7630', got %q", n.Address)
	}

	// Update an existing node.
	cs.AddNode(NodeInfo{NodeID: "peer-1", Address: "10.0.0.1:9999", Status: NodeStatusUnhealthy})
	n, _ = cs.GetNode("peer-1")
	if n.Address != "10.0.0.1:9999" {
		t.Errorf("expected updated address, got %q", n.Address)
	}
	if n.Status != NodeStatusUnhealthy {
		t.Errorf("expected unhealthy status")
	}

	// Remove a node.
	cs.RemoveNode("peer-1")
	if cs.NodeCount() != 1 {
		t.Errorf("expected 1 node after removal, got %d", cs.NodeCount())
	}
	_, ok = cs.GetNode("peer-1")
	if ok {
		t.Error("expected peer-1 to be gone")
	}
}

func TestClusterState_AddRemoveServices(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddService(ServiceEndpoint{
		NodeID: "local", ServiceName: "web", Protocol: 0, LocalPort: 8080, Healthy: true,
	})
	cs.AddService(ServiceEndpoint{
		NodeID: "peer-1", ServiceName: "web", Protocol: 0, LocalPort: 8080, Healthy: true,
	})
	cs.AddService(ServiceEndpoint{
		NodeID: "peer-1", ServiceName: "db", Protocol: 0, LocalPort: 3306, Healthy: true,
	})

	all := cs.AllServices()
	if len(all["web"]) != 2 {
		t.Errorf("expected 2 web endpoints, got %d", len(all["web"]))
	}
	if len(all["db"]) != 1 {
		t.Errorf("expected 1 db endpoint, got %d", len(all["db"]))
	}

	// Update existing endpoint.
	cs.AddService(ServiceEndpoint{
		NodeID: "peer-1", ServiceName: "web", Protocol: 0, LocalPort: 9090, Healthy: true,
	})
	all = cs.AllServices()
	if len(all["web"]) != 2 {
		t.Errorf("expected 2 web endpoints after update, got %d", len(all["web"]))
	}
	for _, ep := range all["web"] {
		if ep.NodeID == "peer-1" && ep.LocalPort != 9090 {
			t.Errorf("expected updated port 9090, got %d", ep.LocalPort)
		}
	}

	// Remove specific service from a node.
	cs.RemoveService("web", "peer-1")
	all = cs.AllServices()
	if len(all["web"]) != 1 {
		t.Errorf("expected 1 web endpoint after removal, got %d", len(all["web"]))
	}

	// Remove last endpoint of a service.
	cs.RemoveService("db", "peer-1")
	all = cs.AllServices()
	if _, ok := all["db"]; ok {
		t.Error("expected db service to be removed entirely")
	}
}

func TestClusterState_RemoveNode_CleansUpServices(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddNode(NodeInfo{NodeID: "peer-1", Address: "10.0.0.1:7630", Status: NodeStatusHealthy})
	cs.AddService(ServiceEndpoint{
		NodeID: "peer-1", ServiceName: "web", Protocol: 0, LocalPort: 8080, Healthy: true,
	})
	cs.AddService(ServiceEndpoint{
		NodeID: "peer-1", ServiceName: "db", Protocol: 0, LocalPort: 3306, Healthy: true,
	})
	cs.AddService(ServiceEndpoint{
		NodeID: "local", ServiceName: "web", Protocol: 0, LocalPort: 8080, Healthy: true,
	})

	cs.RemoveNode("peer-1")

	all := cs.AllServices()
	if len(all["web"]) != 1 {
		t.Errorf("expected 1 web endpoint (local only), got %d", len(all["web"]))
	}
	if _, ok := all["db"]; ok {
		t.Error("expected db service to be removed with peer-1")
	}
}

func TestClusterState_LookupRemote(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddNode(NodeInfo{NodeID: "peer-1", Address: "10.0.0.1:7630", Status: NodeStatusHealthy})
	cs.AddNode(NodeInfo{NodeID: "peer-2", Address: "10.0.0.2:7630", Status: NodeStatusUnhealthy})

	cs.AddService(ServiceEndpoint{NodeID: "local", ServiceName: "web", Healthy: true})
	cs.AddService(ServiceEndpoint{NodeID: "peer-1", ServiceName: "web", Healthy: true})
	cs.AddService(ServiceEndpoint{NodeID: "peer-2", ServiceName: "web", Healthy: true})

	remote := cs.LookupRemote("web")
	if len(remote) != 1 {
		t.Fatalf("expected 1 remote endpoint (peer-2 unhealthy), got %d", len(remote))
	}
	if remote[0].NodeID != "peer-1" {
		t.Errorf("expected peer-1, got %s", remote[0].NodeID)
	}

	// Non-existent service.
	remote = cs.LookupRemote("nonexistent")
	if len(remote) != 0 {
		t.Errorf("expected 0 remote endpoints for nonexistent service, got %d", len(remote))
	}
}

func TestClusterState_LookupRemote_ExcludesUnhealthyEndpoints(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddNode(NodeInfo{NodeID: "peer-1", Address: "10.0.0.1:7630", Status: NodeStatusHealthy})
	cs.AddService(ServiceEndpoint{NodeID: "peer-1", ServiceName: "web", Healthy: false})

	remote := cs.LookupRemote("web")
	if len(remote) != 0 {
		t.Errorf("expected 0 remote endpoints (endpoint unhealthy), got %d", len(remote))
	}
}

func TestClusterState_IsLocal(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddService(ServiceEndpoint{NodeID: "local", ServiceName: "web", Healthy: true})
	cs.AddService(ServiceEndpoint{NodeID: "peer-1", ServiceName: "db", Healthy: true})

	if !cs.IsLocal("web") {
		t.Error("expected web to be local")
	}
	if cs.IsLocal("db") {
		t.Error("expected db to not be local")
	}
	if cs.IsLocal("nonexistent") {
		t.Error("expected nonexistent to not be local")
	}
}

func TestClusterState_HealthyNodes(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddNode(NodeInfo{NodeID: "peer-1", Status: NodeStatusHealthy})
	cs.AddNode(NodeInfo{NodeID: "peer-2", Status: NodeStatusUnhealthy})
	cs.AddNode(NodeInfo{NodeID: "peer-3", Status: NodeStatusHealthy})

	healthy := cs.HealthyNodes()
	if len(healthy) != 2 {
		t.Fatalf("expected 2 healthy nodes, got %d", len(healthy))
	}
}

func TestClusterState_DefaultWeight(t *testing.T) {
	cs := NewClusterState("local")

	cs.AddService(ServiceEndpoint{NodeID: "local", ServiceName: "web", Weight: 0})

	all := cs.AllServices()
	if all["web"][0].Weight != 1 {
		t.Errorf("expected default weight 1, got %d", all["web"][0].Weight)
	}
}

func TestClusterState_ConcurrentAccess(t *testing.T) {
	cs := NewClusterState("local")

	done := make(chan struct{})
	go func() {
		defer func() { done <- struct{}{} }()
		for i := range 100 {
			cs.AddNode(NodeInfo{
				NodeID:  fmt.Sprintf("peer-%d", i),
				Address: fmt.Sprintf("10.0.0.%d:7630", i),
				Status:  NodeStatusHealthy,
			})
			cs.AddService(ServiceEndpoint{
				NodeID:      fmt.Sprintf("peer-%d", i),
				ServiceName: "web",
				Healthy:     true,
			})
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		for range 100 {
			cs.LookupRemote("web")
			cs.IsLocal("web")
			cs.HealthyNodes()
			cs.AllServices()
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		for i := range 100 {
			cs.RemoveNode(fmt.Sprintf("peer-%d", i))
		}
	}()

	// Wait with timeout.
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range 3 {
		select {
		case <-done:
		case <-timer.C:
			t.Fatal("concurrent access test timed out")
		}
	}
}
