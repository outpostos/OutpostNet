package servicehost_test

import (
	"path/filepath"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/service"
)

// TestEntryReachesServiceOnDialingPeer is P1-NET-5 acceptance 1: node-a has
// the smaller node_id, so it dials node-b and node-b only accepts. node-b's
// entry for echo@node-a must still work (it previously failed with "peer not
// in directory" because only the dialer could open streams).
func TestEntryReachesServiceOnDialingPeer(t *testing.T) {
	app := startApp(t, "a")

	// node-b: accepting side, consumes echo@node-a.
	entry := freeAddr(t)
	nodeBCfg := &service.Config{Config: agentconfig.Config{
		NodeID:     "node-b",
		HealthAddr: "127.0.0.1:0",
		Tunnels:    []agentconfig.TunnelConfig{{Role: "server", ListenAddr: "127.0.0.1:0"}},
		Imports:    []agentconfig.ImportConfig{{Service: "echo@node-a", Name: "echo", ListenAddr: entry, Protocol: "tcp"}},
		TLS:        agentconfig.TLSConfig{Insecure: true},
		Auth:       agentconfig.AuthConfig{Token: token},
		Management: agentconfig.ManagementConfig{SocketPath: filepath.Join(sockDir(t), "node-b.sock")},
	}}
	nodeB := startHost(t, nodeBCfg)

	// node-a: dialing side, exports echo.
	nodeACfg := exportingNode(t, "node-a", app, token)
	nodeACfg.Peers = []service.Peer{{NodeID: "node-b", Addr: tunnelAddr(t, nodeB)}}
	nodeA := startHost(t, nodeACfg)
	// node-b learns node-a's address too; by role arbitration it does not dial.
	putPeers(t, nodeBCfg.Management.SocketPath, []map[string]string{{"node_id": "node-a", "addr": tunnelAddr(t, nodeA)}})

	askEventually(t, entry, "a:hello")

	// Restarting the dialer re-registers it on node-b; new connections
	// through the same entry recover.
	nodeA.stop(t)
	nodeA2Cfg := exportingNode(t, "node-a", startApp(t, "a2"), token)
	nodeA2Cfg.Peers = []service.Peer{{NodeID: "node-b", Addr: tunnelAddr(t, nodeB)}}
	startHost(t, nodeA2Cfg)
	askEventually(t, entry, "a2:hello")
}
