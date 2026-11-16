package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/outpostos/OutpostNet/pkg/chronyconf"
)

func newChronyCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "chrony",
		Short: "Generate chrony/NTS group time-sync configuration (ADR-0006 D7)",
	}
	root.AddCommand(newChronyRenderCommand())
	return root
}

type chronyRenderOptions struct {
	nodeID        string
	certFile      string
	keyFile       string
	caFile        string
	peerFlags     []string
	peersSocket   string
	allow         []string
	output        string
	sourcesOutput string
	ntpPort       int
	ntskePort     int
	dumpDir       string
	localStratum  int
	makestepThres float64
	makestepLimit int
	nocertCheck   int
	gnssSHM       int
	gnssSOCK      string
	gnssPPS       string
	gnssRefID     string
	unauthServers []string
}

func newChronyRenderCommand() *cobra.Command {
	var opts chronyRenderOptions
	cmd := &cobra.Command{
		Use:   "render",
		Short: "Render the NTS server/source fragment from the Net peer set",
		Long: "Render a chrony configuration fragment that makes this node an NTS time server " +
			"for its Outpost group and polls group peers over NTS. Peers come from --peer or, " +
			"with --peers-socket, from the agent's /v1/peers management endpoint. No " +
			"unauthenticated NTP source is emitted unless --unauthenticated-server is given.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runChronyRender(cmd, opts)
		},
	}
	f := cmd.Flags()
	f.StringVar(&opts.nodeID, "node-id", "", "This node's identity (must match the certificate)")
	f.StringVar(&opts.certFile, "cert", "", "Node certificate PEM (NTS server)")
	f.StringVar(&opts.keyFile, "key", "", "Node private key PEM (NTS server)")
	f.StringVar(&opts.caFile, "ca", "", "Group CA certificate PEM (NTS peer trust)")
	f.StringArrayVar(&opts.peerFlags, "peer", nil, "Group peer as NODE_ID=HOST[:PORT], repeatable (PORT is not the NTP port)")
	f.StringVar(&opts.peersSocket, "peers-socket", "", "Read the peer set from this agent management Unix socket (/v1/peers)")
	f.StringArrayVar(&opts.allow, "allow", nil, "Extra client IP or CIDR allowed to use this node as NTP server, repeatable")
	f.StringVar(&opts.output, "output", "", "Write the main fragment here (default stdout)")
	f.StringVar(&opts.sourcesOutput, "sources-output", "", "Write NTP sources to this .sources file and emit sourcedir; enables chronyc reload sources")
	f.IntVar(&opts.ntpPort, "ntp-port", chronyconf.DefaultNTPPort, "UDP port served for NTP")
	f.IntVar(&opts.ntskePort, "nts-ke-port", chronyconf.DefaultNTSKEPort, "TCP port served for NTS-KE")
	f.StringVar(&opts.dumpDir, "nts-dump-dir", chronyconf.DefaultDumpDir, "chrony NTS key/cookie directory")
	f.IntVar(&opts.localStratum, "local-stratum", chronyconf.DefaultLocalStratum, "Orphan-mode stratum")
	f.Float64Var(&opts.makestepThres, "makestep-threshold", chronyconf.DefaultMakestepThresh, "makestep threshold in seconds")
	f.IntVar(&opts.makestepLimit, "makestep-limit", chronyconf.DefaultMakestepLimit, "makestep update limit (-1 for no limit)")
	f.IntVar(&opts.nocertCheck, "nocerttimecheck", chronyconf.DefaultNocertTimecheck, "Clock updates before NTS certificate time checks apply")
	f.IntVar(&opts.gnssSHM, "gnss-shm", -1, "gpsd SHM segment for the GNSS refclock (optional)")
	f.StringVar(&opts.gnssSOCK, "gnss-sock", "", "gpsd Unix socket for the GNSS refclock (optional)")
	f.StringVar(&opts.gnssPPS, "gnss-pps", "", "PPS device paired with the GNSS refclock (optional)")
	f.StringVar(&opts.gnssRefID, "gnss-refid", chronyconf.DefaultGNSSRefID, "GNSS refclock reference ID")
	f.StringArrayVar(&opts.unauthServers, "unauthenticated-server", nil, "Explicit unauthenticated NTP server, repeatable (discouraged)")
	for _, name := range []string{"node-id", "cert", "key", "ca"} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(err)
		}
	}
	return cmd
}

func runChronyRender(cmd *cobra.Command, opts chronyRenderOptions) error {
	if cmd.Flags().Changed("nocerttimecheck") && opts.nocertCheck == 0 {
		return fmt.Errorf("--nocerttimecheck 0 disables the ADR-0006 D7 first-sync relaxation; use a positive value")
	}
	peers, err := collectPeers(cmd.Context(), opts)
	if err != nil {
		return err
	}
	cfg := chronyconf.Config{
		NodeID:                 opts.nodeID,
		CertFile:               opts.certFile,
		KeyFile:                opts.keyFile,
		CAFile:                 opts.caFile,
		Peers:                  peers,
		Allow:                  opts.allow,
		NTPPort:                opts.ntpPort,
		NTSKEPort:              opts.ntskePort,
		DumpDir:                opts.dumpDir,
		LocalStratum:           opts.localStratum,
		MakestepThreshold:      opts.makestepThres,
		MakestepLimit:          opts.makestepLimit,
		NocertTimecheck:        opts.nocertCheck,
		UnauthenticatedServers: opts.unauthServers,
	}
	if opts.sourcesOutput != "" {
		if !strings.HasSuffix(opts.sourcesOutput, ".sources") {
			return fmt.Errorf("--sources-output must end in .sources so chrony loads it from sourcedir")
		}
		cfg.SourcesDir = filepath.Dir(opts.sourcesOutput)
	}
	if opts.gnssSHM >= 0 || opts.gnssSOCK != "" {
		g := &chronyconf.GNSS{PPSDevice: opts.gnssPPS, RefID: opts.gnssRefID}
		if opts.gnssSHM >= 0 {
			seg := opts.gnssSHM
			g.SHMSegment = &seg
		}
		g.SOCKPath = opts.gnssSOCK
		cfg.GNSS = g
	} else if opts.gnssPPS != "" {
		return fmt.Errorf("--gnss-pps requires --gnss-shm or --gnss-sock")
	}

	out, err := chronyconf.Render(cfg)
	if err != nil {
		return err
	}
	for _, warning := range out.Warnings {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s\n", warning)
	}

	if opts.output == "" {
		fmt.Fprint(cmd.OutOrStdout(), out.Main)
	} else if err := writeFragment(opts.output, out.Main); err != nil {
		return err
	}
	if opts.sourcesOutput != "" {
		if err := writeFragment(opts.sourcesOutput, out.Sources); err != nil {
			return err
		}
	}
	return nil
}

// collectPeers merges explicit --peer values with the agent peer directory.
// Explicit entries win on node_id. Ports in peer addresses are QUIC ports and
// are not used for NTP.
func collectPeers(ctx context.Context, opts chronyRenderOptions) ([]chronyconf.Peer, error) {
	byID := map[string]chronyconf.Peer{}
	var order []string
	add := func(peer chronyconf.Peer) {
		if _, seen := byID[peer.NodeID]; !seen {
			order = append(order, peer.NodeID)
		}
		byID[peer.NodeID] = peer
	}
	for _, raw := range opts.peerFlags {
		nodeID, host, ok := strings.Cut(raw, "=")
		if !ok || strings.TrimSpace(nodeID) == "" {
			return nil, fmt.Errorf("--peer %q must be NODE_ID=HOST[:PORT]", raw)
		}
		host, err := peerHost(host)
		if err != nil {
			return nil, fmt.Errorf("--peer %q: %w", raw, err)
		}
		add(chronyconf.Peer{NodeID: strings.TrimSpace(nodeID), Host: host})
	}
	if opts.peersSocket != "" {
		peers, err := fetchPeerDirectory(ctx, opts.peersSocket)
		if err != nil {
			return nil, err
		}
		for _, p := range peers {
			host, err := peerHost(p.Addr)
			if err != nil {
				return nil, fmt.Errorf("peer %q from %s: %w", p.NodeID, opts.peersSocket, err)
			}
			if _, explicit := byID[p.NodeID]; explicit {
				continue
			}
			add(chronyconf.Peer{NodeID: p.NodeID, Host: host})
		}
	}
	peers := make([]chronyconf.Peer, 0, len(order))
	for _, id := range order {
		peers = append(peers, byID[id])
	}
	return peers, nil
}

// peerHost returns the host part of an optional host:port value. Bare IPv6
// addresses are kept intact.
func peerHost(value string) (string, error) {
	host := strings.TrimSpace(value)
	if host == "" {
		return "", fmt.Errorf("empty address")
	}
	if strings.HasPrefix(host, "[") {
		h, _, err := net.SplitHostPort(host)
		if err != nil {
			return "", fmt.Errorf("invalid bracketed address: %w", err)
		}
		return h, nil
	}
	if net.ParseIP(host) != nil {
		return host, nil
	}
	if strings.Count(host, ":") == 1 {
		h, port, err := net.SplitHostPort(host)
		if err != nil {
			return "", err
		}
		if _, err := strconv.Atoi(port); err != nil {
			return "", fmt.Errorf("invalid port %q", port)
		}
		return h, nil
	}
	if strings.Contains(host, ":") {
		return "", fmt.Errorf("address %q must be HOST or HOST:PORT", host)
	}
	return host, nil
}

type directoryPeer struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

func fetchPeerDirectory(ctx context.Context, socket string) ([]directoryPeer, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/v1/peers", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read peer directory: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read peer directory: %s", resp.Status)
	}
	var peers []directoryPeer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&peers); err != nil {
		return nil, fmt.Errorf("decode peer directory: %w", err)
	}
	for _, p := range peers {
		if p.NodeID == "" || p.Addr == "" {
			return nil, fmt.Errorf("peer directory returned an entry without node_id or addr")
		}
	}
	return peers, nil
}

func writeFragment(path, content string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".outpostnet-chrony-*")
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("install %s: %w", path, err)
	}
	return nil
}
