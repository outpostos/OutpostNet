package app

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/outpostos/OutpostNet/internal/chrony"
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
	"github.com/spf13/cobra"
)

func newChronyCommand() *cobra.Command {
	var configFile, peersFile, output, unit, sock, pps, socket string
	var shm, ntpPort, ntskePort int
	var apply, watch bool
	var interval time.Duration
	cmd := &cobra.Command{Use: "chrony", Short: "Render or maintain group chrony/NTS configuration", Args: cobra.NoArgs}
	cmd.Flags().StringVar(&configFile, "config", "", "Agent YAML containing node_id and group TLS credentials")
	cmd.Flags().StringVar(&peersFile, "peers", "", "Net peer-set JSON file (same array as PUT /v1/peers)")
	cmd.Flags().StringVar(&socket, "socket", "", "Read the live Net peer set from this management Unix socket instead of --peers")
	cmd.Flags().StringVar(&output, "output", "", "Fragment output file (default stdout)")
	cmd.Flags().BoolVar(&apply, "apply", false, "Validate with chronyd -p, atomically install, and restart chrony")
	cmd.Flags().BoolVar(&watch, "watch", false, "Re-render peers and credentials periodically; requires --apply")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "Watch interval (DNS is refreshed too)")
	cmd.Flags().StringVar(&unit, "unit", "chrony.service", "systemd chrony service to restart on apply")
	cmd.Flags().IntVar(&shm, "gnss-shm", -1, "Configured gpsd SHM unit (0-255)")
	cmd.Flags().StringVar(&sock, "gnss-sock", "", "Configured gpsd chrony SOCK path")
	cmd.Flags().StringVar(&pps, "pps", "", "Optional PPS device; requires a configured GNSS receiver")
	cmd.Flags().IntVar(&ntpPort, "ntp-port", 123, "Group-wide NTP UDP port (listen and per-source)")
	cmd.Flags().IntVar(&ntskePort, "ntske-port", 4460, "Group-wide NTS-KE TCP port (listen and per-source)")
	cmd.MarkFlagRequired("config")

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if (peersFile == "") == (socket == "") {
			return fmt.Errorf("select exactly one of --peers or --socket")
		}
		if watch && !apply || apply && output == "" {
			return fmt.Errorf("--watch requires --apply; --apply requires --output")
		}
		if interval <= 0 {
			return fmt.Errorf("interval must be positive")
		}
		if shm < -1 || shm > 255 || shm >= 0 && sock != "" {
			return fmt.Errorf("select one gpsd SHM unit or SOCK path")
		}
		if !strings.HasSuffix(unit, ".service") || strings.HasPrefix(unit, "-") || strings.ContainsAny(unit, "/ \t\r\n") {
			return fmt.Errorf("invalid chrony service unit")
		}
		if ntpPort < 1 || ntpPort > 65535 || ntskePort < 1 || ntskePort > 65535 {
			return fmt.Errorf("NTP and NTS-KE ports must be 1-65535")
		}
		gnss := chrony.GNSS{PPS: pps}
		if shm >= 0 {
			gnss.Driver = "SHM"
			gnss.Parameter = fmt.Sprint(shm)
		}
		if sock != "" {
			gnss.Driver = "SOCK"
			gnss.Parameter = sock
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		var applied [32]byte
		update := func() error {
			probe, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			data, digest, err := renderChrony(probe, configFile, peersFile, socket, gnss, ntpPort, ntskePort)
			if err != nil {
				return err
			}
			if digest == applied {
				return nil
			}
			if output == "" {
				_, err = cmd.OutOrStdout().Write(data)
			} else {
				err = installChrony(probe, output, data, apply, unit)
			}
			if err != nil {
				return err
			}
			applied = digest
			if apply {
				fmt.Fprintln(cmd.ErrOrStderr(), "chrony group configuration applied")
			}
			return nil
		}
		if err := update(); err != nil {
			return err
		}
		if !watch {
			return nil
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := update(); err != nil && ctx.Err() == nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "chrony update failed (will retry): %v\n", err)
				}
			}
		}
	}
	return cmd
}

func renderChrony(ctx context.Context, configFile, peersFile, socket string, gnss chrony.GNSS, ntpPort, ntskePort int) ([]byte, [32]byte, error) {
	var zero [32]byte
	cfg, err := agentconfig.LoadConfig(configFile)
	if err != nil {
		return nil, zero, fmt.Errorf("load agent configuration failed")
	}
	if cfg.TLS.CAFile == "" || cfg.TLS.Insecure {
		return nil, zero, fmt.Errorf("chrony requires group certificate mode")
	}
	// Resolve exactly as the agent does: relative paths are relative to the caller.
	paths := []*string{&cfg.TLS.CAFile, &cfg.TLS.CertFile, &cfg.TLS.KeyFile}
	hash := sha256.New()
	for _, p := range paths {
		*p, err = filepath.Abs(*p)
		if err != nil {
			return nil, zero, fmt.Errorf("resolve credential path failed")
		}
		b, err := os.ReadFile(*p)
		if err != nil {
			return nil, zero, fmt.Errorf("read group credentials failed")
		}
		// Fixed-length component hashes avoid ambiguous concatenation; never log key material.
		sum := sha256.Sum256(b)
		hash.Write(sum[:])
	}
	if _, err := tlsutil.LoadIdentity(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile, cfg.NodeID, nil); err != nil {
		return nil, zero, fmt.Errorf("invalid group node credentials")
	}
	pair, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, zero, fmt.Errorf("load NTS key pair failed")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, zero, fmt.Errorf("invalid NTS certificate")
	}
	if len(leaf.DNSNames)+len(leaf.IPAddresses) == 0 {
		return nil, zero, fmt.Errorf("NTS requires a node certificate with IP or DNS SANs; reissue with cert issue --san")
	}
	var b []byte
	if socket == "" {
		b, err = os.ReadFile(peersFile)
	} else {
		b, err = readChronyPeers(ctx, socket)
	}
	if err != nil {
		return nil, zero, fmt.Errorf("read Net peer set failed")
	}
	var peers []chrony.Peer
	if err := json.Unmarshal(b, &peers); err != nil || strings.TrimSpace(string(b)) == "null" {
		return nil, zero, fmt.Errorf("Net peers must be a JSON array")
	}
	data, err := chrony.Render(ctx, chrony.Config{NodeID: cfg.NodeID, CAFile: cfg.TLS.CAFile, CertFile: cfg.TLS.CertFile, KeyFile: cfg.TLS.KeyFile, GNSS: gnss, NTPPort: ntpPort, NTSKEPort: ntskePort}, peers, nil)
	if err != nil {
		return nil, zero, err
	}
	hash.Write(data)
	copy(zero[:], hash.Sum(nil))
	return data, zero, nil
}

func installChrony(ctx context.Context, path string, data []byte, apply bool, unit string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".outpostnet-chrony-*.conf")
	if err != nil {
		return fmt.Errorf("create chrony candidate failed")
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0644); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return fmt.Errorf("write chrony candidate failed")
	}
	if apply {
		// No shell or clock-changing probe. A full config restart is required for
		// removals, access rules and the NTS server key (reload sources is insufficient).
		if err := exec.CommandContext(ctx, "chronyd", "-p", "-f", name).Run(); err != nil {
			return fmt.Errorf("chronyd parse check failed")
		}
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("install chrony candidate failed")
	}
	if apply {
		if err := exec.CommandContext(ctx, "systemctl", "restart", unit).Run(); err != nil {
			return fmt.Errorf("chrony restart failed; configuration installed, retry pending")
		}
	}
	return nil
}

func readChronyPeers(ctx context.Context, socket string) ([]byte, error) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/v1/peers", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer directory returned non-success status")
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return nil, fmt.Errorf("peer set exceeds size limit")
	}
	return b, nil
}
