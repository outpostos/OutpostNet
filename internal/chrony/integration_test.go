package chrony_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/internal/chrony"
	"github.com/outpostos/OutpostNet/pkg/state"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// Opt in on Linux with chrony +NTS installed. Uses only per-process virtual
// clocks (-x), private Unix sockets and high loopback ports. No host service changes.
func TestChronyRealNTSGroupAndForeignCA(t *testing.T) {
	if os.Getenv("OUTPOSTNET_CHRONY_TEST") != "1" {
		t.Skip("set OUTPOSTNET_CHRONY_TEST=1 on Linux with chrony +NTS")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("Linux required")
	}
	for _, name := range []string{"chronyd", "chronyc"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	// Use a private executable so the distro's path-based AppArmor profile for
	// /usr/sbin/chronyd does not forbid temporary test files/sockets. The profile
	// and the installed daemon are untouched; every process still runs with -x.
	installed, _ := exec.LookPath("chronyd")
	executable, err := os.ReadFile(installed)
	if err != nil {
		t.Fatal(err)
	}
	daemon := filepath.Join(dir, "chronyd-test")
	if err := os.WriteFile(daemon, executable, 0700); err != nil {
		t.Fatal(err)
	}
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	caPEM, caKey, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := tls.X509KeyPair(caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	foreignPEM, foreignKey, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	foreignCA, err := tls.X509KeyPair(foreignPEM, foreignKey)
	if err != nil {
		t.Fatal(err)
	}
	type node struct {
		id, ip, dir, conf, socket string
		log                       bytes.Buffer
		cmd                       *exec.Cmd
	}
	nodes := []*node{{id: "a", ip: "127.0.0.2"}, {id: "b", ip: "127.0.0.3"}, {id: "foreign", ip: "127.0.0.4"}}
	// Allocate high ports once; bindaddress separates the processes.
	udp, err := net.ListenPacket("udp4", "127.0.0.2:0")
	if err != nil {
		t.Fatal(err)
	}
	ntpPort := udp.LocalAddr().(*net.UDPAddr).Port
	udp.Close()
	tcp, err := net.Listen("tcp4", "127.0.0.2:0")
	if err != nil {
		t.Fatal(err)
	}
	ntsPort := tcp.Addr().(*net.TCPAddr).Port
	tcp.Close()
	for _, n := range nodes {
		n.dir = filepath.Join(dir, n.id)
		if err := os.Mkdir(n.dir, 0700); err != nil {
			t.Fatal(err)
		}
		n.socket = filepath.Join(n.dir, "cmd.sock")
		issuer, pem := ca, caPEM
		if n.id == "foreign" {
			issuer, pem = foreignCA, foreignPEM
		}
		issuedAt := time.Now()
		if n.id == "a" {
			issuedAt = issuedAt.AddDate(1, 0, 0)
		} // Startup outside certificate validity, no host clock change.
		cert, key, err := tlsutil.IssueNodeWithSANs(issuer, n.id, issuedAt, nil, []net.IP{net.ParseIP(n.ip)})
		if err != nil {
			t.Fatal(err)
		}
		write := func(name string, b []byte) string {
			t.Helper()
			p := filepath.Join(n.dir, name)
			if err := os.WriteFile(p, b, 0600); err != nil {
				t.Fatal(err)
			}
			return p
		}
		cfg := chrony.Config{NodeID: n.id, CAFile: write("ca.pem", pem), CertFile: write("node.pem", cert), KeyFile: write("node.key", key), NTPPort: ntpPort, NTSKEPort: ntsPort}
		if n.id == "a" {
			for _, g := range []chrony.GNSS{{Driver: "SHM", Parameter: "0", PPS: "/dev/pps0"}, {Driver: "SOCK", Parameter: "/run/chrony.clk.ttyS0.sock", PPS: "/dev/pps0"}} {
				variant := cfg
				variant.GNSS = g
				data, err := chrony.Render(context.Background(), variant, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				path := write("gnss.conf", data)
				if out, err := exec.Command(daemon, "-p", "-f", path).CombinedOutput(); err != nil {
					t.Fatalf("GNSS parse: %s %v", out, err)
				}
			}
		}
		peers := []chrony.Peer{{NodeID: "a", Addr: "127.0.0.2:4242"}, {NodeID: "b", Addr: "127.0.0.3:4242"}, {NodeID: "foreign", Addr: "127.0.0.4:4242"}}
		data, err := chrony.Render(context.Background(), cfg, peers, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Ports already come from cfg; inject only test-only poll timing and the
		// per-process bind/control sockets. Trust/access/orphan directives stay
		// intact. The foreign source cannot be used even though IP-allowed.
		text := strings.ReplaceAll(string(data), " iburst nts ", " minpoll 0 maxpoll 0 iburst nts ")
		text += fmt.Sprintf("bindaddress %s\nbindacqaddress %s\ncmdport 0\nbindcmdaddress %s\npidfile %s\n", n.ip, n.ip, n.socket, filepath.Join(n.dir, "pid"))
		n.conf = write("chrony.conf", []byte(text))
		parsed, err := exec.Command(daemon, "-p", "-f", n.conf).CombinedOutput()
		if err != nil {
			t.Fatalf("parse: %s %v", parsed, err)
		}
		// New chrony releases wait 300s before enabling an orphan reference.
		// Parse the generated policy above unchanged; shorten only this fixture's
		// timing when the version supports waitunsynced (older versions activate
		// immediately). Trust, access, authentication and certificate rules stay intact.
		fast := strings.Replace(text, "local stratum 10 orphan\n", "local stratum 10 orphan waitunsynced 0\n", 1)
		fastFile := write("fast-orphan.conf", []byte(fast))
		if _, err := exec.Command(daemon, "-p", "-f", fastFile).CombinedOutput(); err == nil {
			n.conf = fastFile
		}
		n.cmd = exec.Command(daemon, "-U", "-x", "-d", "-u", account.Username, "-f", n.conf)
		n.cmd.Stdout = &n.log
		n.cmd.Stderr = &n.log
		if err := n.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			n.cmd.Process.Signal(os.Interrupt)
			n.cmd.Wait()
			if t.Failed() {
				t.Logf("%s chronyd: %s", n.id, n.log.String())
			}
		})
	}
	query := func(n *node, command string) string {
		out, _ := exec.Command("chronyc", "-n", "-c", "-h", n.socket, command).CombinedOutput()
		return string(out)
	}
	deadline := time.Now().Add(30 * time.Second)
	var sources, auth string
	for time.Now().Before(deadline) {
		sources = query(nodes[1], "sources")
		auth = query(nodes[1], "authdata")
		if strings.Contains(sources, "^,*,127.0.0.2,") {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !strings.Contains(sources, "^,*,127.0.0.2,") {
		t.Fatalf("group source not selected:\nsources=%s\nauthdata=%s\nntpdata=%s\nserverstats=%s\nclients=%s", sources, auth, query(nodes[1], "ntpdata 127.0.0.2"), query(nodes[0], "serverstats"), query(nodes[0], "clients"))
	}
	// authdata CSV: address, mode, key-id, type, keylen, last, attempts,
	// NAK, cookies, cookie-length. Both rows must report NTS, only group gets cookies.
	var good, bad bool
	for _, line := range strings.Split(strings.TrimSpace(auth), "\n") {
		f := strings.Split(line, ",")
		if len(f) < 10 {
			continue
		}
		if f[0] == "127.0.0.2" {
			good = f[1] == "NTS" && f[8] != "0"
		}
		if f[0] == "127.0.0.4" {
			bad = f[1] == "NTS" && f[6] != "0" && f[8] == "0"
		}
	}
	if !good || !bad {
		t.Fatalf("NTS cookie evidence missing: %s", auth)
	}
	if strings.Contains(sources, "^,*,127.0.0.4,") {
		t.Fatal("foreign CA selected")
	}
	if out := query(nodes[0], "accheck 127.0.0.9"); !strings.Contains(out, "209") {
		t.Fatalf("non-peer not denied: %s", out)
	}
	if out := query(nodes[0], "accheck 127.0.0.3"); !strings.Contains(out, "208") {
		t.Fatalf("group peer denied: %s", out)
	}
	local, err := state.ParseChronyTracking(query(nodes[0], "tracking"))
	if err != nil || local.Synchronized {
		t.Fatalf("orphan leader must remain unsynchronized: %+v %v", local, err)
	}
	// Boot readiness includes an active orphan without promoting its time quality.
	for _, n := range nodes[:2] {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		args := []string{"--socket", n.socket, "--timeout", "3", "--interval", "0.1", "--on-timeout", "fail"}
		if n.id == "a" {
			args = append(args, "--local-dwell", "0.3") // Shorten only the leader's fixture dwell.
		}
		// The follower keeps the default 60s dwell, longer than this timeout:
		// it must pass through the synchronized-source branch, including on 4.2–4.5.
		out, err := timeReadyCommand(ctx, args...).CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("%s boot gate: %s %v", n.id, out, err)
		}
		want := "ready: local orphan reference"
		if n.id == "b" {
			want = "ready: synchronized source"
		}
		if !strings.Contains(string(out), want) {
			t.Fatalf("%s boot gate: expected %q: %s", n.id, want, out)
		}
		t.Logf("%s boot gate: %s", n.id, strings.TrimSpace(string(out)))
	}
	tracking := query(nodes[1], "tracking")
	observation, err := state.ParseChronyTracking(tracking)
	if err != nil || !observation.Synchronized {
		t.Fatalf("unchanged time-source adapter: %+v %v, %s", observation, err, tracking)
	}
	// Reuse the foreign-CA process as a node with no reference at all. Its
	// group peers are unauthenticated to it; disable only its local fallback.
	if out := query(nodes[2], "local off"); !strings.Contains(out, "200") {
		t.Fatalf("disable fixture local reference: %s", out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	out, err := timeReadyCommand(ctx, "--socket", nodes[2].socket, "--timeout", "1", "--interval", "0.1", "--on-timeout", "fail").CombinedOutput()
	if err == nil || ctx.Err() != nil || time.Since(started) < time.Second || !strings.Contains(string(out), "WARNING: timeout:") {
		t.Fatalf("no-reference gate must wait and fail at bound: %s %v", out, err)
	}
	t.Logf("group selected, NTS cookies obtained, foreign CA has zero cookies; non-peer access denied; adapter reports %s", observation.State)
}
