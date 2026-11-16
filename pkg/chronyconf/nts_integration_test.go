//go:build linux && integration

package chronyconf

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
)

// TestNTSSyncIntegration runs two real chronyd processes in separate network
// namespaces joined by a veth pair, renders their configuration with Render,
// and proves an NTS time exchange under the group CA. It then restarts the
// server with a foreign-CA certificate and a fresh client dump directory and
// asserts the client rejects it. It is gated behind the "integration" build tag
// and skips cleanly unless it runs as root with chrony >= 4.3, chronyc and a
// usable network namespace stack (GitHub #76 acceptance 2).
func TestNTSSyncIntegration(t *testing.T) {
	const (
		serverIP   = "10.42.0.1"
		clientIP   = "10.42.0.2"
		subnet     = "10.42.0.0/24"
		ntpPort    = 10123
		ntskePort  = 14460
		syncWait   = 120 * time.Second
		rejectWait = 45 * time.Second
	)

	if os.Geteuid() != 0 {
		ntsSkip(t, "NTS integration test needs root for network namespaces and privileged chronyd")
	}
	tools := requireNTSTools(t)

	dir := t.TempDir()
	env := newNTSNetEnv(t, tools)
	t.Cleanup(env.close)

	// Group CA plus node certificates whose IP SANs match the addresses the
	// peers dial. node-b is the client, node-a the server.
	groupCA := ntsNewCA(t, dir, "group")
	srvCert, srvKey := ntsIssue(t, dir, "node-a", "node-a", groupCA,
		[]string{"node-a.edge.local"}, []net.IP{net.ParseIP(serverIP)})
	cliCert, cliKey := ntsIssue(t, dir, "node-b", "node-b", groupCA,
		[]string{"node-b.edge.local"}, []net.IP{net.ParseIP(clientIP)})

	base := Config{
		NTPPort:   ntpPort,
		NTSKEPort: ntskePort,
		Allow:     []string{subnet},
	}

	serverCfg := base
	serverCfg.NodeID = "node-a"
	serverCfg.CertFile, serverCfg.KeyFile = srvCert, srvKey
	serverCfg.CAFile = groupCA
	serverCfg.DumpDir = filepath.Join(dir, "dump-server-1")

	clientCfg := base
	clientCfg.NodeID = "node-b"
	clientCfg.CertFile, clientCfg.KeyFile = cliCert, cliKey
	clientCfg.CAFile = groupCA
	clientCfg.DumpDir = filepath.Join(dir, "dump-client-1")
	clientCfg.Peers = []Peer{{NodeID: "node-a", Host: serverIP}}

	serverRendered := ntsRender(t, serverCfg)
	clientRendered := ntsRender(t, clientCfg)

	// The rendered client must trust only the group CA via a dedicated NTS
	// certset; this is the trust scoping this test exists to protect.
	for _, want := range []string{
		"nosystemcert",
		"ntstrustedcerts 1 " + groupCA,
		fmt.Sprintf("server %s nts iburst certset 1 port %d ntsport %d", serverIP, ntpPort, ntskePort),
	} {
		if !strings.Contains(clientRendered, want) {
			t.Fatalf("rendered client config missing %q:\n%s", want, clientRendered)
		}
	}

	srvConf := ntsWriteConfig(t, dir, "server-1", serverRendered)
	cliConf := ntsWriteConfig(t, dir, "client-1", clientRendered)

	// ---- Accept: group CA, fresh state, NTS source authenticates. ----
	server := env.start(t, dir, "server-1", srvConf)
	client := env.start(t, dir, "client-1", cliConf)

	ntsWaitAuthenticated(t, tools, client, serverIP, syncWait)
	ntsAssertConverged(t, tools, client)

	t.Log("group-CA NTS exchange authenticated and converged")

	// Stop both daemons so no cached cookies or keys survive the next phase.
	server.stop(t)
	client.stop(t)

	// ---- Reject: foreign CA, fresh client dump dir, no NTS authentication. ----
	foreignCA := ntsNewCA(t, dir, "foreign")
	foreignCert, foreignKey := ntsIssue(t, dir, "node-a-foreign", "node-a", foreignCA,
		[]string{"node-a.edge.local"}, []net.IP{net.ParseIP(serverIP)})

	serverCfg2 := serverCfg
	serverCfg2.CertFile, serverCfg2.KeyFile = foreignCert, foreignKey
	serverCfg2.CAFile = foreignCA
	serverCfg2.DumpDir = filepath.Join(dir, "dump-server-2")

	clientCfg2 := clientCfg
	clientCfg2.DumpDir = filepath.Join(dir, "dump-client-2")

	srvConf2 := ntsWriteConfig(t, dir, "server-2", ntsRender(t, serverCfg2))
	cliConf2 := ntsWriteConfig(t, dir, "client-2", ntsRender(t, clientCfg2))

	env.start(t, dir, "server-2", srvConf2)
	client2 := env.start(t, dir, "client-2", cliConf2)

	ntsWaitTLSRejection(t, client2, rejectWait)
	ntsAssertNotAuthenticated(t, tools, client2, serverIP)

	t.Log("foreign-CA server certificate rejected by the NTS client")
}

// ntsSkip skips when prerequisites are absent, but fails instead when CI sets
// OUTPOSTNET_NTS_REQUIRE=1 so a missing toolchain cannot silently green-build.
func ntsSkip(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("OUTPOSTNET_NTS_REQUIRE") == "1" {
		t.Fatalf("NTS integration prerequisites missing (OUTPOSTNET_NTS_REQUIRE=1): %s", reason)
	}
	t.Skip(reason)
}

func ntsSkipf(t *testing.T, format string, args ...any) {
	t.Helper()
	ntsSkip(t, fmt.Sprintf(format, args...))
}

// ntsToolset is the set of privileged binaries the scenario drives.
type ntsToolset struct {
	ip      string
	chronyd string
	chronyc string
	prefix  []string // aa-exec unconfined when AppArmor confines chronyd
}

func requireNTSTools(t *testing.T) ntsToolset {
	t.Helper()
	if _, err := os.Stat("/proc/self/ns/net"); err != nil {
		ntsSkipf(t, "/proc/self/ns/net unavailable: %v", err)
	}
	var tools ntsToolset
	for _, b := range []struct {
		name string
		dst  *string
	}{{"ip", &tools.ip}, {"chronyd", &tools.chronyd}, {"chronyc", &tools.chronyc}} {
		path, err := exec.LookPath(b.name)
		if err != nil {
			ntsSkipf(t, "%s not installed: %v", b.name, err)
		}
		*b.dst = path
	}
	if v := chronyVersion(t, tools.chronyd); v != "" && !versionAtLeast(v, 4, 3) {
		ntsSkipf(t, "chronyd %s is older than 4.3", v)
	}

	// Distributions ship an AppArmor profile confining chronyd to /etc/chrony,
	// /var/lib/chrony and /run/chrony, which denies configs and certs in a temp
	// directory. Detect that and fall back to an unconfined profile.
	probe := filepath.Join(t.TempDir(), "probe.conf")
	if err := os.WriteFile(probe, []byte("# probe\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(tools.chronyd, "-p", "-f", probe).CombinedOutput()
	if err != nil && strings.Contains(string(out), "Permission denied") {
		aaExec, err := exec.LookPath("aa-exec")
		if err != nil {
			ntsSkipf(t, "chronyd is AppArmor-confined and aa-exec is unavailable: %s", out)
		}
		if o, err := exec.Command(aaExec, "-p", "unconfined", "--", tools.chronyd, "-p", "-f", probe).CombinedOutput(); err != nil {
			ntsSkipf(t, "cannot run chronyd unconfined: %v\n%s", err, o)
		}
		tools.prefix = []string{aaExec, "-p", "unconfined", "--"}
	}

	// Skip unless a network namespace can actually be created.
	probeNS := fmt.Sprintf("onprobe%d", os.Getpid())
	_ = exec.Command(tools.ip, "netns", "del", probeNS).Run()
	if out, err := exec.Command(tools.ip, "netns", "add", probeNS).CombinedOutput(); err != nil {
		ntsSkipf(t, "network namespaces unavailable: %v\n%s", err, out)
	}
	_ = exec.Command(tools.ip, "netns", "del", probeNS).Run()
	return tools
}

var versionRE = regexp.MustCompile(`version (\d+)\.(\d+)`)

func chronyVersion(t *testing.T, chronyd string) string {
	t.Helper()
	out, err := exec.Command(chronyd, "-v").CombinedOutput()
	if err != nil {
		return ""
	}
	if m := versionRE.FindSubmatch(out); m != nil {
		return string(m[1]) + "." + string(m[2])
	}
	return ""
}

func versionAtLeast(v string, major, minor int) bool {
	parts := strings.SplitN(v, ".", 2)
	if len(parts) != 2 {
		return false
	}
	ma, err1 := strconv.Atoi(parts[0])
	mi, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return ma > major || (ma == major && mi >= minor)
}

// ntsNetEnv owns two network namespaces joined by a veth pair.
type ntsNetEnv struct {
	t      *testing.T
	tools  ntsToolset
	srvNS  string
	cliNS  string
	vethA  string
	vethB  string
	closed bool
}

func newNTSNetEnv(t *testing.T, tools ntsToolset) *ntsNetEnv {
	t.Helper()
	suffix := strconv.Itoa(os.Getpid())
	e := &ntsNetEnv{
		t: t, tools: tools,
		srvNS: "onsrv" + suffix, cliNS: "oncli" + suffix,
		vethA: "onva" + suffix, vethB: "onvb" + suffix,
	}
	e.run("netns", "add", e.srvNS)
	e.run("netns", "add", e.cliNS)
	e.run("link", "add", e.vethA, "type", "veth", "peer", "name", e.vethB)
	e.run("link", "set", e.vethA, "netns", e.srvNS)
	e.run("link", "set", e.vethB, "netns", e.cliNS)
	e.inNS(e.srvNS, "addr", "add", "10.42.0.1/24", "dev", e.vethA)
	e.inNS(e.cliNS, "addr", "add", "10.42.0.2/24", "dev", e.vethB)
	e.inNS(e.srvNS, "link", "set", e.vethA, "up")
	e.inNS(e.srvNS, "link", "set", "lo", "up")
	e.inNS(e.cliNS, "link", "set", e.vethB, "up")
	e.inNS(e.cliNS, "link", "set", "lo", "up")
	return e
}

func (e *ntsNetEnv) run(args ...string) {
	e.t.Helper()
	if out, err := exec.Command(e.tools.ip, args...).CombinedOutput(); err != nil {
		e.t.Fatalf("ip %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func (e *ntsNetEnv) inNS(ns string, args ...string) {
	e.t.Helper()
	e.run(append([]string{"netns", "exec", ns, e.tools.ip}, args...)...)
}

// start launches a daemonized chronyd inside ns. The process detaches, so a
// successful return from Run means startup succeeded; readiness then means the
// command socket exists.
func (e *ntsNetEnv) start(t *testing.T, dir, name, confPath string) *ntsDaemon {
	t.Helper()
	d := &ntsDaemon{
		name:    name,
		ns:      e.cliNS,
		logPath: filepath.Join(dir, name+".log"),
		sock:    ntsSocketPath(dir, name),
		pidFile: filepath.Join(dir, name+".pid"),
	}
	if strings.HasPrefix(name, "server") {
		d.ns = e.srvNS
	}
	args := []string{"netns", "exec", d.ns}
	args = append(args, e.tools.prefix...)
	args = append(args, e.tools.chronyd, "-x", "-u", "root", "-f", confPath, "-l", d.logPath)
	cmd := exec.Command(e.tools.ip, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("start %s: %v\nstderr: %s\nlog:\n%s", name, err, stderr.String(), ntsReadFile(d.logPath))
	}
	// Register cleanup as soon as the pid is known, so a daemon that never
	// becomes ready is still killed when the test fails.
	pidDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(pidDeadline) && d.pidVal == 0 {
		d.pidVal = d.pid()
		time.Sleep(100 * time.Millisecond)
	}
	t.Cleanup(func() { d.stop(t) })

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(d.sock); err == nil {
			return d
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("chronyd %s did not become ready\nlog:\n%s", name, ntsReadFile(d.logPath))
	return nil
}

type ntsDaemon struct {
	name    string
	ns      string
	logPath string
	sock    string
	pidFile string
	pidVal  int
}

func (d *ntsDaemon) pid() int {
	data, err := os.ReadFile(d.pidFile)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}

func (d *ntsDaemon) stop(t *testing.T) {
	t.Helper()
	if d.pidVal == 0 {
		return
	}
	_ = syscall.Kill(d.pidVal, syscall.SIGTERM)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(d.pidVal, 0); err == syscall.ESRCH {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(d.pidVal, syscall.SIGKILL)
	d.pidVal = 0
}

func (e *ntsNetEnv) close() {
	if e.closed {
		return
	}
	e.closed = true
	for _, ns := range []string{e.srvNS, e.cliNS} {
		// Kill any straggler still holding the namespace open.
		if out, err := exec.Command(e.tools.ip, "netns", "pids", ns).Output(); err == nil {
			for _, f := range strings.Fields(string(out)) {
				if pid, err := strconv.Atoi(f); err == nil {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		}
		_ = exec.Command(e.tools.ip, "netns", "del", ns).Run()
	}
}

// ---- assertions ----

func ntsWaitAuthenticated(t *testing.T, tools ntsToolset, d *ntsDaemon, serverIP string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastSources, lastNTP string
	for time.Now().Before(deadline) {
		lastSources = ntsChronyc(tools, d.sock, "-c", "sources")
		lastNTP = ntsChronyc(tools, d.sock, "ntpdata", serverIP)
		if ntpAuthenticated(lastNTP) && sourceSelected(lastSources) {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("client never authenticated the NTS source within %s\nsources:\n%s\nntpdata:\n%s\nlog:\n%s",
		timeout, lastSources, lastNTP, ntsReadFile(d.logPath))
}

func ntsAssertConverged(t *testing.T, tools ntsToolset, d *ntsDaemon) {
	t.Helper()
	fields := strings.Split(strings.TrimSpace(ntsChronyc(tools, d.sock, "-c", "tracking")), ",")
	if len(fields) < 6 {
		t.Fatalf("unexpected tracking output: %q", fields)
	}
	for _, idx := range []int{4, 5} { // system time offset, last offset
		off, err := strconv.ParseFloat(fields[idx], 64)
		if err != nil {
			t.Fatalf("parse tracking field %d %q: %v", idx, fields[idx], err)
		}
		if off > 1 || off < -1 {
			t.Fatalf("client offset did not converge: %fs (tracking %q)", off, fields)
		}
	}
}

func ntsWaitTLSRejection(t *testing.T, d *ntsDaemon, timeout time.Duration) {
	t.Helper()
	patterns := []string{"tls handshake", "not trusted", "certificate verification"}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		log := strings.ToLower(ntsReadFile(d.logPath))
		for _, p := range patterns {
			if strings.Contains(log, p) && strings.Contains(log, "fail") {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("client log never showed an NTS-KE TLS verification failure\nlog:\n%s", ntsReadFile(d.logPath))
}

func ntsAssertNotAuthenticated(t *testing.T, tools ntsToolset, d *ntsDaemon, serverIP string) {
	t.Helper()
	// Give the client a chance to (wrongly) authenticate with any cached state.
	time.Sleep(3 * time.Second)
	if ntpAuthenticated(ntsChronyc(tools, d.sock, "ntpdata", serverIP)) {
		t.Fatalf("client authenticated an NTS source signed by a foreign CA\nlog:\n%s", ntsReadFile(d.logPath))
	}
	if sourceSelected(ntsChronyc(tools, d.sock, "-c", "sources")) {
		t.Fatalf("client selected a source despite NTS-KE trust failure\nsources:\n%s",
			ntsChronyc(tools, d.sock, "-c", "sources"))
	}
}

var ntpAuthRE = regexp.MustCompile(`(?m)Authenticated\s*:\s*Yes`)

func ntpAuthenticated(ntpdata string) bool { return ntpAuthRE.MatchString(ntpdata) }

// sourceSelected reports whether any source is the selected/combined system
// peer (state '*' or '+') in `chronyc -c sources` CSV.
func sourceSelected(csv string) bool {
	for _, line := range strings.Split(strings.TrimSpace(csv), "\n") {
		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}
		if parts[1] == "*" || parts[1] == "+" {
			return true
		}
	}
	return false
}

func ntsChronyc(tools ntsToolset, sock string, args ...string) string {
	full := append([]string{"-h", sock, "-n"}, args...)
	out, _ := exec.Command(tools.chronyc, full...).CombinedOutput()
	return string(out)
}

// ---- config and certificate helpers ----

func ntsRender(t *testing.T, cfg Config) string {
	t.Helper()
	if err := os.MkdirAll(cfg.DumpDir, 0o700); err != nil {
		t.Fatal(err)
	}
	out, err := Render(cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return out.Main
}

// ntsWriteConfig installs the rendered fragment plus the process-local runtime
// directives (pidfile and a private command socket) chronyd needs to run two
// instances side by side.
func ntsWriteConfig(t *testing.T, dir, name, rendered string) string {
	t.Helper()
	sock := ntsSocketPath(dir, name)
	// chrony refuses a Unix command socket in a directory accessible to group
	// or other, and t.TempDir does not guarantee 0700.
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(sock), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name+".conf")
	harness := fmt.Sprintf("\npidfile %s\nbindcmdaddress %s\n",
		filepath.Join(dir, name+".pid"), sock)
	writeFile(t, path, []byte(rendered+harness), 0o644)
	return path
}

func ntsSocketPath(dir, name string) string {
	return filepath.Join(dir, name+"-cmd", "chronyd.sock")
}

func ntsNewCA(t *testing.T, dir, name string) string {
	t.Helper()
	certPEM, keyPEM, err := tlsutil.GenerateCA(time.Now())
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	certFile := filepath.Join(dir, name+".pem")
	writeFile(t, certFile, certPEM, 0o644)
	writeFile(t, filepath.Join(dir, name+".key"), keyPEM, 0o600)
	return certFile
}

func ntsIssue(t *testing.T, dir, fileStem, nodeID, caFile string, dns []string, ips []net.IP) (certFile, keyFile string) {
	t.Helper()
	ca, err := tls.LoadX509KeyPair(caFile, filepath.Join(dir, strings.TrimSuffix(filepath.Base(caFile), ".pem")+".key"))
	if err != nil {
		t.Fatalf("load CA: %v", err)
	}
	certPEM, keyPEM, err := tlsutil.IssueNodeWithSANs(ca, nodeID, time.Now(), dns, ips)
	if err != nil {
		t.Fatalf("IssueNodeWithSANs: %v", err)
	}
	certFile = filepath.Join(dir, fileStem+".pem")
	keyFile = filepath.Join(dir, fileStem+".key")
	writeFile(t, certFile, certPEM, 0o644)
	writeFile(t, keyFile, keyPEM, 0o600)
	return certFile, keyFile
}

func ntsReadFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(unreadable: %v)", err)
	}
	return string(data)
}
