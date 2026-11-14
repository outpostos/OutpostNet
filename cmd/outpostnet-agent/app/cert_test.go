package app

import (
	"bytes"
	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCertificateCLIProvisionInspectAndLoad(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) (string, error) {
		cmd := NewAgentCommand()
		var b bytes.Buffer
		cmd.SetOut(&b)
		cmd.SetErr(&b)
		cmd.SetArgs(args)
		err := cmd.Execute()
		return b.String(), err
	}
	ca := filepath.Join(dir, "group.pem")
	key := filepath.Join(dir, "group.key")
	if _, err := run("cert", "ca", "--cert", ca, "--key", key); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(dir, "node.pem")
	leafKey := filepath.Join(dir, "node.key")
	if _, err := run("cert", "issue", "--ca", ca, "--ca-key", key, "--node-id", "node-a", "--cert", leaf, "--key", leafKey); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"cert", "inspect", leaf}, {"cert", "list", dir}} {
		out, err := run(args...)
		if err != nil || !strings.Contains(out, "node-a") {
			t.Fatalf("%v: %s %v", args, out, err)
		}
		if strings.Contains(out, "PRIVATE KEY") {
			t.Fatal("exposed private key")
		}
	}
	info, err := os.Stat(leafKey)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("key permissions: %v %v", info, err)
	}
	before, _ := os.ReadFile(key)
	if _, err := run("cert", "ca", "--cert", ca, "--key", key); err == nil {
		t.Fatal("overwrote group credentials")
	}
	after, _ := os.ReadFile(key)
	if !bytes.Equal(before, after) {
		t.Fatal("changed existing key")
	}
}

func TestCertificateCLIRejectsInvalidNodeAndLoadsIssuedCredentials(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	caKey := filepath.Join(dir, "ca.key")
	cert := filepath.Join(dir, "node.pem")
	key := filepath.Join(dir, "node.key")
	run := func(args ...string) error {
		cmd := NewAgentCommand()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(args)
		return cmd.Execute()
	}
	if err := run("cert", "ca", "--cert", ca, "--key", caKey); err != nil {
		t.Fatal(err)
	}
	if err := run("cert", "issue", "--ca", ca, "--ca-key", caKey, "--node-id", "bad node", "--cert", cert, "--key", key); err == nil {
		t.Fatal("issued invalid identity")
	}
	if err := run("cert", "issue", "--ca", ca, "--ca-key", caKey, "--node-id", "node-a", "--cert", cert, "--key", key); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsutil.LoadIdentity(cert, key, ca, "node-a", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsutil.LoadIdentity(cert, key, ca, "node-b", nil); err == nil {
		t.Fatal("loaded mismatching local node")
	}
	if err := os.WriteFile(ca, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsutil.LoadIdentity(cert, key, ca, "node-a", nil); err == nil {
		t.Fatal("loaded malformed CA")
	}
}
