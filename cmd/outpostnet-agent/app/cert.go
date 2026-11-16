package app

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/outpostos/OutpostNet/pkg/tunnel/tlsutil"
	"github.com/spf13/cobra"
)

func newCertificateCommand() *cobra.Command {
	root := &cobra.Command{Use: "cert", Short: "Provision and inspect offline group/node certificates"}
	var certFile, keyFile, caFile, caKey, nodeID string
	var dnsNames, ipNames []string
	ca := &cobra.Command{Use: "ca", Short: "Create a group CA (never overwrites files)", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cert, key, err := tlsutil.GenerateCA(time.Now())
		if err != nil {
			return err
		}
		return writeCredentialPair(certFile, keyFile, cert, key)
	}}
	ca.Flags().StringVar(&certFile, "cert", "", "Output CA certificate PEM")
	ca.Flags().StringVar(&keyFile, "key", "", "Output CA private key PEM")
	ca.MarkFlagRequired("cert")
	ca.MarkFlagRequired("key")
	issue := &cobra.Command{Use: "issue", Short: "Issue a node certificate", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		ca, err := tls.LoadX509KeyPair(caFile, caKey)
		if err != nil {
			return fmt.Errorf("load signing CA: %w", err)
		}
		var ips []net.IP
		for _, name := range ipNames {
			ip := net.ParseIP(name)
			if ip == nil {
				return fmt.Errorf("invalid IP SAN")
			}
			ips = append(ips, ip)
		}
		cert, key, err := tlsutil.IssueNodeWithSANs(ca, nodeID, time.Now(), dnsNames, ips)
		if err != nil {
			return err
		}
		return writeCredentialPair(certFile, keyFile, cert, key)
	}}
	issue.Flags().StringVar(&certFile, "cert", "", "Output node certificate PEM")
	issue.Flags().StringVar(&keyFile, "key", "", "Output node private key PEM")
	issue.Flags().StringVar(&caFile, "ca", "", "Group CA certificate PEM")
	issue.Flags().StringVar(&caKey, "ca-key", "", "Group CA private key PEM (offline issuer only)")
	issue.Flags().StringSliceVar(&dnsNames, "dns", nil, "NTS DNS SAN (repeatable or comma-separated)")
	issue.Flags().StringSliceVar(&ipNames, "ip", nil, "NTS IP SAN (repeatable or comma-separated)")
	issue.Flags().StringVar(&nodeID, "node-id", "", "Node identity")
	for _, name := range []string{"cert", "key", "ca", "ca-key", "node-id"} {
		issue.MarkFlagRequired(name)
	}
	inspect := &cobra.Command{Use: "inspect CERT", Short: "Inspect public certificate identities and dates", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error { return inspectCertificate(cmd, args[0]) }}
	list := &cobra.Command{Use: "list DIR", Short: "List certificate PEMs in a directory (nonrecursive)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		files, err := os.ReadDir(args[0])
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			ext := filepath.Ext(f.Name())
			if ext != ".pem" && ext != ".crt" {
				continue
			}
			if err := inspectCertificate(cmd, filepath.Join(args[0], f.Name())); err != nil {
				return err
			}
		}
		return nil
	}}
	root.AddCommand(ca, issue, inspect, list)
	return root
}

func writeCredentialPair(certFile, keyFile string, cert, key []byte) error {
	// Reserve both paths with O_EXCL before writing any material.
	c, err := os.OpenFile(certFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}
	defer c.Close()
	k, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		os.Remove(certFile)
		return fmt.Errorf("create private key: %w", err)
	}
	defer k.Close()
	ok := false
	defer func() {
		if !ok {
			os.Remove(certFile)
			os.Remove(keyFile)
		}
	}()
	if _, err := k.Write(key); err != nil {
		return err
	}
	if err := k.Sync(); err != nil {
		return err
	}
	if _, err := c.Write(cert); err != nil {
		return err
	}
	if err := c.Sync(); err != nil {
		return err
	}
	ok = true
	return nil
}

func inspectCertificate(cmd *cobra.Command, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	found := false
	for len(data) > 0 {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}
		data = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		found = true
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		id, _ := tlsutil.NodeID(cert)
		fmt.Fprintf(cmd.OutOrStdout(), "%s node_id=%s ca=%t not_before=%s not_after=%s dns=%v ips=%v\n", filepath.Base(path), id, cert.IsCA, cert.NotBefore.UTC().Format(time.RFC3339), cert.NotAfter.UTC().Format(time.RFC3339), cert.DNSNames, cert.IPAddresses)
	}
	if !found {
		return fmt.Errorf("no certificate in %s", filepath.Base(path))
	}
	return nil
}
