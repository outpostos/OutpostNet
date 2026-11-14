package agentconfig_test

import (
	"github.com/outpostos/OutpostNet/pkg/agentconfig"
	"testing"
)

func TestCertificateConfigFailsClosed(t *testing.T) {
	for _, yaml := range []string{
		"tls: {ca_file: group.pem}",
		"node_id: node-a\ntls: {ca_file: group.pem, cert_file: node.pem}",
		"node_id: node-a\ntls: {ca_file: group.pem, cert_file: node.pem, key_file: node.key, insecure: true}",
		"tls: {ca_file: group.pem, cert_file: node.pem, key_file: node.key}",
		"tls: {key_file: node.key}",
	} {
		if _, err := agentconfig.ParseConfig([]byte(yaml)); err == nil {
			t.Fatalf("accepted incomplete/insecure credentials: %s", yaml)
		}
	}
	cfg, err := agentconfig.ParseConfig([]byte("node_id: node-a\ntls: {ca_file: group.pem, cert_file: node.pem, key_file: node.key}"))
	if err != nil || cfg.TLS.CAFile != "group.pem" {
		t.Fatalf("valid certificate config: %v %v", cfg, err)
	}
}
