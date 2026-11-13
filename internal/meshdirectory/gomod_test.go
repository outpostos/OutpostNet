package meshdirectory

import (
	"os"
	"strings"
	"testing"
)

// OutpostMesh depends on OutpostNet (pkg/state); Net reading Mesh through its
// REST contract instead of its Go module keeps the module graph acyclic
// (ADR-0003 D2). This guards against an accidental import.
func TestGoModDoesNotRequireOutpostMesh(t *testing.T) {
	data, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "github.com/outpostos/OutpostMesh") {
		t.Fatal("go.mod requires github.com/outpostos/OutpostMesh; use the REST contract instead")
	}
}
