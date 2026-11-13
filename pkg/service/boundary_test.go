package service_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const modulePath = "github.com/outpostos/OutpostNet"

// repoRoot returns the module root (two levels above pkg/service).
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found from %s: %v", wd, err)
	}
	return root
}

// moduleImports returns the in-module, non-test imports of one package dir.
func moduleImports(t *testing.T, root, pkg string) []string {
	t.Helper()
	dir := filepath.Join(root, strings.TrimPrefix(strings.TrimPrefix(pkg, modulePath), "/"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package %s: %v", pkg, err)
	}
	seen := map[string]bool{}
	fset := token.NewFileSet()
	found := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		found = true
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(p, modulePath+"/") {
				seen[p] = true
			}
		}
	}
	if !found {
		t.Fatalf("package %s has no non-test Go files", pkg)
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// transitiveDeps walks the in-module import graph of pkg (non-test files).
func transitiveDeps(t *testing.T, root, pkg string) map[string][]string {
	t.Helper()
	// dep -> import chain that reached it, for readable failures.
	chains := map[string][]string{}
	var walk func(p string, chain []string)
	walk = func(p string, chain []string) {
		for _, dep := range moduleImports(t, root, p) {
			if _, ok := chains[dep]; ok {
				continue
			}
			next := append(append([]string{}, chain...), dep)
			chains[dep] = next
			walk(dep, next)
		}
	}
	walk(pkg, []string{pkg})
	return chains
}

// TestNetCommonLayerDoesNotImportService enforces acceptance item 2: the
// generic connection, stream and reconnect implementation (and the shared
// config schema) must not depend on the Service domain or its hosts.
func TestNetCommonLayerDoesNotImportService(t *testing.T) {
	root := repoRoot(t)
	netPkgs := []string{
		modulePath + "/pkg/delivery",
		modulePath + "/pkg/tunnel",
		modulePath + "/pkg/tunnel/tlsutil",
		modulePath + "/pkg/protocol",
		modulePath + "/pkg/state",
		modulePath + "/pkg/agentconfig",
	}
	forbidden := []string{
		modulePath + "/pkg/service",
		modulePath + "/internal/servicehost",
		modulePath + "/internal/agent",
	}
	for _, p := range netPkgs {
		deps := transitiveDeps(t, root, p)
		for _, f := range forbidden {
			if chain, ok := deps[f]; ok {
				t.Errorf("Net package %s must not depend on %s (chain: %s)", p, f, strings.Join(chain, " -> "))
			}
		}
	}
}

// TestServiceDomainDoesNotDependOnTransportOrHosts keeps pkg/service a pure
// domain module: it reaches Net only through the StreamOpener/RouteTable
// ports injected by a host, never by importing the transport or a host.
func TestServiceDomainDoesNotDependOnTransportOrHosts(t *testing.T) {
	root := repoRoot(t)
	deps := transitiveDeps(t, root, modulePath+"/pkg/service")
	for dep, chain := range deps {
		if strings.HasPrefix(dep, modulePath+"/internal/") || strings.HasPrefix(dep, modulePath+"/pkg/tunnel") {
			t.Errorf("pkg/service must not depend on %s (chain: %s)", dep, strings.Join(chain, " -> "))
		}
	}
}

// TestNetHostStreamAPIIsServiceAgnostic keeps the Net host's stream API in
// transport terms: exported TunnelManager methods in internal/agent must not
// take or return pkg/service types. Service targets are mapped onto the
// transport by service.NetOpener. (internal/agent as the legacy composition
// host may still import pkg/service to assemble entries and exports.)
func TestNetHostStreamAPIIsServiceAgnostic(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "internal", "agent")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		// Local name of the pkg/service import in this file, if any.
		svcName := ""
		for _, imp := range f.Imports {
			if strings.Trim(imp.Path.Value, `"`) == modulePath+"/pkg/service" {
				svcName = "service"
				if imp.Name != nil {
					svcName = imp.Name.Name
				}
			}
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() || len(fn.Recv.List) != 1 {
				continue
			}
			recv := fn.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			if id, ok := recv.(*ast.Ident); !ok || id.Name != "TunnelManager" {
				continue
			}
			checked++
			if svcName == "" {
				continue
			}
			ast.Inspect(fn.Type, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == svcName {
						t.Errorf("%s: TunnelManager.%s signature uses %s.%s; keep the Net host API in transport terms",
							fset.Position(fn.Pos()), fn.Name.Name, svcName, sel.Sel.Name)
					}
				}
				return true
			})
		}
	}
	if checked == 0 {
		t.Fatal("no exported TunnelManager methods found; test is not checking anything")
	}
}

// directImports returns every non-test import of one in-module package dir.
func directImports(t *testing.T, root, pkg string) []string {
	t.Helper()
	dir := filepath.Join(root, strings.TrimPrefix(strings.TrimPrefix(pkg, modulePath), "/"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package %s: %v", pkg, err)
	}
	var out []string
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			out = append(out, strings.Trim(imp.Path.Value, `"`))
		}
	}
	return out
}

// TestNetLibrariesDoNotDependOnOutpostRange keeps the observation contract
// out of the libraries Let and Mesh embed (ticket 10-B): only the producer
// layer (internal/observation, the hosts and cmd/) may import OutpostRange.
// The walk covers each package and its in-module dependencies; third-party
// modules cannot import the private OutpostRange module.
func TestNetLibrariesDoNotDependOnOutpostRange(t *testing.T) {
	root := repoRoot(t)
	const rangeModule = "github.com/outpostos/OutpostRange"
	libraries := []string{
		modulePath + "/pkg/delivery",
		modulePath + "/pkg/state",
		modulePath + "/pkg/tunnel",
		modulePath + "/pkg/tunnel/tlsutil",
		modulePath + "/pkg/service",
		modulePath + "/pkg/protocol",
		modulePath + "/pkg/agentconfig",
	}
	for _, lib := range libraries {
		chains := transitiveDeps(t, root, lib)
		chains[lib] = []string{lib}
		for pkg, chain := range chains {
			for _, imp := range directImports(t, root, pkg) {
				if imp == rangeModule || strings.HasPrefix(imp, rangeModule+"/") {
					t.Errorf("%s must not depend on %s (chain: %s -> %s)", lib, rangeModule, strings.Join(chain, " -> "), imp)
				}
			}
		}
	}
}
