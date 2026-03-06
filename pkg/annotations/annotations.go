// Package annotations provides OutpostNet annotation parsing and validation.
package annotations

import (
	"fmt"
	"strconv"
	"strings"
)

// Annotation keys used by OutpostNet.
const (
	// InjectAnnotation controls sidecar injection. Set to "true" to enable.
	InjectAnnotation = "outpostnet.io/inject"

	// ExportsAnnotation declares exported services.
	// Format: "name:port:protocol,name:port:protocol,..."
	ExportsAnnotation = "outpostnet.io/exports"

	// ImportsAnnotation declares imported services.
	// Format: "name:listenPort:protocol,name:listenPort:protocol,..."
	ImportsAnnotation = "outpostnet.io/imports"

	// SidecarIDAnnotation overrides the auto-generated sidecar ID.
	SidecarIDAnnotation = "outpostnet.io/sidecar-id"

	// RegionAnnotation specifies which edge region the Pod belongs to.
	RegionAnnotation = "outpostnet.io/region"

	// InjectedLabel is set on Pods that have been injected with a sidecar.
	InjectedLabel = "outpostnet.io/injected"
)

// ServiceEntry represents a single exported or imported service.
type ServiceEntry struct {
	Name     string
	Port     uint16
	Protocol string // "tcp" or "udp"
	Region   string // Optional: remote region for cross-region imports (e.g., "cloud")
}

// PodConfig holds the parsed OutpostNet configuration for a Pod.
type PodConfig struct {
	Inject    bool
	SidecarID string
	Exports   []ServiceEntry
	Imports   []ServiceEntry
}

// HasOutpostNet returns true if the annotations contain OutpostNet injection marker.
func HasOutpostNet(annotations map[string]string) bool {
	return annotations[InjectAnnotation] == "true"
}

// Parse extracts OutpostNet configuration from Pod annotations.
// Returns an error if annotations are present but malformed.
func Parse(annotations map[string]string) (*PodConfig, error) {
	cfg := &PodConfig{
		Inject:    annotations[InjectAnnotation] == "true",
		SidecarID: annotations[SidecarIDAnnotation],
	}

	if v, ok := annotations[ExportsAnnotation]; ok && v != "" {
		exports, err := parseServiceList(v)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", ExportsAnnotation, err)
		}
		cfg.Exports = exports
	}

	if v, ok := annotations[ImportsAnnotation]; ok && v != "" {
		imports, err := parseServiceList(v)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", ImportsAnnotation, err)
		}
		// Parse optional @region suffix from import names.
		for i := range imports {
			if name, region, ok := parseRegionSuffix(imports[i].Name); ok {
				imports[i].Name = name
				imports[i].Region = region
			}
		}
		cfg.Imports = imports
	}

	return cfg, nil
}

// Validate checks that a parsed PodConfig is valid for injection.
func Validate(cfg *PodConfig) error {
	if !cfg.Inject {
		return fmt.Errorf("injection not enabled (missing %s: \"true\")", InjectAnnotation)
	}
	if len(cfg.Exports) == 0 && len(cfg.Imports) == 0 {
		return fmt.Errorf("at least one export or import must be specified")
	}
	seen := make(map[string]bool)
	for i, e := range cfg.Exports {
		if err := validateEntry(e, "export", i); err != nil {
			return err
		}
		if seen[e.Name] {
			return fmt.Errorf("duplicate export service name: %q", e.Name)
		}
		seen[e.Name] = true
	}
	seen = make(map[string]bool)
	for i, e := range cfg.Imports {
		if err := validateEntry(e, "import", i); err != nil {
			return err
		}
		if seen[e.Name] {
			return fmt.Errorf("duplicate import service name: %q", e.Name)
		}
		seen[e.Name] = true
	}
	return nil
}

func validateEntry(e ServiceEntry, kind string, idx int) error {
	if e.Name == "" {
		return fmt.Errorf("%s[%d]: name is required", kind, idx)
	}
	if e.Port == 0 {
		return fmt.Errorf("%s[%d] %q: port must be > 0", kind, idx, e.Name)
	}
	if e.Protocol != "tcp" && e.Protocol != "udp" {
		return fmt.Errorf("%s[%d] %q: protocol must be tcp or udp, got %q", kind, idx, e.Name, e.Protocol)
	}
	return nil
}

// HasRegion returns the region annotation value if present.
func HasRegion(annotations map[string]string) (string, bool) {
	v, ok := annotations[RegionAnnotation]
	return v, ok && v != ""
}

// IsCrossRegion returns true if the import entry references a remote region.
func IsCrossRegion(entry ServiceEntry) bool {
	return entry.Region != ""
}

// parseRegionSuffix extracts region from "name@region" format.
func parseRegionSuffix(name string) (string, string, bool) {
	idx := strings.LastIndex(name, "@")
	if idx <= 0 || idx == len(name)-1 {
		return name, "", false
	}
	return name[:idx], name[idx+1:], true
}

// parseServiceList parses "name:port:protocol,name:port:protocol,..." into entries.
func parseServiceList(s string) ([]ServiceEntry, error) {
	var entries []ServiceEntry
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid format %q: expected name:port:protocol", item)
		}
		name := strings.TrimSpace(parts[0])
		portStr := strings.TrimSpace(parts[1])
		proto := strings.ToLower(strings.TrimSpace(parts[2]))

		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("invalid port %q in %q", portStr, item)
		}

		entries = append(entries, ServiceEntry{
			Name:     name,
			Port:     uint16(port),
			Protocol: proto,
		})
	}
	return entries, nil
}
