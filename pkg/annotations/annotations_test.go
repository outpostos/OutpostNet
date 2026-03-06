package annotations

import (
	"testing"
)

func TestHasOutpostNet(t *testing.T) {
	tests := []struct {
		name   string
		annos  map[string]string
		expect bool
	}{
		{"inject true", map[string]string{InjectAnnotation: "true"}, true},
		{"inject false", map[string]string{InjectAnnotation: "false"}, false},
		{"missing", map[string]string{}, false},
		{"nil", nil, false},
		{"other value", map[string]string{InjectAnnotation: "yes"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasOutpostNet(tt.annos); got != tt.expect {
				t.Errorf("HasOutpostNet() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestParseExports(t *testing.T) {
	annos := map[string]string{
		InjectAnnotation:  "true",
		ExportsAnnotation: "web:8080:tcp,dns:53:udp",
	}
	cfg, err := Parse(annos)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.Inject {
		t.Error("expected Inject=true")
	}
	if len(cfg.Exports) != 2 {
		t.Fatalf("exports: got %d, want 2", len(cfg.Exports))
	}
	if cfg.Exports[0].Name != "web" || cfg.Exports[0].Port != 8080 || cfg.Exports[0].Protocol != "tcp" {
		t.Errorf("export[0]: %+v", cfg.Exports[0])
	}
	if cfg.Exports[1].Name != "dns" || cfg.Exports[1].Port != 53 || cfg.Exports[1].Protocol != "udp" {
		t.Errorf("export[1]: %+v", cfg.Exports[1])
	}
}

func TestParseImports(t *testing.T) {
	annos := map[string]string{
		InjectAnnotation:  "true",
		ImportsAnnotation: "db-service:3306:tcp",
	}
	cfg, err := Parse(annos)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Imports) != 1 {
		t.Fatalf("imports: got %d, want 1", len(cfg.Imports))
	}
	if cfg.Imports[0].Name != "db-service" || cfg.Imports[0].Port != 3306 {
		t.Errorf("import[0]: %+v", cfg.Imports[0])
	}
}

func TestParseSidecarID(t *testing.T) {
	annos := map[string]string{
		InjectAnnotation:    "true",
		SidecarIDAnnotation: "my-custom-id",
		ExportsAnnotation:   "svc:80:tcp",
	}
	cfg, err := Parse(annos)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.SidecarID != "my-custom-id" {
		t.Errorf("SidecarID: got %q", cfg.SidecarID)
	}
}

func TestParseProtocolCaseInsensitive(t *testing.T) {
	annos := map[string]string{
		InjectAnnotation:  "true",
		ExportsAnnotation: "svc:80:TCP",
	}
	cfg, err := Parse(annos)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Exports[0].Protocol != "tcp" {
		t.Errorf("protocol: got %q, want tcp", cfg.Exports[0].Protocol)
	}
}

func TestParseInvalidFormat(t *testing.T) {
	tests := []struct {
		name  string
		annos map[string]string
	}{
		{"missing port", map[string]string{ExportsAnnotation: "web:tcp"}},
		{"bad port", map[string]string{ExportsAnnotation: "web:abc:tcp"}},
		{"port zero", map[string]string{ExportsAnnotation: "web:0:tcp"}},
		{"port too large", map[string]string{ExportsAnnotation: "web:99999:tcp"}},
		{"empty item", map[string]string{ExportsAnnotation: "web:80:tcp,,dns:53:udp"}}, // empty between commas is ok (skipped)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.annos)
			if tt.name == "empty item" {
				// Empty items between commas are skipped, not an error
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PodConfig
		wantErr bool
	}{
		{
			"valid export",
			PodConfig{Inject: true, Exports: []ServiceEntry{{Name: "web", Port: 80, Protocol: "tcp"}}},
			false,
		},
		{
			"valid import",
			PodConfig{Inject: true, Imports: []ServiceEntry{{Name: "db", Port: 3306, Protocol: "tcp"}}},
			false,
		},
		{
			"not injected",
			PodConfig{Inject: false, Exports: []ServiceEntry{{Name: "web", Port: 80, Protocol: "tcp"}}},
			true,
		},
		{
			"no exports or imports",
			PodConfig{Inject: true},
			true,
		},
		{
			"invalid protocol",
			PodConfig{Inject: true, Exports: []ServiceEntry{{Name: "web", Port: 80, Protocol: "http"}}},
			true,
		},
		{
			"duplicate export name",
			PodConfig{Inject: true, Exports: []ServiceEntry{
				{Name: "web", Port: 80, Protocol: "tcp"},
				{Name: "web", Port: 8080, Protocol: "tcp"},
			}},
			true,
		},
		{
			"zero port",
			PodConfig{Inject: true, Exports: []ServiceEntry{{Name: "web", Port: 0, Protocol: "tcp"}}},
			true,
		},
		{
			"empty name",
			PodConfig{Inject: true, Exports: []ServiceEntry{{Name: "", Port: 80, Protocol: "tcp"}}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(&tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseEmptyAnnotations(t *testing.T) {
	cfg, err := Parse(map[string]string{})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Inject {
		t.Error("expected Inject=false for empty annotations")
	}
	if len(cfg.Exports) != 0 || len(cfg.Imports) != 0 {
		t.Error("expected empty exports and imports")
	}
}

func TestParseCrossRegionImports(t *testing.T) {
	annos := map[string]string{
		InjectAnnotation:  "true",
		ImportsAnnotation: "cloud-db@cloud:3306:tcp,local-svc:8080:tcp",
	}
	cfg, err := Parse(annos)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Imports) != 2 {
		t.Fatalf("imports: got %d, want 2", len(cfg.Imports))
	}
	// Cross-region import.
	if cfg.Imports[0].Name != "cloud-db" {
		t.Errorf("import[0].Name = %q, want cloud-db", cfg.Imports[0].Name)
	}
	if cfg.Imports[0].Region != "cloud" {
		t.Errorf("import[0].Region = %q, want cloud", cfg.Imports[0].Region)
	}
	if cfg.Imports[0].Port != 3306 {
		t.Errorf("import[0].Port = %d, want 3306", cfg.Imports[0].Port)
	}
	// Local import (no region).
	if cfg.Imports[1].Name != "local-svc" {
		t.Errorf("import[1].Name = %q, want local-svc", cfg.Imports[1].Name)
	}
	if cfg.Imports[1].Region != "" {
		t.Errorf("import[1].Region = %q, want empty", cfg.Imports[1].Region)
	}
}

func TestHasRegion(t *testing.T) {
	tests := []struct {
		name   string
		annos  map[string]string
		want   string
		wantOK bool
	}{
		{"present", map[string]string{RegionAnnotation: "edge-a"}, "edge-a", true},
		{"empty", map[string]string{RegionAnnotation: ""}, "", false},
		{"missing", map[string]string{}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := HasRegion(tt.annos)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("HasRegion() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestIsCrossRegion(t *testing.T) {
	if IsCrossRegion(ServiceEntry{Name: "svc", Region: ""}) {
		t.Error("local service should not be cross-region")
	}
	if !IsCrossRegion(ServiceEntry{Name: "svc", Region: "cloud"}) {
		t.Error("service with region should be cross-region")
	}
}

func TestParseWithSpaces(t *testing.T) {
	annos := map[string]string{
		ExportsAnnotation: " web : 8080 : tcp , dns : 53 : udp ",
	}
	cfg, err := Parse(annos)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Exports) != 2 {
		t.Fatalf("exports: got %d, want 2", len(cfg.Exports))
	}
	if cfg.Exports[0].Name != "web" || cfg.Exports[0].Port != 8080 {
		t.Errorf("export[0]: %+v", cfg.Exports[0])
	}
}
