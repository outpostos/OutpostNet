// Package docker holds static guards for the repository Dockerfile, which
// builds the container images used by the Kubernetes deployment entries
// (deploy/, deploy/kustomize/, hack/e2e-*.sh).
package docker

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// swrRegistry is the only registry Dockerfile base images may come from.
// Direct pulls from Docker Hub / ghcr are not allowed for this build.
const swrRegistry = "swr.cn-east-3.myhuaweicloud.com/risecloud/"

// repoRoot is the repository root relative to this package directory.
const repoRoot = "../.."

// dockerfileInstructions returns the Dockerfile as logical instructions:
// comments dropped, backslash continuations joined.
func dockerfileInstructions(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasSuffix(trimmed, "\\") {
			cur.WriteString(strings.TrimSuffix(trimmed, "\\"))
			cur.WriteString(" ")
			continue
		}
		cur.WriteString(trimmed)
		out = append(out, cur.String())
		cur.Reset()
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// fromStage is one parsed FROM instruction.
type fromStage struct {
	image string // ARG-expanded image or earlier stage name
	name  string // AS <name>, empty when unnamed
}

func dockerfileStages(t *testing.T, ins []string) []fromStage {
	t.Helper()
	args := map[string]string{}
	argRe := regexp.MustCompile(`^ARG\s+([A-Za-z_][A-Za-z0-9_]*)=(\S+)$`)
	var stages []fromStage
	for _, in := range ins {
		if m := argRe.FindStringSubmatch(in); m != nil && len(stages) == 0 {
			// Only global ARGs (before the first FROM) apply to FROM lines.
			args[m[1]] = m[2]
			continue
		}
		fields := strings.Fields(in)
		if len(fields) < 2 || !strings.EqualFold(fields[0], "FROM") {
			continue
		}
		var rest []string
		for _, f := range fields[1:] {
			if !strings.HasPrefix(f, "--") {
				rest = append(rest, f)
			}
		}
		st := fromStage{image: os.Expand(rest[0], func(k string) string { return args[k] })}
		if len(rest) == 3 && strings.EqualFold(rest[1], "AS") {
			st.name = rest[2]
		}
		stages = append(stages, st)
	}
	if len(stages) == 0 {
		t.Fatal("Dockerfile has no FROM instruction")
	}
	return stages
}

// TestDockerfileBaseImagesComeFromSWR guards that every FROM resolves to the
// SWR risecloud namespace (directly or through the SWR ARG default), or to an
// earlier stage of the same Dockerfile.
func TestDockerfileBaseImagesComeFromSWR(t *testing.T) {
	seen := map[string]bool{}
	for _, st := range dockerfileStages(t, dockerfileInstructions(t)) {
		if !seen[st.image] && !strings.HasPrefix(st.image, swrRegistry) {
			t.Errorf("FROM %q does not come from %s", st.image, swrRegistry)
		}
		if st.name != "" {
			seen[st.name] = true
		}
	}
}

// TestDockerfilePrivateModulesUseBuildSecrets guards that the private module
// github.com/outpostos/OutpostRange is fetched with a BuildKit secret mount
// (never ARG/ENV/COPY'd credentials), so no credential can land in an image
// layer or in the image history.
func TestDockerfilePrivateModulesUseBuildSecrets(t *testing.T) {
	ins := dockerfileInstructions(t)
	var download string
	for _, in := range ins {
		if strings.HasPrefix(in, "RUN") && strings.Contains(in, "go mod download") {
			download = in
		}
	}
	if download == "" {
		t.Fatal("Dockerfile has no RUN ... go mod download step")
	}
	if !strings.Contains(download, "--mount=type=secret,id=outpostrange_deploy_key,required=true") {
		t.Errorf("go mod download step does not mount required BuildKit secret %q", "outpostrange_deploy_key")
	}
	credName := regexp.MustCompile(`(?i)(KEY|TOKEN|PASSWORD|SECRET|CREDENTIAL)`)
	for _, in := range ins {
		fields := strings.Fields(in)
		if len(fields) < 2 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "ARG", "ENV":
			for _, f := range fields[1:] {
				name := strings.SplitN(f, "=", 2)[0]
				if credName.MatchString(name) {
					t.Errorf("%s %s: credentials must not be passed as build args or env", fields[0], name)
				}
			}
		case "COPY", "ADD":
			if credName.MatchString(in) || strings.Contains(in, ".ssh") || strings.Contains(in, "id_rsa") || strings.Contains(in, "id_ed25519") {
				t.Errorf("%q: credentials must not be copied into the image", in)
			}
		}
	}
	for _, want := range []string{"GOPRIVATE=github.com/outpostos", "GONOSUMDB=github.com/outpostos"} {
		found := false
		for _, in := range ins {
			if strings.HasPrefix(in, "ENV") && strings.Contains(in, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("builder stage does not set %s", want)
		}
	}
}

// TestDockerignoreKeepsCredentialsOutOfContext guards the build context: the
// builder runs COPY . ., so a stray key file in the working tree would land in
// a builder layer unless .dockerignore excludes it.
func TestDockerignoreKeepsCredentialsOutOfContext(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot, ".dockerignore"))
	if err != nil {
		t.Fatalf("read .dockerignore: %v", err)
	}
	patterns := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			patterns[line] = true
		}
	}
	for _, want := range []string{".git", "**/.ssh", "**/*.key", "**/*.pem", "**/id_rsa*", "**/id_ed25519*", "**/.env*", "**/*-secret.yaml"} {
		if !patterns[want] {
			t.Errorf(".dockerignore does not exclude %q", want)
		}
	}
	if patterns["deploy/docker"] || patterns["deploy"] {
		t.Error(".dockerignore must keep deploy/docker/github_known_hosts in the context")
	}
}

// imageRef matches outpostnet-* image references in deployment manifests,
// hack scripts and the controller's default sidecar image.
var imageRef = regexp.MustCompile(`(?:image:\s*|--sidecar-image=|SidecarImage:\s*")(outpostnet-[a-z]+):`)

// TestDeploymentImagesHaveBuildTargets guards that every outpostnet-* image a
// Kubernetes entry references has a Dockerfile target of the same name that
// builds ./cmd/<name>, so no entry points at an image without a recipe.
func TestDeploymentImagesHaveBuildTargets(t *testing.T) {
	var files []string
	for _, pattern := range []string{"deploy/*.yaml", "deploy/*/*.yaml", "deploy/*/*/*.yaml", "deploy/*/*/*/*.yaml", "hack/*.sh", "cmd/outpostnet-controller/app/*.go"} {
		m, err := filepath.Glob(filepath.Join(repoRoot, pattern))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, m...)
	}
	wanted := map[string][]string{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range imageRef.FindAllStringSubmatch(string(raw), -1) {
			wanted[m[1]] = append(wanted[m[1]], f)
		}
	}
	if len(wanted) == 0 {
		t.Fatal("found no outpostnet-* image references; the scan patterns are stale")
	}

	ins := dockerfileInstructions(t)
	targets := map[string]bool{}
	for _, st := range dockerfileStages(t, ins) {
		targets[st.name] = true
	}
	var build string
	for _, in := range ins {
		if strings.HasPrefix(in, "RUN") && strings.Contains(in, "go build") {
			build = in
		}
	}
	buildList := map[string]bool{}
	if m := regexp.MustCompile(`for c in ([a-z -]+);`).FindStringSubmatch(build); m != nil && strings.Contains(build, "./cmd/$c") {
		for _, c := range strings.Fields(m[1]) {
			buildList[c] = true
		}
	}
	for name, refs := range wanted {
		if !targets[name] {
			t.Errorf("image %s (referenced by %v) has no Dockerfile target %q", name, refs, name)
		}
		if !buildList[name] {
			t.Errorf("Dockerfile go build step (for c in ...; ./cmd/$c) does not build %s", name)
		}
		if _, err := os.Stat(filepath.Join(repoRoot, "cmd", name)); err != nil {
			t.Errorf("image %s has no cmd/%s main package: %v", name, name, err)
		}
	}
}
