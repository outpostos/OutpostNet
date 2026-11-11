# OutpostNet Release SOP

Standard Operating Procedure for releasing OutpostNet versions. This document codifies the process used for v1.0.0/v1.0.1 and serves as the canonical reference for all future releases.

## Overview

OutpostNet releases produce **20 artifacts** (5 components x 4 platforms; v1.0.x–v2.0.0 shipped 16, before `outpostnet-service` existed) as compressed tarballs, published via GitHub Releases. Each release also publishes the single-file Kubernetes manifest `outpostnet-k8s.yaml` and a `SHA256SUMS` checksum file covering every archive and the manifest. The landing page at [net.outpostos.com](https://net.outpostos.com/) is updated to reflect the latest version.

### Components

| Component | Binary | Description |
|-----------|--------|-------------|
| Agent | `outpostnet-agent` | Unified tunnel agent (server/client roles), peer directory — primary 1.0 component |
| Service | `outpostnet-service` | OutpostService stable entries for `<service>@<node_id>` ([service-layer.md](service-layer.md)) |
| Server | `outpostnet-server` | Node-local routing hub for sidecars (Unix socket) |
| Sidecar | `outpostnet-sidecar` | Per-application process that registers exports/imports with the local server |
| Controller | `outpostnet-controller` | Kubernetes controller and mutating webhook (sidecar injection) |

### Platforms

| OS | Architecture |
|----|-------------|
| Linux | amd64, arm64 |
| macOS (Darwin) | amd64 (Intel), arm64 (Apple Silicon) |

## Phase 1: Pre-Release Checks

### 1.1 Code Quality

```bash
make fmt        # Format all Go source
make vet        # Static analysis
make test       # Unit tests
```

All must pass with zero errors. `make test` fetches the private
`github.com/outpostos/OutpostRange` module (see README "Build from source").

### 1.2 E2E Test Suite

```bash
# Run standalone E2E tests
hack/e2e-relay.sh
hack/e2e-cross-region.sh
```

These scripts run in a Kubernetes cluster and expect the images named in each
script header (for example `outpostnet-agent:latest`) to be present on the
nodes. Build them from the repository `Dockerfile` (one target per image, the
OutpostRange read-only deploy key passed as the BuildKit secret
`outpostrange_deploy_key`; see README "Kubernetes") and load them into the
cluster before running the scripts.

Verify TCP tunnel connectivity across supported cross-node scenarios. UDP is verified only node-local (outpostnet-server + outpostnet-sidecar, `TestUDPEndToEnd`); cross-node UDP over the QUIC tunnel is not implemented.

### 1.3 Documentation Audit

Verify consistency across all user-facing materials:

- [ ] `README.md` — install commands, config examples, feature matrix
- [ ] `docs/quickstart.md` — step-by-step instructions match actual CLI behavior
- [ ] `docs/configuration.md` — all YAML fields match `pkg/agentconfig` structs
- [ ] `docs/architecture.md` — diagrams reflect current component relationships
- [ ] `CHANGELOG.md` — new version entry drafted
- [ ] `deploy/examples/` — example configs validated against current schema
- [ ] `docs/observation.md` — OutpostRange version matches `go.mod`; release notes state that Range must be upgraded first

**Language rule:** All public-facing docs must be pure English. Zero Chinese characters.

### 1.4 Cross-Review

Assign at least two independent reviewers:

| Review Type | Focus |
|-------------|-------|
| Technical review | CLI commands, config fields, default values match code |
| Quality review | Language consistency, link validity, user perspective |

## Phase 2: Build Release Artifacts

### 2.1 Tag the Release

```bash
# Ensure working tree is clean
git status

# Create annotated tag
git tag -a v1.X.Y -m "OutpostNet v1.X.Y"

# Verify version injection
go run ./cmd/outpostnet-agent version
```

The version package (`pkg/version`) injects `gitVersion`, `gitCommit`, and `buildDate` via `-ldflags` at build time.

### 2.2 Build All Artifacts

```bash
make release
```

This target:
1. Runs `fmt` and `vet` checks
2. Cross-compiles all 5 components (`RELEASE_COMPONENTS`) for all 4 platforms (`RELEASE_PLATFORMS`) with `CGO_ENABLED=0` (static linking)
3. Strips debug symbols (`-s -w`) for smaller binaries
4. Packages each binary as `<component>-<VERSION>-<os>-<arch>.tar.gz`
5. Generates `outpostnet-k8s.yaml` from `deploy/kustomize/base`, pinning every image tag to `<VERSION>` (`kubectl` must be on `PATH`, or set `KUSTOMIZE` to another kustomize-compatible command)
6. Writes `SHA256SUMS` covering every archive and `outpostnet-k8s.yaml`
7. Outputs everything to `build/release/`

The `release` target removes stale `*.tar.gz` files from `build/release/` first, so the checksums and the publish glob cannot pick up artifacts from an older release.

### 2.3 Verify Artifacts

```bash
# Count: should be 20
ls build/release/*.tar.gz | wc -l

# Spot-check: extract and verify version
tar xzf build/release/outpostnet-agent-v1.X.Y-linux-amd64.tar.gz
./outpostnet-agent-v1.X.Y-linux-amd64 version

# Verify the manifest, image tags, and checksums
kubectl apply --dry-run=client --validate=false -f build/release/outpostnet-k8s.yaml
grep -E "image: outpostnet-.*v1.X.Y" build/release/outpostnet-k8s.yaml
make release-verify          # runs: (cd build/release && sha256sum -c SHA256SUMS)
```

Expected output includes: git version, commit hash, build date, Go version, platform, and one `OK` line per checksummed file.

## Phase 3: Publish

### 3.1 Push Tag (automated release)

```bash
git push origin v1.X.Y
```

Pushing a `v*.*.*` tag triggers [`.github/workflows/release.yml`](../.github/workflows/release.yml). The workflow runs `make release` and `make release-verify`, then creates the GitHub Release with:

- all 20 `<component>-<VERSION>-<os>-<arch>.tar.gz` archives
- `outpostnet-k8s.yaml`
- `SHA256SUMS`

### 3.2 Create a GitHub Release manually

Use this path only when publishing a release built locally (for example, when the workflow did not run). Do not run it after the workflow has already created the release for the same tag.

```bash
gh release create v1.X.Y \
  --title "OutpostNet v1.X.Y" \
  --notes-file CHANGELOG_EXCERPT.md \
  build/release/*.tar.gz \
  build/release/outpostnet-k8s.yaml \
  build/release/SHA256SUMS
```

Or create via GitHub UI:
1. Go to Releases > Draft a new release
2. Select the tag `v1.X.Y`
3. Paste release notes from CHANGELOG.md
4. Upload all 20 archives, `outpostnet-k8s.yaml`, and `SHA256SUMS` from `build/release/`

### 3.3 Update Landing Page

Edit `docs/index.html`:
- Update version badge text (e.g., "Download v1.X.Y")
- Update download link to point to new GitHub Release
- Commit and push to main

### 3.4 Update Public Website

After pushing the landing page changes, verify [net.outpostos.com](https://net.outpostos.com/) reflects the new version.

## Phase 4: Post-Release Verification

### 4.1 Download Verification

- [ ] GitHub Release page loads correctly
- [ ] All 20 artifact download links work
- [ ] Binary extracts and runs `--version` on at least one platform
- [ ] `SHA256SUMS` is attached and `sha256sum -c SHA256SUMS` passes after downloading every asset
- [ ] `outpostnet-k8s.yaml` is attached and its image tags match the release version

```bash
# In a directory containing all downloaded release assets:
sha256sum -c SHA256SUMS
```

### 4.2 Documentation Links

- [ ] README.md release badge points to latest version
- [ ] `docs/index.html` download button targets correct release
- [ ] Quickstart install commands reference correct version

### 4.3 Announcement

Post release summary including:
- Version number and highlights
- Link to GitHub Release
- Link to landing page: [net.outpostos.com](https://net.outpostos.com/)

## Hotfix Release Process

For patch releases (e.g., v1.0.0 -> v1.0.1):

1. Fix the issue on `main` branch
2. Follow the same Phase 1-4 process
3. In CHANGELOG, note this is a patch with specific fixes
4. No need for full E2E re-test if changes are docs-only

**Example:** v1.0.1 was a docs-only patch that removed Multi-Hop Relay references and fixed Chinese/English mixing. The process was: fix on main -> tag v1.0.1 -> rebuild -> publish.

## Artifact Naming Convention

```
outpostnet-{component}-{version}-{os}-{arch}.tar.gz
```

Examples:
```
outpostnet-agent-v1.0.1-linux-amd64.tar.gz
outpostnet-agent-v1.0.1-darwin-arm64.tar.gz
outpostnet-server-v1.0.1-linux-arm64.tar.gz
```

The release also ships two non-archive files: `outpostnet-k8s.yaml` (Kubernetes manifest) and `SHA256SUMS` (checksums for all archives and the manifest).

## Version History

| Version | Date | Type | Notes |
|---------|------|------|-------|
| v1.0.0 | 2026-03-29 | Major | First stable release, FRP parity |
| v1.0.1 | 2026-03-29 | Patch | Doc fixes: removed Multi-Hop Relay refs, pure English |

## References

- Makefile release targets: `release`, `release-archives`, `release-manifest`, `release-checksums`, `release-verify`
- Release workflow: `.github/workflows/release.yml` (tag push builds and publishes all artifacts)
- Kustomize base for the release manifest: `deploy/kustomize/base/`
- Version injection: `pkg/version/version.go`
- Config schema: `pkg/agentconfig/`
- Example configs: `deploy/examples/`
- E2E test scripts: `hack/`
