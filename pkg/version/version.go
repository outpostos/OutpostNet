// Package version provides version information for OutpostNet binaries.
// Values are injected via -ldflags at build time.
package version

import (
	"fmt"
	"runtime"
)

var (
	// gitVersion is the semantic version, set via ldflags.
	gitVersion = "v0.0.0-dev"
	// gitCommit is the git SHA1, set via ldflags.
	gitCommit = "unknown"
	// buildDate is the build timestamp in ISO8601 format, set via ldflags.
	buildDate = "1970-01-01T00:00:00Z"
)

// Info holds version metadata.
type Info struct {
	GitVersion string `json:"gitVersion"`
	GitCommit  string `json:"gitCommit"`
	BuildDate  string `json:"buildDate"`
	GoVersion  string `json:"goVersion"`
	Platform   string `json:"platform"`
}

// Get returns the overall codebase version.
func Get() Info {
	return Info{
		GitVersion: gitVersion,
		GitCommit:  gitCommit,
		BuildDate:  buildDate,
		GoVersion:  runtime.Version(),
		Platform:   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// String returns version info as a human-readable string.
func (i Info) String() string {
	return fmt.Sprintf("%s (commit: %s, built: %s, %s, %s)",
		i.GitVersion, i.GitCommit, i.BuildDate, i.GoVersion, i.Platform)
}
