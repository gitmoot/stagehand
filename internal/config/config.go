// Package config is the runner's own settings file: how agent stages are
// executed on this host. Pipelines never choose their executor; the host
// does.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v3"
)

// Executor names.
const (
	ExecutorNone     = ""
	ExecutorLocal    = "local"
	ExecutorSandboxd = "sandboxd"
)

// Config is the settings file.
type Config struct {
	Agent Agent `yaml:"agent"`
}

// Agent configures agent stages.
type Agent struct {
	// Executor is "sandboxd" (production), "local" (development only, no
	// isolation) or empty (agent stages fail).
	Executor string `yaml:"executor"`
	// AllowUnsafeLocal must be true to use the local executor: it runs omp
	// on the host, where it can read any file the runner can.
	AllowUnsafeLocal bool `yaml:"allow_unsafe_local"`
	// Omp is the omp binary for the local executor (default "omp" on PATH).
	Omp string `yaml:"omp"`
}

// Load reads path. A missing file is an empty config unless required.
func Load(path string, required bool) (Config, error) {
	var c Config
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && !required {
		return c, nil
	}
	if err != nil {
		return c, fmt.Errorf("config: %v", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return c, fmt.Errorf("config %s: %v", path, err)
	}
	return c, c.Validate()
}

// Validate checks the settings.
func (c Config) Validate() error {
	a := c.Agent
	switch a.Executor {
	case ExecutorNone:
	case ExecutorLocal:
		if !a.AllowUnsafeLocal {
			return errors.New(`config: agent.executor "local" runs omp unsandboxed on this host; set agent.allow_unsafe_local: true to accept that (development only)`)
		}
		if a.Omp != "" && !filepath.IsAbs(a.Omp) {
			return fmt.Errorf("config: agent.omp %q must be an absolute path", a.Omp)
		}
	case ExecutorSandboxd:
		return errors.New(`config: agent.executor "sandboxd" is not available in this build yet`)
	default:
		return fmt.Errorf("config: agent.executor %q: use sandboxd or local", a.Executor)
	}
	return nil
}
