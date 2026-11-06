// Package config is the runner's own settings file: how agent stages are
// executed on this host. Pipelines never choose their executor; the host
// does.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

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
	Agent  Agent  `yaml:"agent"`
	Events Events `yaml:"events"`
}

// DefaultMaxPayloadBytes is the default event payload limit (owner, 6 Nov).
const DefaultMaxPayloadBytes = 64 << 20

// Events configures event triggers on this host.
type Events struct {
	// SecretFile holds the HMAC key for the HTTP trigger (0600). Without it
	// the HTTP trigger refuses to start; the local CLI trigger needs none.
	SecretFile string `yaml:"secret_file"`
	// MaxPayloadBytes bounds one event's payload (default 64 MiB).
	MaxPayloadBytes int64 `yaml:"max_payload_bytes"`
}

// PayloadLimit is the configured limit or the default.
func (e Events) PayloadLimit() int64 {
	if e.MaxPayloadBytes > 0 {
		return e.MaxPayloadBytes
	}
	return DefaultMaxPayloadBytes
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

	Sandboxd     Sandboxd     `yaml:"sandboxd"`
	ModelGateway ModelGateway `yaml:"model_gateway"`
}

// Sandboxd locates the sandboxd instance that runs agent stages.
type Sandboxd struct {
	// URL is the instance's -listen address, e.g. http://127.0.0.1:43292.
	URL string `yaml:"url"`
	// APIKeyFile is the instance's 0600 api-key file.
	APIKeyFile string `yaml:"api_key_file"`
	// Template is the strict-profile template id or alias.
	Template string `yaml:"template"`
	// GuestOmp is omp's path inside the VM (default /usr/local/bin/omp).
	GuestOmp string `yaml:"guest_omp"`
	// MaxTTL is the instance's -max-ttl (default 1h).
	MaxTTL string `yaml:"max_ttl"`
}

// ModelGateway is where agents' model calls go.
type ModelGateway struct {
	// Listen is the loopback side of the instance's -fc-host-port, e.g.
	// 127.0.0.1:43293.
	Listen string `yaml:"listen"`
	// GuestURL is the same port as the guest sees it, e.g.
	// http://10.0.2.2:43293.
	GuestURL string `yaml:"guest_url"`
	// Upstream is where calls go next: the tunnel to `stagehand
	// gateway-proxy`, e.g. http://127.0.0.1:43294.
	Upstream string `yaml:"upstream"`
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
	if c.Events.SecretFile != "" && !filepath.IsAbs(c.Events.SecretFile) {
		return errors.New("config: events.secret_file must be an absolute path")
	}
	if c.Events.MaxPayloadBytes < 0 {
		return errors.New("config: events.max_payload_bytes must not be negative")
	}
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
		s, g := a.Sandboxd, a.ModelGateway
		var errs []error
		if err := httpURL(s.URL); err != nil {
			errs = append(errs, fmt.Errorf("agent.sandboxd.url: %v", err))
		}
		if !filepath.IsAbs(s.APIKeyFile) {
			errs = append(errs, errors.New("agent.sandboxd.api_key_file must be an absolute path"))
		}
		if s.Template == "" {
			errs = append(errs, errors.New("agent.sandboxd.template is required"))
		}
		if s.GuestOmp != "" && !filepath.IsAbs(s.GuestOmp) {
			errs = append(errs, errors.New("agent.sandboxd.guest_omp must be an absolute path"))
		}
		if s.MaxTTL != "" {
			if d, err := time.ParseDuration(s.MaxTTL); err != nil || d <= 0 {
				errs = append(errs, fmt.Errorf("agent.sandboxd.max_ttl %q must be a positive duration", s.MaxTTL))
			}
		}
		if host, _, err := net.SplitHostPort(g.Listen); err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			errs = append(errs, fmt.Errorf("agent.model_gateway.listen %q must be a loopback host:port", g.Listen))
		}
		if err := httpURL(g.GuestURL); err != nil {
			errs = append(errs, fmt.Errorf("agent.model_gateway.guest_url: %v", err))
		}
		if err := httpURL(g.Upstream); err != nil {
			errs = append(errs, fmt.Errorf("agent.model_gateway.upstream: %v", err))
		}
		if len(errs) > 0 {
			return fmt.Errorf("config: %w", errors.Join(errs...))
		}
	default:
		return fmt.Errorf("config: agent.executor %q: use sandboxd or local", a.Executor)
	}
	return nil
}

// GuestOmpPath is the configured guest omp path or the default.
func (s Sandboxd) GuestOmpPath() string {
	if s.GuestOmp == "" {
		return "/usr/local/bin/omp"
	}
	return s.GuestOmp
}

// MaxTTLDuration is the configured -max-ttl or 1h.
func (s Sandboxd) MaxTTLDuration() time.Duration {
	if d, err := time.ParseDuration(s.MaxTTL); err == nil && d > 0 {
		return d
	}
	return time.Hour
}

func httpURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q must be an http(s) URL", raw)
	}
	return nil
}
