package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func load(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path, true)
}

// usage_limit_wait is parsed once, when the file is read, and the parsed
// duration is what the runner gets.
func TestUsageLimitWait(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": 0, "0": 0, "5m": 5 * time.Minute, "30m": 30 * time.Minute} {
		body := "agent: {}\n"
		if raw != "" {
			body = "agent:\n  usage_limit_wait: " + raw + "\n"
		}
		c, err := load(t, body)
		if err != nil {
			t.Errorf("%q: %v", raw, err)
			continue
		}
		if got := time.Duration(c.Agent.UsageLimitWait); got != want {
			t.Errorf("%q: %v, want %v", raw, got, want)
		}
	}
	for _, raw := range []string{"31m", "-1m", "2", "soon"} {
		if _, err := load(t, "agent:\n  usage_limit_wait: "+raw+"\n"); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
}
