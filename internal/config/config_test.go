package config

import (
	"testing"
	"time"
)

func TestUsageLimitWait(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": 0, "0": 0, "5m": 5 * time.Minute, "30m": 30 * time.Minute} {
		c := Config{Agent: Agent{UsageLimitWait: raw}}
		if err := c.Validate(); err != nil {
			t.Errorf("%q: %v", raw, err)
		}
		if got := c.Agent.UsageLimitWaitDuration(); got != want {
			t.Errorf("%q: %v, want %v", raw, got, want)
		}
	}
	for _, raw := range []string{"31m", "-1m", "2", "soon"} {
		if err := (Config{Agent: Agent{UsageLimitWait: raw}}).Validate(); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
}
