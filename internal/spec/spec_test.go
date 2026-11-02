package spec

import (
	"strings"
	"testing"
)

const valid = `
name: news-research
description: weekly research
schedule: {interval: 168h, jitter: 30m}
env_file: /etc/stagehand/news.env
env: {LIMIT: "50"}
stages:
  - id: export
    cmd: ./export.sh
    cwd: /srv/app
    timeout: 10m
    retry: 1
    env_keys: [DB_URL, LIMIT]
  - id: research
    needs: [export]
    agent: omp
    model: anthropic/claude-opus-5-5
    effort: high
    prompt_file: prompts/research.md
    inputs: [rows.json]
    output: decisions.json
    tools: [read, web_search]
    max_cost: 5
    timeout: 50m
  - id: write
    needs: [research]
    cmd: ./apply.sh
`

func TestValidSpecLoads(t *testing.T) {
	sp, err := Load([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if sp.Stages[1].Kind() != KindAgent || sp.Stages[0].Kind() != KindShell {
		t.Error("kinds")
	}
	if got := sp.Dependents("research"); strings.Join(got, ",") != "research,write" {
		t.Errorf("Dependents = %v", got)
	}
}

func TestInvalidSpecs(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"unknown key", "name: p\nstages:\n  - {id: a, cmd: x, comand: y}\n", "comand"},
		{"no stages", "name: p\n", "at least one stage"},
		{"bad name", "name: P Q\nstages:\n  - {id: a, cmd: x}\n", "name"},
		{"duplicate id", "name: p\nstages:\n  - {id: a, cmd: x}\n  - {id: a, cmd: y}\n", "duplicate"},
		{"unknown need", "name: p\nstages:\n  - {id: a, cmd: x, needs: [b]}\n", "unknown stage"},
		{"cycle", "name: p\nstages:\n  - {id: a, cmd: x, needs: [b]}\n  - {id: b, cmd: x, needs: [a]}\n", "cycle"},
		{"both executors", "name: p\nstages:\n  - {id: a, cmd: x, agent: omp}\n", "not both"},
		{"no executor", "name: p\nstages:\n  - {id: a}\n", "set cmd"},
		{"relative cwd", "name: p\nstages:\n  - {id: a, cmd: x, cwd: rel}\n", "absolute"},
		{"agent key on shell", "name: p\nstages:\n  - {id: a, cmd: x, max_cost: 1}\n", "only for agent"},
		{"reserved env", "name: p\nstages:\n  - {id: a, cmd: x, env_keys: [STAGEHAND_WORK]}\n", "reserved"},
		{"retry too high", "name: p\nstages:\n  - {id: a, cmd: x, retry: 99}\n", "retry"},
		{"short interval", "name: p\nschedule: {interval: 10s}\nstages:\n  - {id: a, cmd: x}\n", "shorter"},
		{"jitter too big", "name: p\nschedule: {interval: 1h, jitter: 2h}\nstages:\n  - {id: a, cmd: x}\n", "jitter"},
		{"agent without cap", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, output: o.json, timeout: 1m}\n", "max_cost"},
		{"agent without timeout", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, output: o.json, max_cost: 1}\n", "timeout is required"},
		{"agent without output", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, max_cost: 1, timeout: 1m}\n", "output is required"},
		{"agent write tool", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, output: o.json, max_cost: 1, timeout: 1m, tools: [bash]}\n", "not allowed"},
		{"agent env", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, output: o.json, max_cost: 1, timeout: 1m, env_keys: [A]}\n", "no environment"},
		{"output path", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, output: ../o.json, max_cost: 1, timeout: 1m}\n", "plain file name"},
		{"two prompts", "name: p\nstages:\n  - {id: a, agent: omp, prompt: x, prompt_file: y, output: o.json, max_cost: 1, timeout: 1m}\n", "exactly one"},
		{"other agent", "name: p\nstages:\n  - {id: a, agent: codex, prompt: x, output: o.json, max_cost: 1, timeout: 1m}\n", "only \"omp\""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load([]byte(c.yaml))
			if err == nil {
				t.Fatalf("accepted:\n%s", c.yaml)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
}
