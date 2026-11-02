# stagehand

stagehand is a small scheduled pipeline runner. A pipeline is a YAML file describing a set of
stages and how they depend on each other (a DAG). stagehand runs the stages in order on a
schedule or on demand, and keeps a record of every run. When a stage fails, the run stops and
waits; you fix the problem and resume it.

- It is one static Go binary (no cgo) with one SQLite file for state.
- It needs no GitHub, daemon fleet or org registry.
- It is the pipeline runner from [Gitmoot](https://github.com/gitmoot/gitmoot) (`gitmoot pipeline`),
  extracted so a production job can depend on it without depending on Gitmoot.

> **Status:** v1 is in progress ([epic #2](https://github.com/gitmoot/stagehand/issues/2)).
> Shell stages, schedules, run records, resume, status, and omp agent stages (with a usage cap
> and checked JSON output) work today. Running agent stages inside a sandboxd VM is being added
> in #5–#6; until then the only agent executor is the unsandboxed development one.

## Install

```sh
CGO_ENABLED=0 go build -trimpath -o /usr/local/bin/stagehand ./cmd/stagehand
stagehand version
```

## A pipeline

```yaml
name: nightly-export
description: Export the rows to research and summarise them
schedule:
  interval: 24h          # how often
  jitter: 10m            # random delay added to each run, 0..jitter
  at: "2026-11-08T03:00:00+01:00"   # optional anchor: runs at at + k*interval
env_file: /etc/stagehand/nightly.env   # KEY=VALUE secrets, must be chmod 600
env: {LIMIT: "50"}                     # non-secret defaults
stages:
  - id: export
    cmd: ./export.sh --limit "$LIMIT" --out "$STAGEHAND_WORK/rows.json"
    cwd: /srv/app
    timeout: 10m
    retry: 1
    env_keys: [DB_URL, LIMIT]
  - id: summary
    needs: [export]
    cmd: jq length "$STAGEHAND_WORK/rows.json"
```

### Top-level keys

| Key | Meaning |
|---|---|
| `name` | Pipeline name: lowercase letters, digits, `.`, `_`, `-` |
| `description` | Free text |
| `schedule.interval` | Go duration, at least `1m`. Without a schedule, the pipeline runs only on demand |
| `schedule.jitter` | Random extra delay before each due time, from 0 up to (not including) `jitter`. Must be smaller than `interval` |
| `schedule.at` | RFC 3339 time. Due times become `at + k*interval` |
| `env_file` | Absolute path to a `KEY=VALUE` file of secrets. It must be private (mode `0600`) |
| `env` | Non-secret default values |
| `stages` | The stages |

### Stage keys

| Key | Meaning |
|---|---|
| `id` | Stage name, unique within the pipeline |
| `needs` | Stages that must succeed before this one starts |
| `cmd` | Shell command, run as `sh -c` |
| `cwd` | Absolute working directory. Default: the run's work directory |
| `timeout` | Time limit, as a Go duration. Default: `1h` for shell stages |
| `retry` | How many extra attempts a failed stage gets (0–10) |
| `env_keys` | Variables the stage may see, taken from `env_file` first, then `env`. Nothing else is passed |

Agent stage keys (`agent: omp`):

| Key | Meaning |
|---|---|
| `agent` | `omp` |
| `model`, `effort` | omp `--model` and `--thinking` (`off`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, `auto`) |
| `prompt` or `prompt_file` | The instructions. `prompt_file` is resolved relative to the spec's directory |
| `inputs` | Files from `$STAGEHAND_WORK` copied into the agent's working directory |
| `output` | Required. File in `$STAGEHAND_WORK` that the **runner** writes from the agent's final answer, which must be JSON |
| `tools` | Read-only omp tools: `read`, `web_search`, `grep`, `glob`. Default: `read, web_search` |
| `max_cost` | Required. Usage cap per attempt, in omp's list-price units (USD for API pricing) |
| `timeout` | Required for agent stages |

Agent stages take no `cwd` or `env_keys`: the agent receives no environment and no secrets.

Unknown keys are errors, so a typo fails when you add the pipeline instead of being silently
ignored.

### Agent stages

An agent stage runs omp headless:

```
omp -p --mode=json --no-session --no-skills --no-rules --no-extensions --no-lsp --no-pty --tools=<tools> …
```

- **The agent writes nothing.** It has only read-only tools. The runner takes its final
  message, accepts either pure JSON or a single fenced block, and writes that as `output`. An
  answer that is not valid JSON fails the stage, and nothing is written. A leftover output from
  an earlier attempt is deleted before the stage starts.
- **Success is read from omp's event stream, not its exit code.** omp exits 0 even when a turn
  fails. A failed turn, a retry omp gave up on, a cut-off answer or a stream with no final
  `agent_end` all fail the stage.
- **Usage cap:** the runner adds up `usage.cost.total` from every assistant message and stops
  omp as soon as the total passes `max_cost`. The stage then fails with `over budget`, and the
  cost so far is recorded. The cap can be overshot by up to the cost of one model call.
- **Time limit:** `timeout` applies, and omp also gets `--max-time` set to 90% of the time left,
  so it can report before it is stopped.
- **Prompt:** the runner adds a short note to the prompt listing the input files and saying the
  answer must be JSON only.

Where omp runs is a host setting, not part of the pipeline. Set it in the runner settings file
(`--config`, `$STAGEHAND_CONFIG`, or `<home>/config.yaml`):

```yaml
agent:
  executor: local            # development only: omp runs on this host, unsandboxed
  allow_unsafe_local: true   # required with local
  omp: /usr/local/bin/omp    # optional; default "omp" on PATH
```

The `local` executor is **not a sandbox**. omp's `read` tool can read any file the runner can
read and can fetch URLs. Production agent stages run in a sandboxd Firecracker VM instead
(#5). With no executor configured, an agent stage fails with "no agent executor is configured on this runner".

## How a run works

- **Order:** a stage starts once every stage in its `needs` has succeeded. Stages that don't
  depend on each other run at the same time.
- **Shell stages:**
  - The command runs as `sh -c` in its own process group, with an **exact** environment.
  - A timeout or cancel kills the whole group.
  - The exit code decides the result.
  - To stop the run without failing it, a stage can write
    `{"decision":"blocked","summary":"…","needs":["…"]}` to `$STAGEHAND_RESULT`.
- **What a shell stage sees:**
  - `PATH`, `HOME`, `LANG` and `TZ` from the runner;
  - `STAGEHAND_PIPELINE`, `STAGEHAND_RUN_ID`, `STAGEHAND_STAGE`, `STAGEHAND_ATTEMPT`;
  - `STAGEHAND_WORK`, the run's shared work directory, where stages pass files to each other;
  - `STAGEHAND_RESULT`;
  - the variables in its `env_keys`, and nothing else.
- **A failed or blocked stage parks the run.** Stages that depend on it are skipped. Stages that
  don't depend on it still finish. The run ends as `failed` or `blocked`.
- **Resume:** `stagehand resume <run> [--from <stage>]` reruns these stages, keeping the rest:
  - the given stage (by default, the one the run stopped on);
  - every stage that depends on it;
  - every stage that hasn't succeeded.

  A resumed stage gets a fresh `retry` budget.
- **Spec snapshot:** each run keeps the spec it started with. Editing or replacing the pipeline
  never changes a run that is already in progress.
- **One run at a time:** a pipeline has at most one running run. A schedule that comes due while
  a run is still going fires as soon as that run ends. Missed slots become a single run.
- **Crash recovery:** if stagehand stops or crashes during a stage, the next start marks that
  attempt `interrupted` and parks the run. It never retries the stage on its own, because a half-done
  stage may have side effects. It also kills the stage's leftover process group if that group is
  still running.

## Commands

```
stagehand [--home DIR] [--config FILE] <command>

add <spec.yaml> [--replace] [--enable]   register (or update) a pipeline
list [--json]                            pipelines with schedule and last run
show <name|run-id> [--json]              a pipeline, or one run with its stages
enable <name> | disable <name>           turn the schedule on or off
remove <name>                            delete a pipeline (its runs stay)
run <name> [--wait] [--json]             start a run
runs <name> [--limit N] [--json]         newest runs of a pipeline
resume <run-id> [--from STAGE] [--wait]  re-run a parked run from a stage
cancel <run-id>                          stop a running run
serve [--status-addr 127.0.0.1:PORT]     run schedules and stages until stopped
tick                                     one pass: fire due schedules, run them to the end
status [--json]                          last and next run per pipeline
version
```

- **Home directory:** the database, logs and work files live in the home. It is `--home`, else
  `$STAGEHAND_HOME`, else `/var/lib/stagehand` for root, else `$XDG_STATE_HOME/stagehand`.
- **One executor per home:** only one process runs stages for a home at a time, enforced by a
  lock on `stagehand.lock`.
  - When `serve` is running, `run`, `resume` and `cancel` only record the request, and `serve`
    carries it out.
  - When it is not running, those commands run the stages themselves and return when the run
    ends.
- **Exit status:** `run` and `resume` exit non-zero unless the run succeeded.

### Status for dashboards

`stagehand status --json`, or `GET /status` on a `serve --status-addr` listener, returns:

```json
{
  "generated_at": "2026-11-03T10:00:00Z",
  "version": "v0.1.0",
  "pipelines": [
    {
      "name": "nightly-export",
      "enabled": true,
      "schedule": {"interval": "24h", "jitter": "10m"},
      "next_due_at": "2026-11-04T03:04:12Z",
      "active_run_id": null,
      "last_run": {
        "id": 12, "trigger": "schedule", "state": "succeeded",
        "created_at": "2026-11-03T03:02:00Z", "finished_at": "2026-11-03T03:09:41Z",
        "cost": 0, "input_tokens": 0, "output_tokens": 0
      },
      "spec_hash": "…"
    }
  ]
}
```

The status listener accepts only loopback addresses. Put an authenticating proxy in front of it
if anything off the host should read it.

## Running as a service

```ini
# /etc/systemd/system/stagehand.service
[Unit]
Description=stagehand pipeline runner
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/stagehand --home /var/lib/stagehand serve --status-addr 127.0.0.1:8795
Restart=on-failure
KillMode=control-group
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

- **Stopping:** on `SIGTERM`, `serve` stops the running stages and parks their runs as
  `interrupted`, so they can be resumed later.
- **Crashes:** `KillMode=control-group` makes systemd kill any stage processes left behind if the
  runner crashes.

## Development

```sh
go vet ./...
go test -race ./...
```

## License

Apache-2.0
