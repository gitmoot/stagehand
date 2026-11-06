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
> and checked JSON output) work today. Agent stages run in sandboxd VMs; deployment is tracked
> in #7–#8.

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
| `events.max_queued` | Opt in to event triggers; at most this many events wait (see [Starting pipelines from events](#starting-pipelines-from-events)) |
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
read, and it can fetch URLs. With no executor configured, an agent stage fails with "no agent
executor is configured on this runner".

### Production: sandboxd

In production, each attempt of an agent stage runs in a fresh
[sandboxd](https://github.com/gitmoot/sandboxd) Firecracker VM (strict profile, guest user
uid 1000), and the VM is destroyed afterwards.

- **What goes into the VM:** only the stage's `inputs`. The VM gets no environment variables and
  no credentials from the host.
- **Model calls:** the guest's omp sends them to `http://10.0.2.2:<host-port>`, the only host port
  a sandboxd instance exposes. There, stagehand runs a **run gateway** for the length of the
  attempt. It accepts only that attempt's random token, removes it, and forwards the call.
- **Credentials:** the real model credential is added further along, by
  `stagehand gateway-proxy`, on the host that holds the omp auth-gateway token.
- **Where things run:** the runner (and so the shell stages) runs next to the data its shell
  stages need. The sandboxd instance can be on another host: one ssh tunnel forwards its API to
  the runner (`-L`) and its guest host port back to the runner's run gateway (`-R`).

```
guest omp ── token for this attempt ──▶ 10.0.2.2:43293 (sandbox host's -fc-host-port)
          ──▶ ssh -R ──▶ run gateway (stagehand serve on the runner host, 127.0.0.1:43293)
          ──▶ stagehand gateway-proxy (127.0.0.1:43295, adds bearer)
          ──▶ omp auth-gateway (127.0.0.1:4011) ──▶ provider
```

```yaml
agent:
  executor: sandboxd
  sandboxd:
    url: http://127.0.0.1:43292              # the instance's -listen (via the tunnel's -L)
    api_key_file: /etc/stagehand/sandboxd-api-key
    template: stagehand-omp-strict
    guest_omp: /usr/local/bin/omp            # default
    max_ttl: 1h                              # the instance's -max-ttl
  model_gateway:
    listen: 127.0.0.1:43293                  # reached from the instance's -fc-host-port (tunnel -R)
    guest_url: http://10.0.2.2:43293
    upstream: http://127.0.0.1:43295         # stagehand gateway-proxy
```

- **Which models work:** the VM's omp gets only `ANTHROPIC_BASE_URL` and `ANTHROPIC_API_KEY`
  (the token for that attempt), so `anthropic/*` models work through this path.
- **One stage at a time:** each instance has one host port, so only one agent stage runs at a
  time. Agent stages that become ready meanwhile, in any pipeline, wait as `pending` and start
  when the slot frees; their time limit starts only then.
- **Crash recovery:** the sandbox job id is recorded before the VM is requested. Cleanup finds
  VMs by that id in the instance's inventory, so even a VM whose create response was lost gets
  destroyed.

Unit files and an example config are in [`deploy/`](deploy/).

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
  - `STAGEHAND_TRIGGER`, and for event runs `STAGEHAND_EVENT_ID`, `STAGEHAND_EVENT_FILE`, `STAGEHAND_EVENT_CONTENT_TYPE`;
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
  attempt `interrupted` and parks the run. It never retries the stage on its own, because a
  half-done stage may have side effects. A shell stage's command is held until its process group
  has been recorded, so a crash never leaves an untracked command running.
- **Cleanup:** every process group or sandbox an attempt starts is recorded as a pending cleanup
  before it starts, and the record is removed only once it is confirmed gone. Anything left over
  (after a crash, or a sandbox delete that failed) is retried at the next start and every minute
  while `serve` runs.
- **Stopping:** on `SIGTERM`, stagehand stops the running stages and starts nothing new. A run
  whose next stage was ready stays `running`, and the next runner continues it.

## Starting pipelines from events

A pipeline can also be started by something that happens outside stagehand, for example a mail
poller seeing a new message or a webhook receiver getting a call. Each accepted event queues one
run, and its payload becomes that run's input. Receivers live outside stagehand; they just call the
trigger.

```yaml
name: deck-review
events:
  max_queued: 20        # opt in; at most 20 events wait, more are refused
schedule: {interval: 24h}   # optional: schedule and events can share a pipeline
stages:
  - id: intake
    cmd: cp "$STAGEHAND_EVENT_FILE" "$STAGEHAND_WORK/mail.eml"
  - id: score
    needs: [intake]
    agent: omp
    inputs: [mail.eml]
    # …
```

**How events run**
- Runs start in arrival order. A pipeline still has at most one running run at a time, and agent
  stages still share the sandbox one at a time.
- When `max_queued` events are already waiting, a new trigger is refused and nothing is stored, so
  the caller knows to retry. Accepted events are never lost or run twice.
- A disabled pipeline refuses events; its queued events wait until it is enabled again.

**What a stage sees.** The payload is passed **only as a file**. stagehand never puts event content
into a command line or an environment value. Shell stages get:

| Variable | Value |
|---|---|
| `STAGEHAND_TRIGGER` | `event`, `schedule` or `manual` |
| `STAGEHAND_EVENT_ID` | the event id (the caller's, or a generated one) |
| `STAGEHAND_EVENT_FILE` | path to the payload, exactly the bytes received |
| `STAGEHAND_EVENT_CONTENT_TYPE` | the sender's media type, if it was well formed |

Agent stages see the payload only if a shell stage copies it into `$STAGEHAND_WORK` and lists it in
`inputs`.

**Idempotency.** Send an event id. The same id for the same pipeline always gives one event and one
run, so a webhook that retries cannot start duplicates. Without an id, every trigger is a new
event. Ids are 1–128 characters from `A-Z a-z 0-9 . _ : -`.

### HTTP trigger

`stagehand serve --trigger-addr 127.0.0.1:8798` serves `POST /v1/pipelines/{name}/events`.

- **Address:** loopback only, unless `--trigger-allow-remote` is also given. Put a TLS proxy in
  front of it before exposing it.
- **Body:** the raw payload.
- **Authentication:** every request must be signed with HMAC-SHA256, using the key in the host
  config's `events.secret_file` (a 0600 file):

```
X-Stagehand-Timestamp: <unix seconds, within 5 minutes of the runner's clock>
X-Stagehand-Event-Id:  <optional idempotency key>
X-Stagehand-Signature: sha256=<hex HMAC-SHA256(key, timestamp "\n" pipeline "\n" event_id "\n" body)>
```

```sh
KEY=$(cat /etc/stagehand/trigger.key); TS=$(date +%s); ID=msg-123
SIG=$( { printf '%s\n%s\n%s\n' "$TS" deck-review "$ID"; cat mail.eml; } | openssl dgst -sha256 -hmac "$KEY" -r | cut -d' ' -f1)
curl -sS -X POST http://127.0.0.1:8798/v1/pipelines/deck-review/events \
  -H "X-Stagehand-Timestamp: $TS" -H "X-Stagehand-Event-Id: $ID" -H "X-Stagehand-Signature: sha256=$SIG" \
  -H 'Content-Type: message/rfc822' --data-binary @mail.eml
```

| Status | Meaning |
|---|---|
| 202 | Accepted and queued (JSON gives the event id, size and SHA-256) |
| 200 | Duplicate: this event id was already received; the original event and its run are returned |
| 401 | Missing, stale or wrong signature |
| 404 | No such pipeline, or it has no `events:` block |
| 409 | Pipeline disabled |
| 413 | Payload larger than `events.max_payload_bytes` (default 64 MiB) |
| 429 | Queue full (`Retry-After: 60`); nothing was stored |

### CLI trigger

```sh
stagehand trigger deck-review --event-id msg-123 --payload-file mail.eml [--content-type message/rfc822] [--wait] [--json]
```

- Reads the payload from stdin if `--payload-file` is not given.
- Needs no signature: whoever can run it can already read the home.
- `--wait` waits for the event's run and exits non-zero unless it succeeded. If no `serve` is
  running, it runs the queued events itself.

Related commands:

| Command | What it does |
|---|---|
| `events [--pipeline N] [--state queued]` | Lists accepted events |
| `drop-event <id>` | Discards a queued event and its payload |
| `status --json` | Shows `queued_events` for each pipeline |
| `show <run-id>` | Shows the run's event: id, source, size, SHA-256; never the payload |

### Payload retention

Payloads can be confidential (for example, mail with attachments).

- **What is kept:** an event's payload, and the files of its run (work, logs, results), while the
  event is queued and while its run is running or parked, so a parked run can be resumed.
- **When it is deleted:** as soon as the run **succeeds**. The run's database record stays: states,
  exit codes, cost and summaries.
- **Purging a parked run:** `stagehand purge <run-id>` deletes a parked event run's files and payload
  once it will not be resumed; after that it cannot be resumed.

### Host settings

```yaml
# runner config
events:
  secret_file: /etc/stagehand/trigger.key   # required for --trigger-addr; 0600
  max_payload_bytes: 67108864               # default 64 MiB
```

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
serve [--status-addr ADDR] [--trigger-addr ADDR]   run schedules, events and stages until stopped
tick                                     one pass: fire due schedules and queued events, run them to the end
status [--json]                          last and next run per pipeline
trigger <name> [--event-id ID] [--payload-file F] [--wait]   queue an event run
events [--pipeline N] [--state S] [--json]                   accepted events
drop-event <event-id>                    discard a queued event and its payload
purge <run-id>                           delete a finished event run's files and payload
version
```

- **Home directory:** the database, logs and work files live in the home. It is `--home`, else
  `$STAGEHAND_HOME`, else `/var/lib/stagehand` for root, else `$XDG_STATE_HOME/stagehand`.
- **One executor per home:** only one process runs stages for a home at a time, enforced by a
  lock on `stagehand.lock`. The lock file records whether its holder is `serve` or a one-off
  command.
  - When `serve` holds it, `run`, `resume` and `cancel` only record the request, and `serve`
    carries it out. With `--wait`, the command waits for the result and takes over if `serve`
    goes away.
  - When another one-off command holds it, `run` and `resume` wait for the lock and then run
    their own stages. They are never left queued with nobody to run them.
  - When nobody holds it, the command runs the stages itself and returns when the run ends.
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
      "queued_events": 0,
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

### Upgrading and rolling back

1. Check that nothing is running: `stagehand status --json` shows `active_run_id: null` for every pipeline.
2. Put the new binary next to the old one, then `mv` it over `/usr/local/bin/stagehand`. Renaming
   on the same filesystem is atomic and avoids "text file busy".
3. `systemctl restart stagehand`, then check `stagehand version` and `GET /status`.

To roll back, do the same with the previous binary. The database only gets new migrations: an older
binary refuses a newer schema rather than misreading it (`database schema version N is newer than
this binary supports`). Rolling back across a schema change therefore needs the database file
backed up before the upgrade.

The sandbox tunnel (`deploy/stagehand-hetzner2-tunnel.service`) reconnects on its own after a
network drop. Before each connection, `stagehand tunnel-clear-stale` ends a session on the far side
that still holds the forwarded port, but only one it can prove is this host's own dead
connection. That means an sshd-session on exactly that address and port, whose client is this host
and whose TCP connection no longer exists here. Live sessions and other hosts' sessions are left
alone.

## Development

```sh
go vet ./...
go test -race ./...
```

## License

Apache-2.0
