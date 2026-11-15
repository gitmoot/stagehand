# Video runner on hetzner2

Video pipelines run on hetzner2, next to their data, so the fleet host stays usable
(owner direction 2026-11-15; gitmoot/stagehand#28).

| Item | Value |
|---|---|
| Unit | `stagehand-video.service` |
| Home | `/var/lib/stagehand-video` |
| Config | `/etc/stagehand-video/config.yaml` |
| Trigger key | `/etc/stagehand-video/trigger.key` (0600) |
| Status | `127.0.0.1:8807/status` |
| Trigger | `127.0.0.1:8808` |
| Agent stages | sandboxd instance `stagehandvid` (API 43297, guest port 43298) |
| Operator CLI | `stagehand-video <command>` (sets the home and the config) |

## Install

```sh
install -m 755 stagehand /usr/local/bin/stagehand
install -m 755 stagehand-video /usr/local/bin/stagehand-video
install -d -m 700 /etc/stagehand-video /var/lib/stagehand-video
install -m 600 config.yaml /etc/stagehand-video/config.yaml
(umask 077; openssl rand -hex 32 > /etc/stagehand-video/trigger.key)
install -m 644 stagehand-video.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now stagehand-video
```

## Moving a pipeline here

Never run a pipeline on both runners at once. Do the cutover between runs:

1. `stagehand disable <name>` on the fleet host.
2. Move its state.
3. `stagehand-video add <spec> --enable` here.

## Model credentials

`agent.model_gateway.upstream` points at `127.0.0.1:43299`, where nothing listens until the owner
approves a credential route. Until then, shell stages work and agent stages fail at their first
model call.
