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

On the fleet host:

```sh
install -m 644 fleet-host/stagehand-video-hetzner2-tunnel.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now stagehand-video-hetzner2-tunnel
```

## Moving a pipeline here

Never run a pipeline on both runners at once. Do the cutover between runs:

1. `stagehand disable <name>` on the fleet host.
2. Move its state.
3. `stagehand-video add <spec> --enable` here.

## Model credentials

Owner decision, 2026-11-15: use a tunnel. No model credential is stored on hetzner2.

- `agent.model_gateway.upstream` is `127.0.0.1:43299`. The fleet host's
  `stagehand-video-hetzner2-tunnel.service` (`fleet-host/` here) reverse-forwards that port to its
  own `stagehand-gateway-proxy` on `127.0.0.1:43295`, which adds the credential.
- Guests never reach 43299. They hold only per-run tokens for the run gateway on 43298.
- On hetzner2 only root can open 43299. The firewall rejects loopback for the CI runner uids
  (`h2ci_*`, chain `ci`) and for the sandboxd jail uids (chain `guest`).
- If the tunnel is down, agent stages fail at their first model call, and shell stages are
  unaffected.

## Resources

The unit caps the runner and its shell stages at `MemoryMax=8G` and `MemoryHigh=7G`, with no
swap, `CPUQuota=600%`, and CPU and IO weights of 50. These come from the hetzner2 memory budget.
After the first week, check `memory.peak` in the unit's cgroup. If it stays under 5 GiB, lower
the cap to 6 GiB.
