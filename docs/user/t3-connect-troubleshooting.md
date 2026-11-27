# T3 Connect troubleshooting

A T3 Connect connection passes through four stages:

1. **This device** reaches T3 Connect over the internet.
2. **T3 Connect** verifies your sign-in and host link.
3. **The host's tunnel** (`cloudflared`) forwards the request to the host.
4. **T3 Code on the host** answers.

When a connection fails, open **Settings → Environments**, select the environment, and choose **Details**—the first failed stage identifies the cause below. On the host itself, run `t3 connect status` or check **Settings → Sharing & access**.

## The host's tunnel is offline

T3 Connect could not reach the host's tunnel (host is asleep/offline, T3 Code isn't running, or outbound port 7844 is blocked).

1. Confirm the host is awake and running T3 Code.
2. Run `t3 connect status` on the host.
3. If the host was offline for 7+ days on an older version, start and update T3 Code on the host to recreate its tunnel.

## Port 7844 is blocked on the host

`cloudflared` connects outbound to Cloudflare on **TCP and UDP port 7844** (normal web traffic still works when 7844 is blocked).

- **Tailscale exit node**: Run `tailscale set --exit-node=` on the host or allow outbound 7844 on the exit node.
- **Work VPN / firewall**: Allow outbound TCP/UDP 7844, or bypass T3 Connect by adding a direct Tailscale route ([Reach one machine several ways](./remote-access.md#reach-one-machine-several-ways)).

## The tunnel works, but T3 Code does not answer

The tunnel is up, but T3 Code didn't respond on `127.0.0.1` (crashed, starting up, port changed, or `T3CODE_HOST` bound only a non-loopback IP).

Restart T3 Code on the host (`t3 service restart` for a background service) and run `t3 connect status`.

## The T3 Connect address does not resolve or answer

The host's T3 Connect address failed DNS, TLS, connection, or timed out.

1. Make sure the host is awake and online.
2. On the host, run `t3 connect status`.
3. If the check fails only on the host, check the host's DNS, VPN, or proxy.

## This device can't reach T3 Connect

The issue is on the client device's network:

1. Check for captive portals or dead Wi-Fi.
2. Temporarily disable VPNs, firewalls, ad blockers, or DNS filters (on iPhone, test turning off Wi-Fi **Connectivity Assist**).
3. Try another network or add a direct Tailscale route ([Reach one machine several ways](./remote-access.md#reach-one-machine-several-ways)).

## Sign-in, account, or link problems

This device's T3 Connect sign-in is not ready yet, or T3 Connect does not have
an active link between your account and the host.

- **The host is linked to a different account.** Sign in to the same T3 Connect
  account on this device and on the host. On the host, run `t3 connect logout`,
  then `t3 connect`.
- **T3 Connect is off on the host, or the host only publishes agent activity.**
  On the host, open **Settings > Sharing & access** and turn **T3 Connect** on, or
  run `t3 connect`.
- **The host was linked, but the server did not apply the link yet.** Restart
  the server on the host: quit and reopen the T3 Code desktop app, run
  `t3 service restart`, or stop `t3 serve` and run it again.
- **This device's sign-in expired, or the host no longer accepts this
  device.** Sign out of T3 Connect and sign in again.
- **This device is still loading its T3 Connect sign-in.** Wait a moment;
  T3 Code retries on its own. If it keeps waiting, reload or restart T3 Code on
  this device.
- **Your account reached its tunnel limit.** Remove an environment you do not
  use from your T3 account's T3 Connect page, or
  [link a paid T3 Chat account](./remote-access.md#more-machines-with-t3-chat)
  for a higher limit. Then restart T3 Code on the host.

## The clock is off

T3 Connect uses short-lived signed proofs. Enable automatic date and time on both the host and client device, then retry.

## The host runs an older T3 Code

Update T3 Code on the host machine ([Updating T3 Code](./updating.md)).

## T3 Connect returned an error

| Error                                                     | Fix                                                                                                                                                                                                                                          |
| --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `environment_link_limit_exceeded` or managed tunnel limit | Remove an unused environment or link a paid T3 Chat account, then restart T3 Code on the host.                                                                                                                                               |
| `auth_invalid` or `invalid_bearer`                        | Run `t3 connect login`. If credentials were revoked, run `t3 connect logout`, then `t3 connect` again. Restart the server after signing in.                                                                                                  |
| Expired or invalid link proof                             | Check the host's date and time, update T3 Code, then restart it.                                                                                                                                                                             |
| HTTP 403 from the host's anonymous address check          | This does not prove a signed-in device cannot connect. Try that device, then use **Details > Copy diagnostics** if it fails. Compare the host's local identity check and external path; a response Ray ID alone does not identify the cause. |
| HTTP 408, 429, or 5xx                                     | Wait a few minutes. T3 Code retries temporary failures on its own.                                                                                                                                                                           |

After you fix a permanent rejection, restart the host's server with
`t3 service restart`. For a foreground server, stop it and run `t3 serve` again.

After fixing a permanent error, restart the host (`t3 service restart` or restart `t3 serve`). If T3 Code stops when SSH disconnects, see [background-service troubleshooting](./background-service.md#troubleshooting).

## Report a problem

Open an issue on [GitHub](https://github.com/pingdotgg/t3code/issues) with:

- **Copy diagnostics** from the environment's **Details**
- Output of `t3 connect status` on the host
- Trace ID (if shown)
- `~/.t3/userdata/logs/relay-client.ndjson` (tunnel tokens are already stripped)
