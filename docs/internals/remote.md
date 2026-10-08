# Remote architecture

Each connection joins a client to one environment over HTTP and WebSocket. The
environment owns providers, execution, files, and durable state. Direct access,
Tailscale, SSH, and T3 Connect change how the client reaches that server; they do
not introduce another execution model. See
[remote access](../user/remote-access.md) for setup.

## Identity is independent of the route

An environment keeps its ID across server restarts and endpoint changes. Saved
connections are local to a client profile; the server's identity and state are
not. A repository identity can correlate clones across environments, but never
routes work between them. A project and its threads belong to one environment.
The canonical key follows the `upstream` remote when one exists, so pull request
features target the repository a fork tracks. A fork also reports its own
`origin`, and clients group and label by that, so a fork never collapses into a
checkout of its upstream.

[Environment ID initialization](../../apps/server/src/environment/ServerEnvironment.ts)
must publish a complete ID atomically. Repair of an empty ID file retains a
recovery file so concurrent or delayed initializers choose the same winner.
Removing that recovery state as ordinary temporary-file cleanup can change the
identity underneath an already-running server.

Advertised endpoints are reachability hints. Only the connecting device can
prove that a route works. In particular, a host's loopback address refers to a
different machine when another device opens it. Endpoint selection must not
silently fall back to loopback when a shareable endpoint is unavailable.

A saved environment holds an ordered list of routes, and the
[driver](../../packages/client-runtime/src/connection/driver.ts) connects over
the first that works. Each direct route is first checked with the public
descriptor, so a saved LAN address that a different machine answers on another
network receives no credential. That check is not proof of a working route:
when every route stays silent, each is still tried. A route that fails to
connect, including a blocked one such as a signed-out T3 Connect, moves on to
the next; only an incompatible server stops the walk, because it is the same
server on every route. While connected over a later route the
[supervisor](../../packages/client-runtime/src/connection/supervisor.ts)
preflights the earlier ones and replaces the session when one would connect.
Preflight includes authorization so a route that answers but rejects this
client never costs a working session; a route that still fails afterwards is
held back for a cooldown so a flaky network cannot bounce the connection.

A connected server reports the LAN and tailnet addresses it is bound to, and the
client saves them as learned routes. A learned route reuses the credential of
the route it was learned over: the T3 Connect access token, which is not bound
to an origin because each DPoP proof names the URL it signs, or the paired
bearer token. Learned routes the server stops reporting are dropped, which is
how a changed LAN address replaces the old one; routes the user saved are never
touched. The reported addresses are hints like any advertised endpoint, so a
learned route still has to answer as this environment before it is used.

GitHub routing trust covers the whole route list. Adding or changing a route
revokes it; reordering does not, because the same addresses remain trusted.

## Hosted web is a client

The hosted web app stores its connection catalog in the browser and connects
directly to each environment. It does not proxy traffic or hold server-side
pairing state. Hosting the UI over HTTPS therefore cannot make a plain HTTP LAN
backend accessible from that browser context.

A [hosted pairing URL](../../apps/web/src/hostedPairing.ts) identifies the backend
in its query and carries the pairing secret in its fragment. Fragments stay out
of requests to the hosted origin. The browser exchanges the secret with the
environment and strips it from its history. Moving the token into a query
parameter would disclose it to the wrong origin.

## Access and process ownership are different

Tailscale supplies an endpoint for ordinary pairing, so it needs no separate
environment type. Authentication remains the environment's responsibility for
every route. See [environment authentication](./environment-auth.md) and the
[T3 Connect trust boundary](./t3-connect.md).

SSH can launch a server as well as forward a port. Desktop main owns that
transport lifecycle because it can spawn SSH and handle authentication prompts.
The renderer uses the forwarded endpoint through the shared connection runtime.
[SSH cleanup](../../packages/ssh/src/tunnel.ts) releases only the local forward
on disconnect, connection removal, or desktop shutdown. The host server and
provider processes remain running, including a server initially launched over
SSH. Reconnection restores the forward and reuses the host runtime before
opening the application transport.

Older desktop clients retain the previous cleanup behavior and can stop servers
they launched over SSH. Persistence on this path requires compatible updated
clients; a server update alone cannot change an older client's cleanup.

Remote servers can outlive several client releases. Clients must use advertised
capabilities and handle their absence, rather than assume their own version
describes the server. Reconnection must not replace a running host server to
match the client's version. Host service restart, update, and uninstall are
separate operations that can interrupt active work; see the
[update protocol](./server-updates.md) and
[background service](../user/background-service.md).

For persistent Jones hosts, use a host-owned Jones service on a stable loopback
port, reached through the existing direct pairing protocol over Tailscale HTTPS.
The systemd unit `jones-code.service` and macOS LaunchAgent
`com.jones-systems.jones-code.service` are separate from upstream T3 identities.
Local artifact staging verifies Jones repository/source provenance as well as
archive and extracted-entry checksums. The default upstream runtime download is
refused; an explicit network mirror permits checksum-based downloads but does
not establish Jones provenance. Cached reuse requires matching local provenance.
See the [persistent-host rollout guide](../operations/persistent-hosts.md).

The route and server have different lifecycle owners. Host setup records the
ports and disables server-owned Serve; guarded `t3 pair --tailscale` creates or
reuses the persistent tailscaled mapping. A Jones server that finds a pre-existing
exact mapping does not adopt it for shutdown cleanup. Jones writers refuse
conflicting or unknown ownership, except the existing development repoint proven
by a same-environment probe. Guarded removal acts only on the exact recorded
claim. CLI inspection and writes have no compare-and-swap, so a concurrent route
change remains possible and post-write readback detects it after the fact.
Upstream binaries and `dev-share` do not use this guard: reserve a separate
Serve port, distinct from 443 and their configured ports, and inspect drift.
A failed endpoint probe alone never proves a mapping is free to overwrite.

Desktop, web, and mobile still use ordinary pairing, saved connections, and the
existing reconnect supervisor; there is no new client protocol. The SSH fallback
retains its default `~/.t3` discovery and Node-free helper download from
`pingdotgg/t3code`. It can install that helper before discovering a running
server, without replacing the server. It does not provision a Jones service or
discover a custom Jones base directory. Prefer direct private pairing for that
base and do not start a second foreground or SSH-launched server against it.

Host availability remains separate from client connectivity. Linux needs linger
for startup at boot and survival after logout. The macOS user LaunchAgent starts
at GUI login; a pre-login boot service needs a separate operational choice.
A laptop disconnect does not stop host-owned providers, but host sleep, logout,
reboot, and service restart can interrupt them. Source support does not prove
that a host has adopted the runtime or that a peer can reach its endpoint.

### Desktop without a local environment

Desktop normally launches its own primary server, but the desktop setting `localEnvironmentEnabled`
(`apps/desktop/src/settings/DesktopAppSettings.ts`) turns that off. Changing it relaunches the app;
no local state is deleted. On the next start the main process skips port selection, server exposure,
and the primary and WSL backends, and opens the window right away. The renderer sees this through
`desktopBridge.getLocalEnvironmentEnabled()`: `readPrimaryEnvironmentTarget` returns null, so primary
auth and platform-managed discovery are skipped and only saved environments (pairing, relay, SSH)
connect. This is possible because the desktop renderer is not served by the backend: the `t3code://`
scheme serves the bundled client from disk (Vite in development) and API traffic always goes to the
environment's own URL.
