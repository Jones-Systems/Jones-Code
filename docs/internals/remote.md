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

For persistent Jones hosts, prefer a host service with direct private-network
or Tailscale pairing. The server must be a verified Jones build on the intended
host and base directory. The default SSH archive resolver still downloads from
`pingdotgg/t3code`; changing tunnel lifetime does not provision Jones Code.
Connecting can resolve and install the requested CLI helper archive before
discovering an existing server, without replacing that server's running version.
Service installation also resolves a release archive, so a Jones source CLI
alone does not prove the service runs Jones: use a verified Jones-built pinned
artifact or mirror, or an approved exact Jones launcher. The default SSH
discovery path uses `~/.t3`; use direct private pairing for a custom base
directory, and do not start a second foreground or SSH-launched server for the
same base.

Host availability remains separate from client connectivity. Linux systemd
user services need lingering to survive host logout and start at boot. The
current macOS user LaunchAgent needs the Mac logged in and awake. A laptop
disconnect is not a Mac Mini logout; persistence does not guarantee active work
survives host sleep, logout, or reboot.

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
