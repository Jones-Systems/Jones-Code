# Preview browser hosts

Ordinary Preview works without a companion. By default, its browser runs on
the environment's server, including when that server is an Apple Silicon Mac
Mini. A desktop running alongside the server may provide native rendering.
Existing saved host selections still apply.

Use the optional Jones Preview Companion when you want a different, independently
paired computer to render Preview tabs while your projects, threads and provider
sessions stay on their existing server. This browser-only desktop app uses its
own browser profile and application state. It does not start a local server or
provider processes.

Open the companion on the computer that will run the browser. Add and
authenticate the existing server connection in **Settings → Connections**;
do not copy credentials or browser profiles from another app. In
**Settings → Integrations → Browser hosts**, choose that saved environment,
name the host and enable its connection.

From the existing dashboard, select the registered host in Browser hosts to
set the environment default. Preview's host control can override the default
for a thread. These choices affect **new tabs**. Existing tabs remain bound to
their original host, including after a disconnect. To use a different host,
explicitly select it and open a new tab; login sessions do not move with it.

When the selected host is offline, its tabs stay unavailable. They do not move
to the server or laptop automatically. Reconnect the companion, then use
**Try again** to reconnect the tab; this does not replay an interrupted action.
An interrupted action can have an unknown result. A page snapshot or browser
status alone may not establish whether it happened. Use evidence from the
original action, including retained recording-operation evidence where available,
before repeating it. If the result remains unknown, leave the action unresolved
and report it; changing hosts or restarting does not establish its outcome.

Browser host selection controls where Preview renders. **Connect with ChatGPT**
in [provider settings](./providers-codex.md#connect-with-chatgpt) signs the Codex
provider in; it does not select a browser host. ChatGPT's workspace connector
provides workspace access, and an explicitly named visible ChatGPT SDK workflow
uses its own browser bridge. That bridge is separate from Preview; follow the
named workflow's prerequisites.

The companion supports plain-text clipboard interaction and server-held
screenshot artifacts. Uploads, downloads, recording, profile clearing and
popup windows are unavailable. Website sign-in must stay in the same tab;
sites requiring popup sign-in are unsupported. Running the browser locally
does not guarantee that a website will accept it.

Direct server connections and supported SSH connections can host a browser.
Relay connections and URLs with a base path are unsupported. Browser-only
mode is fixed in the companion, including while unpaired or disconnected.
Companion updates are installed manually; keep this application separate from
the normal Jones Code or T3 Code installation.
