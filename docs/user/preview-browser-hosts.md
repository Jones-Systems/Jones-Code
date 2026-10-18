# Preview browser hosts

A browser host runs Preview tabs on another computer while your projects,
threads and agents stay on their existing server. Jones Preview Companion is
the browser-only desktop app for this purpose. It uses its own browser profile
and application state, and does not start a local server or provider processes.

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
**Try again**. An interrupted agent action can have an unknown result; inspect
the page before issuing a new action.

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
