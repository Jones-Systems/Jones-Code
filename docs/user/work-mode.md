# Work mode

Work mode is an experimental way to refresh idle top-level threads after
55 minutes. These refreshes can consume provider usage. Cache savings are not
guaranteed.

Only existing top-level conversations with an idle, ready provider session are
refreshed. Subagents, forked conversations, settled or archived threads, snoozed
threads, stopped sessions, and threads that are running or waiting for input
are skipped. While on, Work mode retains eligible live sessions past their normal idle
timeout. It never reopens stopped or expired sessions. Turning it off lets
normal idle cleanup resume. Regular conversation activity resets the 55-minute interval.
Each refresh sends `@@@@@` and asks for the same reply. Plain messages consisting
only of `@@@@@` are hidden from the visible conversation; unexpected replies
remain visible.

On web and desktop, use **Work** beside the active-thread filter in the sidebar.
The check mark shows that Work mode is on. The tooltip identifies the primary
environment being changed. Other connected environments are unaffected.

On mobile, open **Settings → Environments**, choose an environment, and turn
**Work mode** on or off in its detail screen. This changes only that environment.

The setting is saved by the environment and survives a client reload. It starts
off by default. Connect the environment and wait for its settings to load before
changing it. Servers that do not support Work mode show an unavailable control.

Turn Work mode off with the same control. If saving fails, reconnect to confirm
the saved state before trying again.
