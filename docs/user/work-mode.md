# Work mode

Work mode is an experimental way to refresh idle top-level threads after
55 minutes. These refreshes can consume provider usage. Cache savings are not
guaranteed.

Only existing top-level conversations with an idle, ready provider session are
refreshed. Subagents, forked conversations, settled or archived threads, snoozed
threads, stopped sessions, and threads that are running or waiting for input
are skipped. So are threads with an unfinished goal that is not paused, because
a refresh could let the agent resume it; pause, finish, or clear the goal to allow
refreshes. While on, Work mode retains eligible live sessions past their normal idle
timeout. It does not reopen stopped or expired sessions. The one exception is a
planned update with **Continue threads after restarts** on: a thread Work mode was
keeping warm resumes its same conversation for its next refresh. Turning it off lets
normal idle cleanup resume.

An automatic server scheduler checks every five seconds; an agent does not decide
when to refresh. The 55-minute clock starts from the latest recorded model processing
in that provider conversation, including processing user or agent input and tool
results. Merely receiving a message, editing it, completing a run, or reporting
context size does not reset the clock. Missing model-usage evidence is skipped.
A conversation's first refresh must be due and no more than 90 minutes after that
activity. After a successful refresh with model-usage evidence, that conversation
continues on the 55-minute cadence; the initial age limit does not recur after a
restart or toggling Work off and on. A new provider conversation starts fresh.
A refresh without usable usage evidence cannot establish this sequence, and its
start time prevents rapid retries.
Refreshes do not count as activity for automatic settlement, so an idle thread
still settles on its usual schedule, and refreshing stops when it does.
Each refresh sends `@@@@@` and asks for the same reply. Plain messages consisting
only of `@@@@@` are hidden from the visible conversation; unexpected replies
remain visible.

On web and desktop, use **Work** beside the active-thread filter in the sidebar.
The button applies to every connected environment that supports Work mode and has
loaded its settings. If some are on and others are off, clicking turns Work on
for all of them. When all are on, clicking turns them all off. The tooltip lists
each environment's saved state and identifies unavailable environments. Failed
saves name the affected environments; retrying finishes the same on/off action.

Each server saves its own setting and shares changes with clients connected to
that server. Offline environments and environments connected later keep their
saved value; click Work again after connecting them to include them.

On mobile, open **Settings → Environments**, choose an environment, and turn
**Work mode** on or off in its detail screen. This changes only that environment.

The setting is saved by the environment and survives a client reload. It starts
off by default. Connect the environment and wait for its settings to load before
changing it. Servers that do not support Work mode are listed as unavailable and skipped.

Turn Work mode off with the same control. If saving fails, reconnect to confirm
the saved state before trying again.
