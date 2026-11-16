# Linked threads

Open **Linked** in a thread to find related conversations and continue work
without losing its origin. The section starts collapsed. It brings parents,
delegated tasks, forks, continuations and peers together; forks remain ordinary
forks with their existing merge-back behavior.

Related threads are grouped under **Continuations**, **Parents**,
**Delegated tasks**, **Children**, and **Siblings & peers**. A thread launched
from another appears as a child of its launcher, and threads with the same
launcher can appear as siblings. Delegated tasks keep their task controls and
completed agents remain under **Previous agents**. Navigation links do not
turn ordinary conversations into subagents, transfer task ownership, or change
permissions or visibility.

## Continue or connect work

Ordinary threads launched by an agent gain a navigation link automatically when
the running server supports linked threads. For existing conversations, use
**Link thread…** in the Linked menu and choose **Continued in**, **Continues
from**, or **Peer**. Choose a continuation when one conversation takes work
forward from another; use a peer for related work without a direction.

A continuation stays **offered** until the receiving thread accepts it. In the
receiving thread, choose **Accept** on the continuation, or ask its agent to
accept when it takes on the work. Sending or delivering a handoff does not
itself accept responsibility. The agent's `t3_thread_handoff` tool can send a
handoff to an existing thread or start a new one with that handoff.

Choose **Unlink** on a relationship to remove its navigation link. This does
not delete either conversation, cancel a task, or undo accepted work. Existing
parent, fork and delegated-task relationships keep their own behavior.

## Copy a portable handoff

Choose **Copy handoff text…** in the Linked menu and enter the goal, scope,
status and sources. Paste the result into the conversation that should receive
the work. It includes the origin title, full thread ID, environment name and
ID, and a return route so the receiver can identify the source and report back.
Copying text alone sends nothing and records no continuation; the receiving
thread can accept using the instruction in the handoff.

This is separate from [provider context transfer](./portable-handoffs.md),
which carries conversation history when changing or restarting providers.
Keep the handoff's origin and return route when relaying it manually.

## Work across environments

Remote entries show their environment. Open them when that environment is
connected; offline entries remain visible but cannot be opened. A one-sided
link means the relationship could not be recorded on the other side. A `+`
beside the count or a footer warning means more links may exist than the
current connected environments can show.

A thread link grants no connection or access. Peer connections are directional:
the receiver must verify its own route back to the origin, its access and the
origin's message policy before sending a native reply. A route from your laptop
to a server does not establish a route from that server back to your laptop;
the reverse route remains unverified until checked from the receiver. If a
reply route is unavailable or unverified, give the report to the user to relay.
See [remote access](./remote-access.md) for connecting to an environment.

Availability depends on the running client and servers. Older servers show
existing lineage without the new linking controls, and a launch through an
older peer may succeed without recording a link. Updating source alone does
not add these capabilities to an already-running server.
