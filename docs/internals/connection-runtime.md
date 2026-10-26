# Connection runtime

Web, the desktop renderer, and mobile share one connection owner per environment
in `packages/client-runtime`. Platform code supplies storage, credentials, network
signals, and application lifecycle events. React views consume the runtime.
Keeping retries and session lifetime here prevents competing reconnect loops when
several views need the same environment.

## One transport retry owner

The [supervisor](../../packages/client-runtime/src/connection/supervisor.ts) owns
transport retry policy; resolving an endpoint and opening an RPC session are single
attempts. Transient failures retry with jittered exponential backoff, capped at
five minutes, that resets only after a connection stays up. Without jitter, every
client of a restarted server reconnects in the same second; with a short cap, a
client that can never connect retries all day. Offline states and authentication
failures wait for a wakeup instead of spending attempts on unchanged conditions.

Foregrounding, an explicit retry, and an offline report probe the established
session, and only a failed probe reconnects. Offline reports are often wrong, for
example for a loopback server. A long mobile background suspension is the one
exception: it replaces the session at once, because the OS can kill a socket
without reporting closure, and a probe would hold a dead socket in "Resuming"
until it times out. That fresh attempt runs even while the network reports
offline. Foregrounding also wakes a pending retry immediately and
leaves an ordinary in-flight attempt alone.

The [registry](../../packages/client-runtime/src/connection/registry.ts) scopes
connections by environment. An involuntary disconnect retains the registration
and cached data. Explicit removal closes the scope and clears credentials,
projections, and platform-owned state such as drafts. Cloud-account changes apply
to relay registrations; they must not discard directly paired environments.

## HTTP authorization

RPC sessions authenticate at socket upgrade, while HTTP requests need current
credentials from the
[authorization service](../../packages/client-runtime/src/authorization/service.ts).
Replacing a healthy socket for HTTP renewal would interrupt conversations and
change the transport generation without a transport failure. Credential expiry
does not close the socket, and refresh failure belongs to the HTTP operation.

Session listings must retain unrevoked connected sessions after credential expiry
so an open connection does not disappear from connection management. This does
not extend the credential's lifetime. New HTTP requests and socket upgrades still
require valid credentials.

## Transport health and data freshness are separate

A socket opening is insufficient evidence that the environment is usable. The
[RPC session](../../packages/client-runtime/src/rpc/session.ts) waits for the
initial server configuration before becoming ready. Shell and thread data then
have their own synchronization state. A failed shell subscription can coexist
with a healthy connection; labeling that state "reconnecting" promises a
transport retry that will never happen.

Cached projections remain readable offline. They must neither imply a live
connection nor overwrite newer live data during a reconnect. Loading and
resuming snapshots belongs to the shared state services, so every view agrees
on which data is current.

Shell snapshots can leave rows out when the client asks. Servers that advertise
`shellThreadFamilies` leave out idle subagent children: a child stays while it
has a live or queued run, a pending request or background work, or a subagent
child of its own, so badge and working walks never meet a gap. Servers that also
advertise `shellOlderSettled` keep only the 25 newest settled top-level threads
(and any tied with the 25th, pinned, snoozed, live, or above a kept child); the
snapshot's `olderSettled` names that cut and counts the rest per project. Left-out
rows load on demand and merge behind shell rows
([thread families](../../packages/client-runtime/src/state/threadFamilies.ts)):
the family a thread view or a queued message mounts holds every active thread
sharing the thread's root, so it also brings back an old settled thread opened by
link; the Settled shelf reads pages below the cut; searches return the rows of
older matches. A missing shell row therefore does not prove a thread is gone:
code that deletes data or a worktree on absence must check the family or the
server first.

The Settled count is local Settled rows, minus local rows below the cut, plus the
server's counts
([older settled threads](../../packages/client-runtime/src/state/olderSettledThreads.ts)).
The counts are re-read when a removal or an old row's arrival or change could
move them, so archiving any thread costs one small read. A shelf lists an older
row only above the highest page frontier of the environments that can still
load, so the merged list stays a correct prefix while a page loads. An
environment with no snapshot this session, or a failed page, holds no one back.
On-demand reads wait for the session's first authoritative snapshot
(`generation > 0`), and each is keyed by generation. A resume that skips that
snapshot must still bump `generation` once verified, and may reuse a cached
snapshot only when its omissions match what the session asks for.

[Thread detail](../../packages/client-runtime/src/state/threads.ts) separates
subscription lifetime from cache lifetime. Mounted consumers share one live
stream, which stops when the last consumer unmounts; hidden mounted routes still
count. A registry-local cache retains state and its replay cursor for five idle
minutes so back navigation can resume without another snapshot download.

The desktop app adds one consumer: a
[keep-alive](../../apps/web/src/state/threads.ts) mounts every thread whose
session is starting or running, in each enabled environment. Opening a running
thread then needs no replay. The shell and detail streams are independent, so
the shell can report a stop before the detail loads or catches up. A stopped
thread stays mounted until its own stream is live and shows the stop, and the
stream then closes and saves the settled state.
Web and mobile do not keep threads alive.

Retain state and cursor together only after an update finishes. Cancellation must
not advance the cached cursor beyond the applied data, and an old scope must not
overwrite its successor's cache. Preserve pagination data on reuse, but clear
canceled loading state.

The [RPC boundary](../../packages/client-runtime/src/rpc/client.ts) resolves
requests against the current session at execution time. Durable subscriptions
follow replacement sessions. After a transport failure they wait for the
supervisor; an expected domain failure may resubscribe on the same healthy
session. Reconnection does not automatically replay mutations, whose retry and
idempotency rules belong to the operation.

## Legacy thread corpus reads

The [thread corpus capability](../../packages/contracts/src/threadCorpusProtocol.ts)
requires these legacy compatibility semantics:

Required legacy read semantics for compatible shell and thread detail endpoints.
Advertise only after the receiving server proves these behaviors; the DTO
schemas alone do not establish runtime compatibility.
The shell carries global snapshotSequence and updatedAt; projects carry id,
workspaceRoot and optional nullable repositoryIdentity. Threads join by projectId
and carry nullable worktreePath/branch, archivedAt/settledAt, latestUserMessageAt,
updatedAt and nullable latestTurn/session. Optional lifecycle fields retain their
existing schema defaults; missing activity timestamps do not prove inactivity.

Placement is observed worktreePath, or project workspaceRoot when it is null.
Neither the path nor repositoryIdentity verifies cwd, enrollment or access grants.
This advertisement grants no dispatch, creation or provider quota capability.

Detail pagination is opt-in: turnLimit is a positive integer counting user-anchored
turns. Associated fan-out rides along, subject to the native 150 raw-turn page cap.
Without turnLimit the full snapshot has no page metadata. With it, page carries
hasMore, nullable opaque exclusive beforeCursor, snapshotSequence and optional
threadSequence. A null cursor marks the oldest page; an absent threadSequence is
unknown. Native malformed or foreign-thread cursors reload the first page, so a
consumer must check progression instead of assuming each response advances.

Detail messages carry string text without a native character limit or truncation
flag. turnLimit bounds turns, not bytes; consumer text/response limits must report
their own incomplete coverage. Page snapshotSequence mirrors the enclosing
snapshot; threadSequence is the per-thread watermark needed before merging pages.

## Foreground requests and shared capacity

Opening a thread, loading its messages, sending, and stopping are the requests a
user waits on. Background work runs in the same server: pull-request discovery
and settlement sweeps, VCS status polling, source-control discovery, and storage
cleanup. Client activity leases gate some of it, such as VCS status polling,
but it has no general foreground priority. These resources are shared today:

- One WebSocket per environment carries every RPC call and subscription in
  order. Cold thread snapshots and history pages normally use HTTP. The server
  runs RPC requests concurrently; orchestration commands serialize per thread.
- The SQLite [writer](../../packages/shared/src/nodeSqliteClient.ts) is one
  synchronous connection on the server's event loop. Each statement on it blocks
  the loop, socket frames included, while it executes, and a transaction holds
  the writer until it ends, so other writer work, reads included, queues behind
  it. Outside a write transaction, only work wrapped in `NodeSqliteClient.readOnly`
  uses the single reader worker, which also runs one transaction at a time.
  File-backed server databases require that reader for marked reads: an
  unavailable reader fails the read instead of running it on the serving loop.
  A startup timeout retires the worker; later demand retries after termination
  and a bounded cooldown. Other SQLite consumers retain their default writer
  fallback, and reads inside an existing write transaction retain that writer.
  Organization metadata listing marks its complete snapshot transaction, so
  its rows and sequence watermark come from the same committed reader snapshot.
  [Foreground reads and the SQLite writer](./performance-regressions.md#foreground-reads-and-the-sqlite-writer)
  lists the foreground reads routed there.
- Git subprocesses take permits from process-wide FIFO pools: one in
  [GitVcsDriverCore](../../apps/server/src/vcs/GitVcsDriverCore.ts) for Git
  manager, status, and pull-request work, and one in
  [VcsProcess](../../apps/server/src/vcs/VcsProcess.ts) for VCS driver work,
  including the checkpoint baseline a turn waits for before its provider starts.
  Automatic PR discovery and its fresh confirmation take a Jones admission
  permit before each eligible GitVcsDriverCore permit, limiting that discovery
  work to four of the eight shared permits. Commands with a null timeout or a
  timeout over 30 seconds retain their existing bypass of both limits. Other
  callers can fill the remaining permits; no capacity is reserved for a specific
  foreground request, and queue waits have no deadline. A foreground caller
  joining the same cached lookup retains that lookup's original classification.
  Settlement, status polling and cleanup retain their existing admission.
  VcsProcess has no foreground reservation; source-control discovery probes use
  its separate, smaller pool.
- The orchestration [effect worker](../../apps/server/src/orchestration-v2/EffectWorker.ts)
  shares a few slots among turn start, steer, and checkpoint capture.
- On the client, most thread commands, Send and Stop included, share one serial
  lane per environment and thread. Mutations therefore dispatch in submission
  order, and a slow command delays that thread's next Send. Earlier-history loads
  have their own lane, because a page can take two sequential HTTP requests. VCS
  commands serialize per working directory.

When adding or changing work that uses these resources:

- Isolate at the scope of the resource that actually contends. Do not add a
  global serial lane, or widen a key, for unrelated reads or mutations; separate
  threads, working directories, and environments keep running concurrently.
- Keep dependent mutations ordered under the narrowest key that expresses the
  dependency. Give independent work, such as a history page, its own key so it
  neither waits behind nor delays Send or Stop.
- Keep pure reads off the writer when they do not need its transaction. Each
  move adds queueing on the single reader, so check reader wait before routing
  more work there.
- Bound background subprocess work with its own concurrency limit, a deadline,
  and coalescing of repeated requests for a target that is still queued, so a
  burst cannot hold the permits a foreground operation needs.
- A timeout that retries, or falls back to another transport, still reaches the
  same server queues; it adds load rather than avoiding the contention.
- Keep batching that reduces total work, such as the pull-request sweep's
  width, which lets GitHub branch lookups share one GraphQL request. Lowering it
  to reduce contention loses that saving. Do not add batching windows or delays
  to foreground paths.
- Measure queue wait separately from work time before and after a change, and
  record the build, host, environment, and workload. An instantaneous CPU sample
  does not rule out event-loop stalls.
  [Observability](../operations/observability.md#separate-lifetimes-from-request-latency)
  shows which spans separate them. Without that evidence, describe a change by
  what it removes from the shared path, not as a latency gain.
