# Worktree ownership leases

T3 permits multiple threads to describe and read the same checkout. Reading a
thread, subscribing to it, reviewing a diff, or changing non-execution metadata
does not claim that checkout. Before T3 starts work that can mutate files, one
thread must own an exclusive lease for the checkout.

## Authority and identity

The `worktree_ownership_leases` SQLite table is authoritative. Thread
projections remain descriptive and may intentionally contain duplicate branch
and worktree metadata.

The resource key is the server-canonicalized physical checkout root. A worktree
thread uses its worktree path; a local thread uses its project's workspace root.
The branch is recorded for diagnostics but is not identity: different branches
in one checkout conflict, while the same branch name in independent clones does
not.

Each acquisition has a generated `lease_id`. The owner identity combines the
thread ID with the server-generated ID of its latest `thread.created` event, so
deleting and recreating a thread with the same ID or client timestamp cannot
inherit uncertain ownership.
Renewal and release compare the resource path, owner identity, and lease
generation so a stale generation cannot renew or release its successor.

## Lifecycle

- **Acquire:** `thread.turn.start`, checkpoint restore, bootstrap setup work,
  and terminal open or restart paths acquire before their first native
  mutation. Reacquisition by the same thread incarnation rotates the
  generation and is recovery; a different owner gets a typed conflict before
  its command records a message or turn or starts a shell. Terminal paths are
  canonicalized and must remain inside the projected checkout they acquire.
- **Renew:** the owning server renews once per minute. `expires_at_ms` is a
  health signal, not permission for another thread to take over. T3 cannot
  fence arbitrary filesystem writers, so timeout-only takeover is unsafe.
- **Recover:** the same thread incarnation may reacquire an expired or current
  lease. Startup reacquires for verified live provider sessions and resumes
  cleanup for leases whose owner no longer exists. A different owner remains
  blocked until the old owner's provider and terminals have been
  authoritatively stopped and the exact lease generation is released.
- **Release:** thread deletion releases only after provider and owned terminal
  cleanup both succeed. Failed or ambiguous cleanup retains the lease, is
  surfaced in server diagnostics, and is retried rather than granting a
  timeout-only takeover.

The engine exposes a read-only lease reader for collision diagnostics. It
returns current and stale records; consumers compare `expires_at_ms` with their
own current time when presenting renewal health.

## Boundary

This contract coordinates mutators started by one T3 environment. It does not
claim to fence shells or tools started outside T3, nor does it turn expiry into
proof that an operating-system process stopped. Git-ref coordination across
independent clones would require a separate identity based on Git common-dir
and ref; branch labels alone are deliberately insufficient.

## Verification record

The focused contract covers atomic acquisition, same-incarnation generation
rotation, thread-ID reuse, conflict rejection, stale-generation
renewal/release, checkpoint mutation, startup recovery, and post-cleanup
release. The implementation intentionally leaves ordinary read paths
lease-free.
