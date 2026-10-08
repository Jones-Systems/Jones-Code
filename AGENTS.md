# Jones Code

Jones Code is a standalone T3-derived repository. A Node server runs provider
CLIs and serves web, desktop, and mobile clients. Contributions here follow the
local guidance below; upstream product claims and maintainer identities do not
describe this repository.

## Start here

Before source writes, verify the repository, physical worktree, branch, HEAD,
status, task scope, and Git owner against the assignment. Use the declared
non-main task branch. Stop on `main` or `master`, detached HEAD, stale identity,
unowned changes, overlapping write scope, or an unknown prior effect. Preserve
unrelated work. One owner controls Git mutations in each physical worktree.

Read [contributor guidance](docs/operations/contributor-guidance.md) when a
contribution touches behavior, tests, documentation, or a pull request. It owns
coverage, implementation conventions, verification, documentation, and PR
practice. Read [development](docs/operations/development.md) before setup,
starting a development process, using test data, or sharing a development server.

Source changes do not update an installed application, adopt a runtime, publish
a release, or authorize deployment. Report source and operational results
separately.

## Jones-owned changes

Before adding or changing Jones-specific behavior, follow the
[extraction rules](docs/operations/contributor-guidance.md#keep-jones-changes-separate-from-upstream).
Keep Jones logic and tests in Jones-owned modules, with small recorded hooks
into T3-owned files. This reduces recurring conflicts when importing T3 Code;
it does not eliminate compatibility work. Explain necessary shared-file edits.

## Boundaries that apply throughout

- Execution belongs to the server environment that owns the workspace:
  its filesystem, provider credentials, processes, and state. A remote client
  must not substitute its own machine's resources. The desktop renderer follows
  this boundary even when the desktop app also hosts a server.
- Clients and servers upgrade independently. Preserve wire compatibility and
  capability negotiation in `packages/contracts`, and preserve decoding and
  replay of persisted events.
- For an affected behavior, check applicable clients, provider adapters, MCP
  and scheduled-task access, entry points, reverse states, and connection modes.
  Use the contribution checklist before calling the change complete.
- Never kill by name or path pattern: no `pkill -f`, `pgrep | kill`, or PID
  selection from a matching process name, path, or worktree string. Stop only
  a process this task owns, using its captured spawn PID or its confirmed
  listening-port owner after verifying the process's worktree.
- Never start a development server against live `~/.t3/userdata`, open that
  state read-write, or clean it up. Reading or copying private state requires
  exact authorization; its presence on this machine is not permission.
- M Jones grants standing browser and computer-use permission for behavioral
  testing of owner-built local apps, including Jones Code and T3 Code. Do not
  ask again per task; follow [local app testing permission](docs/operations/contributor-guidance.md#local-app-testing-permission)
  for isolated test state and protected-effect boundaries.
- Keep `VITE_HTTP_URL` and `VITE_WS_URL` unset for browser development.
  The dev runner owns desktop's loopback configuration.
- Do not commit implementation plans, research notes, or agent scratch.
  Keep temporary working material outside the worktree with an explicit
  lifetime and cleanup owner.
- Use focused checks. Do not run repository-wide checks unless the owner asks.
  Do not invoke automated PR review services or reviewer bots for Jones-Systems
  pull requests.
- These instructions do not authorize credential access, shared route changes,
  process termination outside the task, privileged operations, or publication.

## Load architecture guidance when it applies

- For environment ownership, orchestration, contracts, persisted events, or
  checkpoints, read the [architecture overview](docs/internals/overview.md).
  Preserve the pure decider, recorded intent before reactor side effects,
  durable command receipts, and the distinction between provider completion
  and settled checkpoint work.
- For shared domain terminology, read the
  [glossary](docs/internals/glossary.md). A runtime test receipt is distinct
  from a durable command receipt.
- For provider identity or launch/recovery changes, read
  [provider constraints](docs/internals/providers.md).
  Requested configuration is not observed identity; absent evidence stays
  unknown or unavailable.
- For connection, retry, authentication renewal, subscription, or cache changes,
  read [connection runtime](docs/internals/connection-runtime.md). Preserve one
  transport retry owner and distinguish transport health from data freshness.
- Before adding server code, read
  [Effect services](docs/internals/effect-services.md). Keep capabilities in
  domain services and transports thin. Use `.repos/effect-smol/LLMS.md` for
  Effect library guidance when needed; vendored references are read-only.

## Pull request merge qualification

Ordinary PR merges require passing applicable checks on the current head.
For an atomically selected, contiguous complete stack, follow the standing
[atomic stack qualification policy](docs/operations/contributor-guidance.md#atomic-stack-qualification).
A qualified current top may cover lower failures repaired higher in that group;
record their repair evidence instead of making every intermediate layer green
or requesting a new exception for each eligible stack. Broken prefixes cannot
merge independently. Merge authority, required reviews and thread resolution
still apply; the policy does not authorize bypasses or ruleset changes.

## Test data

Follow [test data](docs/operations/development.md#test-data) for synthetic
fixtures and explicitly authorized, isolated snapshots.

## Documentation

Follow the [documentation rules](docs/operations/contributor-guidance.md#documentation)
before changing or adding documentation.
