# Contributor guidance

This page owns contribution practice for Jones Code. Read it when changing
behavior, tests, documentation, or a pull request. The root
[AGENTS.md](../../AGENTS.md) owns startup boundaries;
[development](./development.md) owns setup and operating procedures.

Prefer the smallest coherent model that makes correct behavior unsurprising.
Understand the constraint before preserving existing complexity or adding new
machinery. Keep scope aligned with the owner's task. If a task conflicts with
applicable guidance, identify the conflict and obtain the owner's direction.

## Keep Jones changes separate from upstream

Jones Code regularly imports changes from T3 Code. Adding Jones logic directly
to upstream files makes those imports repeatedly conflict and obscures which
side owns the behavior. Use the extracted structure by default for new features
and repairs: keep Jones implementations and their tests together in Jones-owned
modules, and keep upstream integration points small. This reduces edit overlap;
upstream API changes can still require adapter and compatibility repairs.

| Jones-owned code                                            | Default location                     |
| ----------------------------------------------------------- | ------------------------------------ |
| Wire schemas and small derived contract helpers             | `packages/contracts/src/jones/`      |
| Shared client state, commands and capability handling       | `packages/client-runtime/src/jones/` |
| Server services, upstream adapters and registration bundles | `apps/server/src/jones/`             |
| Jones persistence and migration loader                      | `apps/server/src/jones/persistence/` |
| Web components, views and controllers                       | `apps/web/src/jones/`                |
| Mobile features                                             | `apps/mobile/src/jones/`             |
| Desktop shell and IPC features                              | `apps/desktop/src/jones/`            |
| Boundary checks and Jones development tools                 | `scripts/jones/`                     |

Use the existing feature owner within these locations before creating another
module. Keep coherent existing Jones-owned feature directories where they are
when a move would add churn without an integration benefit. Shared utilities
and additional packages follow the same ownership principle; do not duplicate
code merely to fit this table.

- **Hooks and adapters.** Upstream-owned files should register or call a narrow,
  typed Jones interface. Put Jones business logic, retries and lifecycle work
  behind that interface, rather than growing the hook. Jones modules may use
  specific upstream APIs through adapters. Keep contract modules independent
  of runtime services and avoid barrel cycles.
- **Tests.** Put Jones behavior tests beside their Jones implementation and use
  the existing runner's discovery or a small explicit registration. Preserve
  upstream tests and assertions. Keep a Jones case in an upstream test only
  when the integration harness requires it, and record that exception. Keep
  meaningful application integration coverage for both sides.
- **Boundary accounting.** Update the affected feature fragment under
  `scripts/jones/boundary/fragments/`, and the applicable
  [inventory](../../scripts/jones/boundary/inventory.json),
  [feature coverage](../../scripts/jones/boundary/features.json) and
  [check mapping](../../scripts/jones/boundary/checks.json).
  Record hooks, accepted shared edits, imports from upstream into Jones,
  test exceptions and removed paths. Run the following from the repository root
  on a supported Node version:

  ```sh
  node scripts/jones/check-boundary.ts --strict
  ```

  Do not weaken
  its path, ancestry, hash or resolution checks to hide new divergence.

- **Stored state.** Preserve wire names, persisted fields, event decoding,
  migration identities and guards. Keep Jones migrations on their independent
  loader and tracking ledger; source extraction must not rewrite stored history.
  Database changes follow the [Jones database boundary](#jones-database-boundary).
- **Practical exceptions.** A small direct shared-file edit is acceptable when
  isolating it would require disproportionate complexity or duplicated upstream
  machinery. State the reason and classify the edit. Do not build a generic
  plugin framework or perform unrelated moves to eliminate every conflict.

An upstream import and Jones restoration are separate concerns. Follow
[the upstream sync procedure](./upstream-sync.md#upstream-sync-procedure):
import upstream in the bottom PR, restore Jones behavior in stacked follow-ons,
and qualify the complete stack before merging any layer. A similar upstream
feature is not an accepted replacement without behavior evidence and M Jones's
explicit decision.

### Jones database boundary

T3 Code can open a database that Jones Code has used, and upstream migrations
and maintenance assume only upstream schema. Upstream owns every table, column,
index, trigger and view created by `apps/server/src/persistence/Migrations`,
and its `effect_sql_migrations` ledger. It may prune, delete, drop or rebuild
those objects without knowing about Jones.

- **No upstream schema changes.** Jones migrations must not alter, drop or
  rebuild upstream tables, or add columns, indexes, triggers or views to them.
  They must not write the upstream ledger or upstream rows.
- **Jones-owned tables.** Store Jones state only in tables created through the
  Jones loader and recorded in `jones_sql_migrations`. Name new tables, indexes
  and triggers with a `jones_` prefix. Released unprefixed names such as
  `native_creation_*` and `workstreams_native_*` keep their names.
- **Sparse extension rows.** To attach Jones metadata to an upstream record,
  key a Jones table by that record's stable ID, such as a thread, command,
  effect or session ID. Write a row only when Jones has something to record,
  in the transaction of the Jones operation that produces it. Do not mirror
  upstream rows eagerly, copy upstream payloads, or keep rows in step with
  upstream writes through triggers.
- **No references to upstream tables.** Do not declare foreign keys from Jones
  tables to upstream tables. With foreign keys enabled, such a key makes
  upstream deletes, pruning and table rebuilds fail. Verify the upstream row in
  the transaction that writes the Jones row, and treat a missing upstream row
  as absent when reading. References between Jones tables are allowed.
- **Upstream payloads and hot paths.** Do not add Jones fields to upstream JSON
  payloads or columns. Read upstream rows from Jones-owned queries through
  existing upstream keys and indexes. Do not add joins, scans or schema checks
  to upstream per-event or per-command paths; a necessary hook does a
  primary-key lookup and is recorded as a hook.
- **Environment identity.** Jones rows are meaningful only beside the upstream
  rows of the same database. Do not copy or restore Jones tables separately
  from the upstream data they describe.
- **Released history.** Never renumber, rename or delete a released Jones
  migration. When one breaks these rules, keep its identity, change its effect
  only if no stored data depends on it, and repair existing databases with a
  new data-preserving Jones migration. Removing upstream residue that released
  migrations left behind, such as `runtime_identity_json` on the V1 thread
  session projection, needs a separately authorized operation.

[`UpstreamIsolation.test.ts`](../../apps/server/src/jones/persistence/UpstreamIsolation.test.ts)
checks the schema rules against the complete Jones manifest. Some earlier
integrations predate these rules, for example the native-creation reference in
effect outbox payloads and Jones table checks in the event sink. They are not
precedent for new work.

## Coverage before completion

The common failure is a change that works on the path tested but is missing
from another supported path. For the affected behavior, state which entries
apply and how they were covered:

- **Entry points.** Chat, Settings, the command palette, and keybindings may
  reach the same capability. Fixing one entry point may leave the others broken.
- **Clients.** Cover applicable hosted and locally served web clients, desktop,
  and mobile. Desktop wraps the web renderer and adds Electron shell/IPC;
  mobile uses React Native and separate navigation. Shared client behavior
  belongs in `packages/client-runtime`. Consider the marketing app when the
  task affects its shared dependencies or behavior.
- **Providers.** Codex, Claude Code, Cursor, Grok, OpenCode, and Antigravity
  have separate adapters. For provider-shaped behavior, make an explicit
  decision for each applicable adapter, including unsupported cases.
- **Agents and scheduled tasks.** Consider whether agents should reach the
  capability through MCP and whether scheduled tasks or the CLI use the same
  behavior. Put the capability in a service method so transports can share it.
- **Contracts and stored history.** Update the applicable schema and consumers
  in `packages/contracts`, server, web, mobile, and desktop. Independently
  upgraded clients and servers need compatible behavior. Persisted events
  must remain replayable, including history written before the change.
- **Reverse states.** Add the way out and the way to see the result.
  Snooze needs unsnooze; close needs reopen.
- **Connection modes.** Consider local, direct remote, relay, and tunnel
  connections, including multiple devices and environments.
- **Documentation.** Correct guidance made inaccurate by the change.
  Apply the documentation rules below before adding text.

## Implementation conventions

Complexity belongs at adapter boundaries. Keep orchestration pure and UI
components focused on presentation and interaction.

Server capabilities are methods on domain services. Extend the existing owner
when possible. WebSocket RPC handlers, HTTP routes, MCP tools, scheduled tasks,
and CLI entry points should decode input, call the service, and map errors.
Filesystem, Git, process work, retries, and rollback belong in the service.
Read [Effect services](../internals/effect-services.md) before adding server code.

Keep contracts in `packages/contracts` limited to schemas and small derived
helpers; do not put heavy runtime logic there. Shared runtime utilities use
`packages/shared` subpath exports rather than a barrel. Shared client logic
belongs in `packages/client-runtime`.

For `apps/web/src/components/ui`, use the component's `variant` or `size`
instead of restyling it through `className`. Add a reusable variant when the
look is a generic concept. Keep feature-specific looks in that feature's
component. Put layout classes such as width, flex, margin, and position on the
parent. `shadcn/no-restyle` enforces this boundary.

Prefer inferred types over unnecessary annotations and avoid `any`.
Comments should explain use and constraints rather than narrate each line.
Move or correct them when the code they describe changes.

Consider performance across the wire and in rendering: oversized subscriptions,
large lists, stale labels, misleading activity indicators, and continuously
repainting animations all affect daily use. Avoid animations that keep
high-refresh displays repainting without useful work.

`.repos/` contains vendored read-only references. Consult their established
patterns; never edit or import from them. When the task includes updating the
matching dependency, use the repository's `vpr sync:repos` workflow for its
reference copy.

Apply scrutiny proportionate to the actual effect. A maintainer-only or
development feature does not justify unrelated security work, and it does
not relax the explicit access and runtime boundaries in AGENTS.md.

## Verification

Use the smallest proof that demonstrates the changed behavior. Run
`vp test run <files>` for affected tests, targeted lint, and the affected
package's typecheck. See [checks](./development.md#checks) for commands.

Do not run `vp check`, `vp run -r test`, `vp run -r typecheck`, or another
repository-wide check unless the owner asks. CI owns the full suite.
When applicable guidance calls for an unused-code check, use the affected
workspace scope rather than expanding to a repository-wide audit.

Backend behavior changes need focused tests of that behavior. Test meaningful
logic or observable outcomes. Do not render components to static markup merely
to assert props or attributes, assert only callback wiring, or mirror the
implementation in the test.

The server is event-sourced. Test asynchronous milestones with typed runtime
receipts and the relevant worker drains, not sleeps or polling. An empty
queue does not prove its current item finished. Runtime receipts are test
signals; production behavior uses durable state and events. Durable command
receipts have a separate role in idempotent dispatch.

### UI evidence

For user-visible changes, select the affected client and retain evidence.
The primary agent exercises the affected flow
once after integration, checks observable results, and retains captures.
A screenshot alone does not prove the interaction worked. Subagents do not
launch their own development servers.

M Jones's [standing local app testing permission](#local-app-testing-permission)
covers browser and native app verification with isolated synthetic test state.
Do not request per-task reconfirmation for covered testing.

Use [test-t3-app](../../.agents/skills/test-t3-app/SKILL.md) and T3's Browser panel
for seeing, clicking, typing, and inspecting the shared web/desktop renderer.
Unavailable Preview tools do not authorize switching to a standalone browser.

Renderer checks do not establish desktop-shell or packaged-build behavior.
When those are affected, record the native platform and packaging coverage
required by the task; unavailable harnesses remain a verification gap.

Use [test-t3-mobile](../../.agents/skills/test-t3-mobile/SKILL.md) for mobile.
For authorized mobile verification, a missing or outdated native client is a
build step. Before starting Metro, run this on the authorized simulator host:

```sh
node scripts/mobile-native-client.ts ensure <ios|android> <device-id>
```

It checks the local Expo fingerprint and builds or installs when needed.
Follow the mobile skill for the complete workflow and host scope.

### Local app testing permission

M Jones authorizes browser and computer use across tasks to test behavior in
owner-built local applications, including Jones Code and T3 Code. Opening the
app, navigating, clicking, typing synthetic inputs, inspecting results, and
retaining test captures need no per-task reconfirmation. Reuse this standing
owner authorization when a testing workflow asks for browser permission.

Bind verification to the task's exact source/build and use isolated, task-owned
synthetic state with a defined lifetime and cleanup owner. Reuse separately
authorized sanitized fixtures only within their approved scope. Use T3's Browser
panel for the shared renderer and the applicable qualified desktop or device
harness for native behavior; preserve the routes and evidence requirements above.

This permission does not authorize external-site or ChatGPT automation,
credential access, private or live application state, production changes,
deployment, shared route changes, sudo, or interaction with unrelated apps.
Those effects retain their existing exact authorization gates. In particular,
use only the task's isolated development pairing token, never the user's token
or a live application credential. Continue covered verification and surface the
exact uncovered effect if the flow requires one.

## Pull requests

Create a PR only when the owner explicitly asks. That request does not
authorize merge, release, deployment, or cleanup.

Use a conventional commit title in plain language, such as
`fix(web): new threads no longer spike CPU`. Describe the problem in a sentence
or two, then explain the fix. End with the model and harness that performed
the work. Keep one concern per PR; split unrelated additions.

UI changes need before/after images; motion or timing changes need a short
video. For each evidence set, record:

- Comparison and candidate revisions; for a dirty tree, its base and patch
  identity.
- Build identity when available, route/platform, viewport, scale, theme,
  fixture or scenario, observable action and readback, and coverage limits.
- Whether before/after shows interaction states within one build or two exact
  source builds.
- The integration revision and constituent heads for a combined preview.

Refresh exact-head claims after the PR head changes. Upload PR evidence to
GitHub; never commit PR-only screenshots or assets such as `.github/pr-assets/`.

Do not invoke automated PR review services or reviewer bots for Jones-Systems
PRs, including `@codex review`, Codex Code Review, or Copilot PR review.
When asked to watch a PR, inspect current-head checks and new comments, validate
reported problems against the source, and follow the task's authority for
disposition and fixes. Stay quiet when nothing changes. Report current-head
results and unresolved findings; absence of a bot review is not a failure.

### Atomic stack qualification

Ordinary PR merges require passing applicable checks on the exact current head.
The standing policy for a complete atomic stack qualifies the composed
result rather than every intermediate layer. It applies only when all of the
following hold:

- The selected group is contiguous and complete: the import or foundational
  change and every repair it needs are included, with the receiving base and
  constituent PR heads recorded. The current top contains all those changes.
- That exact top is qualified for the complete group's upstream behavior and
  Jones parity, with passing applicable checks and full applicable coverage.
  Skipped, unavailable or unrelated checks do not prove coverage. Refresh
  qualification if the top, a constituent head or the receiving base changes.
- The failure-to-repair ledger maps each lower failure to its repair higher in
  the selected group and the passing check on the composed top that proves
  restoration. No unexplained or unrepaired lower failure remains.
- Integration lands the complete qualified group atomically. A broken import
  or intermediate prefix cannot merge independently. A non-atomic queue or
  sequence must qualify every group it actually lands before landing it.

For an eligible group, keep lower CI failures visible as resolved-by-higher
entries in the ledger. Do not add work merely to make already-repaired lower
layers green, and do not request a separate CI exception for each occurrence
of this accepted pattern. If a failure lacks a qualified repair or the group
cannot land atomically, this policy does not cover it.

Normal merge authorization, required reviews, resolved review threads and
receiving-ref/tree readback remain required for the selected group. This
qualification policy grants no merge authority and never permits bypassing
required checks or changing repository rulesets. If repository enforcement
prevents the intended atomic integration, hold that operation and report the
specific constraint.

## Documentation

Most code changes do not need an internal documentation change. Prefer code,
types, and tests for facts they already express.

- `docs/internals/` owns architectural decisions and their reasons,
  constraints spanning components, and implementation traps that are hard
  to discover from source. Before adding a paragraph, ask what a maintainer
  would get wrong without it. If reading the code answers the question,
  leave it out.
- Do not enumerate every feature, field, or method, narrate control flow,
  maintain file catalogs, or append PR summaries. The glossary defines shared
  vocabulary; it is not a feature index.
- Put a local implementation explanation in a nearby code comment. Use an
  internal document when the reasoning crosses boundaries or needs context
  the code cannot carry. Link to source instead of copying it.
- Rewrite or remove a documented decision when it changes. Do not append a
  competing account. A new internal page needs a distinct, durable purpose.
- `docs/user/` helps users accomplish tasks. Give each major feature a concise
  explanation of what it does, how to start, and what is unintuitive. A settings
  path can help; descriptions of every visible button, icon, layout, animation,
  or UI state do not.
- Keep user documentation in the shipped product's voice, without contributor
  tooling or implementation details. Update the relevant feature section when
  usage changes. A UI tweak does not need an entry, and a new control does not
  need its own page.
- `docs/operations/` owns maintainer setup, release, and debugging procedures.
  Instructions for operating an installed server belong in user guides.

## Plans and working material

Do not commit implementation plans, research notes, or agent scratch files.
Keep temporary working material outside the worktree with a known lifetime
and cleanup owner. `.plans/` is ignored only as a legacy tooling safety net;
it is not a documentation destination.

Track active maintainer work in its GitHub issue or project item. External
proposals follow `CONTRIBUTING.md` and belong in Ideas discussions.

The merged PR is the implementation record. Close or update its tracking item
when the work lands; do not preserve a second checklist in this repository.
