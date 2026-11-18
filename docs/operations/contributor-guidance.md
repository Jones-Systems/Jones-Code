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

For an upstream UI conflict with an explicit owner request that existing
direction does not settle, follow [owner UI choices](./upstream-sync.md#owner-choices-for-conflicting-ui-changes)
before selecting what to retain, adopt or blend.

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
  primary-key lookup and is recorded as a hook. A hook that needs an optional
  Jones table checks it through `JonesTablePresence`, which reads the schema
  only until it finds the table.
- **Environment identity.** Jones rows are meaningful only beside the upstream
  rows of the same database. Do not copy or restore Jones tables separately
  from the upstream data they describe.
- **Released history.** Never renumber, rename or delete a released Jones
  migration. When one breaks these rules, keep its identity, change its effect
  only if no stored data depends on it, and repair existing databases with a
  new data-preserving Jones migration. Removing upstream residue that released
  migrations left behind, such as `runtime_identity_json` on the V1 thread
  session projection, needs a separately authorized operation.
- **Append-only allocation.** Allocate new Jones migration IDs above the maximum
  on freshly fetched canonical `Jones-Systems/Jones-Code` `main`. Numeric gaps
  and allocations in concurrent branches do not reserve IDs. Before merging a
  migration-changing PR, fetch main again and requalify the candidate against
  that exact revision; update its unreleased allocation if main has overtaken
  it. Never renumber a released migration to make room.
  [CI admission](../../scripts/jones/ci/jones-migration-order.cjs) preserves all
  baseline ID/name pairs and rejects lower insertions. PR runs fetch current
  canonical main; main pushes compare the exact pre-push revision with the
  pushed revision. An unrelated stale PR is selected out only when its manifest
  source equals its common ancestor's source, with both manifests readable and
  valid. CI proves the revisions printed in that run, without reserving IDs or
  making a later main advance safe. The merge owner must repeat qualification
  immediately before merge, for example with
  `node scripts/jones/ci/jones-migration-order.cjs --baseline <fresh-main-commit> --candidate <candidate-commit>`.
- **Update checks.** Trust upstream migrations through the normal T3 startup
  and recovery process. Before stopping the active server, the staged candidate
  reads the migration ledgers without writing them and reports pending IDs;
  a candidate older than the database is blocked. Add targeted checks inside
  Jones migrations for their declared effects and interactions, such as row
  counts around a Jones table rebuild and removal of upstream references.
  A failed check aborts the migration transaction. Routine updates do not need
  full database scans, content hashing or a separate migration rehearsal.

[`UpstreamIsolation.test.ts`](../../apps/server/src/jones/persistence/UpstreamIsolation.test.ts)
checks the schema rules against the complete Jones manifest.
[`ForeignHistoryCompatibility.test.ts`](../../apps/server/src/jones/persistence/ForeignHistoryCompatibility.test.ts)
checks that receiving startup preserves populated foreign Jones history.
Two earlier integrations predate these rules and are not precedent:

- **Native effect envelope.** Native-creation effects wrap their request with
  a native reference in the outbox payload. Upstream workers cannot decode the
  wrapper, so they never run a native effect. Keep this interlock until native
  effects have their own queue. Do not put other Jones metadata in upstream
  payloads; a new interlock of this kind needs owner approval.
- **Foreign history 7-13.** Databases from those foreign Jones migrations hold
  `orchestration_v2_*` tables that reference upstream rows. Jones preserves
  them as found. Their references block deletes of the rows they name and
  would block an upstream rebuild of the referenced tables. Rebuilding or
  removing them needs an owner decision.

### Install path changes

Jones Code is a personal development tool. Keep installation close to the
upstream flow: verify the selected artifact, preflight the candidate, stop the
owned backend, snapshot the database, and trial-start before committing.

- Verify downloaded artifacts by their published digest. Do not pin emitted
  bundle bytes, source hashes, lockfiles, or generated-code offsets in install
  admission or artifact production. Ordinary application changes must not
  require renewing an installation allowlist.
- Keep updates within the launcher's owned child and state directory. Do not
  introduce privileged observers or whole-host process scans as prerequisites
  for installing an otherwise valid build.
- Use one recovery snapshot of the database, WAL/SHM, and required settings.
  Release the task's snapshot after commit or durably recorded rollback;
  do not copy browser caches or retain a second failed database indefinitely.
  Existing historical backups are outside routine update cleanup.
- Make a refusal actionable: identify what failed and how to recover. Optional
  fleet bookkeeping must not prevent a local app update.
- Preserve launcher and startup-gate protocols when possible. A necessary
  incompatible change must describe the one-time host refresh in the PR.

Use small synthetic fixtures instead of installing across the fleet. Migration
or startup-wiring changes run
[`migrationPlan.test.ts`](../../apps/server/src/jones/updates/migrationPlan.test.ts)
with the real migrations, a few retained rows, and a deliberately failing
transaction. Launcher and recovery changes run
[`serviceLauncher.test.ts`](../../apps/server/src/serviceLauncher.test.ts) and
the affected backup tests. Desktop handoff or recovery changes run the affected
[`native helper tests`](../../apps/desktop/src/jones/updates/jonesNativeHelperSource.test.ts)
and updater tests. Keep fixtures isolated, clean them on success and failure,
and retain the original failure result. Unrelated UI changes do not need an
installation rehearsal. The normal read-only candidate preflight remains the
per-install migration check.

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

Use the partial [local UI map](../ui-map/README.md) and maintained [Frontend view-map guide](https://github.com/Jones-Systems/Frontend-Development/blob/main/docs/guides/ui-view-maps.md) (access required) to find affected journeys and shared consumers.

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

Ordinary owner-scoped implementation includes source completion, PR publication,
current-head CI repair and watching, and qualifying merge under the standing
contract below. An earlier owner-selected finish line or review hold takes
precedence. Do not request separate permission for each covered step; report
an actual unmet gate and continue independent work within scope.

Use a conventional commit title in plain language, such as
`fix(web): new threads no longer spike CPU`. Keep one concern per PR; split
unrelated additions. Write the description in this order:

- When true, start with `**Needs M Jones:** <one owner-only decision>`,
  `**Hold:** <what releases it>`, `Stacked on #N`, or `Supersedes #N`.
  Use at most two; already-authorized delivery steps are not owner decisions.
- Give one to three summary bullets, result first, in plain words.
- For a small PR, add one `Checked:` bullet and `Not checked:` only for a real
  gap, then stop. For larger PRs, use only the needed headings, in order:
  `Problem`, `Change`, `Visuals`, `Verification`, `Not in this PR`.
- Bottom lines appear only when they exist: `Fixes #N` for an issue actually
  resolved by this PR, otherwise `Refs #N`; `Record: <link>`; contributing
  threads; model and harness. Copy thread names, IDs and roles as recorded;
  publish names and IDs only when public-safe, otherwise use the role alone.
  Use one line for up to two contributors, a collapsed block for more, and
  list threads used only as sources separately.

Open with a clear sentence or lead bullet explaining what changes and why.
Keep top-level bullets to one clear point, usually one visual line and at most
two where practical. Add one or more indented sub-bullets wherever a short lead
would be ambiguous: explain what it means, why it matters or what remains
unfinished. Supporting bullets may use short sentences and take more space
when needed; clarity wins over rigid length limits or unexplained fragments.
Link deeper reproducibility detail from the evidence comment. Bold key statuses,
decisions or important words sparingly, rather than every line. Use descriptive
Markdown hyperlinks for PRs, issues, documents, evidence and available thread
references; avoid bare URLs and unlinked numbers. Preserve machine-consumed
reference syntax where required.

Keep the effect, reason, material trade-offs, checks, material limits, holds and
owner decisions in the body. Keep the few numbers and identities that make a
claim true, including distinct capture and candidate revisions. Put full hashes,
commands, tool versions, admission samples and other reproducibility detail in
one author-written evidence comment. Publish it and open its link before moving
text out of the body; otherwise retain that text in a collapsed block at the
bottom. Link the comment with `Record:`. An authoritative findings ledger stays
where the PR declares it. Do not repeat GitHub's draft, check or merge status or
file counts. Never commit working notes or PR-only media, and never use a local
path, an expiring artifact alone, or a private link as the only public record.

When a measurable claim is made, report observed before/after numbers, scenario
and revisions, with a link to the run. A deterministic repair needs the scenario
that failed before and passes after, not invented rates. On an already-measured
path, report the existing check's result even without a performance claim.
Reuse the CI thread-transfer budget (job summary and 30-day
`thread-transfer-results` artifact), `scripts/render-metrics.ts`, the agent
context budget test and `vp run test:perf:v2-wire` as applicable. If no harness
exists, say "not measured" and why; do not create a benchmark or reporting bot
for one PR.

Decide visual evidence needs before implementation so a before state is still
available. When rendered UI or other rendered output changes, include `Visuals`
with exactly one outcome: **Captured**, **Blocked** (exact gap, evidence location
and blocker owner), or **No visible change** (reason). Omit the section when
there is nothing to see; record that decision in task-local notes. If applicability
is unclear, say so in Verification. UI changes need before/after images; motion,
timing or sequence claims need a recording and words saying what to watch for.
Stills never prove motion. Use a two-column table for one before/after pair;
stack larger sets. Phone rendering of fixed-width tables remains unverified.

The body keeps one capture identity line stating its relation to the PR head,
and the coverage limits. A base plus a patch is never described as a clean
commit capture. For each evidence set, keep the detailed record in the evidence
comment (or the collapsed fallback):

- Comparison and candidate revisions; for a dirty tree, its base and patch
  identity.
- Build identity when available, route/platform, viewport, scale, theme,
  fixture or scenario, observable action and readback, and coverage limits.
- Whether before/after shows interaction states within one build or two exact
  source builds.
- The integration revision and constituent heads for a combined preview.

**Captured** means hosted links are in the body, each image has been opened and
each recording played in the reader's view, and the evidence remains current
after the last body edit and head change affecting the UI. Label older captures
historical; refresh exact-head claims after the PR head changes. A Markdown
link or HTTP success alone does not prove rendering. Public evidence uses only
synthetic or example data; private capture publication requires separate authority.
Upload PR evidence to GitHub; never commit PR-only screenshots or assets such
as `.github/pr-assets/`. A chat attachment, localhost link or expiring artifact
is not published PR evidence. Use the
[CLI attachment workflow](../../.agents/skills/capture-ui-evidence/references/runner-output-and-limits.md#sharing-evidence)
with existing GitHub CLI authentication before requesting browser sign-in.

### Editing a published description

The current raw published body is the only source; never replace it from an old
local draft. One writer saves the complete raw body and head OID as a preimage,
edits that text, then re-reads both immediately before writing. If either moved,
rebuild from the new text. Write once and read back the complete body. Preserve
machine lines (`Lift-PR: N`, `Form-PR: N`), HTML comments, the Blacksmith footer
block, closing keywords and hosted media links byte for byte; preserve stack
lines, authoritative findings ledgers, holds and stated unknown effects in meaning.
Confirm protected content and media survived and no local media path remains.
Reconcile interleaved edits; never restore a stale whole body or blindly repeat
a write with an unknown effect. Recheck that pictures open and recordings play
after the edit and immediately before merge.

### Waiting and closing without merging

Open PRs carry no labels. For a real unanswered owner-only decision, use the
`Needs M Jones` top line and assign the PR to M Jones; unassign when every ask
is answered. Other holds use `Hold` without a label.

When authorized to close without merging, post one closing comment whose first
line is one of:

- `Landed via #N`: base containment verified, or explicitly "claimed, not verified".
- `Superseded by #N` (or `OWNER/REPO#N`): replacement, not proof of landing.
- `Superseded — replacement not yet published`: add the actual link later as a reply.
- `Not planned`: dropped.

Follow with two short bullets, `**Why:**` and `**Kept:**` (preserved commit,
branch or other material). Order: comment, close, then one label: `superseded`
for the first three forms, `wontfix` for `Not planned`. The `superseded` label
means "Closed without merging; the closing comment says where the work went."
No label means nothing was recorded; it does not mean in progress or merged.
Keep `resolution:*` on issues; do not reuse `duplicate` for a rewrite. Do not
rewrite a merged successor to add a reciprocal `Supersedes` line. These rules
do not authorize closure or change issue lifecycle, merge authority or holds.

Do not invoke automated PR review services or reviewer bots for Jones-Systems
PRs, including `@codex review`, Codex Code Review, or Copilot PR review.
When asked to watch a PR, inspect current-head checks and new comments, validate
reported problems against the source, and follow the task's authority for
disposition and fixes. Stay quiet when nothing changes. Report current-head
results and unresolved findings; absence of a bot review is not a failure.

### Required and advisory checks

The required merge check is **`Check`**, the final required job of the `CI` workflow
(`.github/workflows/ci.yml`). It aggregates the required pull-request jobs in that
workflow and fails on failed or cancelled jobs, failed change detection, and
unexplained skips. A skip is intentional only when a successful classifier
explicitly selects that job out. Linux selection preserves full runs for main
pushes, unknown or unreadable changes, and complete-stack qualification; the
macOS enrollment and native-source gates remain independent. No branch
protection or ruleset enforces it today; this contract does.

Every other check is advisory:

- `Jones update qualification`;
- `Server parallel qualification` (serial Linux/macOS source, bundle, Electron Node
  and installed SEA measurements; failed or unknown evidence is not qualification);
- `Server Result Cache` cold and warm pilots (one weighted Linux shard against
  the ordinary fresh run, using GitHub Actions cache);
- `Native enrollment candidate`;
- `Linux CLI artifacts`;
- `Mac Desktop Artifact`, including the opt-in #873 original-worker reproduction job;
- `Mac CLI artifact`;
- `Rotate macOS dependency cache`;
- any native E2E workflow.

An advisory failure gets investigated, fixed, or filed as an issue, but it
does not hold a merge. A pending advisory check is not a reason to wait.
When a workflow or job is added, renamed or moved, update this list in the
same change. The organization-wide rules live in the Codex V3
[CI budget and placement](https://github.com/Jones-Systems/Codex-V3/blob/main/docs/design/test-performance-practice.md#ci-budget-and-placement)
practice.

Run the server result-cache pilot with the `server_cache_pilot` input on a
manual **CI** run. Same-repository pull requests that change the pilot driver
also start it automatically. The cold job saves per-file results for that
run and attempt. The warm job restores them on a separate Linux runner,
re-executes one hit without caching, and compares every file and case against
the fresh shard report. The transfer-budget test runs fresh in all three.
If the selected shard includes the system service control test, that test also
runs fresh because Vite's cache tracer would alter the environment it checks.
The warm job summary records the comparison and elapsed times. The pilot
result archive is scoped to the experiment.

Set the repository Actions variable `JONES_SERVER_RESULT_CACHE=on` to enable
per-file result reuse on every selected Linux server shard of eligible
same-repository pull requests to the default branch. Each PR and shard has
its own archives. A cache hit needs matching inputs and recorded producer
evidence. Each shard with hits reruns one of them fresh as an audit. The
transfer-budget and service-control tests always run fresh. Shards using the
cache path do not contribute timings. Main pushes run fresh and supply the
shard weights. Manual, stacked, fork and unknown-scope runs also stay fresh.
New archives require headroom within GitHub's included cache allowance. When
headroom is low or unknown and there is no usable archive, the shard runs fresh.

For a rollback, go to **Settings → Secrets and variables → Actions → Variables**
and set `JONES_SERVER_RESULT_CACHE` to `off` or remove it. Re-run the affected
CI jobs at the same commit. Each restarted server shard reads the switch
again and uses the original full-shard command when it is off. To replace
every shard's result with fresh evidence, choose **Re-run all jobs**. Reruns
that predate the rollout retain their original workflow definition.

Apple Silicon PR jobs install dependencies from the lockfile without saving
or restoring the large macOS pnpm archive. The archive is available to main
builds. After successful CI for the current main commit, rotation retires older
main macOS pnpm keys once the replacement is visible in the Actions cache.

### Standing delivery and merge contract

M Jones authorizes ordinary scoped task delivery in the canonical
`Jones-Systems/Jones-Code` repository through a pull request into `main`, using
a normal merge commit when the repository's current settings and rules permit
it. This standing authority covers the scoped commit, normal task-branch push,
PR publication, CI repair/watch, and qualified merge; it does not depend on a
new per-PR merge request. It does not extend to other repositories or bases.

- **Publish the scoped result.** Verify repository, isolated worktree, task
  branch, Git owner, exact HEAD, clean or fully accounted status, and open PRs
  for overlapping work. Stage only confirmed task paths, preserve hooks, and
  push the exact task branch with normal fast-forward protections. Open or
  continue one non-draft PR against `main` so CI runs. Use a draft only for an
  explicitly requested early checkpoint or visibility-only handoff. An owner
  merge/review hold still permits publication unless it also limits publication.
- **Qualify the current head.** Inspect live repository merge settings, branch
  protections and rulesets; require the [required checks](#required-and-advisory-checks)
  to pass on the exact PR head, all required reviews, and resolved review
  threads. Repair in-scope CI failures and refresh evidence after every head
  change. A missing, pending, skipped, unavailable, stale or unrelated required
  check is not passing evidence. Absence of branch protection does not waive
  these policy requirements. Advisory checks never hold a merge.
  Use app-owned PR watching when available; otherwise use bounded observation.
  Do not invoke reviewer bots or bypass a required review.
- **Merge with a guard.** Immediately before merging, re-read the PR head and
  base, current checks, reviews, thread resolution, mergeability, repository
  rules and any owner hold. Require the verified scoped head, canonical `main`
  base, and normal merge-commit method. Use an expected-head guard, for example
  `gh pr merge <number> --repo Jones-Systems/Jones-Code --merge --match-head-commit <verified-head>`.
  Never use `--admin`, force push, direct protected-ref updates, ruleset changes,
  or another bypass. If a native queue or rule requires a different method,
  hold and report the exact constraint; do not silently change this contract.
- **Read back the result.** Verify the PR is merged, record its merge commit,
  and read the receiving `main` ref and tree to prove the reviewed head landed.
  If `main` has advanced, verify the merge commit and reviewed head are ancestors
  of its current tip. A failed or lost response with an unknown effect requires
  read-only reconciliation before any retry. Report publication, CI, merge,
  and operational results separately.

This contract excludes release, deployment, installation, sudo or equivalent
elevation, credential/private-state access, branch or worktree deletion,
history rewriting, and unrelated changes. Those retain their own authorization
boundaries. Stop the affected operation on an owner hold, changed scope,
unresolved prior effect, failed qualification, or unavailable native evidence;
do not turn a blocked merge into a bypass or repeated permission request for
steps already covered here.

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
