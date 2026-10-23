# Test runtime CI optimization

<!-- codex-section:begin id="worknote.test-scheduling#ctx.artifact-header.001" -->

Artifact Type: `work-note`

Artifact ID: `worknote.test-scheduling`

Purpose: Bind the scheduling candidate to its unchanged coverage and measured campaign.

Governing artifact: none

Continuity owner: Cross-Repo Tests Under6:30 parent thread
`486fcff1-4c33-40b0-9a64-7f4aa4227ccc`

Consumers: CI builder and campaign coordinator

Authority effect: none
<!-- codex-section:end id="worknote.test-scheduling#ctx.artifact-header.001" -->

## Scope and finish line

The owner explicitly requested this work note in the writer cone. Base is
PR #562, `f6826eb7935dfaffa44e68761750982e21adaa23`, on
`fix/ci-macos-queue-relief-20261023`. The builder owns Git only in
`work/test-runtime-ci-20261023`. The parent owns dispatch, watching and final
measurement decisions. Publication and ready transition are authorized;
merge, deployment, sudo on this host, runner changes and worktree cleanup are not.
No agents are spawned. Frontier Max remains the explicit workstream pin;
ordinary implementation uses Astra/medium. Local preference CLI revision 8
cannot represent Frontier Max; no shared preference was changed.

Preserve every upstream test/assertion and supported runtime. Targets are every
full job and first-test-start through final-required-check finish below 390s,
and at most 10% runner-seconds tax against a passing, matched N1. Queue time is
reported separately. Same-source A/B/C only compares incremental tax against
six server shards; it cannot establish absolute N1 tax.

## Candidate

- Two web runners execute the unchanged package test command, with native
  `--shard 1/2` and `2/2`. Manual `web_shards=1` retains a complete baseline.
  The package already selects both unit and browser projects.
- Desktop, mobile and client-runtime each get an independent job. The remaining
  workspace command retains concurrency four and all existing exclusions,
  plus desktop/mobile; client-runtime no longer runs a second time as a tail.
  All four existing serial Python/Node checks remain. Every new job gates Check.
- Native enrollment keeps the shared YAML steps, Linux leg, macOS gate, exact
  Node 26.8.2/SQLite 3.53.4/platform/architecture assertions and direct Node
  runner. Only macOS dependency caching is disabled; full install remains.
  Native change detection, fail-open behavior, manual override and final-gate
  conditional skip policy remain unchanged. Mobile native analysis is unchanged.
- Server choices are 1/6/7/8; ordinary default remains six. Only a manual
  one-shard measurement receives a 40-minute timeout. Weighted assignment,
  serial files, timing capture and aggregation remain unchanged.
- Add JSON beside effective console/GitHub reporters for web, server and the
  three moved suites. Each runner uploads results plus a receipt bound to
  checkout/event commit, run/attempt, command, outcome, runtime, config and
  weights. Server progress reporting remains. Do not append Vitest flags to the
  mixed remaining workspace command, whose scripts package also invokes Node.

No test hot paths were changed. The baseline failure is the supplied hosted
measurement, not a fabricated local failing test. New selector cases exercise
one and seven entries and retain invalid-count rejection coverage.

## Source and collision evidence

PR #504 head `1ebaf353aa778eea91ea412fd11ae0eba357d980` changes Node to
26.12.1 and adds Electron setup; PR #505 head
`0c65c846820f9369d474a807b37e53a12c3d8cfe` changes Chromium/native setup.
Their exact CI patches were inspected. Neither branch was imported; overlapping
setup hunks will require integration review. This candidate retains Node
24.22.0 and native qualification 26.8.2 from #562.

GitHub primary action source resolved `setup-vp@v1` to
`250f29ce396baf5e8f24498e17c0dfdebabc26eb`; its cache input is a boolean and
run-install is independent. Touched/new checkout, cache and upload actions use
verified full revisions. The lockfile declares Vite+ 1.0.0 and Vitest 5.0.1;
receipts record observed versions instead of assuming them.

Campaign weights source: run `39900034357`, artifact `12478627514`,
`server-shard-weights-a1`. Downloaded archive SHA-256 matches GitHub's
`1d2a49507420d196919b4c5327cc166d6fc85727ecad2c79a097a69d1c60226b`.
Extracted weights SHA-256:
`a813eacecbd90e9f3b0dcbda2ba5d021c64038c5df758e48dbd42dc3af898202`.
Source envelope names 710 files, six shards and source commit
`38cf9d720d34a4b0ea16bd34496f173e057fb3f2`. Pin that run on every campaign
invocation and verify the emitted digest. Ordinary CI retains latest-main
selection and its checked-in fallback.

## Measurement and parity contract

Supplied passing run `39906273606`: web 611 files/9376 passing cases,
475 unit/136 browser files, 526s test/595s job; desktop 260s; mobile 232s
(started 82s later); remaining workspace 566s with a 49s client-runtime tail.
Six server jobs took 401/293/361/446/427/378s. Modeled maximum test loads at
6/7/8 shards are 300.66/257.75/225.53s, plus 71–95s setup; these are models.
Mac enrollment took 497s including 284.7s cache restore, 160.2s install and
16s tests. No-cache savings and cold-install wall are unmeasured.

Parent dispatches sequential frozen-source A=server6/web1, B=7/2, C=8/2,
all with the pinned weights. Then, if needed, same-head manual server1/web1.
Include all moved-job setup in runner cost. Web's model is 664 versus 595
runner-seconds (+11.6%); a wall win alone does not pass the 10% target.
No source change claims to improve scarce macOS queue time.

Compare MULTISETS of normalized repository-relative file, project, ancestor
array, fullName, title and status; preserve duplicate names, skipped and todo.
For web, derive unit/browser only from disjoint globs at config blob
`413be1a29be40904de2b5f50764589420c94abe7`. Require successful commands and
complete files; missing or uncollected cases are missing evidence. Remaining
workspace and unchanged native checks do not yet emit these JSON receipts, so
full cross-job case parity must not be claimed from the new artifacts alone.

## Evidence lifecycle and verification

No local dependency tree, suite run, benchmark or persistent scratch has yet
been created. Weight download/verification used memory only. New hosted JSON
files are job-local, uploaded with seven-day retention, then removed by an
always step; hosted ephemeral runner disposal covers hard cancellation.
Existing test fixtures and caches retain their existing owners/lifecycles.
The builder owns cleanup of any new local transient analysis files on success,
failure or cancellation. The retained worktree is not authorized for deletion.

Direct Node 24.22.1 selector checks passed for defaults, complete 1/6/7/8
matrices and invalid 0/2/10/non-numeric counts. They ran under the shared flock
with fresh CPU 14.60%, 32.32 GiB available RAM; one core is 6.25% of capacity.
YAML/anchor, complete Check-needs, unchanged native gate and final-gate policy,
workspace partition, serial checks, reporter/pin and Bash syntax checks passed
under the shared flock (CPU 11.21%, RAM 32.37 GiB). `git diff --check` passed.
The boundary classifier requires an exact inventory entry for the new note;
that entry is the only additional source path. Embedded Node syntax checks passed
(CPU 17.98%, RAM 31.12 GiB). Oxfmt 0.73.0 formatted only the five scoped files
with defaults; this is not the project's dependency-backed formatter check.
Strict boundary execution is unavailable: Node exits with ERR_MODULE_NOT_FOUND
for typescript-legacy. Existing Vitest tests and project-configured formatting
are unavailable without a dependency install. The requested parent install slot
was not granted during implementation; no installation was started.
Hosted timing, cold-cache performance, full parity and absolute N1 tax remain
unavailable until the campaign. No measurement dispatch has been issued.
