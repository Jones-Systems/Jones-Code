# Native self-configuration lifecycle scenario

This on-demand Linux Electron scenario exercises the real composer, server,
authenticated MCP route, provider adapter, durable orchestration and checkpoints
with a **synthetic Codex subprocess**. It proves no live-provider inference,
cross-machine transport, background-child continuity (SC-COR-002), macOS or
packaged-build behavior. It changes no live account or model default.

The fixture creates two Codex instances with one disposable account home and a
real Git commit and a genuine linked execution worktree. Both its common Git
storage and worktree live inside the disposable scratch namespace. The synthetic
peer verifies the app-issued cwd, distinct git-dir/common-dir and fixture HEAD;
checkpoint capture remains mandatory. Their only executables are generated inside the private sandbox.
The peer gets its MCP endpoint and authorization header from the app's actual
thread/start or thread/resume request. No fabricated invocation scope, live home,
external network, renderer dispatch of messages, or database write is involved.
A/B use high effort and the explicit Standard/default tier.

The entry point is
[thread-self-configure.mjs](../../../../apps/desktop/scripts/ui-evidence/scenarios/thread-self-configure.mjs).
Its support module is in the top-level harness directory, which the existing
runner stages. The scenario has a source/staged import locator; arbitrary
scenario sibling imports remain unsupported.

## Assertions and evidence

Before starting A, the scenario uses native Settings / General to set the
disposable app's Follow-up behavior to Steer and verifies the selected value.
The manifest and a screenshot record that setup. Ordinary submissions then use
Enter; this is not a claim about the product's default Queue preference. The
comparison uses the identical UI setup and continuity oracle.

A starts through the composer, reads its configuration, saves B with its actual
revision and execution precondition, retries identically, rejects a conflicting
retry, and makes another authenticated read. A stays held while the UI shows B.
An ordinary UI follow-up, without a picker change, must steer the same A run,
attempt, session and runtime generation exactly once, without interruption or
replacement. Only then does the scenario release A, check its continuation marker
and ready checkpoint, send a fresh UI message, verify B's captured request and
actual outbound model/effort/tier, and reload saved B.

Controls check legacy self-configuration rejection without a selection revision,
identical-request acceptance without another selection event, conflicting-request
rejection, an unchanged control thread, and one explicit picker transition on a
separate active fixture thread.

The manifest's fixture links the hashed self-configuration-evidence.json sidecar.
It retains synthetic receipts, authenticated thread projections, separated
saved/requested/provider-observed fields and an allowlisted subprocess trace.
Endpoint/header/session credentials and raw protocol requests are never written
to this sidecar. Screenshots are ordered interaction states in one build.
Failure artifacts remain in the private output directory. The runner's generic
notCovered entries for message content and provider/network flows remain broad
exclusions: this scenario only adds its named synthetic message/lifecycle
assertions, not general provider or network qualification. Reasoning-effort
observation remains unavailable; outbound requested effort is separate evidence.

## Source-bound native dependencies

The harness binds dependency admission to the canonical tested source, including
the unchanged comparison checkout. A source under `.t3/worktrees` is allowed;
protected suffixes and dotenv files beneath that source remain rejected. Only
the source's root, desktop and server `node_modules` storage can supply packages.
Every initial, transitive, optional and peer edge is checked before metadata is
read and checked again during staging. External worktree/store links, malformed
names, package-container paths and escaping manifest/content links reject.
Only individual closed package trees enter the existing narrow sandbox mounts.

Required/optional peer enumeration and relative staged links preserve the
behavior of PR507 at `238b185a9c05fccce5aedbcc485205f62b61a99f`; its focused
peer fixture is retained. This is a narrow integration of that behavior with
source-bound admission, not adoption or qualification of PR507/508's other
startup, restart or workflow changes.

## Serial verification capsule

Run only after both source writers are quiescent, the parent has integrated the
harness and continuity repair, and the VPS test slot is released. Refresh
same-host CPU/memory telemetry before each build/test/native launch. CPU at or
above 85%, missing telemetry, or a peer owning the slot means zero starts.
Otherwise run one process at a time. These commands do not install dependencies.

Use the parent's final integrated source, not the harness-only branch. Create a
new private parent evidence directory before running this capsule; never reuse
existing output names. Set these to the frozen candidate and a new private evidence directory:

```sh
SC_SOURCE=/absolute/path/to/integrated-candidate
SC_HARNESS="$SC_SOURCE"
SC_EVIDENCE=/absolute/private/evidence/native-self-configuration
export SC_SOURCE SC_HARNESS SC_EVIDENCE
cd "$SC_SOURCE"
node --test --test-concurrency=1 \
  apps/desktop/scripts/ui-evidence/scenario-api.test.mjs \
  apps/desktop/scripts/ui-evidence/thread-self-configure.test.mjs
pnpm exec vp lint \
  apps/desktop/scripts/ui-evidence/scenario-api.mjs \
  apps/desktop/scripts/ui-evidence/scenario-api.test.mjs \
  apps/desktop/scripts/ui-evidence/thread-self-configure-support.mjs \
  apps/desktop/scripts/ui-evidence/thread-self-configure.test.mjs \
  apps/desktop/scripts/ui-evidence/scenarios/thread-self-configure.mjs
node scripts/jones/check-boundary.ts --strict
```

After fresh build admission, build and create the correspondence receipt. This
requires clean, frozen source and rejects drift. Existing lockfile dependencies,
Electron 44.4.5, Playwright 1.60.0, Xvfb, Git and rootless Bubblewrap must already
be available; doctor reports missing prerequisites without installing them.

```sh
node --input-type=module <<'JS'
import * as fs from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";
const source = process.env.SC_SOURCE;
const harness = process.env.SC_HARNESS;
const evidence = process.env.SC_EVIDENCE;
const git = (...args) => execFileSync("git", ["-C", source, ...args], { encoding: "utf8" });
const head = git("rev-parse", "HEAD").trim();
if (git("status", "--porcelain=v1").trim()) throw new Error("Freeze clean source first");
const env = { ...process.env };
for (const key of Object.keys(env))
  if (/^(VITE_HTTP_URL|VITE_WS_URL|ELECTRON_RUN_AS_NODE|T3CODE_(DESKTOP|DEV|PACKAGED))/.test(key))
    delete env[key];
const startedAt = new Date().toISOString();
execFileSync("pnpm", ["exec", "vp", "run", "build:desktop"],
  { cwd: source, env, stdio: "inherit" });
const finishedAt = new Date().toISOString();
if (git("rev-parse", "HEAD").trim() !== head || git("status", "--porcelain=v1").trim())
  throw new Error("Source changed during build");
const { collectProvenance } = await import(pathToFileURL(
  harness + "/apps/desktop/scripts/ui-evidence/provenance.mjs"));
const artifacts = evidence + "/build-correspondence";
await fs.mkdir(artifacts, { recursive: false, mode: 0o700 });
const result = await collectProvenance({ source, harness, artifacts,
  buildPaths: { desktop: source + "/apps/desktop/dist-electron",
    server: source + "/apps/server/dist", web: source + "/apps/server/dist/client" } });
await fs.writeFile(evidence + "/build-receipt.json", JSON.stringify({
  schema: "jones-code-ui-evidence-build/v1",
  source: { head: result.source.head, statusSha256: result.source.statusSha256,
    patchSha256: result.source.patch.sha256, untrackedSha256: result.source.untracked.sha256 },
  build: Object.fromEntries(["desktop", "server", "web", "boot"]
    .map(name => [name, result.build[name].sha256])),
  invocation: "pnpm exec vp run build:desktop (development/packaged overrides unset)",
  startedAt, finishedAt,
}, null, 2), { flag: "wx", mode: 0o600 });
JS
node "$SC_HARNESS/apps/desktop/scripts/ui-evidence.mjs" doctor --source "$SC_SOURCE"
```

The build itself is not exact-harness qualification. Run the existing
sidebar-rename qualification with the same harness, candidate and receipt, then
the lifecycle scenario cold and warm, serially with fresh telemetry before each.
No caches are deleted: cold means the first candidate/fixture run, warm means the
repeated candidate with host caches retained; every run gets a new profile and DB.

```sh
timeout --foreground --signal=TERM --kill-after=15s 240s \
  env XDG_STATE_HOME="$SC_EVIDENCE/state" \
  node "$SC_HARNESS/apps/desktop/scripts/ui-evidence.mjs" run \
  --source "$SC_SOURCE" --scenario sidebar-rename \
  --build-receipt "$SC_EVIDENCE/build-receipt.json" --out "$SC_EVIDENCE/qualification"

timeout --foreground --signal=TERM --kill-after=15s 240s \
  env XDG_STATE_HOME="$SC_EVIDENCE/state" \
  node "$SC_HARNESS/apps/desktop/scripts/ui-evidence.mjs" run \
  --source "$SC_SOURCE" \
  --scenario "$SC_HARNESS/apps/desktop/scripts/ui-evidence/scenarios/thread-self-configure.mjs" \
  --comparison 31a37d0b4196ae2c1800e92e5e6c964d128672e1 \
  --build-receipt "$SC_EVIDENCE/build-receipt.json" --out "$SC_EVIDENCE/positive-cold"
```

Repeat the second command with output positive-warm. Retain timings separately,
including the existing 20-second resource settling interval. The proposed
overall bound is 240 seconds plus teardown grace. Existing launch (60 seconds)
and RPC (15 seconds) limits remain intact; fixture barriers have explicit
15/30/45-second deadlines. The runner forwards cancellation to its captured
namespace child and escalates after five seconds. Timeout is failure even if
cleanup succeeds. Require namespaceProcess.exited=true and cleanup.rootAbsent=true
in every manifest. Unknown/failed teardown is not passing evidence; use the
existing exact registered-run recovery route, never path-pattern cleanup.

## Negative control without product contamination

**--comparison records an OID only; it executes no control.**

The parent prepares a separately built, clean source checkout at exactly
31a37d0b4196ae2c1800e92e5e6c964d128672e1 with its own receipt. Keep SC_HARNESS
pointing to the final harness above, and set SC_SOURCE to that comparison checkout.
Build/receipt it separately, then run the identical scenario/command with the
comparison source and receipt and a new negative-control output directory.
The CLI stages product builds from --source and harness modules from its own
checkout. Thus the minimum overlay on the tested old product is **zero product
files**. The final harness includes scenario-api.mjs, the scenario and support module,
source-bound runtime admission, matching dependency staging and canonical-source
doctor entry points. These execute from SC_HARNESS; no harness file is overlaid
on SC_SOURCE. Tests/docs/boundary records do not enter the app build. Never cherry-pick production repair files into the comparison or
rebuild it from the repaired branch.

Accept the negative control only when startup, authenticated save/replay controls,
and B UI readback pass, ordinaryUiSubmitted is true, and the unchanged
SC-COR-001 ordinary UI follow-up continuity step fails after composer submission.
A missing dependency, catalog/protocol mismatch, timeout before submission, or
failed launch is a harness qualification failure, not the required negative
control. Preserve first failures, sidecar, assertions, screenshots, manifests,
receipts, hashes, timings and cleanup readback privately. No native result is
claimed until the parent executes and inspects these checks.

## Explicit restart failure diagnostics

The picker control retains its latest projection on every poll, including timeout.
The synthetic wire records only the interrupt request ID, thread/turn IDs, empty
acknowledgement ordering and terminal emission. On failure, the scenario extracts
only recognized nested error classes and restart stages from the disposable
server trace; raw trace lines and MCP connection configuration are not copied.
Missing diagnostics never replace the original scenario failure.
