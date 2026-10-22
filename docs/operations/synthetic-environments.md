# Synthetic environments for workflow testing

This recipe creates disposable, provider-free fixtures for client workflows.
It is source-verified at `86f048544c9462df722f2c4901f6e8bb681ead10`, but
unrehearsed as a complete recipe. It does not establish native Electron,
Simulator, physical-device or distinct-host qualification. Recheck the linked
contracts when adapting it to a later build.

Follow [development state and ports](./development.md#state-and-ports) and
[test data](./development.md#test-data). Never copy private databases, settings,
provider homes or credentials to make a fixture work. The worktree setup action
can link configuration from another checkout; it is not part of this recipe.

## Select the topology and oracle

| Topology                                | State to observe                                                                                                                                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| One server, multiple clients            | Both clients observe the same server-owned mutation without a second write.                                                                                                                       |
| Independent servers shown in one client | Read the owning server; assert the unrelated server is unchanged. Disconnects retain cached data under the [connection runtime](../internals/connection-runtime.md), without claiming it is live. |
| Owner registry membership               | Read the registry's membership/reference separately from the thread's owning environment. Follow [workstream ownership](../user/workstreams.md).                                                  |

Independent SQLite databases do not replicate merely because their rows appear
in one UI. Pin state and workstream membership need their own assertions.
Names such as laptop, VPS and Mac Mini are fixture labels; loopback servers do
not qualify remote networks, host operating systems or fleet dispatch. HQ and
Jones Code must not be assumed to share a backend.

The signed-HTTP server integration introduced by
[PR #499](https://github.com/Jones-Systems/Jones-Code/pull/499) exercises its
server/auth boundary. It is distinct from a native client action through
Electron preload/IPC or iOS native modules. A capture of preseeded rows likewise
does not prove a command ran.

## Allocate and start an owned server

The existing implementation pattern is
[`isolatedEnvironment` and `startServer`](../../scripts/jones/updates/install-preview-qualification.mjs#L55).
These are source references, not a new public fixture CLI.

1. Allocate a fresh scratch root with a defined cleanup owner. For each server,
   create separate `home`, `tmp`, `t3` and workspace directories. Use the source
   environment allowlist: owned `HOME`, `TMPDIR` and XDG directories, without
   inherited provider credentials or application homes.
2. Allocate an available loopback port using the existing `unusedPort` pattern.
   A released reservation can race: startup must still prove that the captured
   child owns the intended endpoint. Never adopt an existing listener.
3. With the built server from the exact candidate, spawn the ordinary command
   below under that isolated environment and capture its process identity:

   ```text
   node apps/server/dist/bin.mjs serve --base-dir <owned>/t3 --host 127.0.0.1 --port <allocated-port>
   ```

   Resolve the binary path against the source checkout, even when the child
   working directory is scratch. The command is bound by
   [`startServer`](../../scripts/jones/updates/install-preview-qualification.mjs#L407).
   Building dependencies and the server remains governed by
   [development](./development.md); this command does not build missing output.

4. Require the owned child to remain alive, bounded startup completion and the
   owned `t3/userdata/environment-id`. The environment-file readiness pattern
   is in [`mobile-showcase.ts`](../../scripts/mobile-showcase.ts#L1538).
   Read `GET /.well-known/t3/environment` from that endpoint and match its
   identity to the fixture. Record its `orchestrationProtocolVersion`; an open
   port alone is insufficient. Retain build/source identity with the result.

## Pair the fixture driver and the client

The ordinary pairing command is:

```text
node apps/server/dist/bin.mjs pair --base-dir <owned>/t3
```

[`pairCommand`](../../apps/server/src/cli/pair.ts#L487) defaults to standard
client scopes. Capture the credential-bearing output privately in memory;
never emit it in logs, command receipts or screenshots. The existing
[`issueFixturePairing`](../../scripts/jones/updates/install-preview-qualification.mjs#L320)
shows bounded output handling and exact child cleanup. Its repeated `--scope`
arguments replace the defaults and grant administrative scopes because that
fixture must mint additional client links; do not copy that broader grant
without the corresponding fixture need.

For a driver session, `POST /api/auth/browser-session` with JSON
`{ "credential": <issued-token> }`. Require success and retain the returned
`Set-Cookie` name/value pairs for subsequent `Cookie` headers. This is the
real local auth route used by
[`startServer`](../../scripts/jones/updates/install-preview-qualification.mjs#L437).
Do not substitute a credential from an installed app or populated base directory.

Issue a separate pairing credential for the native client using the same CLI
against the same owned base. Alternatively, an appropriately scoped driver may
`POST /api/auth/pairing-token` with `{ "label": <fixture-label> }` and use its
`credential` in `<origin>/pair#token=<encoded-credential>`.
The [handler](../../apps/server/src/auth/http.ts#L428) requires `access:write`
and all delegated scopes; a default driver session must not be assumed to have
that authority.

On iOS, fill **Address** with the loopback origin and **Code** with the separate
credential, then submit **Add environment**. Those accessibility labels and the
submit action belong to
[`ConnectionsNewRouteScreen`](../../apps/mobile/src/features/connection/ConnectionsNewRouteScreen.tsx#L199).
Its `pairingUrl` deep-link prefill and `autoConnect` are `__DEV__`-only
([source](../../apps/mobile/src/features/connection/ConnectionsNewRouteScreen.tsx#L54));
a Release run must use the pairing UI. The
[pairing parser](../../apps/mobile/src/features/connection/pairing.ts#L30) owns
URL construction. Retain neither filled-code screenshots nor raw driver argv
containing the code.

## Seed through commands, then read back

Use fresh command/project/thread IDs and a real directory under the owned
workspace. The provider-free payload example is
[`sidebar-rename.mjs`](../../apps/desktop/scripts/ui-evidence/scenarios/sidebar-rename.mjs#L15).
Its `modelSelection` is metadata; creating this thread does not require starting
a provider turn.

| Operation         | Current wire contract                                                                                                                                                                                                                                                                                                                                                                                           |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Create project    | Authenticated `POST /api/projects/mutate`: `type: "project.create"`, `commandId`, `projectId`, `title`, `workspaceRoot`.                                                                                                                                                                                                                                                                                        |
| Obtain RPC access | Authenticated `POST /api/auth/websocket-ticket` with `{}`; take `ticket` from the response.                                                                                                                                                                                                                                                                                                                     |
| Open RPC          | `ws://127.0.0.1:<port>/ws?orchestrationProtocol=<descriptor-version>&wsTicket=<encoded-ticket>`.                                                                                                                                                                                                                                                                                                                |
| Create thread     | RPC request `{ _tag: "Request", id: "0", tag: "orchestration.dispatchCommand", payload, headers: [] }`. Payload: `type: "thread.create"`, fresh `commandId`, `threadId`, `projectId`, `title`, `createdBy: "user"`, `creationSource: "web"`, `modelSelection: { instanceId: "codex", model: "gpt-5" }`, `runtimeMode: "approval-required"`, `interactionMode: "default"`, `branch: null`, `worktreePath: null`. |
| Read thread       | Authenticated `GET /api/orchestration/threads/<threadId>` with `x-t3-orchestration-protocol: 2`; response includes `snapshotSequence` and `projection`.                                                                                                                                                                                                                                                         |

The [fixture transport](../../apps/desktop/scripts/ui-evidence/scenario-api.mjs#L105)
owns project HTTP routing and the RPC envelope. Match the RPC response's
`requestId`, require a successful `Exit`, handle `Ping`/`Pong`, and close the
socket on completion or failure. The desktop harness uses its owned local
bridge bearer token; the standalone driver uses its own cookie session and
ticket. Do not borrow one client's credential to bypass the other's pairing.
The [HTTP contract](../../packages/contracts/src/environmentHttp.ts#L704) and
[snapshot schema](../../packages/contracts/src/orchestrationV2.ts#L3489) own
readback; optional history-window fields do not imply complete event history.

Create two threads for an isolation assertion. After the native action, verify
the intended thread's state and sequence, assert the other thread is unchanged,
then relaunch/reconnect the client and read back again. Preserve the actual
state owner in the claim. Do not bypass the Electron fixture dispatch allowlist.
[`mobile-showcase-environment.ts`](../../scripts/mobile-showcase-environment.ts#L356)
writes projections for visual hydration; those rows are not command-created
history and cannot establish mutation correctness.

## Native qualification limits and teardown

Saved remote environments in native Linux Electron remain held until the
qualified Secret Service/safeStorage route is established. A browser-renderer
substitute or `basic_text` fallback does not clear that boundary.

For a source-bound iOS run, use a self-contained Release bundle as described in
the [mobile README](../../apps/mobile/README.md#development). Set
`T3CODE_MOBILE_UPDATES_ENABLED=0` when generating native configuration
([config owner](../../apps/mobile/app.config.ts#L240)); verify the built app's
`Expo.plist` disables updates and identify its embedded `main.jsbundle`.
Use owned derived data and a newly created Simulator UDID, not a device found
by name. The source-owned fallback is
[`ios-pin-workflow.ts`](../../scripts/jones/mobile/ios-pin-workflow.ts), with a
finite XCUITest journey generated by the opt-in
[`withJonesE2eUITests`](../../apps/mobile/plugins/withJonesE2eUITests.cjs) plugin.
It does not use agent-device or Maestro. Neither the fallback's source nor its
injected tests qualify native execution or sentinel survival.

### Qualify the owned XCUITest fallback

The root coordinator assigns the serial host slot. Before build or execution,
require fresh same-host CPU below 85% and measured available memory, pressure
and swap. Normal or warning pressure passes; critical pressure holds only above
10 MiB/s combined swap reads and writes. Unknown samples hold. Laptop runs also
require at least 30 GiB free on the scratch filesystem. Do not use the disk-limited
Mac Mini. Keep native generation, CocoaPods caches, DerivedData and artifacts in
an explicitly owned disposable build tree; do not link `.env`, start Metro,
change host `xcode-select`, or enable provisioning. Preserve that tree through
the positive and negative runs, then its creating owner removes it.

1. Generate iOS from the exact clean task revision, with `JONES_IOS_E2E=1` and
   `T3CODE_MOBILE_UPDATES_ENABLED=0`. The plugin adds `JonesE2eUITests` and a
   same-named shared scheme only to generated, ignored iOS output. Normal builds
   do not enable it. Use a clean generated project; repeated plugin injection
   into an existing test target deliberately fails.
2. After the resource gate, run the selected Xcode's `build-for-testing` through
   the captured-child helper in
   [`xcuitest.ts`](../../scripts/jones/mobile/xcuitest.ts), with the generated
   workspace, `-scheme JonesE2eUITests -configuration Release -sdk iphonesimulator
-destination 'generic/platform=iOS Simulator' -derivedDataPath <owned>
CODE_SIGNING_ALLOWED=NO`. Set `DEVELOPER_DIR` only in that child's environment.
   This is a build recipe, not evidence that the current plugin has compiled.
3. Record `sourceSha`, `developerDir`, `xcodeVersion`, exact `buildArgv`, and the
   captured build `children` receipts and absolute `ownedRoot`. The build record's `artifacts` object has
   `app`, `testHost`, `testBundle`, and `xctestrun` entries, each containing an
   absolute `path` and `sha256`. Use the helper's `treeDigest` for the three
   bundle directories and `fileDigest` for `.xctestrun`; `bundleSha256` is the
   app's `main.jsbundle` digest. Verify Release/embedded JS, simulator platform,
   updates disabled, and the `.xctestrun` references to these same products.
4. Supply `--admission <root-record.json>` with `sourceSha`, `hostname`,
   `serialSlot`, epoch-millisecond `measuredAt`, `cpuUsed`,
   `availableMemoryBytes`, `memoryPressure`, `swapBytesPerSecond`, and `freeDiskBytes`.
   Admission must be less than 60 seconds old at launch; it records the root's
   external gate and does not grant authority. The runner also reads current
   free disk space. `--plan` lists the other required arguments without running
   native tools. `--preflight` verifies source and artifacts without invoking
   xcodebuild or simctl. `--execute` creates a new device and invokes one owned
   `test-without-building`, with explicit destination and result bundle,
   parallel testing disabled, one iteration, and no behavior retry.
5. IOS-01 must pair through Address/Code, pin, pass bounded authenticated target
   readback with unchanged control, terminate/relaunch the app, observe Unpin,
   unpin, and terminate/relaunch to observe Pin and cleared server state.
   Repeat with `--no-action true` on a fresh run: the same pin oracle must fail.
   The negative invocation must exit nonzero; an unrelated startup failure is
   not a passed negative control. Use `--control-pinned true` to cover a pinned
   control. Receipts distinguish this from injected rehearsal.
6. Independently run the native cleanup qualification with a separately owned,
   unrelated Simulator and captured sleep sentinel. Both must survive success,
   failure, startup timeout and cancellation; the run's captured process groups,
   created UDID and scratch must be gone. The adapter deliberately reports
   sentinel qualification as not established by an ordinary journey receipt.
   Then qualify hosted macOS. IOS-02 (two environments, mutate B while A remains
   byte-unchanged, disconnect B while A stays live) follows IOS-01; it is not
   supplied by this single-environment test.

### GitHub-hosted iOS qualification

[`jones-ios-native.yml`](../../.github/workflows/jones-ios-native.yml) is a
non-required `macos-26` job with a 75-minute limit, mobile pull-request paths
and manual dispatch. It checks out the exact head SHA on a named task branch.
The separate measured budget remains 50 minutes: `job-budget.json` measures
same-host monotonic elapsed time from admission before installation through
owned cleanup, and exceeding it fails the job even within the 75-minute timeout.
The source addition and pure tests do not qualify native execution. IOS-01
still needs both hosted and laptop evidence; the laptop helper-policy block
requires resolution without a security bypass, and IOS-02 remains separate.

The job selects the highest actually installed Xcode at or above 26.4, the
highest available iOS runtime within that SDK, and iPhone 17. It records this
metadata in `tooling.json`; moving runner-image documentation is not installed
evidence. Missing tooling, helper-policy rejection or disk exhaustion holds the
hosted route. Do not switch to paid runners, delete other Xcodes or retry an
external blocker.

Only a genuine GitHub-hosted disposable macOS26 job may use
`hostClass: "github-hosted-ephemeral"`, with matching run ID, attempt and
`gh-<run-id>-<attempt>` serial slot. It has no protective disk floor or claimed
safe footprint. The build records actual free disk at the start, every ten
seconds and at the end; ENOSPC is `hosted-disk-exhausted`. Laptop protection
remains 30 GiB. Both hosts use the same memory rule and fresh CPU gate.

The canonical helpers are `ios-admission.ts`, `ios-native-build.ts` and
`ios-cleanup-sentinel.ts` under `scripts/jones/mobile/`. The build wrapper uses
captured children for source stamping, the server build, Expo prebuild,
CocoaPods and `build-for-testing`. It preserves and restores the two package
manifests touched by the existing stamping helper before checking source
cleanliness. Raw build output, caches and native results stay in the private
job root. Build records and redacted evidence remain outside that root.

The sentinel harness runs four real cases: normal success, `--no-action true`
oracle failure, `--fixture-readiness-timeout-ms 1` after device/server capture,
and SIGTERM after `test-started`. A separately owned booted Simulator and
captured sleep must survive each case. The harness verifies each run's device,
groups, scratch and fixture port are gone, then removes only its sentinels.
Any uncertainty leaves `qualification: "not-established"` and retains scratch.
After qualification, one additional `--control-pinned true` run completes the
hosted IOS-01 journey set. Stage-only progress contains no pairing information.
Any non-default fixture timeout forces `nativeClaim: false`.

For an authorized laptop invocation, first produce a fresh admission with the
root-issued serial slot, then pass its output to the existing runner:

```sh
node scripts/jones/mobile/ios-admission.ts --source-sha <sha> \
  --serial-slot <root-issued-slot> --scratch <owned-root> --output <new-admission.json>
```

The hosted commands consume `JONES_IOS_SOURCE_SHA`, `JONES_IOS_SERIAL_SLOT`,
`JONES_IOS_ROOT` and `JONES_IOS_OUTPUT`: run `ios-native-build.ts --admission`,
`--tooling`, `--build`, then `ios-cleanup-sentinel.ts --qualify` and
`--control-pinned`. The workflow finally runs `ios-native-build.ts --cleanup`;
it removes the exact owned build/generated-iOS roots only after recorded child
and native cleanup is known. Its artifact allowlist retains redacted receipts,
tooling, build identity, disk samples and exactly two safe menu captures per
passed positive journey. Raw XCTest output and `.xcresult` are never uploaded.

The finite Swift test exports only the two menu screenshots after authenticated
relaunch readback. Raw XCTest output and `.xcresult` may contain typed pairing
codes: they remain in the private run root and are removed with it. Retain the
redacted receipt and those safe screenshots outside that root. Cleanup failures
remain failures with the original error; unknown process/device effects hold
scratch deletion and require exact reconciliation, never a broad cleanup sweep.

Preserve redacted assertions, build identities and failure evidence first.
Close owned client/driver sessions, terminate the owned app, shut down and delete
only the Simulator created by the run, then stop captured server children and
verify their exit and released ports. Remove only the exact owned scratch root,
or record its retained evidence and lifetime. Unknown cleanup effects remain
unknown; never use name-pattern kills or widen cleanup to another task's state.

Record whether evidence is source-only, injected, real local service,
native workflow or capture-only. A complete native claim requires the UI action,
owner readback and persistence check to have executed on the identified build;
authoring this recipe or passing server integration checks does not supply it.
