# Runner output and evidence limits

## Run lifecycle

The runner uses Xvfb and launches the candidate's built Electron app inside a rootless Bubblewrap sandbox with private namespaces. The sandbox receives the staged app/build inputs and an allowlisted environment, not the real home, `.git`, dotenv files, provider credentials, or host/live network access; its backend uses a private loopback endpoint. It verifies its isolation before app launch and records the probe and resolved backend topology in the manifest. A failed probe rejects the run before launch; report that result and stop. Chromium receives a recorded CDP navigator-online marker so the real renderer attempts its isolated loopback connection; this does not add network interfaces or permit external access.

Runs use separate scratch and artifact directories. The runner registers the exact scratch root before launch, forwards interruption to the owned process, and removes registered scratch on success, failure, or cancellation. Captures and the manifest remain in the artifact directory. Use `cleanup --run <run-id>` only to recover a registered leftover; it rejects unknown IDs, active recorded processes, and paths outside the registered scratch root. Do not remove paths by pattern or clean another run's artifacts.

## Scenarios and captures

A custom scenario is an absolute-path ESM module with a default async function. Its content hash is recorded. Keep it self-contained (Node built-ins are available); only the scenario file is staged, not sibling imports. The context provides the Electron app and Playwright page, `step`, `capture`, `dispatch`, `readSnapshot`, `reload`, `completeOnboarding`, `setTheme`, `setWindowSize`, a disposable workspace, and non-secret logging. Fixture dispatch is limited to `project.create`, `thread.create`, and `thread.metadata.update`. On protocol V2, projects use the authenticated project mutation HTTP endpoint, thread commands use ticket-authenticated WebSocket RPC, and shell readback sends the required protocol header. Tokens and tickets stay inside the private renderer and are not returned as evidence. Create fixture state through the app dispatch or UI; do not write the database directly or send a provider turn.

`setTheme` controls a recorded native-theme and CSS-media fixture and reads the rendered appearance back. It does not prove user theme-preference persistence. Use `ctx.reload()` to restore the recorded navigator-online fixture after navigation.

The built-in `sidebar-rename` scenario completes the normal provider-free first-run UI, then creates a synthetic project and metadata-only thread, captures `before`, renames through the Sidebar, checks the rendered title and backend snapshot, captures `after`, reloads and verifies persistence, then switches to dark theme with readback and captures `after-reload` and `after-reload-dark`.

**Those four capture names describe interaction states in one candidate build. They are not source-code before/after evidence.** The `--comparison OID` option records a declared comparison commit; it does not build or run that revision. To compare a code change, run the same scenario against separately built, exact base and candidate revisions, and retain both run manifests and capture sets.

## Reading the manifest

Each run writes `manifest.json` using schema `jones-code-ui-evidence/v1`. It records the run, harness and source identities, build digests, runtime and window readbacks, fixture and scenario steps, assertions, captures, isolation probe and topology, resource measurements, coverage claims, comparison declaration, status, cleanup outcome, and any error. Capture entries include the PNG hash and actual dimensions, scale, rendered theme, window bounds, and viewport.

Source provenance includes the source HEAD, dirty status, a digest of the tracked diff and untracked-file content, and the declared comparison OID. A dirty-tree capture must be described as that base plus its recorded patch identity, not as a commit-only result. A source OID alone does not prove which code produced existing build outputs: `build.sourceCorrespondence` is `receipt-matched` only when the exact source and build-output receipt match; otherwise describe it as unproved. The unpackaged runtime identity may be `null`.

Use the manifest to state what the run proves and what it leaves open. The built-in scenario covers project/thread metadata and a Sidebar rename with backend readback and reload persistence. It does not cover message content, provider turns, or external services. The Linux harness does not establish behavior for macOS, packaged/fused builds, signing, native dialogs, provider/network flows, or GitHub attachment upload.

## Build receipts

Build the candidate through the repository's desktop build command (`vp run build:desktop`) with development and packaged-executable overrides unset. Keep the source revision and dirty identity unchanged from immediately before the build until the final output digest collection. The harness does not attest an arbitrary receipt's honesty: the operator owns that build sequence.

For automated build orchestration, import `collectProvenance` from `apps/desktop/scripts/ui-evidence/provenance.mjs`. Supply `source`, the pinned `harness`, a new artifact directory, and `buildPaths` with `desktop: <source>/apps/desktop/dist-electron`, `server: <source>/apps/server/dist`, and **`web: <source>/apps/server/dist/client`** (the served client). Collect before and after the build into separate directories, and reject any changed source identity. Write this receipt from the post-build result:

```js
{
  schema: "jones-code-ui-evidence-build/v1",
  source: {
    head: result.source.head,
    statusSha256: result.source.statusSha256,
    patchSha256: result.source.patch.sha256,
    untrackedSha256: result.source.untracked.sha256
  },
  build: Object.fromEntries(
    ["desktop", "server", "web", "boot"].map(name => [name, result.build[name].sha256])
  ),
  invocation: "vp run build:desktop (development/packaged overrides unset)",
  startedAt: /* ISO timestamp immediately before build */,
  finishedAt: /* ISO timestamp after successful build */
}
```

Pass its absolute path through `--build-receipt`. The command rejects changed source or outputs. Without a receipt, exploratory runs remain usable, but source-to-build correspondence is explicitly unproved. An installed command's pin identifies the harness, not an independently supplied candidate source; use `--source` for a different candidate.

## Sharing evidence

The default `.t3/ui-evidence/runs/` output is task-local and untracked. Attach screenshots or video through the repository's authorized GitHub workflow and verify that each attachment can be retrieved and renders in the PR. The runner does not upload evidence or commit generated captures. Keep the manifest with the PR evidence record so reviewers can identify the exact build, fixture, route, and coverage.

For every evidence set, identify the comparison and candidate revisions, build identity when available, route and platform, actual viewport, scale and theme, fixture and scenario, observable action/readback, and coverage limits. A combined preview must name its integration revision and constituent heads. Refresh any exact-current-head claim after the PR head changes.
