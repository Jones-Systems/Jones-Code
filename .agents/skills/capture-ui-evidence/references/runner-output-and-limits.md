# Runner output and evidence limits

## Run lifecycle

The runner uses Xvfb and launches the candidate's built Electron app inside a rootless Bubblewrap sandbox with private namespaces. The sandbox receives the staged app/build inputs and an allowlisted environment, not the real home, `.git`, dotenv files, provider credentials, or host/live network access; its backend uses a private loopback endpoint. It verifies its isolation before app launch and records the probe and resolved backend topology in the manifest. A failed probe rejects the run before launch; report that result and stop. Chromium receives a recorded CDP navigator-online marker so the real renderer attempts its isolated loopback connection; this does not add network interfaces or permit external access.

Runs use separate scratch and artifact directories. The runner registers the exact scratch root before launch, forwards interruption to the owned process, and removes registered scratch on success, failure, or cancellation. Captures and the manifest remain in the artifact directory. Use `cleanup --run <run-id>` only to recover a registered leftover; it rejects unknown IDs, active recorded processes, and paths outside the registered scratch root. Do not remove paths by pattern or clean another run's artifacts.

## Scenarios and captures

A custom scenario is an absolute-path ESM module with a default async function. Its content hash is recorded. Keep it self-contained (Node built-ins are available); the scenario entry is staged, not arbitrary sibling imports. Top-level harness modules are also staged; the native self-configuration entry uses that explicit support-module location. The context provides the Electron app and Playwright page, `step`, `capture`, `dispatch`, `readSnapshot`, `reload`, `completeOnboarding`, `setTheme`, `setWindowSize`, a disposable workspace, and non-secret logging. Fixture dispatch is limited to `project.create`, `thread.create`, and `thread.metadata.update`. On protocol V2, projects use the authenticated project mutation HTTP endpoint, thread commands use ticket-authenticated WebSocket RPC, and shell readback sends the required protocol header. Tokens and tickets stay inside the private renderer and are not returned as evidence. Create fixture state through app dispatch or UI; do not write the database directly. Metadata dispatch still cannot send provider turns. The narrowly allowlisted native self-configuration hooks can create only fixed synthetic A/B provider registrations and read fixture-prefixed thread detail. Its ordinary messages and explicit picker changes go through the actual UI. This opt-in scenario may send synthetic provider turns inside the private namespace; see [native lifecycle verification](native-self-configuration.md). No arbitrary settings patch, executable path, live provider, or renderer message-dispatch hook is added.

`setTheme` controls a recorded native-theme and CSS-media fixture and reads the rendered appearance back. It does not prove user theme-preference persistence. Use `ctx.reload()` to restore the recorded navigator-online fixture after navigation.

The built-in `sidebar-rename` scenario completes the normal provider-free first-run UI, then creates a synthetic project and metadata-only thread, captures `before`, renames through the Sidebar, checks the rendered title and backend snapshot, captures `after`, reloads and verifies persistence, then switches to dark theme with readback and captures `after-reload` and `after-reload-dark`.

**Those four capture names describe interaction states in one candidate build. They are not source-code before/after evidence.** The `--comparison OID` option records a declared comparison commit; it does not build or run that revision. To compare a code change, run the same scenario against separately built, exact base and candidate revisions, and retain both run manifests and capture sets.

## Reading the manifest

Each run writes `manifest.json` using schema `jones-code-ui-evidence/v1`. It records the run, harness and source identities, build digests, runtime and window readbacks, fixture and scenario steps, assertions, captures, isolation probe and topology, resource measurements, coverage claims, comparison declaration, status, cleanup outcome, and any error. Capture entries include the PNG hash and actual dimensions, scale, rendered theme, window bounds, and viewport.

Source provenance includes the source HEAD, dirty status, a digest of the tracked diff and untracked-file content, and the declared comparison OID. A dirty-tree capture must be described as that base plus its recorded patch identity, not as a commit-only result. A source OID alone does not prove which code produced existing build outputs: `build.sourceCorrespondence` is `receipt-matched` only when the exact source and build-output receipt match; otherwise describe it as unproved. The unpackaged runtime identity may be `null`.

Use the manifest to state what the run proves and what it leaves open. The built-in scenario covers project/thread metadata and a Sidebar rename with backend readback and reload persistence. It does not cover message content, provider turns, or external services. The Linux harness does not establish behavior for macOS, packaged/fused builds, signing, native dialogs, live-provider/network flows, or GitHub attachment upload. The opt-in native self-configuration scenario adds only its named synthetic-provider lifecycle assertions; its hashed sidecar separates saved/requested/observed evidence. Generic manifest exclusions are not general message-content or live-inference qualification.

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

The default `.t3/ui-evidence/runs/` output is task-local and untracked. The runner
does not upload or commit captures. Never commit PR-only media in Jones Code;
there is no committed-media fallback. Keep the manifest and the detailed
per-evidence-set record with one author-written PR evidence comment. Publish
that comment and open its link before moving detail out of the body; otherwise
keep it in a collapsed block. The body retains one capture identity line stating
its relation to the PR head, claim-critical identities and numbers, and limits.

Use the official GitHub CLI attachment support with existing authentication.
Check `gh pr edit --help` for `--attach` before concluding browser sign-in is
needed; a signed-out Browser profile does not establish that CLI upload fails.
Publish in this order:

1. Prepare the body without any local media path. Do not put local image or
   video references in a body file, even when attaching those files separately.
2. Upload with `gh pr create … --attach 'file#caption'` or, for an existing PR:

   ```bash
   gh pr edit 123 --repo OWNER/REPO --attach './before.png#Before the change' --attach './after.png#After the change'
   ```

   Repeat `--attach` for multiple images or videos. Without a body flag, the
   command preserves the body and appends hosted links. Prefer H.264 MP4 for
   recordings. See the [official CLI manual](https://cli.github.com/manual/gh_pr_edit).
3. Read the complete published body back. To place the media, move the hosted
   links in that raw text, following the
   [published-body edit procedure](../../../../docs/operations/contributor-guidance.md#editing-a-published-description).
   Never write from an older local body draft; preserve comments, footers and
   existing media. Use `--body-file` only with text derived from that readback.
4. Confirm no local media path remains. Open every image and play every recording
   in the reader's rendered PR view, signed in for private evidence. A Markdown
   link or HTTP success alone does not prove visibility.
5. Repeat the rendering and protected-content checks after the last body edit
   and immediately before merge. Refresh captures after a head change affecting
   the UI, or label them historical rather than current.

An upload can succeed for some files and return nonzero for others. Retain the
command result and printed URLs; read the body back to identify what arrived
before retrying only missing files. A timeout is unknown until reconciled; never
blindly repeat it. If the CLI lacks the flag or GitHub rejects the upload,
report the actual limitation and use only an authorized fallback. No extension
or browser session is required when the installed CLI and authentication support
upload; this procedure does not claim that a particular upload has been qualified.

For every evidence set, identify the comparison and candidate revisions (base
and patch identity for dirty source), build identity when available, route and
platform, actual viewport, scale and theme, fixture and scenario, observable
action/readback, coverage limits, and whether images show interaction states in
one build or separately built source revisions. A combined preview names its
integration revision and constituent heads. Preserve the source-to-build limits
above. Stills never prove motion: a timing or sequence claim needs a recording
and an explanation of what to watch for.

Use only synthetic or example data on a public PR. A private capture needs
separate publication authority. A chat attachment, local path, localhost link,
expiring artifact alone or private link as the only public record is not PR
evidence. **Captured** means published, visibly rendered and current. If blocked,
state the exact gap, evidence location and blocker owner; missing tooling is not
"not applicable". Follow the
[PR evidence outcomes](../../../../docs/operations/contributor-guidance.md#pull-requests)
when deciding whether there is rendered output to show.
