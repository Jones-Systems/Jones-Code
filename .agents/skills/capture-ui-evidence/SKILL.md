---
name: capture-ui-evidence
description: Use the standard Jones Code UI verification and evidence workflow for UI changes and screenshots. Route interactive shared-renderer checks to T3 Browser, native Linux desktop scenarios to the Electron harness after exact-revision qualification, and mobile checks to test-t3-mobile.
---

# Capture Jones Code UI evidence

This is the default workflow for verifying user-visible UI changes and recording evidence. Choose the affected client:

- Shared web/desktop renderer: [test-t3-app](../test-t3-app/SKILL.md) and T3's Browser panel for live inspection, clicks, typing, screenshots and iteration.
- Linux native desktop shell: the Electron runner below for repeatable screenshots and scripted interactions after exact-revision qualification.
- Native mobile: [test-t3-mobile](../test-t3-mobile/SKILL.md).

Exercise the changed flow, assert its observable result, and check backend readback or reload persistence when the change depends on them. Use the same evidence format on every route: full candidate/comparison revisions (plus patch identity for dirty source), build correspondence, client/route, actual viewport/scale/theme, meaningful fixture, action and readback, captures, and coverage limits. The source/build distinction in [runner output and evidence limits](references/runner-output-and-limits.md) applies to Browser evidence too; a dev source OID alone is not an attestation of the served bundle.

T3 Browser remains the shared-renderer route. An unavailable Preview does not authorize switching to a standalone browser or using Electron as a Browser fallback.

The Linux Electron runner is a separate route for behavior that depends on the desktop shell. Require a recorded passing qualification for the exact harness revision; exploratory output alone leaves Electron-specific behavior unverified. This nightly source port does not qualify a host or adopt an installed wrapper. A separately installed host command must remain pinned to its qualified revision, with readback in `~/.local/state/jones-code-ui-evidence/qualification.json`; an older source qualification does not qualify this port.

The runner launches existing built outputs; it does not build the app. On a host with the installed command, first run `jones-code-ui-evidence doctor`, then use `jones-code-ui-evidence run --source /absolute/candidate --build-receipt /absolute/candidate-receipt.json` with a scenario that exercises the changed flow. The default `sidebar-rename` is a qualification/smoke scenario; it does not verify an unrelated UI change. A custom scenario receives a Playwright page and Electron app for inspecting and interacting with the candidate inside the isolated run.

From the Jones Code repository root, use:

```sh
node apps/desktop/scripts/ui-evidence.mjs doctor [--source DIR]
node apps/desktop/scripts/ui-evidence.mjs setup [--source DIR]
node apps/desktop/scripts/ui-evidence.mjs run [--source DIR] [--scenario sidebar-rename|/absolute/path/scenario.mjs] [--size 1280x800] [--scale 1] [--theme system|light|dark] [--comparison OID] [--build-receipt FILE] [--out DIR]
node apps/desktop/scripts/ui-evidence.mjs cleanup --run <run-id>
```

The repository also exposes the CLI and its bounded test group as package scripts `ui:evidence` and `test:ui-evidence`, invoked through the existing `vp run <script>` pattern. The test group does not qualify the live Electron route.

`doctor` is read-only and reports prerequisites or exact missing system package names; it installs nothing. `setup` downloads the lockfile-matched Electron runtime into its owned user cache and checks the official checksum. No command elevates privileges. `run` requires an existing built app, defaults to the harness source and `sidebar-rename`, and writes to `<source>/.t3/ui-evidence/runs/<run-id>/` unless `--out` names a path that does not already exist. A full commit OID may be supplied to `--comparison`; `--build-receipt FILE` records a receipt for the exact source and output hashes. Every command emits JSON with an `outcome` of `complete`, `no_change`, `rejected`, `partial`, or `unknown`. A run returns 0 for a pass, 1 for a scenario failure, 2 for a rejected precondition, or 130/143 for cancellation.

Read [runner output and evidence limits](references/runner-output-and-limits.md) before interpreting captures, comparing revisions, or writing a PR claim. Do not weaken isolation or retry a rejected probe by disabling Chromium's sandbox.
