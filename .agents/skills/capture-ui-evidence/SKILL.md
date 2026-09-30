---
name: capture-ui-evidence
description: Capture Jones Code UI evidence. Keep the T3 Browser panel for shared-renderer work; use the Linux Electron harness only for desktop-shell behavior after its isolation qualification passes.
---

# Capture Jones Code UI evidence

Use [test-t3-app](../test-t3-app/SKILL.md) and T3's Browser panel for shared web-renderer work. This skill does not change that route or authorize switching to a standalone browser when T3 Preview is unavailable.

The Linux Electron runner is a separate route for behavior that depends on the desktop shell. Require a recorded passing qualification for the exact harness revision; exploratory output alone leaves Electron-specific behavior unverified. The installed host command is pinned to its qualified revision, with readback in `~/.local/state/jones-code-ui-evidence/qualification.json`.

The runner launches existing built outputs; it does not build the app. From the Jones Code repository root, use:

```sh
node apps/desktop/scripts/ui-evidence.mjs doctor [--source DIR]
node apps/desktop/scripts/ui-evidence.mjs setup [--source DIR]
node apps/desktop/scripts/ui-evidence.mjs run [--source DIR] [--scenario sidebar-rename|/absolute/path/scenario.mjs] [--size 1280x800] [--scale 1] [--theme system|light|dark] [--comparison OID] [--build-receipt FILE] [--out DIR]
node apps/desktop/scripts/ui-evidence.mjs cleanup --run <run-id>
```

The repository also exposes the CLI and its bounded test group as package scripts `ui:evidence` and `test:ui-evidence`, invoked through the existing `vp run <script>` pattern. The test group does not qualify the live Electron route.

`doctor` is read-only and reports prerequisites or exact missing system package names; it installs nothing. `setup` downloads the lockfile-matched Electron runtime into its owned user cache and checks the official checksum. No command elevates privileges. `run` requires an existing built app, defaults to the harness source and `sidebar-rename`, and writes to `<source>/.t3/ui-evidence/runs/<run-id>/` unless `--out` names a path that does not already exist. A full commit OID may be supplied to `--comparison`; `--build-receipt FILE` records a receipt for the exact source and output hashes. Every command emits JSON with an `outcome` of `complete`, `no_change`, `rejected`, `partial`, or `unknown`. A run returns 0 for a pass, 1 for a scenario failure, 2 for a rejected precondition, or 130/143 for cancellation.

Read [runner output and evidence limits](references/runner-output-and-limits.md) before interpreting captures, comparing revisions, or writing a PR claim. Do not weaken isolation or retry a rejected probe by disabling Chromium's sandbox.
