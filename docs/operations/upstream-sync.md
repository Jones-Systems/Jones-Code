# Maintaining Jones-Code

Jones-Code is a standalone public repository based on [T3 Code](https://github.com/pingdotgg/t3code). It is not a GitHub fork. The initial source is the unmodified upstream commit `d2c9281b8112dc3b2991642c4bdb985e4b08b9bb`; local changes begin in pull requests after that commit. The old `Jones-Systems/t3code` fork and its pull requests remain separate history until each useful change is reviewed and moved.

## Remotes and boundaries

In the Jones-Code development clone, `origin` is `Jones-Systems/Jones-Code`, `upstream` is `pingdotgg/t3code`, and `oldfork` is `Jones-Systems/t3code`. Disable pushing to `upstream` and `oldfork`. Work on a task branch in an isolated linked worktree; keep one Git owner for each worktree. Do not edit `main`, force-push, or update a protected ref to perform a sync.

This repository starts public so its pull request checks can use GitHub-hosted Actions. Public source and Git history remain public even if the repository later becomes private. Keep credentials, private host data, deployment configuration, and machine-specific state out of commits. Do not copy upstream repository secrets or configure deployment credentials as part of a source sync.

## Workflow policy

Only `ci.yml` is active. It uses GitHub-hosted Ubuntu and macOS runners. Other upstream workflows are absent from `.github/workflows/` so a sync cannot quietly activate a release, deployment, privileged PR event, or Blacksmith job. A removed workflow remains available from the pinned upstream commit. Classify any new upstream workflow before merging the sync PR; default to deferred until its trigger, runner, permissions, and secrets are reviewed.

| Upstream workflow | Main trigger | Jones-Code disposition | Reason |
| --- | --- | --- | --- |
| `ci.yml` | PR, push to `main` | Active | Checks, tests, and builds on GitHub-hosted runners. |
| `cursor-hygiene-webhook.yml` | Push, PR, issue, discussion | Not applicable | Upstream webhook and secrets. |
| `deploy-relay.yml` | Push, manual | Deferred | Deploys a service using upstream credentials. |
| `desktop-macos-preview-publish.yml` | Workflow completion, privileged PR event | Deferred | Publishes preview artifacts with secrets. |
| `desktop-macos-preview.yml` | PR | Deferred | Preview build requires a separate hosted-runner design. |
| `issue-labels.yml` | Push, manual, issue | Not applicable | Upstream issue taxonomy. |
| `mobile-eas-preview.yml` | PR | Deferred | EAS preview publishing and credentials. |
| `mobile-eas-production.yml` | Manual, push | Deferred | Production mobile release and credentials. |
| `mobile-fingerprint-check.yml` | PR | Deferred | Native fingerprint coverage needs a hosted-runner conversion. |
| `mobile-showcase-screenshots.yml` | Manual | Not applicable | Upstream marketing assets. |
| `pr-size.yml` | Privileged PR event | Not applicable | Upstream labeling policy. |
| `pr-vouch.yml` | Privileged PR event, comment, push | Not applicable | Upstream contributor policy and secrets. |
| `publish-aur.yml` | Reusable call, manual | Deferred | Package publication. |
| `release-desktop.yml` | Reusable call | Deferred | Desktop release build and signing secrets. |
| `release.yml` | Push, schedule, manual | Deferred | Scheduled and tag-driven release pipeline. |
| `thread-transfer-report.yml` | Workflow completion | Not applicable | Upstream report publishing. |
| `web-preview.yml` | PR | Deferred | Web preview deployment and credentials. |
| `windows-tests.yml` | Manual | Deferred | Blacksmith Windows runner; Windows coverage is pending. |

The removed `setup-apt-mirrors` action configured a Blacksmith-specific apt mirror. CI now uses the hosted Ubuntu image's apt sources. The active checks do not create desktop or CLI release artifacts, deploy the relay, run Windows tests, or run the mobile fingerprint check. Those are explicit coverage and delivery gaps. Restore a deferred workflow only in a separate reviewed PR, replace its runner and credentials with Jones-Code-owned capabilities, and verify its first run. Do not re-enable upstream schedules or privileged events as a side effect of merging upstream.

## Upstream sync procedure

1. Fetch `upstream` and record its exact `main` commit. Compare it with the last upstream commit incorporated into Jones-Code. Inspect upstream release notes and changes to contracts, migrations, provider runtimes, CI, and new or modified `.github/workflows/` files.
2. Check open Jones-Code PRs for overlapping paths and behavior. Create `sync/upstream-YYYYMMDD` from current Jones-Code `main` in a new linked worktree, with one Git owner. Merge the pinned upstream commit into that branch with a merge commit. Resolve conflicts in that worktree; preserve intentional Jones-Code behavior and record each nontrivial choice in the PR.
3. Reapply the workflow policy above. Keep removed workflows removed, classify any new ones, and convert changes to `ci.yml` to GitHub-hosted runners. Confirm `.github/workflows/` contains only `ci.yml`, it has no Blacksmith labels or scheduled trigger, and no active step depends on the removed apt action. Keep a newly needed workflow disabled until its separate review.
4. Run the smallest affected local checks in the isolated worktree. Let GitHub-hosted CI run the full suite on the PR. Inspect the diff, migration direction, required checks, and the exact PR head. Merge the sync PR using a merge commit so upstream ancestry stays visible. Read back `main` and record both the Jones-Code merge commit and pinned upstream commit in the PR.
5. When CI is slower or fails after a runner change, measure the failing job and adjust its runner, sharding, or timeout through a focused PR. Do not silently skip coverage. If a workflow unexpectedly runs or requests a Blacksmith runner, disable Actions while reconciling that exact run and source revision.

Sync regularly during active development and before a large feature branch diverges far from upstream. A monthly check is a reasonable idle cadence; a security or compatibility fix may justify an earlier sync. A sync is a source integration, not a deployment or an update of any running laptop or VPS installation.

## Moving work from the old fork

Keep `Jones-Systems/t3code` and its open PRs intact while reviewing its delta. Inventory each useful change against current upstream; skip obsolete CI changes and duplicates. Decide whether the old Workstreams history is integrated as a coherent merge or ported selectively only after a commit inventory, merge-tree dry run, contract and migration review, and overlap review. Recreate needed PRs in Jones-Code from the bottom of each dependency stack, linking back to the original PR and carrying its unresolved review points. GitHub cannot directly retarget an old-fork PR to this unrelated repository. Closing or archiving old work is a later, separate decision.

## Functional follow-ups

The first functional change is web/server provider-switch continuity: a compatible account switch on a stopped thread must resume from its saved context without a user message that identifies the thread. The T3 thread ID is stable across account changes; a provider instance ID identifies the saved account configuration, not the short-lived server process. Preserve the native conversation cursor when the new account can access the same session store. Supplying a bounded summary of T3's own history to a genuinely fresh provider session is a separate later improvement, not equivalent to native resume. The current mobile account picker only offers the selected account. Mobile account-switch parity is a later client change; the server behavior should work for a thread previously switched on web.

The next proposed slice adds **Kill Thread** immediately below **Settle Thread** in the web thread menu. It must stop only the selected provider session without settling, hiding, or changing pin/snooze state. The existing stop command does not by itself prove that the provider's parent process and descendants exited. Before using the Kill label, the implementation must establish provider-owned process termination and a verifiable result for the supported providers. This is a separate change that can be stacked on the provider-switch PR.
