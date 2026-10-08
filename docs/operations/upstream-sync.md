# Maintaining Jones-Code

Jones-Code is a standalone public repository based on [T3 Code](https://github.com/pingdotgg/t3code). It is not a GitHub fork. The initial source is the unmodified upstream commit `d2c9281b8112dc3b2991642c4bdb985e4b08b9bb`; local changes begin in pull requests after that commit. The old `Jones-Systems/t3code` fork and its pull requests remain separate history until each useful change is reviewed and moved.

## Remotes and boundaries

In the Jones-Code development clone, `origin` is `Jones-Systems/Jones-Code`, `upstream` is `pingdotgg/t3code`, and `oldfork` is `Jones-Systems/t3code`. Disable pushing to `upstream` and `oldfork`. Work on a task branch in an isolated linked worktree; keep one Git owner for each worktree. Do not edit `main`, force-push, or update a protected ref to perform a sync.

This repository starts public so its pull request checks can use GitHub-hosted Actions. Public source and Git history remain public even if the repository later becomes private. Keep credentials, private host data, deployment configuration, and machine-specific state out of commits. Do not copy upstream repository secrets or configure deployment credentials as part of a source sync.

## Workflow policy

The Jones-Code workflow allowlist is `ci.yml`, `artifact-cli-linux.yml`, `artifact-cli-mac.yml`, and `artifact-desktop-mac.yml`, using GitHub-hosted Ubuntu and macOS runners. CI runs on pull requests and main pushes. The Linux CLI and Mac desktop artifact workflows also run on main pushes, matching pull requests, and manual dispatch; the Mac desktop PR trigger is restricted to base `main`. The separate Mac CLI artifact workflow runs only on manual dispatch, with no push or PR trigger. There are no tag, release, schedule, privileged PR, workflow-completion, or repository-dispatch triggers. Additional Jones-Code-owned workflows require their own reviewed PRs. Other upstream workflows are absent from `.github/workflows/` so a sync cannot quietly activate a release, deployment, privileged PR event, or Blacksmith job. A removed workflow remains available from the pinned upstream commit. Classify any new upstream workflow before merging the sync PR; default to deferred until its trigger, runner, permissions, and secrets are reviewed.

| Upstream workflow                   | Main trigger                             | Jones-Code disposition | Reason                                                        |
| ----------------------------------- | ---------------------------------------- | ---------------------- | ------------------------------------------------------------- |
| `ci.yml`                            | PR, push to `main`                       | Active                 | Checks, tests, and builds on GitHub-hosted runners.           |
| `cursor-hygiene-webhook.yml`        | Push, PR, issue, discussion              | Not applicable         | Upstream webhook and secrets.                                 |
| `deploy-relay.yml`                  | Push, manual                             | Deferred               | Deploys a service using upstream credentials.                 |
| `desktop-macos-preview-publish.yml` | Workflow completion, privileged PR event | Deferred               | Publishes preview artifacts with secrets.                     |
| `desktop-macos-preview.yml`         | PR                                       | Deferred               | Preview build requires a separate hosted-runner design.       |
| `issue-labels.yml`                  | Push, manual, issue                      | Not applicable         | Upstream issue taxonomy.                                      |
| `mobile-eas-preview.yml`            | PR                                       | Deferred               | EAS preview publishing and credentials.                       |
| `mobile-eas-production.yml`         | Manual, push                             | Deferred               | Production mobile release and credentials.                    |
| `mobile-fingerprint-check.yml`      | PR                                       | Deferred               | Native fingerprint coverage needs a hosted-runner conversion. |
| `mobile-showcase-screenshots.yml`   | Manual                                   | Not applicable         | Upstream marketing assets.                                    |
| `pr-size.yml`                       | Privileged PR event                      | Not applicable         | Upstream labeling policy.                                     |
| `pr-vouch.yml`                      | Privileged PR event, comment, push       | Not applicable         | Upstream contributor policy and secrets.                      |
| `publish-aur.yml`                   | Reusable call, manual                    | Deferred               | Package publication.                                          |
| `release-desktop.yml`               | Reusable call                            | Deferred               | Desktop release build and signing secrets.                    |
| `release.yml`                       | Push, schedule, manual                   | Deferred               | Scheduled and tag-driven release pipeline.                    |
| `thread-transfer-report.yml`        | Workflow completion                      | Not applicable         | Upstream report publishing.                                   |
| `web-preview.yml`                   | PR                                       | Deferred               | Web preview deployment and credentials.                       |
| `windows-tests.yml`                 | Manual                                   | Deferred               | Blacksmith Windows runner; Windows coverage is pending.       |

The removed `setup-apt-mirrors` action configured a Blacksmith-specific apt mirror. CI now uses the hosted Ubuntu image's apt sources. Core `ci.yml` checks do not publish desktop or CLI releases, deploy the relay, run Windows tests, or run the mobile fingerprint check. The three artifact workflows build downloadable trial files without publishing a Release. Restore a deferred workflow only in a separate reviewed PR, replace its runner and credentials with Jones-Code-owned capabilities, and verify its first run. Do not re-enable upstream schedules or privileged events as a side effect of merging upstream.

CI retains the release smoke checks for package versions, lockfile regeneration, and updater manifests. Only the relay state-output test is omitted while `release.yml` is absent, because it reads a shell step from that workflow; restoring the workflow automatically restores that check. The CLI archive verifier remains in source as an explicit Knip entry and is used by the Linux and Mac CLI artifact workflows. Keep these boundaries when syncing release tooling from upstream.

## Upstream sync procedure

1. Fetch `upstream` and record its exact `main` commit. Compare it with the last upstream commit incorporated into Jones-Code. Inspect upstream release notes and changes to contracts, migrations, provider runtimes, CI, and new or modified `.github/workflows/` files.
2. Check open Jones-Code PRs for overlapping paths and behavior. Create `sync/upstream-YYYYMMDD` from current Jones-Code `main` in a new linked worktree, with one Git owner. Merge the pinned upstream commit into that branch with a merge commit. Resolve conflicts in that worktree; preserve intentional Jones-Code behavior and record each nontrivial choice in the PR.
3. Reapply the workflow policy above. Keep removed upstream workflows removed, classify any new ones, and convert changes to `ci.yml` to GitHub-hosted runners. Compare `.github/workflows/` with the reviewed Jones-Code workflow set at the sync base; account for every addition or trigger change. Confirm no Blacksmith label, unreviewed schedule, or active step depending on the removed apt action appears. Keep a newly needed workflow disabled until its separate review.
4. Run the smallest affected local checks in the isolated worktree. Let GitHub-hosted CI run the full suite on the PR. Inspect the diff, migration direction, required checks, and the exact PR head. Merge the sync PR using a merge commit so upstream ancestry stays visible. Read back `main` and record both the Jones-Code merge commit and pinned upstream commit in the PR.
5. When CI is slower or fails after a runner change, measure the failing job and adjust its runner, sharding, or timeout through a focused PR. Do not silently skip coverage. If a workflow unexpectedly runs or requests an unsupported runner, stop publication and reconcile that exact run and source revision. Changes to repository Actions settings require separate authorization.

Sync regularly during active development and before a large feature branch diverges far from upstream. A monthly check is a reasonable idle cadence; a security or compatibility fix may justify an earlier sync. A sync is a source integration, not a deployment or an update of any running laptop or VPS installation.

## Fork-only migrations

Keep upstream and Jones migration histories in separate loaders and tracking
tables. Do not assign a Jones migration the next upstream ID or a large reserved
ID in the upstream stream: a highest-ID loader can then skip future upstream
migrations. Before porting a change from the old fork, verify its migration
identity, schema compatibility and any bridge needed for databases that already
ran it. Source integration does not authorize migration of a running installation.
