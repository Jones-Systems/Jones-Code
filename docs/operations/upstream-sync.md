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

CI retains the release smoke checks for package versions, lockfile regeneration, and updater manifests. The relay state-output and Discord published-release workflow integration tests are conditionally omitted while `release.yml` is absent, because they read shell steps from that workflow; restoring the workflow automatically restores both checks. The remaining Discord formatting, HTTP and CLI tests stay enabled. The CLI archive verifier remains in source as an explicit Knip entry and is used by the Linux and Mac CLI artifact workflows. Keep these boundaries when syncing release tooling from upstream.

## Upstream sync procedure

1. Pin the chosen upstream tag and full commit, the receiving branch and full commit, and the previously incorporated upstream commit. Use the owner-assigned receiving branch; do not assume `main` when the workstream targets staging. Inspect the complete delta, including nonconflicting changes to contracts, migrations, dependencies, provider runtimes, browser automation, clients, CI and workflows.
2. Check open Jones-Code PRs for overlapping paths and behavior. Create a dedicated non-main import branch in an isolated linked worktree with one Git owner. Merge the pinned upstream commit with a merge commit. Keep the bottom PR focused on upstream incorporation and conflict resolution, favoring upstream implementations with minimal new divergence. Retain unaffected Jones-owned modules. Preserve repository identity, workflow restrictions, authorization boundaries and separate migration histories; record every necessary exception.
3. Reapply the workflow policy above. Keep disabled upstream workflows disabled and classify new ones. Compare workflows with the reviewed receiving set and account for every trigger, runner or permissions change. Do not activate release/deployment workflows, unreviewed schedules, privileged events, unsupported runners or upstream secrets as a side effect of a source sync.
4. Maintain the repair ledger as each conflict is resolved or changed dependency, API or pathway is inspected; do not wait until the import is finished. Record the affected Jones behavior, source path/change and reason, expected failure or changed result, proposed follow-on PR scope and dependencies, and the check that will prove restoration. Include changes that merge cleanly but break compatibility. Update each entry with its actual PR and verification as restoration progresses. Temporary Jones functionality or compilation failures in the bottom PR are repair obligations, not reasons to rebuild Jones behavior inside it or weaken checks. Restore behavior in moderate, coherent PRs stacked above the import, using the [extracted contribution structure](./contributor-guidance.md#keep-jones-changes-separate-from-upstream). Each feature records its restoration PR and evidence, or M Jones's explicit acceptance of an upstream replacement or deferral. Similar names or APIs do not establish parity.
5. Do not merge the import or any prefix while restoration is unfinished. Run focused checks per layer and the deduplicated affected union on the final composed candidate; CI owns full-suite verification. Qualify upstream behavior and Jones parity, including guards, stored history and applicable clients. For an atomically selected contiguous complete group, apply the standing [atomic stack qualification policy](./contributor-guidance.md#atomic-stack-qualification): the qualified current top must contain every repair and cover the complete group. Map each lower failure to its higher repair and passing top-of-stack check in the ledger. Record resolved lower failures instead of separately fixing each layer's CI or asking for another exception for an eligible stack. Broken prefixes cannot land independently; non-atomic queues require qualification of each group actually landed. Ordinary PRs retain current-head check requirements, and normal merge authority, required reviews and thread resolution still apply. Never bypass checks or alter rulesets to make a stack eligible.
6. Once the complete stack and merge gates are satisfied, use the accepted merge-commit method to preserve upstream ancestry. Read back the receiving ref and integrated tree, and record the pinned upstream and Jones merge commits. Runtime promotion, deployment and live database migration are separate operations.
7. If CI is slower or fails after a runner change, measure the failing job and adjust through a focused PR. Do not silently skip coverage. Stop publication and reconcile any unexpected workflow or unsupported runner. Repository Actions settings require separate authorization.

Keep the feature disposition ledger and verification in the PRs or task-owned
working evidence, not committed agent scratch. If an earlier candidate mixes
import and restoration, preserve its branch and commits and split into new
normal branches/PRs at file or hunk granularity. Do not force-push, reset or delete
the reference candidate as part of this separation.

Sync regularly during active development and before a large feature branch diverges far from upstream. A monthly check is a reasonable idle cadence; a security or compatibility fix may justify an earlier sync. A sync is a source integration, not a deployment or an update of any running laptop or VPS installation.

## Fork-only migrations

Keep upstream and Jones migration histories in separate loaders and tracking
tables. Do not assign a Jones migration the next upstream ID or a large reserved
ID in the upstream stream: a highest-ID loader can then skip future upstream
migrations. Before porting a change from the old fork, verify its migration
identity, schema compatibility and any bridge needed for databases that already
ran it. Source integration does not authorize migration of a running installation.
