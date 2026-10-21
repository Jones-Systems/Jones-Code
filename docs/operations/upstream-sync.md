# Maintaining Jones-Code

Jones-Code is a standalone public repository based on [T3 Code](https://github.com/pingdotgg/t3code). It is not a GitHub fork. The initial source is the unmodified upstream commit `d2c9281b8112dc3b2991642c4bdb985e4b08b9bb`; local changes begin in pull requests after that commit. The selected conclusions from the retired development work in `Jones-Systems/t3code` are preserved below; that legacy repository is not required for ongoing source syncs.

## Remotes and boundaries

In the Jones-Code development clone, `origin` is `Jones-Systems/Jones-Code` and `upstream` is `pingdotgg/t3code`. Disable pushing to `upstream`. The historical `oldfork` remote is not required; if retained, keep pushing disabled. Work on a task branch in an isolated linked worktree; keep one Git owner for each worktree. Do not edit `main`, force-push, or update a protected ref to perform a sync.

This repository starts public so its pull request checks can use GitHub-hosted Actions. Public source and Git history remain public even if the repository later becomes private. Keep credentials, private host data, deployment configuration, and machine-specific state out of commits. Do not copy upstream repository secrets or configure deployment credentials as part of a source sync.

## Legacy fork retirement record

On 2026-10-20, M Jones selected the useful conclusions from the nine open legacy
`Jones-Systems/t3code` PRs for preservation here and discarded remaining legacy
work. This record covers those PRs, not the repository's other metadata. It
records source lineage and historical decisions, not deletion, installation,
current behavioral parity, or runtime qualification. Successor PR links are the
implementation records; the legacy links are optional provenance, not required
reading.

| Legacy PR                                                                         | Exact legacy head                          | Merged Jones-Code successors and merge commits                                                                                                                                                                                                                                                                                            |
| --------------------------------------------------------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [#1](https://github.com/Jones-Systems/t3code/pull/1), conversation library        | `8904051fd3d28ba5c1be1858a6c5450d8c665269` | [#23](https://github.com/Jones-Systems/Jones-Code/pull/23) `6a79a9157e8a3add80abf81ff3c664997b79dde7`; [#30](https://github.com/Jones-Systems/Jones-Code/pull/30) `d6b7b39ed43ca232f87c13ecddab4242f6f2aeef`; nightly restoration [#124](https://github.com/Jones-Systems/Jones-Code/pull/124) `c36dc815f9a608e9e12348a4020a85e23de8ea88` |
| [#5](https://github.com/Jones-Systems/t3code/pull/5), registry placements         | `4a28895224abb98e05c175ab5ff0cb1eb7da4b0c` | Scoped port [#22](https://github.com/Jones-Systems/Jones-Code/pull/22) `cb481b9e727d6a4f4595528bcdc758b49d675c84`                                                                                                                                                                                                                         |
| [#7](https://github.com/Jones-Systems/t3code/pull/7), goal-aware Stop             | `85ec3d5433f8b9e8874ac37e18f2ebf33ea04f94` | [#24](https://github.com/Jones-Systems/Jones-Code/pull/24) `75cace29e55a50bc3a1fdf8449af50e77cdcc8f1`; bounded Stop restoration [#103](https://github.com/Jones-Systems/Jones-Code/pull/103) `03c46f47176993c0c78ed7c95d585ae8a0e0e9f2`                                                                                                   |
| [#14](https://github.com/Jones-Systems/t3code/pull/14), observed runtime identity | `2de030b47b8df410a4077b3ffd432f5f94b1101a` | Adaptation [#27](https://github.com/Jones-Systems/Jones-Code/pull/27) `b2ee8574659d849298ed6ceb2241403619021b09`; [#116](https://github.com/Jones-Systems/Jones-Code/pull/116) `eed58ca79ba5fd12e91ee69e5137c352712d1b1b`                                                                                                                 |
| [#15](https://github.com/Jones-Systems/t3code/pull/15), physical-checkout leases  | `a1760691e42f7f3cc44b8c53e141e70f8014b0ed` | Port [#13](https://github.com/Jones-Systems/Jones-Code/pull/13) `3cb528448ba681f693f9abee7b861cd1a6d268e8`                                                                                                                                                                                                                                |
| [#16](https://github.com/Jones-Systems/t3code/pull/16), terminal turn evidence    | `3dc5705452abba9dcf6fcb87840cf2da409fdf76` | Exact-head adaptation [#11](https://github.com/Jones-Systems/Jones-Code/pull/11) `4b22e1e6c02df5ee8067ca1a088a33fb890cf58d`                                                                                                                                                                                                               |
| [#17](https://github.com/Jones-Systems/t3code/pull/17), Workstream lifecycle      | `eb9a9d599aa2eb7f19f4f280881cf9c41cbd19fe` | Scoped port [#22](https://github.com/Jones-Systems/Jones-Code/pull/22) `cb481b9e727d6a4f4595528bcdc758b49d675c84`                                                                                                                                                                                                                         |
| [#19](https://github.com/Jones-Systems/t3code/pull/19), conversation HTTP API     | `fb26fb259f7613b40e7e6a9df91786fef1d31067` | Semantic mapping to [#23](https://github.com/Jones-Systems/Jones-Code/pull/23) `6a79a9157e8a3add80abf81ff3c664997b79dde7`, restored by [#124](https://github.com/Jones-Systems/Jones-Code/pull/124) `c36dc815f9a608e9e12348a4020a85e23de8ea88`; exact old-head cherry-pick or file equality is unproved                                   |
| [#22](https://github.com/Jones-Systems/t3code/pull/22), cookie-origin restriction | `dc4bfd68c4a4c853aa6dc4ae20ad640f18da7d12` | No direct port identified or required by this retirement. The receiving source already includes upstream guard commit [`48595c787cf5ffe3fcc089ce36754d7c601e4007`](https://github.com/Jones-Systems/Jones-Code/commit/48595c787cf5ffe3fcc089ce36754d7c601e4007); see the finding disposition below.                                       |

The decisions retained for future syncs are:

- **Conversation imports (#1/#19):** import explicitly supplied exports into a
  separate local conversation database, outside coding-thread history. Local
  removal does not remove source-account content. Import is not live extraction,
  monitoring or sending. Authenticate before body/store access; reads require
  read authority and mutations also require operate authority. Bound streamed
  bytes and close acquired stores. `accountId` groups imported sources within an
  environment; it is not per-user isolation. The historical payload bound was
  16 MiB. Successor #23 describes the corrected HTTP predecessor's contract,
  supporting the semantic mapping without proving an exact port of #19's head.
- **Registry and Workstreams (#5/#17):** bound placement lookup by identity count
  and serialized bytes, and disclose partial coverage. Historical bounds were
  1000 identities and 256 KiB (262144 bytes), not newly verified current limits. Bind
  cancellation, reconciliation and multi-step reorder to one canonical
  authorization/controller identity. Thread settlement, member disposition, PR
  state and Workstream completion are separate facts. Completion needs
  snapshot-bound owner evidence; settlement needs current attestation and
  mutation permission. Read-only bindings expose no mutation controls, and
  restoration remains possible. Successor #22's source delivery left configured
  groups in the full app, physical pointer dragging and mobile-device execution
  unverified; its merge does not supply that evidence.
- **Stop and terminal evidence (#7/#16):** user Stop pauses an active native goal
  before bounded child/root interruption; internal recovery interruption remains
  goal-neutral. Child deadlines must leave time to submit root interruption, and
  timeouts must disclose possibly continuing work. Availability, finalized text
  and checkpoint capture do not prove terminal completion. Preserve attributed
  terminal evidence through persistence, replay and live views; late checkpoints
  must not turn interrupted work into completed work.
- **Identity and leases (#14/#15):** requested model, backend, account or tier is
  not observed runtime identity. Correlate native evidence with driver, instance
  and launch generation; reject stale events and retain unknown/unavailable
  dimensions. Lease the canonical physical checkout to an authoritative thread
  incarnation, acquire atomically, renew with generation checks, reconcile
  startup, and release only after cleanup succeeds. Successor #13 kept Jones
  migrations separate from upstream but left the bridge for databases carrying
  the old upstream-numbered fork migration unproved before runtime activation.
  That historical limitation is not a determination about a current database.

### Historical finding dispositions

Legacy #7's reviewed head `ddd243afed87bd76fc73d6febdb11783989073b5`
had two **P2 BLOCKING** findings: a five-second outer recovery deadline could
cancel ten-second child interruption before submitting the root request, and
ordinary root interruption had no request timeout. Both were remediated and
independently confirmed at legacy head
`85ec3d5433f8b9e8874ac37e18f2ebf33ea04f94`, using a ten-second child
budget, a separate three-second root deadline and a derived fourteen-second
recovery budget. Earlier **STOP-CORRECTNESS-001, P1 BLOCKING** (recovery omitted
live child interruption), **STOP-LIFE-MAINT-001, P4 NON-BLOCKING** (duplicate
suppression), and **STOP-LIFE-MAINT-002, P4 NON-BLOCKING** (overstated comment)
were also remediated and independently confirmed. None was accepted as debt.
These are historical dispositions, not current Jones-Code test results.

Legacy #22 retained a **P2 deferred compatibility finding** for plain-HTTP LAN
access through Vite when Fetch Metadata is absent and the LAN origin is not
explicitly allowed. Its body did not claim that topology worked or that a
running application had changed; it did not specify a separate blocking label.
The retained security decision is to check browser origins when ambient cookies
authenticate HTTP/WebSocket requests, because same-site content on another port
can reuse those cookies, while preserving distinct bearer, ticket and native
authentication paths. Upstream commit `48595c787cf5ffe3fcc089ce36754d7c601e4007`
is an ancestor of the receiving Jones-Code revision
`b47d42469e2cd9ad7eaf1975ed1057630159bafd`; that source has a
cookie-credential origin guard in `EnvironmentAuth.ts`. This establishes why an
old implementation need not be imported, not parity of every historical
connection mode. The P2 remains historical and unassessed in the successor;
it is neither declared fixed nor asserted to be a current Jones-Code defect.

Unpublished UI alternatives, expired delivery holds and old CI observations do
not create remaining port obligations under the owner's retirement decision.
Historical check counts are not current verification. Source preservation here
does not perform repository deletion or authorize runtime changes.

## Workflow policy

The Jones-Code workflow allowlist is `ci.yml`, `artifact-cli-linux.yml`, `artifact-cli-mac.yml`, `artifact-desktop-mac.yml`, and `jones-update-qualification.yml`, using GitHub-hosted Ubuntu and macOS runners. CI runs on pull requests, main pushes, and manual dispatch. The Linux CLI and Mac desktop artifact workflows also run on main pushes, matching pull requests, and manual dispatch; the Mac desktop PR trigger is restricted to bases `main` and `work/jones-rebuild/staging`. The separate Mac CLI artifact workflow runs only on manual dispatch, with no push or PR trigger. There are no tag, release, schedule, privileged PR, workflow-completion, or repository-dispatch triggers. The Jones update qualification workflow runs affected regression and strict boundary checks on matching pull requests and manual dispatch. Additional Jones-Code-owned workflows require their own reviewed PRs. Other upstream workflows are absent from `.github/workflows/` so a sync cannot quietly activate a release, deployment, privileged PR event, or Blacksmith job. A removed workflow remains available from the pinned upstream commit. Classify any new upstream workflow before merging the sync PR; default to deferred until its trigger, runner, permissions, and secrets are reviewed.

| Upstream workflow                   | Main trigger                             | Jones-Code disposition | Reason                                                              |
| ----------------------------------- | ---------------------------------------- | ---------------------- | ------------------------------------------------------------------- |
| `ci.yml`                            | PR, push to `main`                       | Active                 | Checks, tests, and builds on GitHub-hosted runners.                 |
| `cursor-hygiene-webhook.yml`        | Push, PR, issue, discussion              | Not applicable         | Upstream webhook and secrets.                                       |
| `deploy-relay.yml`                  | Push, manual                             | Deferred               | Deploys a service using upstream credentials.                       |
| `desktop-macos-preview-publish.yml` | Workflow completion, privileged PR event | Deferred               | Publishes preview artifacts with secrets.                           |
| `desktop-macos-preview.yml`         | PR                                       | Deferred               | Preview build requires a separate hosted-runner design.             |
| `issue-labels.yml`                  | Push, manual, issue                      | Not applicable         | Upstream issue taxonomy.                                            |
| `mobile-android-apk.yml`            | Schedule, manual                         | Deferred               | Play credential use, release artifact writes and Blacksmith runner. |
| `mobile-eas-preview.yml`            | PR                                       | Deferred               | EAS preview publishing and credentials.                             |
| `mobile-eas-production.yml`         | Manual, push                             | Deferred               | Production mobile release and credentials.                          |
| `mobile-fingerprint-check.yml`      | PR                                       | Deferred               | Native fingerprint coverage needs a hosted-runner conversion.       |
| `mobile-showcase-screenshots.yml`   | Manual                                   | Not applicable         | Upstream marketing assets.                                          |
| `pr-size.yml`                       | Privileged PR event                      | Not applicable         | Upstream labeling policy.                                           |
| `pr-vouch.yml`                      | Privileged PR event, comment, push       | Not applicable         | Upstream contributor policy and secrets.                            |
| `publish-aur.yml`                   | Reusable call, manual                    | Deferred               | Package publication.                                                |
| `release-desktop.yml`               | Reusable call                            | Deferred               | Desktop release build and signing secrets.                          |
| `release.yml`                       | Push, schedule, manual                   | Deferred               | Scheduled and tag-driven release pipeline.                          |
| `thread-transfer-report.yml`        | Workflow completion                      | Not applicable         | Upstream report publishing.                                         |
| `web-preview.yml`                   | PR                                       | Deferred               | Web preview deployment and credentials.                             |
| `windows-tests.yml`                 | Manual                                   | Retired                | Jones Code does not support Windows.                                |

The removed `setup-apt-mirrors` action configured a Blacksmith-specific apt mirror. CI now uses the hosted Ubuntu image's apt sources. Core `ci.yml` checks do not publish desktop or CLI releases, deploy the relay, run Windows tests, or run the mobile fingerprint check. The three artifact workflows build downloadable trial files without publishing a Release. Restore a deferred workflow only in a separate reviewed PR, replace its runner and credentials with Jones-Code-owned capabilities, and verify its first run. Do not re-enable upstream schedules or privileged events as a side effect of merging upstream.

Jones Code supports Apple Silicon macOS and Linux build targets only. Keep Windows
and Intel or universal Mac builds, package shortcuts, and dedicated test runners
disabled during upstream imports. Preserve the Jones target guards in both artifact
builders and the Windows workflow tombstone. Retained upstream helper tests run on
supported hosts; they do not build or execute unsupported platform artifacts.

CI retains the release smoke checks for package versions, lockfile regeneration, and updater manifests. The relay state-output and Discord published-release workflow integration tests are conditionally omitted while `release.yml` is absent, because they read shell steps from that workflow; restoring the workflow automatically restores both checks. The remaining Discord formatting, HTTP and CLI tests stay enabled. The CLI archive verifier remains in source as an explicit Knip entry and is used by the Linux and Mac CLI artifact workflows. Keep these boundaries when syncing release tooling from upstream.

## Upstream sync procedure

1. Pin the chosen upstream tag and full commit, the receiving branch and full commit, and the previously incorporated upstream commit. Use the owner-assigned receiving branch; do not assume `main` when the workstream targets staging. Inspect the complete delta, including nonconflicting changes to contracts, migrations, dependencies, provider runtimes, browser automation, clients, CI and workflows.
2. Check open Jones-Code PRs for overlapping paths and behavior. Create a dedicated non-main import branch in an isolated linked worktree with one Git owner. Merge the pinned upstream commit with a merge commit. Keep the bottom PR focused on upstream incorporation and conflict resolution, favoring upstream implementations with minimal new divergence. Retain unaffected Jones-owned modules. Preserve repository identity, workflow restrictions, authorization boundaries and separate migration histories; record every necessary exception.
3. Reapply the workflow policy above. Keep disabled upstream workflows disabled and classify new ones. Compare workflows with the reviewed receiving set and account for every trigger, runner or permissions change. Do not activate release/deployment workflows, unreviewed schedules, privileged events, unsupported runners or upstream secrets as a side effect of a source sync. Also review the non-workflow install surfaces the Jones update feed and host adoption depend on: the artifact workflows' version stamp, artifact names and `ARTIFACT.json` fields; `scripts/stamp-jones-build.ts`; `scripts/update-release-package-versions.ts`; `scripts/build-cli-archive.ts`; `scripts/build-desktop-artifact.ts` (update provider and DMG name); `packages/shared/src/cliRelease.ts`; `packages/shared/src/jones/jonesActions.ts`; and `apps/server/src/jones/hostService/`. A changed name, version pattern or field can make new main builds silently ineligible for updates. `scripts/install.sh`, `scripts/install.ps1` and `docs/operations/release.md` stay upstream-only; keep them pointed at upstream and do not present them as Jones install paths.
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
migrations. For a database carrying a historical fork migration, verify its
migration identity, schema compatibility and any bridge needed before adopting
the separate Jones migration history. Source integration does not authorize
migration of a running installation.
