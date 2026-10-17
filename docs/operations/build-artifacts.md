# Trial build artifacts

The **Linux CLI artifacts** workflow produces unsigned Linux x64 and arm64
archives for a headless server trial. These are GitHub Actions artifacts retained
for seven days. They are not GitHub Releases, package publications, or a deployed
Jones-Code installation. Native GitHub-hosted Ubuntu 24.04 runners build each
architecture; the minimum compatible Linux environment is not yet established.

Main pushes build trial artifacts for each source change. For a manual trial, run the
workflow from Actions on the default branch, selecting the intended source ref. Pull requests touching the workflow or its selected build
inputs run it automatically. The narrow PR trigger validates packaging; main pushes build every source change. Check both architecture jobs and the source
commit in the run summary. PR runs build GitHub's merge ref; `SOURCE_COMMIT` records
the exact built commit, which may differ from the PR head.

Each successful job packages the executable, web client, resource monitor, and
native runtime dependencies, then extracts the archive and verifies `--version`
and a loopback HTTP response with an empty executable search path and isolated
home. No Node installation is required to run the resulting archive. The smoke
check does not prove provider login, remote access, or compatibility with a real
user database. Build and smoke scratch live inside the disposable runner workspace;
scoped script cleanup and the final always-run cleanup own their removal. GitHub
owns disposal of the runner if the job is forcibly terminated.

## Download and verify

Choose a successful run in **Actions → Linux CLI artifacts**, download the artifact
for your architecture, and unzip it into a new empty directory. Alternatively:

```bash
gh run download RUN_ID --repo Jones-Systems/Jones-Code \
  --name jones-code-cli-linux-x64-RUN_ID-RUN_ATTEMPT--VERSION --dir ./jones-code-trial
cd ./jones-code-trial
sha256sum --check SHA256SUMS
cat SOURCE_COMMIT
```

Replace the placeholders with the run's identifiers and preview version; use `arm64` on an ARM host.
Confirm the recorded source matches the intended workflow checkout. Checksums
detect download corruption; these unsigned archives have no independent signature.

## Isolated manual trial

Only run a build whose source you trust. On a separate approved test environment,
extract the archive and run it in the foreground with a new, empty home. Replace
`VERSION` with the preview version in the archive filename. Choose an unused
loopback port; this example uses 47991.

```bash
tar -xzf t3-VERSION-linux-x64.tar.gz
mkdir trial-home
env -i PATH=/usr/bin:/bin HOME="$PWD/trial-home" \
  T3CODE_HOME="$PWD/trial-home" \
  "$PWD/t3-VERSION-linux-x64/t3" serve \
  --host 127.0.0.1 --port 47991 --no-browser
```

Stop this foreground process with Ctrl-C. Keep the extracted tree together because
the executable needs its adjacent files. The trial directory and its home are
owned by the person running the trial; retain them only as long as needed and
remove only that exact directory after stopping the trial. Do not copy credentials
or real user state into this first smoke trial.

Versions use `BASE-preview.YYYYMMDD.RUN_ID.RUN_ATTEMPT`, where `BASE` is the
checked-in server package version. Both the CLI channel parser and desktop build
recognize that preview form. The archive retains T3 branding. Each workflow run
stamps its own run ID, so the Linux CLI, Mac CLI and Mac desktop builds of one
commit carry different versions; compare hosts by the recorded source commit and
tree, not the version string. Rebuilding a commit produces a new version. These Actions
outputs do not create GitHub Releases or populate a release channel: download
another successful workflow artifact for subsequent trials. Once an artifact
expires it cannot be downloaded again, so keep any build needed for rollback or a
later install in a retained local copy. Upstream installers and release discovery
remain separate from these Jones trial workflows: `scripts/install.sh` and
`scripts/install.ps1` resolve `pingdotgg/t3code` Releases and install upstream
T3 Code, never a Jones build. On a Jones preview runtime, `t3 update` refuses a
version-only release update.

Publishing this workflow does not authorize a VPS service restart, installation,
replacement of an active binary, or use of the real T3 home. Those steps need a
separate decision covering the target, state backup, rollback, and verification.

## Mac CLI artifact

The separate **Mac CLI artifact** workflow builds a headless Apple Silicon CLI
archive on a GitHub-hosted `macos-15` runner and asserts the runner is `arm64`.
It runs only through manual dispatch: no main push or pull request starts it.
Once this workflow policy has been reviewed and a native run is authorized,
select the intended source ref in Actions. A successful job packages
`t3-VERSION-darwin-arm64.tar.gz`, then extracts it and runs `--version` and a real
loopback server with an empty `PATH` and an isolated home. Its build and smoke
scratch use the exact workspace directory `.artifact-cli-mac-scratch`; scoped
script cleanup and the always-run job cleanup own removal, with disposable-runner
teardown covering forced termination.

Download the successful run's seven-day artifact into a new empty directory and
verify it on a Mac:

```bash
gh run download RUN_ID --repo Jones-Systems/Jones-Code \
  --name jones-code-cli-mac-arm64-RUN_ID-RUN_ATTEMPT--VERSION --dir ./jones-code-mac-trial
cd ./jones-code-mac-trial
shasum -a 256 -c SHA256SUMS
cat SOURCE_COMMIT
cat ARTIFACT.json
```

Confirm the source, version, architecture, checksum, and run identity before an
approved isolated foreground trial. Use the manual trial procedure above with
`t3-VERSION-darwin-arm64.tar.gz` and its matching extracted directory, keeping all
adjacent runtime files together. The archive builder ad hoc signs the CLI and
native dependencies. `ARTIFACT.json` records `signing: "ad-hoc"`, `signed: false`,
and `notarized: false`: `signed` means publisher-authenticated signing, which ad
hoc signing does not provide. There is no Developer ID identity, notarization,
GitHub Release, or release feed.

Workflow source alone proves no successful native artifact. A successful smoke
run would establish native archive startup and loopback serving, but not
Gatekeeper acceptance, provider login or work, private pairing, installed service
startup, boot behavior, or host rollout. Those require separately authorized
native evidence. Publishing this manual workflow does not authorize dispatch or
installation.

## Mac desktop artifact

The companion **Mac Desktop Artifact** workflow builds an Apple Silicon
DMG on `macos-15`. It runs on main pushes and manual dispatch, and on matching pull requests whose
base is `main` or `work/jones-rebuild/staging` and whose head belongs to the same repository. Other dependency-base
pull requests do not trigger this workflow;
use a separate manual run for DMG evidence when needed. It follows the same seven-day
Actions download process, with artifact name
`desktop-mac-arm64-RUN_ID-RUN_ATTEMPT--VERSION`. Confirm the source revision in its run
summary and downloaded `SOURCE_COMMIT`/`ARTIFACT.json`. After downloading into an empty directory on a Mac, verify:

```bash
shasum -a 256 -c T3-Code-VERSION-arm64.dmg.sha256
hdiutil verify T3-Code-VERSION-arm64.dmg
```

The DMG has no Developer ID signing or notarization. Container and checksum
validation do not establish Gatekeeper acceptance or successful application
launch. Installation, Gatekeeper changes, application launch against a real T3
home, and replacing an existing Mac installation require a separate approved
trial plan. Download and verification alone do not perform those steps.

### Installed Mac desktop facts

These facts hold for the DMG above and affect any install or update plan:

- The DMG is named `T3-Code-VERSION-arm64.dmg`, but it contains `Jones Code.app`
  with bundle identifier `com.t3tools.t3code`. Upstream T3 Code builds, such as
  `T3 Code (Nightly).app` from the upstream installers or Homebrew cask, use the
  same identifier. Launch Services can then route `t3code://` links and app
  lookups to either app. If both are installed, verify which app Launch
  Services selects before using those links or lookups.
- In one observed laptop trial, Gatekeeper rejected the ad hoc signed bundle;
  first launch there required a deliberate local override.
- There is no Electron or Squirrel update feed. Packaged preview builds use the
  Jones updater instead (`apps/desktop/src/updates/DesktopUpdates.ts`). It
  checks 15 seconds after launch and then every 4 minutes, on the fixed
  `jones-main` channel. It offers successful main-branch runs of this workflow
  with green CI, read through the GitHub CLI. The updater only downloads and
  installs when you choose to, and it does not install on quit.
- The Jones updater needs several things on the laptop. `gh` must be installed
  and authenticated in the login-shell `PATH`. A missing CLI reports
  `gh-missing`; an authentication failure reports `authentication-required`.
  `/usr/bin/python3` runs the activation helper. The
  app must run from a stable, writable Applications folder: updates are refused
  from a mounted DMG, an App Translocation path, or a temporary directory.
- On a packaged install that already has `userdata/statev2.sqlite` and
  `userdata/environment-id`, the Jones updater adopts the install. It records
  the admitted generation in `$T3CODE_HOME/runtime/jones-active-install.json`
  (default `~/.t3`). For builds containing the startup fence, ordinary startup
  is refused when the version or source differs from that record or an
  activation transaction requires reconciliation. A qualified activation trial
  can start with its matching trial descriptor in an allowed transaction phase.
  The startup fence
  (`packages/shared/src/jones/nativeWriterFence.ts`) prints the reason to
  stderr and exits with status 1. For example:
  `The native writer does not match the admitted source generation.` A
  Finder or Dock launch shows no message: the app simply does not appear. So
  simply copying a newer build containing this fence over an adopted install
  does not update its admission record. Use the Jones updater or the
  [native helper's manual-install route](../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts)
  (`--inspect-manual-install` and `--manual-install`) with an approved,
  hash-bound plan. To see a startup refusal while no Jones Code process is
  running, use this terminal command:

  ```bash
  "/Applications/Jones Code.app/Contents/MacOS/Jones Code"
  ```

  Never edit the admission record or transaction files to get past the fence.

- A successful activation keeps the replaced bundle beside the stable app as
  `Jones Code.app.jones-previous-GENERATION`. Staging can also leave
  `Jones Code.app.jones-incoming-GENERATION`. These siblings are recovery
  material. Remove them only after confirming that no update transaction or
  recovery hold still refers to them.

## Public Connect configuration and provenance

All three trial workflows copy the tracked `.env.example` before compiling. It contains
public production Clerk and relay identifiers documented in
[T3 Connect](../internals/t3-connect.md); no credential
or signing material is supplied. Process variables still override those public
inputs. The web client and bundled server embed them; the desktop main process
also embeds the publishable key. An existing bundle reused with `--skip-build`
requires separately recorded public-configuration provenance.

Each workflow uploads `ARTIFACT.json` with repository, built source commit,
version, platform, architecture, artifact filename, SHA-256, run/attempt and
public configuration source. CLI jobs upload `SHA256SUMS` and `SOURCE_COMMIT`;
Mac desktop jobs upload the DMG checksum and `SOURCE_COMMIT`. Compare all three identities
before using an artifact. The source stamp records the committed Git tree even
when build-only package versions have changed the workspace; it refuses a checkout
whose HEAD differs from `GITHUB_SHA`. Public build configuration, archive/container
verification, publisher-authenticated signing, binary login and peer connection are separate checks;
these workflows leave login and peer connection explicitly untested. Select the artifact for the target host's architecture.

A future Jones release needs an approved exact source/tag, distinct version,
per-platform archives, `SHA256SUMS`, source descriptors and desktop update assets
(including ZIP/update metadata where required). Trial artifacts expire and do not
constitute a release feed. Installing or updating each host separately requires
an approved binary/state/service envelope, a consistent state snapshot and a
retained compatible prior binary plus prior state. Preserve the desktop profile
and saved connection data separately from server state. Repointing a launcher
alone does not prove a compatible state rollback.
