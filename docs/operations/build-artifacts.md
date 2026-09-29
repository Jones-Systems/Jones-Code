# Trial build artifacts

The **Linux CLI artifacts** workflow produces unsigned Linux x64 and arm64
archives for a headless server trial. These are GitHub Actions artifacts retained
for seven days. They are not GitHub Releases, package publications, or a deployed
Jones-Code installation. Native GitHub-hosted Ubuntu 24.04 runners build each
architecture; the minimum compatible Linux environment is not yet established.

Run the workflow manually from Actions once it is on the default branch, selecting
the intended source ref. Pull requests touching the workflow or its selected build
inputs run it automatically. This narrow trigger is packaging validation, not a
build of every application change. Check both architecture jobs and the source
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
  --name jones-code-cli-linux-x64-RUN_ID-RUN_ATTEMPT --dir ./jones-code-trial
cd ./jones-code-trial
sha256sum --check SHA256SUMS
cat SOURCE_COMMIT
```

Replace the placeholders with the run's identifiers; use `arm64` on an ARM host.
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

Versions use `0.0.0-preview.YYYYMMDD.RUN_ID`, which the existing CLI recognizes as
the preview channel. The archive still carries upstream T3 branding and upstream
update endpoints. Do not run `t3 update`, install a service, or use an upstream
installer to manage these builds: an explicit update can replace the fork with an
upstream preview. Download a new successful workflow artifact for subsequent trials.

Publishing this workflow does not authorize a VPS service restart, installation,
replacement of an active binary, or use of the real T3 home. Those steps need a
separate decision covering the target, state backup, rollback, and verification.
