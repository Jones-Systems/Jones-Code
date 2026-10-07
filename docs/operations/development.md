# Development

## First checkout

Install `vp` using the [root README](../../README.md#install-vp). The checkout
requires Node `^24.13.1` and declares `pnpm@11.10.0`; Bun is optional.
For an authorized dependency install and development run, use the repository root:

```sh
vp i
vp run dev
```

The `t3.json` Setup Worktree action runs `scripts/setup-worktree.ts`: it installs
dependencies, links the main checkout's gitignored `.env` and `infra/relay/.env`
when present, and warms the web dependency cache. Those are separate effects,
including access to potentially sensitive configuration. An assignment to edit
source does not itself authorize running setup or accessing those files.
Confirm the applicable scope before invoking it; do not print their contents.

Give the printed pairing URL to the authorized tester. The bare origin does not
authenticate a new browser. [Standing local app testing permission](./contributor-guidance.md#local-app-testing-permission)
covers isolated synthetic verification; agents must not consume the user's
pairing token.

Prefer a container? See [Dev container](../internals/devcontainer.md) for VS Code and Codespaces setup.

## Choosing a dev process

Use `vp run dev` for server and web, or `vp run dev:desktop` for the Electron client.
`dev:server` and `dev:web` start those processes separately.
See the [mobile README](../../apps/mobile/README.md) for native builds and Metro.

Flags go directly after the task name, for example
`vp run dev --home-dir /absolute/task-owned/t3-home`.
Use an isolated, task-owned directory with a defined lifetime and cleanup owner.
The [standing testing permission](./contributor-guidance.md#local-app-testing-permission)
covers `--browser` for isolated synthetic verification.

### State and ports

The state-home precedence is an explicit `--home-dir`, then a linked worktree's
gitignored `.t3`, then ambient `T3CODE_HOME`. Without an override, a linked
worktree uses `<worktree>/.t3/userdata`; the main checkout defaults to
`~/.t3/dev/userdata`.

An explicit `--home-dir` bypasses worktree isolation. Confirm its resolved
destination before starting a process, including when reusing an existing
launch command. Never run a development server against live `~/.t3/userdata`.
Reading or copying private state also requires exact authorization.
Follow [test data](#test-data) for isolated fixtures and snapshots.

Read ports and the selected base directory from `[dev-runner]` output.
Worktrees derive stable port preferences from their paths, but occupied ports
can shift them. `T3CODE_PORT_OFFSET` or `T3CODE_DEV_INSTANCE` can select a
different preference when needed.

Track the process you start and stop it by its captured PID. If recovering its
identity from a listening port on Linux, confirm the port owner with
`ss -H -ltnp` and verify `/proc/<pid>/cwd` is the task worktree before stopping
it. A matching name, path, or worktree string is not process ownership.
Never use pattern-based kills or stop another task's server.

### Test data

Prefer synthetic fixtures or a sanitized dataset already authorized for the
task. Use enough representative state to exercise the behavior; an empty
database may miss the relevant case.

Real data, including `~/.t3/userdata` and `~/.t3/dev`, requires explicit
authorization for the exact source and read or copy operation. Agree on the
isolated destination, necessary data, permitted use, retention, and cleanup
before accessing it. Authorization for a database snapshot does not include
credentials, secrets, settings, or unrelated files.

For an authorized SQLite snapshot:

1. Choose a fresh destination under the task-owned T3 home, normally
   `<worktree>/.t3/userdata/statev2.sqlite`. Verify that the destination and its
   parents are isolated from live state and that no server is using them.
   If the destination already exists, stop and reconcile ownership; do not
   remove `statev2.sqlite*` or other state unconditionally.
2. Open the approved source read-only and use SQLite's online backup API or
   `VACUUM INTO` to create a consistent snapshot at the fresh destination.
   `VACUUM INTO` requires a destination file that does not already exist.
   Do not copy a live SQLite file with `cp`, including its WAL or SHM siblings
   as a substitute for a consistent snapshot.
3. Close the source connection and validate the copied V2 database locally.
   The server uses `statev2.sqlite`; a V1 `state.sqlite` copy is not a substitute.
   Run the test server only against the isolated copy.
4. Copy additional files only when the flow requires them and their access
   and copying are separately authorized. In particular, do not bring
   `secrets` or `settings.json` merely to make the copy look complete.
5. Copy data into the sandbox; never symlink runtime state to the source and
   never write test state back. Keep copied private data out of commits,
   logs, screenshots, PR attachments, and other output unless that output
   is separately authorized. Clean up only the exact task-owned destination
   under its agreed lifecycle.

The setup action's configuration symlinks are a separate, explicitly scoped
effect; they are not permission to symlink test state or reuse live credentials.

### Sharing and remote debugging

`vp run dev --share` changes this machine's Tailscale Serve configuration to
publish the web port. It can clear an existing mapping for that port before
creating its own, and the mapping can outlive the process. Obtain authorization
covering the exact shared route and its cleanup before running it. Do not
manually change Tailscale routes as a workaround.

Wait for the runner's actual share result and `pairingUrl:` output. Sharing can
fail while the server continues locally; a running process does not prove
tailnet access. Give the authorized tester the complete pairing URL through
the approved private channel. Do not open or consume the user's pairing token,
or put it in commits, PRs, or public output. A browser already holding the
reusable dev cookie can use the bare origin.

The runner attempts to remove its mapping on exit, but cleanup can fail and
overlapping runs can affect the same mapping. Verify cleanup through the
authorized route and report any remaining or unknown effect. Do not claim
removal from process exit alone or perform an unapproved manual cleanup.

If an authorized tester consumed a normal one-time token and needs another,
the development server's `node apps/server/src/bin.ts pair` command can mint
one. Run it only against the exact authorized isolated environment. It grants
standard scopes; the startup pairing URL carries administrative scopes needed
for Connections settings.

Leave `VITE_HTTP_URL` and `VITE_WS_URL` unset for browser development.
Vite proxies `/api`, `/ws`, `/oauth`, and `/.well-known` through the browser's
origin, so local and remote browsers reach the owning environment.
The dev runner manages desktop's deliberate loopback URLs; do not apply the
browser configuration to the desktop renderer. `--share` is unsupported for
`dev:desktop`; use an authorized browser development run for sharing.

Shared runs enable bundled dev to avoid a network round trip for each import level.
`T3CODE_BUNDLED_DEV=0` opts out when debugging bundler differences. Two reload traps matter
when changing this setup:

- The web entry must dynamically import the app so React refresh initializes before application
  chunks. Static imports can work on first load and fail after a route split.
- Bundled dev rebuilds Tailwind through watched files. Its ordinary Vite hot-update hook expects
  a server/module graph that Rolldown does not provide.

The workarounds live in the [web entry](../../apps/web/src/bootstrap.ts) and
[Tailwind plugin](../../apps/web/vite/tailwind.ts).

#### Reusable dev credential

Configure or reuse this credential only when the task authorizes the exact
configuration access and change. This procedure does not grant permission to
read existing `.env` values, copy credentials, or start a shared server.
Browser verification is covered separately by [standing local app testing
permission](./contributor-guidance.md#local-app-testing-permission); credential
configuration still needs exact authorization.

Use this only on a hostname where you trust every service. Browsers send cookies to all ports
on that hostname. Any service you visit there can receive the reusable admin credential,
including services unrelated to T3 Code. If you run untrusted services on that hostname, keep
normal per-environment pairing instead.

To use one browser profile across web dev worktrees on the same hostname, generate one fixed
value once:

```sh
openssl rand -hex 32
```

Put that value in the main checkout's gitignored `.env`:

```dotenv
T3CODE_DEV_AUTH_TOKEN=<the value generated above>
```

The `t3.json` Setup Worktree action links that file to each worktree's `.env`. The dev runner reads repository env files at startup. `.env.local` and inherited process
environment values override `.env`, so no per-worktree export is needed after setup.

For a manual worktree or launcher without that link, export the same fixed value instead:

```sh
export T3CODE_DEV_AUTH_TOKEN="<the value generated above>"
```

Do not generate a new value at startup. For an authorized shared run, start or
restart `vp run dev --share` after configuration and have the authorized tester
open its printed startup pairing URL once per browser profile on that hostname.
Later web dev servers on the same hostname accept the shared cookie across
ports. The cookie expires after 30 days. Reload an old tab if its URL now serves
a replacement environment.

The token and startup pairing URLs are reusable administrative secrets. Never put them in a
commit, pull request, or public output. Every server still seeds its own auth database record at
startup and keeps its own SQLite data, signing key, and revocation state. Desktop and non-dev
servers ignore the value. See [environment authentication](../internals/environment-auth.md#reusable-dev-credential)
for the security model.

## Checks

Run checks for the files and packages you changed:

```sh
vp test run <files>
vp lint <files>
vp run --filter <package> typecheck
```

Use `vp run lint:mobile` for native mobile changes. CI owns the full suite; see
[ci.yml](../../.github/workflows/ci.yml) for its current jobs.

### Unused code

`vp run knip:check` checks unused files and dependencies across the repo, then
unused runtime exports in `apps/server`, `apps/desktop`, `apps/web`, and every internal package under
`packages/`. CI enforces both checks.
Exported types and Effect schemas are allowed without consumers. The schema preprocessor
recognizes schema types, including aliases and schema classes; functions that create or decode
schemas remain checked. Canonical Effect service construction APIs stay exported with an explicit
`@public` annotation, which Knip recognizes. Completely unused files remain checked too.
Named exports in web UI component modules are kept as complete component sets. Knip ignores
unused exports in `apps/web/src/components/ui/*.tsx`, while still reporting an entire unused file.
Use `vp run knip --workspace apps/web` to audit one workspace, including exports,
or `vp run knip:production --workspace apps/web` to find code kept alive only by tests.
The full export audit still has findings and is not a repo-wide CI gate. Extend the
export check's workspace selectors as more workspaces become clean. Review callers before
deleting code; production mode can also report development scripts and test fixtures.
Runtime-discovered entrypoints and dependency exceptions belong in [knip.jsonc](../../knip.jsonc).

## Desktop artifacts

Local artifact builds are unsigned by default and write to `release/`:

```sh
vp run dist:desktop:dmg
vp run dist:desktop:linux
vp run dist:desktop:win
```

DMGs default to the host architecture. Use `--arch` to choose another target and `--keep-stage`
to retain packaging files for inspection. Run `vp run dist:desktop:artifact --help` for other
options.

### Linux AppImage prerequisites

Build on Linux because the browser-secret helper links against the host's libsecret. Install
Rust, C/C++ build tools, libsecret development headers, pkg-config, and ImageMagick.

Ubuntu and Debian:

```sh
sudo apt-get update
sudo apt-get install cargo rustc build-essential libsecret-1-dev pkg-config imagemagick
```

Fedora:

```sh
sudo dnf install rust cargo gcc gcc-c++ make libsecret-devel pkgconf-pkg-config ImageMagick
```

Arch Linux:

```sh
sudo pacman -S rust base-devel libsecret pkgconf imagemagick
```

The C toolchain, pkg-config, and libsecret headers are also needed for Linux desktop development.

### macOS DMG prerequisites

Install the Xcode Command Line Tools with `xcode-select --install` and install Rust.
For a cross-architecture or universal build, add the requested Rust targets:

```sh
rustup target add aarch64-apple-darwin x86_64-apple-darwin
```

### Windows installer prerequisites

Install Rust, Python 3, and Visual Studio Build Tools with **Desktop development with C++**.
Include the Windows SDK and the MSVC build tools and Spectre-mitigated libraries for the target
architecture. Add its Rust target:

```powershell
rustup target add x86_64-pc-windows-msvc
# For an ARM64 installer:
rustup target add aarch64-pc-windows-msvc
```

NSIS is downloaded by electron-builder. WSL support additionally needs the Linux CLI archive
passed as `--wsl-runtime`; see the
[release runbook](./release.md#windows-payload-topology-and-update-validation).

### Signing and passkeys

Add `--signed` after configuring the platform credentials in the
[release runbook](./release.md). macOS passkeys need a signed, provisioned app; follow the
[Connect setup](./connect-setup.md#desktop-passkeys) for local signing and renderer HMR.
