# Install T3 Code

T3 Code runs coding agents on your computer (or server) and lets you control them from the desktop app, web app, or mobile app. Install T3 Code on the machine where your agents will run first.

> **Jones Code:** every installer on this page installs upstream T3 Code, not
> Jones Code. That includes the `install.sh` and `install.ps1` scripts, `npx t3`,
> GitHub Releases, package managers, the Intel Mac source build, and the mobile
> store apps. Jones Code supports Apple Silicon Macs and Linux; Windows and Intel
> Macs are unsupported.
>
> - **Get Jones Code:** Mac builds are preview DMGs from the
>   [Mac desktop artifact](../operations/build-artifacts.md#mac-desktop-artifact)
>   workflow, kept for 7 days. The app inside is ad hoc signed, with no Developer
>   ID signing or notarization. Linux uses the
>   [CLI artifacts](../operations/build-artifacts.md).
> - **Update Jones Code:** use **Install status** in the app, as described in
>   [Updating Jones Code](./updating.md#install-status). Do not drag a newer DMG
>   over an installed copy; startup refuses it. If you already completed a
>   manual install, [clear the old update plan](./updating.md#clear-an-old-plan-after-a-manual-install).
> - **Run it in the background:** use `t3 jones host setup` from
>   [Running in the background](./background-service.md), not `t3 service install`.
>
> On a Mac that has Jones Code, do not run the upstream commands on this page.
> `install.sh` replaces a `~/.local/bin/t3` link that points at the Jones app's
> launcher in `~/.t3/bin/t3`, so `t3` runs upstream T3 Code instead. `npx t3`
> and the upstream Homebrew cask install upstream builds. `t3 update` and
> `t3 uninstall` refuse a Jones runtime. Older Jones builds, including every
> build made before November 17, 2026, lack the uninstall check: there,
> `t3 uninstall` removes `~/.t3/runtime`, which holds the Jones install record
> and update recovery data. Both apps share the default
> `~/.t3` home.
>
> The preview DMG uses the same bundle identifier as upstream T3 Code. If both
> apps are installed, verify which one opens `t3code://` links and app lookups
> before using them.

## Requirements

You need at least one installed, authenticated coding agent (provider) before starting a thread. You can launch T3 Code first and set up providers from the [Welcome page](./welcome-page.md) or **Settings → Providers**.

## Command line

Install the `t3` command to `~/.local/bin`:

```bash
# macOS / Linux
curl -fsSL https://t3.codes/install.sh | sh
```

```powershell
# Windows (PowerShell)
irm https://t3.codes/install.ps1 | iex
```

Set `T3CODE_CHANNEL=nightly` for nightly builds or `T3CODE_VERSION=<ver>` to pin a version. To try once without installing, run `npx t3@latest` (requires Node.js; always runs the latest stable release).

| Task                          | Command                                                                 |
| ----------------------------- | ----------------------------------------------------------------------- |
| Start server and open web app | `t3` (or `t3 ./my-project`)                                             |
| Start server without browser  | `t3 serve`                                                              |
| Set up remote access          | `t3 up` ([Remote access](./remote-access.md))                           |
| Run in the background         | `t3 service install` ([Background service](./background-service.md))    |
| Update                        | `t3 update`                                                             |
| Reclaim database space        | `t3 doctor` ([Details](./background-service.md#reclaim-database-space)) |
| Uninstall                     | `t3 uninstall`                                                          |

If `t3` reports a server is already running, connect with `t3 pair` or pass a separate `--base-dir`. Run `t3 --help` for all commands.

### Intel Macs

There is no standalone `t3` binary for Intel Macs—use the desktop app, or build a server from source with `vp` ([development runbook](../operations/00_development.md#first-checkout)):

```bash
git clone https://github.com/pingdotgg/t3code
cd t3code && vp i && vp run build:desktop
node apps/server/dist/bin.mjs
```

## Desktop app

Download from [GitHub Releases](https://github.com/pingdotgg/t3code/releases) or install via a package manager:

| Platform                | Command                                                          |
| ----------------------- | ---------------------------------------------------------------- |
| macOS                   | `brew install --cask t3-code`                                    |
| Windows                 | `winget install T3Tools.T3Code` or `scoop install extras/t3code` |
| Debian / Ubuntu         | `sudo apt install ./T3-Code-*.deb`                               |
| Arch Linux              | `yay -S t3code-bin` (or `yay -S t3code-nightly-bin`)             |
| Nix (flakes, Nix 2.30+) | `nix profile add github:NixOS/nixpkgs/nixos-unstable#t3code`     |

- **Linux AppImage**: `chmod +x ./T3-Code-*.AppImage && ./T3-Code-*.AppImage` (x86_64 and ARM64). `.deb` self-updates with a password prompt; update AUR packages via your AUR helper (`yay -Syu`).
- **NixOS / Home Manager**: Add `pkgs.t3code` to `environment.systemPackages` or `home.packages` (`t3code-desktop` launches the app; `t3` is included). Update via Nix rather than the in-app updater.

### The `t3` command

The desktop app includes the `t3` command-line tool. A new install sets it up
on first launch; after an update, or any time, choose **Install** next to
**t3 command** in **Settings → About**. On macOS and Linux it adds a `t3` link
in `~/.local/bin` (or `~/bin`); on Windows it adds the app's command folder to
your `PATH`. Open a new terminal afterwards. If the folder is not on your
`PATH`, **Add to PATH** on the Welcome page or in **Settings → About** adds one
line to your shell's startup file. **Remove** deletes the `t3` link, and the app
does not add it again. On macOS and Linux it leaves the `PATH` line in place;
delete the line marked `# Added by T3 Code` yourself if you no longer want it. If you already have another `t3`, it stays as it is.

### Windows Subsystem for Linux

Choose a WSL distro in **Settings → Sharing & access** to run agents and projects
there. Install the provider CLIs inside that distro. T3 Code installs its own
server runtime there automatically; the first launch after an app update can
take longer.

### Open a project from a terminal

- **Open a folder in the running desktop app**: Run `t3 app` (or `t3 app ../my-project`) from a terminal, or drag folders from your file manager onto the desktop sidebar.

### Windows Subsystem for Linux

Select a WSL distro in **Settings → Sharing & access** to run agents and projects inside WSL (install provider CLIs inside that distro). T3 Code manages its own server runtime inside WSL automatically.

## Mobile app

Install from the [App Store](https://apps.apple.com/us/app/t3-code-remote-claude-more/id6787819824) or [Google Play](https://play.google.com/store/apps/details?id=com.t3tools.t3code) (QR codes are also in **Settings → About → Mobile app**). The mobile app connects to a server on another machine via [remote access](./remote-access.md).

- Store builds work with both Stable and Nightly servers. Optional Android beta APKs are under [Android beta releases](https://github.com/pingdotgg/t3code/releases?q=android-beta-).
- If the mobile app crashes on launch, open **Settings → Diagnostics** on the next launch to copy recent startup crash reports.

## Providers

Open **Settings → Providers** (web or desktop), select the environment, and enable the providers you use. Installation and sign-in always happen on the **environment's machine**.

| Provider         | Install and authenticate                                                                                                 | Guide                                     |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------- |
| **Codex**        | Connect with ChatGPT in Settings, or install [Codex CLI](https://developers.openai.com/codex/cli) and run `codex login`. | [Codex](./providers-codex.md)             |
| **Claude**       | Install [Claude Code](https://claude.com/product/claude-code) and run `claude auth login`.                               | [Claude](./providers-claude.md)           |
| **Cursor**       | Sign in in Settings or install [Cursor CLI](https://cursor.com/cli) and run `agent login`.                               | [Cursor](./cursor.md)                     |
| **Grok Build**   | Install [Grok Build CLI](https://x.ai/cli) and run `grok login`.                                                         | [Grok](./providers-grok.md)               |
| **Antigravity**  | Install managed runtime and sign in with Google in Settings.                                                             | [Antigravity](./providers-antigravity.md) |
| **OpenCode**     | Install [OpenCode](https://opencode.ai) and run `opencode auth login`.                                                   | [OpenCode](./providers-opencode.md)       |
| **Kiro**         | Install [Kiro CLI](https://kiro.dev/docs/cli/) and run `kiro-cli login`.                                                 | [Kiro](./providers-kiro.md)               |
| **Pi**           | Install [Pi](./providers-pi.md) (Node.js 22.19+) and run `pi` → `/login`.                                                | [Pi](./providers-pi.md)                   |
| **Muse Code**    | Install [Muse Code](https://dev.meta.ai/docs/muse-code) and run `muse login`.                                            | [Muse Code](./providers-muse.md)          |
| **fx**           | Install [fx](https://fx.sh) and run `fx login`.                                                                          | [fx](./providers-fx.md)                   |
| **ACP Registry** | Add GitHub Copilot and other agents in **Settings → Providers → Add provider → Show more**.                              | [ACP](./providers-acp.md)                 |

- **Binary path**: If a CLI isn't on the server's `PATH` (common with version managers), set **Binary path** in the provider's settings.
- **Updates**: Click **Update now** on outdated provider cards (supports Homebrew, mise `latest`, npm/pnpm/Yarn/Bun/Volta/Vite+, or the CLI's own updater; update Nix, Chocolatey, or pinned mise installs manually).
- **Multiple instances**: Add extra provider instances for separate accounts, API keys, or custom endpoints (mark secret env vars as sensitive).

### Provider check timeouts

If a working CLI times out during T3 Code's readiness check, increase its probe deadline (in milliseconds) in the server's environment (or systemd override / `service.env`) and restart:

```bash
T3CODE_CODEX_PROBE_TIMEOUT_MS=60000 T3CODE_CLAUDE_PROBE_TIMEOUT_MS=60000 t3 serve
```

| Provider     | Environment variable                   |
| ------------ | -------------------------------------- |
| Codex        | `T3CODE_CODEX_PROBE_TIMEOUT_MS`        |
| Claude       | `T3CODE_CLAUDE_PROBE_TIMEOUT_MS`       |
| Cursor       | `T3CODE_CURSOR_PROBE_TIMEOUT_MS`       |
| Antigravity  | `T3CODE_ANTIGRAVITY_PROBE_TIMEOUT_MS`  |
| Grok         | `T3CODE_GROK_PROBE_TIMEOUT_MS`         |
| Kiro         | `T3CODE_KIRO_PROBE_TIMEOUT_MS`         |
| fx           | `T3CODE_FX_PROBE_TIMEOUT_MS`           |
| Pi           | `T3CODE_PI_PROBE_TIMEOUT_MS`           |
| Muse Code    | `T3CODE_MUSE_PROBE_TIMEOUT_MS`         |
| OpenCode     | `T3CODE_OPENCODE_PROBE_TIMEOUT_MS`     |
| ACP Registry | `T3CODE_ACP_REGISTRY_PROBE_TIMEOUT_MS` |

## Next steps

- [User guide index](./README.md): overview of how T3 Code works.
- [Working with threads](./thread-sidebar.md): start tasks and organize parallel work.
- [Permission modes](./permission-modes.md): control when agents ask before acting.
- [Remote access](./remote-access.md): connect from another device.
- [Updating T3 Code](./updating.md): keep apps, servers, and providers up to date.
