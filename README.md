# Jones Code

Jones Code is a standalone public repository derived from
[T3 Code](https://github.com/pingdotgg/t3code). It provides a workspace for
controlling coding agents through web, Electron desktop, and mobile clients.

A Node server owns project files, provider processes, credentials, and durable
thread history. Clients connect to that environment to send work, follow
conversations, inspect changes, and manage projects. Remote clients use the
server's workspace rather than their own machine's filesystem.

## What is in this repository

The source includes:

- Web, desktop, and React Native mobile clients, with shared connection and
  client state in `packages/client-runtime`.
- Provider adapters for Codex, Claude Code, Cursor, Grok, OpenCode, and
  Antigravity.
- Project and thread management, terminals, source-control integration, and
  checkpoint-based workspace diffs and restore.
- Jones-specific Workstreams, Voice, host-status views, saved usage accounting,
  and updater integration.

These describe integrated source, not a guarantee that every feature is
configured or usable in a particular installation. Provider access and
Jones-specific integrations depend on the owning server's configuration,
authentication, and available services.

The development route below starts from this repository's source. Upstream
T3 installers, npm packages, app-store listings, and hosted services are
separate distributions; they do not install Jones Code. Jones trial artifact
workflows produce unsigned, expiring build artifacts rather than published
releases. See [trial build artifacts](docs/operations/build-artifacts.md) for
qualification and isolated trial boundaries.

## Run from source

Use a fresh task branch and linked worktree so development state stays separate
from other checkouts. You need Git, Node `^24.13.1`, and the Vite+ `vp` command.
The repository pins `pnpm@11.10.0`.

### Install `vp`

Follow the official [Vite+ setup guide](https://viteplus.dev/guide/) to install
`vp`. Then prepare a development worktree:

```sh
git clone https://github.com/Jones-Systems/Jones-Code.git
git -C Jones-Code worktree add ../Jones-Code-dev -b dev/local
cd Jones-Code-dev
```

From that worktree's repository root, install dependencies and start the server
and web client:

```sh
vp i
vp run dev
```

Installation writes dependencies and runs repository lifecycle scripts.
The development process creates state in the linked worktree's gitignored
`.t3` directory. Read the selected ports and base directory from the
`[dev-runner]` output, then open the printed pairing URL in your browser.
The bare origin does not authenticate a new browser.

A successful first run displays the paired web client connected to its local
environment. To send an agent turn, configure and authenticate a supported
provider on the machine running the server. Browser access alone does not
supply provider credentials.

These commands were checked against repository source; dependency installation
and first-run behavior were not smoke-tested for this guide.

See the [development runbook](docs/operations/development.md) for state
selection, focused checks, desktop development, test data, and sharing.
See the [mobile README](apps/mobile/README.md) for native client development.
Desktop and mobile builds have additional platform prerequisites.

## Find your way around

| Area                                      | Purpose                                                               |
| ----------------------------------------- | --------------------------------------------------------------------- |
| [Server](apps/server)                     | Provider execution, RPC, orchestration, persistence, and checkpoints. |
| [Web](apps/web)                           | Browser UI and the renderer shared with desktop.                      |
| [Desktop](apps/desktop)                   | Electron shell and bundled-server integration.                        |
| [Mobile](apps/mobile)                     | React Native client and native build instructions.                    |
| [Contracts](packages/contracts)           | Schemas shared across clients and servers.                            |
| [Client runtime](packages/client-runtime) | Shared connections and client state.                                  |

Start with the [architecture overview](docs/internals/overview.md) for ownership
and lifecycle boundaries, or the [glossary](docs/internals/glossary.md) for
shared terms. The [documentation index](docs/README.md) links user guides and
maintainer procedures. Some inherited installation, hosting, and release
guides describe upstream T3 services; consult the development runbook for this
repository's source setup.

Coding agents should start at [AGENTS.md](AGENTS.md). Contribution details live
in [contributor guidance](docs/operations/contributor-guidance.md).

## Help and contributions

Report non-sensitive Jones Code bugs through
[repository issues](https://github.com/Jones-Systems/Jones-Code/issues).
Include reproduction steps, the source or build revision, and what you
observed. Keep credentials, pairing URLs, private conversations, and private
host data out of public reports.

Read the [contribution policy](CONTRIBUTING.md) before proposing work.
Agree on direction and scope with the Jones Code maintainer where approval
is required. That inherited policy contains upstream discussion and review
references; it does not establish a Jones-specific discussion service or
override this repository's local agent guidance.

The existing [security policy](.github/SECURITY.md) covers upstream T3 Code
and T3 Tools-operated infrastructure. It does not identify a private reporting
channel for Jones-specific vulnerabilities. Do not put sensitive vulnerability
details in public issues.

## License

[MIT](LICENSE). The license retains the upstream T3 Tools copyright notice.
