# Preview companion setup and qualification

Build the fixed companion variant rather than renaming an ordinary desktop
artifact. It has product name **Jones Preview Companion**, application ID
`com.jonessystems.jonespreviewcompanion`, no OS URL schemes and no automatic
update feed. Its startup marker enforces browser-only mode before services
load. Removing embedded server assets is not required: startup must prove
that none execute.

## Build

Use the exact reviewed source on a supported build host. For an Apple Silicon
Mac, with the repository's Node, Vite+, Rust and Xcode prerequisites satisfied:

```sh
vp run dist:desktop:preview-companion --platform mac --target dmg --arch arm64 \
  --build-version 0.0.45-preview.20261008.1 \
  --output-dir /absolute/task-owned/companion-artifacts
```

Choose a distinct version for each candidate. The packager builds its inputs;
avoid `--skip-build` unless matching source/build evidence is retained. Avoid
`--keep-stage`. Record the source revision, dirty patch identity when applicable,
artifact hash, architecture and public build configuration. The default local
artifact is unsigned; container verification does not prove Gatekeeper
acceptance or successful launch. Do not access signing identities or change
Gatekeeper policy as an implied setup step.

## Install and first launch

First verify the authorized host route and inventory the GUI user/session,
architecture, existing app IDs, application paths, URL handlers, login items,
browser profiles and listeners. Install in a dedicated owner-local destination;
never replace the ordinary Jones/T3 bundle or another project's browser.
The companion connects outward and needs no public CDP or control listener.

Packaged startup binds Electron storage to the platform's app-data directory
under `Jones Preview Companion`, with separate session storage, and application
state to `~/.jones-preview-companion`. It sets those paths before `main.cjs` and
does not inherit ordinary storage overrides. A missing companion configuration
initializes unpaired, with browser-only mode enabled. Invalid metadata or
configuration stops startup. Do not clear the fixed role to recover connectivity.

Before pairing, retain a process/listener/state receipt proving zero local
Jones backend or provider children, no companion orchestration database, and
separation from ordinary settings and activation sockets. Then follow the
[browser-host guide](../user/preview-browser-hosts.md) using owner-performed
authentication against the exact existing server. Do not copy credentials.

## Native qualification and recovery

Exercise one synthetic tab through the existing server: create and attach,
navigate, agent action, human takeover/input, plain-text copy/paste, screenshot
artifact readback, disconnect and explicit retry. Verify the physical host and
connection/attachment generations, no headless fallback, no action replay and
no duplicate tab on reconnect. Confirm rendering while minimized, hidden,
locked and with display sleep separately; source tests cannot prove these Mac
states. Change no login, sleep or power policy implicitly.

Login autostart is a separate bounded operation. Use the owner's GUI session,
one dedicated label and the exact companion executable. Inventory collisions
first, preserve existing launch agents, and make no pre-login availability
claim. A verified launch must precede autostart adoption.

For recovery, stop only the captured companion process. Preserve its isolated
profile and unknown-action evidence. Restore only a previously verified
companion bundle at its dedicated destination, after checking profile/version
compatibility. Never downgrade shared server state, delete a browser profile,
or replace proxy/queue ownership to recover the companion.

Installing the companion does not update the VPS. Server adoption requires its
own exact runtime, compatibility, rollback and environment-identity evidence.
Report source checks, artifact qualification, installed revision and live
browser success separately.
