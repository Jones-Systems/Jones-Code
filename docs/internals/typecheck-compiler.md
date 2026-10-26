# Typecheck compiler selection

Workspace CLI commands invoke the verified official Effect-patched TSRS
launcher on all 33 TypeScript surfaces. Root `typecheck` and `tc` run uncached
compiler admission before cached recursive tasks. `pnpm typecheck:incremental`
uses the same pinned compiler and Go assignment in a separate v2 namespace.
Marketing continues using `astro check`.

Manifest workspaces expose `typecheck` as `vp run tsrs:typecheck`; their
`typecheck:native` slot retains the exact verified compiler command. Package
Vite configurations read that slot and apply the shared task-cache policy.
Web and desktop retain their existing `typecheck` task names. For example,
`pnpm --filter effect-acp typecheck` uses the configured task. The launcher accepts only qualified one-shot checking arguments. Root commands still run uncached admission first.
Automatic input and output tracking remain enabled. Only package build-info
files, private `.jones-tsrs-*.tsbuildinfo` files, the ordinary build-info namespace
and the exact workspace patch lock/reclaim files are excluded; source,
configuration, manifests, libraries and compiler binary identity stay tracked.
Cached tasks must never restore mutation locks or build-info files.

The candidate integration remains under
[qualification](https://github.com/Jones-Systems/Jones-Code/issues/711) before
main adoption. Unguarded native build-info reuse can miss a required Effect
diagnostic after an off-to-error configuration change. The ordinary adapter
therefore binds native state to raw configuration bytes and uses private working
files. Its focused regression and representation controls pass; complete current
workflow qualification remains pending.
The initial 33-surface direct screen preserved incumbent
errors/warnings and non-standard-library coverage after constructive fixture
corrections, but complete rule, lifecycle, cache, performance and platform gates
remain. No complete current-workflow speed improvement has been qualified.
Schema-class connection targets in partial supervisor doubles still use genuine
Schema instances; diagnostic settings stay unchanged.

## One-shot CLI scope

The same argument contract applies to the supported compiler and local fallback.
Default project discovery and `-p`/`--project` directory or config-file selection
are supported, along with `--noEmit`, `--pretty`, `--incremental`, and
`--tsBuildInfoFile`. Boolean flags take an optional lowercase `true` or `false`;
repeated options use the last value. The native case-insensitive option names
and one- or two-dash spellings are accepted. `--noEmit false` is outside this
checking interface. Diagnostic output flags are limited to `--diagnostics`,
`--extendedDiagnostics`, `--listFiles`, `--explainFiles`, and `--traceResolution`.
`--version` is informational; `--admit` remains a separate admission command.

Build/watch modes and aliases, response files, positional source files, equals
syntax, null values and unknown options fail explicitly before compiler lookup
or spawn. They cannot bypass project configuration or the build-info guard.
Use `pnpm typecheck:incremental` for repeated retained checks. Editor APIs,
application emission, bundling and runtime entry points retain their existing
interfaces.

## Native state and output compatibility

Incremental/composite one-shot checks retain a checksummed envelope in
`node_modules/.cache/jones/tsrs/build-info/v1`. Its identity binds the compiler,
platform, arguments, logical output and every admitted raw configuration input,
including inherited Effect settings. Existing unbound native files never become
seeds. A cache miss still runs the compiler; nonincremental checks remain
nonincremental. The supported configuration topology is finite: no inheritance,
the terminal worktree base, or the qualified mobile Expo base. Unknown topology,
project references and unqualified output relocation refuse explicitly.

The native working file is created exclusively in the logical output directory.
Only its `options.tsBuildInfoFile` self-reference changes between private and
logical names; other JSON values and their relative-path base stay intact. The
public destination contains raw native build info. Custom destinations must be
`.tsbuildinfo` files beside the project config, and `.jones-tsrs-` is reserved.
Symlinks, hard links, foreign ownership or changed output parents are refused.
Configuration drift cannot yield successful retention. Failed checks preserve
the preceding valid envelope. Short accounting/publication locks do not span
compiler execution.

State lives for the worktree dependency-cache lifetime, with 16 MiB native,
24 MiB envelope and 1 GiB aggregate admission bounds, plus a 4096-entry cap.
Accounting charges external private allocations, crash residue and retained raw
outputs as well as envelopes. Admission is not a kernel write quota. Unresolved
residue or a full budget causes refusal, with no eviction or sweeper. Normal
cleanup removes only invocation-owned files after confirmed exit. Unknown exit
or ownership preserves exact paths/accounting for investigation; do not delete
shared build-info or coordination files to bypass that refusal.

## Publisher and rationale

[Cloudflare's announcement](https://blog.cloudflare.com/typescript-rust-port)
describes Oxc's `tsrs`. The candidate combines that compiler with a replacement
binary published by the **official Effect-TS organization**, which also owns
[Effect](https://github.com/Effect-TS/effect). The replacement is distributed by
[Effect-TS/tsgo](https://github.com/Effect-TS/tsgo/tree/999942fa41462eb46dfcb2d718e7671db8e14156),
not by Cloudflare. Its Rust integration is experimental.

The exact combination is `tsrs@0.18.0-ts7.1.0-dev.20260929` and
`@effect/tsgo@0.57.0`. Effect's
[release manifest](https://github.com/Effect-TS/tsgo/blob/999942fa41462eb46dfcb2d718e7671db8e14156/_packages/tsgo/upstream.json)
binds TSRS source `5f92808bc92bb97b06212153ab73d28d551c0b9b` and TypeScript source
`b85298b6a81f772d080b0455de0ca9d744cd6fd6`. Merely checking Effect's TypeScript
types does not prove preservation of Effect's custom diagnostics. That is why
stock TSRS or a different fast checker cannot replace the incumbent based on
vendor timings alone. Compatibility and total workflow cost decide adoption.

The candidate checks types and diagnostics. It does not speed up runtime code,
replace Vite's bundling, or establish a saving on every build. Fresh compiler
runs, retained checks, and task-cache hits are different workloads.

## Installation and commands

Root `prepare` invokes the verified patch wrapper before `vp config`. In a
fully installed isolated worktree, these commands repair, verify and run it:

```sh
node scripts/jones/tsrs/patch.ts
node scripts/jones/tsrs/patch.ts --verify
node scripts/jones/tsrs/typecheck.ts --noEmit -p packages/shared/tsconfig.json
```

The patch wrapper invokes the official
`effect-tsgo patch --no-typescript --tsrs`; it never hand-copies an Effect
replacement. It verifies exact package versions and the platform binary's
SHA-256 against the pins in
[`identity.ts`](../../scripts/jones/tsrs/identity.ts). Qualification verified
registry tarball integrity and computed member hashes; it did not
cryptographically verify Sigstore certificates or transparency-log proofs.
A version string or `.original` file alone does not prove patched identity.

Supported replacements are Linux x64/arm64 with glibc 2.35 or newer, and macOS
arm64. Musl, Windows and macOS x64 have no artifact in this pinned Effect
release. The candidate launcher selects the exact incumbent fallback on an
unsupported local host. Its `--admit` mode refuses unsupported CI hosts; the
root uncached admission task runs before cached work, even with top-level
`--cache`. Compiler
selection inside the launcher does not depend on `CI` or another environment
switch.

The pinned local fallback is `@tsc-rs/typescript@0.8.0`. It misses the configured
`deterministicKeys` error TS377049: an unsupported host can report success for
an invalid service key that supported-platform checking rejects. Its observed
Effect 4 corpus coverage is 48 of 49 applicable configured rules, compared with
49 of 49 for the official Effect-patched TSRS candidate. The remaining configured
`missingEffectServiceDependency` rule supports Effect 3 only and is not applicable
to the installed Effect 4; it is not counted as passed. The original strict
fallback-parity criterion failed; the versioned Q2-R1 qualification retains this
pre-existing limitation. Local and cached fallback success cannot establish
complete diagnostics. Supported-platform CI remains authoritative. Track the
limit in [issue 711](https://github.com/Jones-Systems/Jones-Code/issues/711);
closure requires a separately selected and qualified fallback that enforces
TS377049. Changing compiler pins, relevant settings or rule applicability requires
affected qualification again.

The launcher holds the mutation lock through verification and successful native
spawn, then releases it while checking continues. Parallel compiler processes
therefore do not serialize for the duration of their checks. A concurrent restore
cannot replace the verified executable before it starts.

The native TSRS invocation pins `--checkerAssignment go --checkers 2`.
Do not infer reproducibility or performance from the defaults of another build.
The existing TypeScript library/editor alias remains the incumbent.

## Installation safety and recovery

Patching requires this worktree's literal, nonsymlinked
`node_modules/.pnpm`, with the resolved platform directory on the same
filesystem. Shared virtual stores, a symlinked `node_modules` or `.pnpm`, an
outside store, and hoisted layouts are refused. Disable a shared virtual-store
configuration and reinstall using pnpm's isolated linker. A same-filesystem
bind mount cannot be distinguished by this metadata proof; it must not point
to shared package state.

The wrapper authenticates pristine and patched hashes, preserves the original
binary as `tsrs.original`, and requires the replacement to be a private inode.
The patch lock records wrapper/child process identities. Concurrent live
patching is refused. Cancellation forwards TERM, escalates after ten seconds,
and observes child exit before releasing the lock. An unconfirmed child-spawn
window or interrupted lock reclamation fails closed and needs inspection;
never remove a lock merely because it looks old.

For a recognized interrupted patch, rerunning the wrapper authenticates the
backup, restores the pristine binary and invokes the official patcher again.
It never attempts to finish a partial replacement itself. To restore the
original without patching again:

```sh
node scripts/jones/tsrs/patch.ts --restore
```

Unknown backups, symlinks, unexpected entries or shared partial files are
preserved and reported. Stop and inspect the exact paths; do not blindly
remove the backup or lock. Upstream `unpatch` cannot discover every state where
the executable is missing. The Jones recovery path exists for that case.

Upgrade package pins, source provenance and binary hashes together. Then
requalify configured-rule controls, every replacement surface, retained/fresh
parity, cache admission, actual workflow cost, interruption recovery and actual
supported-platform execution before changing the default commands. The
[separate test-directory override issue](https://github.com/Jones-Systems/Jones-Code/issues/661)
is not resolved by compiler substitution.

## Retained checks and cache lifetime

The incremental runner uses guarded raw JSONC inputs rather than unsupported
TSRS `--showConfig`. Its v2 identity includes exact pins, patched binary bytes,
adjacent standard libraries, checker options, workspace and installed dependency
bindings, and recognized project/base configuration bytes. Workspace discovery
accepts only the exact public alias/native-command pair or the original exact
launcher command; it never evaluates package configuration code. Every retained check
still executes the verified compiler. Success may atomically retain a bounded
build-info envelope; failures preserve the earlier seed. `--fresh` reads and
publishes no retained state. Retained mode refuses unsupported hosts instead of
changing compiler; use regular `typecheck` there.

V1 retained entries are ignored and preserved, never migrated or deleted
automatically. Stop old invocations before separately removing the exact legacy
path printed by the runner. Task-cache hits, fresh checks and retained checks
have different costs. After an OS or libc upgrade, invalidate local task-cache
entries with `vp cache clean` before relying on cached typechecks. Shared remote
typecheck caches require separate libc-aware qualification.

The unverified `typecheck:tsrs:experimental` runner is retired. It did not verify
the Effect-patched binary and cannot serve as a diagnostics-preserving fallback.
