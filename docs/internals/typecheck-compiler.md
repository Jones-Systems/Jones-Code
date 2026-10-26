# Typecheck compiler selection

Workspace CLI commands invoke the verified official Effect-patched TSRS
launcher on all 33 TypeScript surfaces. Root `typecheck` and `tc` run uncached
compiler admission before cached recursive tasks. `pnpm typecheck:incremental`
uses the same pinned compiler and Go assignment in a separate v2 namespace.
Marketing continues using `astro check`.

The candidate integration remains under
[qualification](https://github.com/Jones-Systems/Jones-Code/issues/711) before
main adoption. The initial 33-surface direct screen preserved incumbent
errors/warnings and non-standard-library coverage after constructive fixture
corrections, but complete rule, lifecycle, cache, performance and platform gates
remain. No complete current-workflow speed improvement has been qualified.
Schema-class connection targets in partial supervisor doubles still use genuine
Schema instances; diagnostic settings stay unchanged.

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
bindings, and recognized project/base configuration bytes. Every retained check
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
