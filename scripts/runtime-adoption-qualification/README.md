# Synthetic runtime-adoption qualification

Run with Node 24 or newer after a frozen workspace install:

```sh
node scripts/runtime-adoption-qualification/run.mjs \
  --mode dev \
  --evidence-dir /absolute/task-owned/evidence/run-01 \
  --lock-file /absolute/project-owned/serial-test.lock \
  --admission-tool /absolute/collect_work_capacity.py \
  --descriptor /absolute/artifact-inputs.json
```

Supply a fresh evidence directory outside the source worktree and shared `/tmp`.
The caller owns the existing advisory lock and bounded admission collector. The
runner opens the lock read-only, acquires it nonblocking, collects fresh CPU/RAM
admission, and starts one serial focused test process. Exit 75 means no test
started. Admission denial also starts no test. Evidence contains source and
artifact bindings, admission, test output, per-scenario receipts, and cleanup
readback. Scratch is unique to the invocation and removed after owned resources
close. Cancellation signals only the captured test process group; unknown
survivors preserve scratch for reconciliation.

`--mode final` requires a clean worktree and a completed
`jones-runtime-artifact-inputs/v1` descriptor. It verifies the exact accepted
source and its retained accepted T2 ancestor, candidate
version/platform/architecture/channel and file hashes. A checkout with a
harness-only difference is allowed only when its production-tree diff against
the descriptor's actual package source is empty. The package source commit and
qualification checkout commit/tree remain separate evidence identities.
Development runs with incomplete artifacts remain explicitly unbound.

The six test modules exercise production SQLite migrations and service launcher
behavior using populated synthetic state and versioned synthetic executables.
They cover upstream 53/54 adoption, repeat startup, failures before and after
migration writes, matching and wrong prepared identities, database rollback,
backup failure, cancellation, persisted provider resume-request reconstruction,
environment identity, desktop catalog/profile seams and separate browser stores.
Settings, keybindings, attachment bytes and project/worktree references are
checked separately from database state. Launcher rollback restores the database
and its sidecars; these checks do not establish rollback of modified non-database
files. Fixture snapshots use `VACUUM INTO` after all prior SQLite scopes close.

Artifact binding verifies bytes and source provenance supplied by the packaging
lane; it does not execute the candidate archive. Package smoke evidence remains
separate. Mock provider clients prove resume-path selection and serialized native
identity, with fresh-start fallback rejection. Native provider continuation,
installed adoption, real Electron keychain decryption, browser durability,
Connect login/peer access, signing and notarization remain unproved by this
harness. It reads no installed homes or provider/browser credential stores.
