# Recover an orphan staged selection

Use this maintainer script only when Download already produced a valid native
staged handle but failed before retaining its selection. It verifies the existing
handle and retains it through the native writer. It does not install a version,
restart the service, enroll a runtime, replace another selection or remove files.

Build a standalone ESM bundle from the reviewed source commit using a Node
bundler with `platform: "node"`, `format: "esm"`, and bundled relative imports.
Only Node built-ins may remain external. The native verifier resolves Electron's
`original-fs` dynamically when running under Electron. Record the source commit
and SHA256 of the resulting bundle in the separately authorized host operation.
Source and bundle creation do not authorize host execution.

```sh
node recover-staged-selection.mjs \
  --base-dir /canonical/owned/home \
  --active-version <exact-active-version> \
  --expected-active-source <full-active-source-sha> \
  --environment-id <exact-environment-id> \
  --target-source <full-target-source-sha> \
  --target-version <exact-target-version> \
  --staged-handle <existing-native-handle>
```

Dry-run is the default. It requires the previously created, owner-controlled
`qualified-runtime-lock.sqlite`; it will not initialize a home. A missing pointer
returns `would-retain`. Add `--apply` only within the exact approved recovery
operation. Successful retention returns `retained`; verified replay returns
`already-retained` after syncing the existing pointer and its directories.
Exit codes are 0 for success, 2 for `refused`, and 3 for `unknown`.

Before applying across runtime versions, prove parity with the active server and
launcher for the selection path, binding serialization order, receipt decoder,
file mode and ownership checks. The newly staged runtime is not that proof.
Stop on a conflicting, invalid, foreign or revoked selection. A refusal preserves
it. After `unknown`, perform read-only inspection before any apply retry; an
incomplete pointer requires separately authorized reconciliation. Dry-run can
reestablish durability for an already complete exact pointer but never repairs
an incomplete one.

A subsequent restart and native Install remain separate authorized operations
with their normal continuity, backup capacity, trial and rollback checks.
