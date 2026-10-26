# Native Workstreams source setup

Native source issuance is an explicit owner operation on the selected native home.
Registry initialization alone does not issue a source session or append a source
binding. Ordinary startup and reads create neither. The released
`workstreams_native_enrollments` ledger remains unchanged.

The source setup service supports `plan`, `apply` and exact `readback`. Its CLI
mount is `workstreams registry source-setup <action> --home <absolute-home>
--key-provider server-secret-store --request <JSON>`. The owner CLI must first
qualify the existing standalone runtime, database, source authority and build
through `makeTrustedNativeEnrollmentCliContext`, bind authority to that same SQL
client, and preserve `verifyUnchanged`. A desktop integration must use its
exclusive owner boundary. Configuration values select evidence; they do not
establish authority. Missing authority, keys, issuer, owner evidence or store
refuses the operation.

The internal `jones-code.native-source-setup/v1` request contains the existing
closed native enrollment request, `rebind`, and an `expected` object with the
registry ID, owner ID, home environment and generation, exact registry version,
singleton binding version, grant ID/version, and latest source binding or null.
A source binding pin contains `binding_version`, `authority_namespace`,
`store_generation`, and `state`. The enrollment carries stable enrollment and
AuthSession IDs, actual source authority, principal, build and session lifetime.
All pins and issuance bytes are covered by the request digest. Save that exact
request for readback; a different request under the same ID is a conflict.

Apply checks the current registry, principal mapping, active grant, singleton,
source binding and authentic source authority in one SQL transaction. It reserves
the AuthSession through its owning repository, appends source-binding history,
and stores an immutable encrypted native issuance proof. The source-binding
trigger advances registry version once. Authority and owner qualification are
checked again before commit. Any failure rolls back all three writes. An
ambiguous commit must be reconciled by exact readback before another attempt;
replaying an already committed identical request returns its original proof.

Source-generation rebinding requires `rebind: true`, current authentic authority,
and exact prior binding pins. It appends history and preserves old proofs. Another
registry, owner, principal or home is refused. Old sessions do not inherit new
source or home generations. Missing, revoked, expired or changed session/grant/
binding evidence fences lookup and recovery.

The native-only composition mounts `NativeSourceEnrollments` and its compatibility
provider lookup with the application's existing SQL/key services. The validated
lookup also exposes full source/session/binding pins for recorded-caller recovery.
Legacy lookup remains separately mounted in legacy mode; it is never relabelled
as new native issuance. Bearer materialization is a separate reserved-session
operation and must remain in memory at the authorized transport boundary. Public
proofs and CLI output contain no bearer or signing key.

Migration 126 records native source issuance in the Jones migration ledger. The
server mounts validated native lookup only when native home mode is selected;
the registry CLI mounts the explicit owner commands. Source availability does
not authorize installation, runtime adoption, migration of live state, merge,
or fleet activation.
