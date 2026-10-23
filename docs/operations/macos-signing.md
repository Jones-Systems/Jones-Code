# Production macOS signing

Production desktop signing is a separate, fail-closed build mode. Development
and unsigned preview builds retain `com.t3tools.t3code`. The owner-approved
production identity is `com.jonessystems.jonescode`, Developer ID team `2CUPSQS986`.
Identity approval settles the source mapping. Workflow activation, signing,
installation and identity migration remain separate held operations; adding this
source does not enable them or enroll credentials.

The `macos-signing` GitHub environment must restrict deployment to `main` in
`Jones-Systems/Jones-Code`. Review that environment before enabling the workflow.
Set its `JONES_MACOS_PRODUCTION_ID` variable to the approved identity only during
a separately authorized signing setup. Both manual runs and main push builds
require `JONES_MACOS_SIGNING_ENABLED=true` at repository scope. Keep it unset
through partial credential enrollment; missing credentials are not the activation
control. A manual run signs the exact canonical main event commit and accepts
no input ref. Production artifacts retain the existing preview-version convention
for source/run provenance; the production artifact name and workflow are distinct.
An admitted installed production bundle selects only that production workflow.
Legacy callers retain preview discovery; unknown IDs fail closed. Push provenance
and exact main CI remain required, so manual signing runs are not updater candidates.
Pull requests and fork/branch dispatches cannot enter the signing job.

Required environment secrets:

- `MACOS_SIGNING_CERTIFICATE_P12_BASE64`
- `MACOS_SIGNING_CERTIFICATE_PASSWORD`
- `APPLE_NOTARY_API_KEY_P8_BASE64`
- `APPLE_NOTARY_KEY_ID`
- `APPLE_NOTARY_ISSUER_ID`

The importer takes P12 bytes and their password through stdin into
Security.framework. It never passes a real password to `security -P`, `-p` or
`-k`. The exclusively created job keychain uses an intentionally empty password;
its directory is private and its search-list preimage is restored in cleanup.
The private notary key is a temporary 0600 file. Public key/issuer identifiers
and that file's path are arguments to `notarytool`; private bytes are not.
Only DMG, checksum and public provenance are uploaded. Interrupted hosted jobs
rely on runner disposal as the final cleanup boundary; do not reuse a runner
with an interrupted credential operation.

Signing uses hardened runtime, secure timestamps and distinct stable component
identifiers. The app is notarized and stapled before packaging. Missing signing
or notary prerequisites fail production without an ad-hoc fallback. Developer ID
renewal within the same Team preserves the identifier/team requirement; a Team
change needs an explicit migration and fresh consent.

## Optional native browser passkeys

Baseline Developer ID signing does not require Clerk RP domains, a provisioning
profile or Apple's browser-passkey capability. It retains the baseline's absence
of `t3codeWebAuthn` metadata. General Clerk authentication/passkeys are unchanged.

To opt in, set `JONES_MACOS_NATIVE_PASSKEYS=enabled`, provide environment secret
`MACOS_PROVISIONING_PROFILE_BASE64`, and public configuration
`T3CODE_CLERK_PUBLISHABLE_KEY` or `T3CODE_CLERK_PASSKEY_RP_DOMAINS`. The profile must
cover the production Team and application ID, Associated Domains, the exact
`.webauthn` keychain group and browser-passkey capability. Incomplete or mismatched
configuration fails; it never silently switches back to baseline. Apple profile
issuance and Clerk/RP readiness are separate prerequisites for that opt-in.

## Installation and recovery

No source change here grants Full Disk Access or other macOS consent. The existing
`t3code-v2` userData path and Jones state remain unchanged. An identity migration
requires an exact owner-authorized bootstrap with preserved paired state and
readback; the ordinary updater rejects legacy-to-production identity changes.
This slice does not implement or execute that bootstrap.

For production bundles, native activation validates the Developer ID requirement,
sealed resources, Gatekeeper assessment and stapled ticket before renaming the
active bundle. Retained rollback bundles are checked again before restoration.
Ad-hoc, tampered, wrong-team and wrong-ID bundles are refused. Routine rollback
is between compatible signed generations; legacy ad-hoc recovery is exceptional
owner-directed bootstrap recovery. Existing provenance, writer exclusion and
paired-state recovery remain required.

The CLI and ChatGPT helper policy are outside this desktop slice. Source tests
with stubbed signing commands do not establish cryptographic acceptance, actual
notarization, app launch, or consent continuity. Qualify those on the first
trusted signed artifact before installation.
