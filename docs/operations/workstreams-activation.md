# Workstreams activation

The server reads one activation snapshot at startup for both Workstreams and
registration-context requests. Existing shell/environment configuration remains
supported: if any of the six `T3_WORKSTREAM_*` activation variables is defined,
including an empty value, the complete environment group must be valid. Missing
values are never filled from a file.

When all six are absent, the server reads only
`<ServerConfig.baseDir>/workstreams/activation.env`. Use the control plane's
`stage-jones` output: exactly one literal assignment for each of
`T3_WORKSTREAM_CONTROL_PLANE_URL`, `T3_WORKSTREAM_OWNER_ID`,
`T3_WORKSTREAM_PRINCIPAL_ID`, `T3_WORKSTREAM_KEY_ID`,
`T3_WORKSTREAM_SIGNING_SECRET`, and `T3_WORKSTREAM_AUTHORIZATION_REVISION`.
The file is data, not shell input: no `export`, quotes, interpolation, comments,
extra keys, or duplicate keys. The URL is a canonical HTTPS root origin, or
HTTP to literal `127.0.0.1` or `[::1]` on the same host.

Keep the resolved base directory owned by the server user and not writable by
its group or others (`0755` and `0700` are accepted). The `workstreams` directory
must belong to that user at `0700`, and `activation.env` must be a regular file
at `0600` owned by that user. No path component may be a symlink. Ancestors must
belong to root or the server user and must not be group/world writable; a
root-owned sticky ancestor such as the system temporary directory is accepted.
The opened file is checked again for owner, mode, identity and size, with a
64 KiB maximum and strict UTF-8 decoding.

An absent file means unconfigured. Partial, malformed, unreadable or unsafe
configuration fails closed as invalid/unavailable without contacting the control
plane or exposing file contents. Correct the configuration through the authorized
provisioning operation, then restart the ordinary server child. Runtime updates
load the same installation file on the next child startup; they do not require
copying credentials into a runtime package, provider environment or settings.
Changes to the file during a running server do not reload activation.

Keep credential staging and permission changes within the installation's exact
operational authorization. Do not print the file or copy it into diagnostics.
