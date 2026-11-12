# Workstream summaries

Native rollups run at the registry home. The runtime composes the encrypted
home store, evidence and generation ports, one shared inference admission owner,
and the bounded `RollupApplication.processOnce` sweep. Start the sweep only
under an active home authority. Server lifecycle code owns its timer and shutdown.

Evidence uses `workstreams-rollup-evidence/1.0.0`. An adapter supplies exact
account/source selection and current authorization witnesses; generation supplies
its observed extractor identity. Neither port discovers live accounts or keys.
Leave live ports explicitly unconfigured until their configuration is approved.
Unmapped references remain unavailable; they are never reassigned to the home.

Imported summaries need no regeneration provider. Reading one still requires
current registry and account-content/dashboard authorization. A missing key,
changed authorization decision, missing manifest or failed digest check fails
closed. Do not recover by returning an older payload or writing a plaintext cache.

The application records provider failures, timeouts and expired leases as dead
letters. Only an unchanged `input_unavailable` manifest below attempt fence three
is eligible for automatic requeue. An unknown provider outcome is not retried.
A newer manifest supersedes its old claim. Published native queue rows remain
as `published` tombstones because the native foundation preserves history; they
consume no dirty capacity. Reverse exporters must omit those logical queue
absences while retaining their native history.

The dashboard adapters return private, no-store list/detail/summary reads through
normal authenticated admission. Collection continuation cursors remain visible;
a first page does not claim complete history. The native summary read uses the
existing `workstreams-rollups/1.0.0` shape. Client panels display stale and unknown
freshness explicitly. Source modules and synthetic checks alone do not establish
that routes, timers or client panels are mounted in an installed application.
