# Provider runtime identity

A requested model is routing intent. An observed model describes native evidence
from the process that serves a particular conversation. Keep them separate even
when their values happen to match. Neither provider settings, authentication
metadata nor a model catalog establishes observed backend, model, account or tier.
The [identity contract](../../packages/contracts/src/providerRuntimeIdentity.ts)
uses unknown and unavailable states so missing evidence stays visible.

Identity belongs to a provider thread, not a shared session. One Codex app-server
can serve native conversations with different models. Its process generation is
reserved before launch and captured by its callbacks; a logical session ID or an
idle-timer generation cannot substitute for that incarnation. Typed Codex
thread-open responses provide model/backend/tier evidence. Native reroute
notifications update only observed model. Claude SDK init provides model evidence only. Neither boundary safely
binds an account to the process.

[Session management](../../apps/server/src/orchestration-v2/ProviderSessionManager.ts)
publishes the successful binding boundary before releasing observations. A failed
or interrupted candidate cannot become current. Replacement invalidates the old
issuer. Codex token rotation resumes the same native cursor with the current
request before sending the next prompt. It refuses replacement while shared
active or background work remains. A capacity retry remains attached to its original prompt and
producer; runtime drift cancels it rather than replaying the prompt.

Claude rewind closes the old query and preserves or resets its continuation.
The replacement generation exists only when the next send actually launches a
query. Rollback itself is not a replacement-process observation. Historical
identity in JSON likewise records past evidence; replay never activates a producer.

The [event sink](../../apps/server/src/orchestration-v2/EventSink.ts) validates the
native binding inside the same transaction as the projection write. Evidence
revision can advance within an incarnation. With a pinned generation, later
revisions are valid only for the same application thread, provider thread, session,
driver, instance and native conversation; revision regression remains invalid.
Without a generation, a captured revision must match exactly. Late snapshots
preserve newer requested configuration and observations. A start captured before an
observation-only revision advance may still commit when the exact previous
instance, driver, model and explicit tier remain current. A requested-configuration
change rejects that stale writer even when the process generation is unchanged.

Full and detail views retain each provider thread's own identity. Shell identity
comes only from the exact active provider thread of that application thread. Old snapshots may omit identity. A `runtime_identity_json` column left by released
Jones migration 002 is unused residue, not evidence of a live V2 runtime. Explicit service-tier options
are recorded as requested; normalization of the fast-mode alias remains deferred.
A native launch followed by binding-publication failure remains an unknown effect:
fail visibly and stop replay. Automatic recovery of that boundary is deferred.
