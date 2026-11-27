# Context in portable handoffs

T3 Code transfers conversation context when you switch providers, continue through a portable
restart, or use a fork that the provider cannot resume itself.
Provider context transfer is separate from [thread continuation](./linked-threads.md): it does not
offer or accept responsibility for work in another thread.

## What transfers

- Short conversations transfer intact.
- Longer conversations prioritize the original request, recent user/assistant turns (including partial work from interrupted turns), and key activity like command outcomes.
- Your new prompt is never shortened to make history fit.
- Codex receives historical messages directly when supported; other providers receive attributed conversation context.
- Reasoning traces, raw tool-call state, and attachments are not copied.
- Omitted history is referenced so the agent can retrieve saved messages on demand using T3 Code's thread-reading tool. Still, repeat any critical constraint in your next message if you want to be sure.

## Context limits and configuration

If the target session doesn't have enough context room, T3 Code automatically starts a fresh provider session with a bounded history selection and sends your request—no manual `/compact` needed. If your request and attachments alone exceed the model's window, shorten the message or choose a larger-context model.

A handoff must leave room for existing provider context, your request and attachments, instructions,
tools, and subsequent work. If the target conversation has no room for the handoff, T3 Code starts
a fresh provider conversation with a bounded selection of saved history and sends your request
automatically. You do not need to compact manually. If the request and attachments leave too little
room even in a fresh conversation, shorten the request or select a larger-context model. T3 Code
never shortens your request to make it fit.

Recovery leaves running background work intact. Let it finish or stop it before retrying the handoff.

Server operators can set `T3CODE_CONTEXT_HANDOFF_TOKEN_CAP` to change the initial history allowance
(default 16,000; clamped to 1,024–64,000). This is an upper bound, not a provider context-window
guarantee. Text accounting conservatively charges one token per UTF-8 byte, including attribution
and JSON escaping. Imported history has a separate 64,000-byte ceiling; your current input and
attachment payloads do not consume that ceiling.

T3 Code uses available capacity information for your selected model and options, together with
provider context telemetry. Starting a fresh provider conversation clears prior usage, while keeping
known model capacity. Changing models or context-window options discards stale usage and compaction
thresholds.

When no capacity information is available, T3 Code assumes a 128,000-token window. It reserves at
least 16,000 tokens, or a quarter of the window when larger, for instructions, tools, and subsequent
work. Images reserve an estimated 8,192 tokens each, independent of their file size; other
attachments reserve 4,096 each for their references. Existing context is estimated from saved
activity when usage telemetry is unavailable. Known smaller windows still constrain the handoff.
These are fallback estimates, not exact token counts. Image resolution, custom models, and hidden
native context can differ, so the provider may still reject an input.

## Goal checkpoints

Jones Code can keep an explicitly initialized goal's intent, finish criteria, progress, blockers
and evidence with its exact provider conversation. Ask the agent to initialize continuity for the
goal and checkpoint meaningful progress. Finishing requires evidence covering every finish item,
with no remaining blocker or unresolved effect. Checkpointing does not start or resume a native goal.

Codex can receive fresh checkpoint context when a turn starts and after compaction. Other providers
can use explicit checkpoint reads when their conversation identity is available; automatic recovery
is unsupported. A provider switch or fork needs an explicit continuity handoff. If recovery delivery
is uncertain, continuation stays held for reconciliation. An acknowledgement does not resume a paused
goal, and recovery does not guarantee that no model output occurred before context was restored.
The native feature requires no external Goal-Autonomy CLI or project hook; existing external state
and hooks are left in place.
