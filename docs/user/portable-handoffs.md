# Context in portable handoffs

T3 Code transfers conversation context when you switch providers, continue through a portable
restart, or use a fork that the provider cannot resume itself.
Provider context transfer is separate from [thread continuation](./linked-threads.md): it does not
offer or accept responsibility for work in another thread.

Short conversations transfer intact. For longer conversations, T3 Code favors recent requests and
answers, the original request, and relevant activity such as command outcomes. Selected messages
keep their full text, order, and user or assistant role, including partial work from failed or
interrupted turns. Your new request stays separate and is never shortened to make history fit.

Codex receives historical messages directly when its installed version supports that operation.
Other providers receive the same selection as attributed conversation context. A handoff does not
copy the outgoing provider's reasoning, tool-call state, or attachments.

The handoff includes references to omitted history. The agent can use T3 Code's thread-reading tool
to retrieve saved messages and activity, including the remainder of a long item. For an important
constraint, you can still repeat it in your next message. A handoff is a budgeted selection, not an
agent-written summary.

## Context limits

A handoff must leave room for existing provider context, your request and attachments, instructions,
tools, and subsequent work. If even its retrieval references cannot fit, T3 Code reports an error
instead of shortening your request. Compact the target conversation or select a larger-context
model before trying again.

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
