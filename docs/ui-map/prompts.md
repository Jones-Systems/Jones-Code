# Prompts

[Map index and shared recipes](README.md). Maintainer journey for reviewing voice
prompts and read-only queue samples in the shared web/desktop renderer. These are
source-derived expectations, not runtime acceptance evidence.

## Entry, scope and shared consumers

Open `/work-queue` directly, `/voice-review`, or **Prompts** in the sidebar
utility menu. Both routes mount
[WorkQueuePage](../../apps/web/src/components/workQueue/WorkQueuePage.tsx), which
waits for client settings and then renders
[UnifiedQueuePage](../../apps/web/src/jones/workQueue/UnifiedQueuePage.tsx) on the
saved default stage unless a link supplies a stage for that visit.

Each environment probes its voice source and queue independently. Checking,
disabled, disconnected, read-failed and absent sources are distinct messages; an
absent source on every connected server is the only empty catalog. A disconnected
environment keeps its last observation and disables actions.

Pending, Queued and Sent are tabs over one item model,
[buildPromptItems](../../apps/web/src/jones/workQueue/promptItems.ts). Sent uses
[classifySentPrompts](../../apps/web/src/jones/workQueue/classifySentPrompts.ts)
unchanged: voice text is attached only for one exact delivery match on a queue
source bound to the native environment. Tab counts appear once a source reports;
`+` marks a capped or partial sample and `?` marks an unread or stale source.
Sent has no count because the sample is not a complete history.

Each stage is a master–detail list
([PromptList](../../apps/web/src/jones/workQueue/PromptList.tsx)). At `md` and
wider the detail is a sticky right column and the first row is selected; below
`md` the detail opens inline under the selected row and takes focus. Rows use
the destination thread's live title and status only when that thread is loaded
for the same environment.

## Synthetic journeys

Use the common isolated recipe with a synthetic voice source and queue sample.
Do not release real prompts.

1. Seed held, paused, blocked (`route_required`) and editing-elsewhere drafts.
   Pending groups them as Needs you, Releasing and Paused. A blocked draft says
   it needs a destination and offers no way to set one. Edit, then switch rows:
   **Switch prompts?** keeps the unsaved text with its prompt. Change tabs and
   environments; the editor stays mounted, and leaving the page asks **Leave
   Prompts?**.
2. Make an action's outcome uncertain. The row is marked, the badge reads
   **Outcome unknown**, actions are unavailable and **Check current state**
   reconciles without retrying.
3. Seed a released voice prompt, an unlinked queue request and a request with an
   exact native handoff. Queued shows the first two read-only; the proved one is
   one Sent row with its voice text. Its progress list marks the handoff and the
   turn state, and the last line is always **Finished: Not tracked**.
4. Let the sample expire or fail a refresh. The queue header says the sample is
   stale or from an earlier read, and the Queued count gains `?`.
5. Open a queued voice prompt's Workstream section. Corrections are disabled
   while thread labels, recent prompts or the server are unavailable, and an
   unconfirmed correction is never retried. Check that thread membership, voice
   suggestions and queue requests remain distinct even when the names match.
6. Search for text or an ID in the current sample. Filter by attention or click a
   Workstream badge. Change the grouping, then clear filters and open a linked
   prompt by its row link. A missing key reports that the item is outside the
   current sample.
7. Hold focus on a prompt row and refresh the source with a changed sample.
   The focused rows stay in order, and a row that moved becomes unavailable
   until focus leaves.

## Evidence

Runtime journeys are `unverified` until a candidate-bound record is added here.
Focused browser tests cover the list, detail evidence, tab counts and the
retained-editor guard; they are render checks, not interaction proof in the app.
