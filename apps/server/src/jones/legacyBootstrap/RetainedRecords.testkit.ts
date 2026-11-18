import type { OrchestrationV2ThreadProjection, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { EventSinkV2 } from "../../orchestration-v2/EventSink.ts";
import type { EventStoreV2 } from "../../orchestration-v2/EventStore.ts";
import type {
  RecordedRun,
  RecordedStoredLifecycleEvent,
  RecordedThreadProjection,
} from "../../orchestration-v2/RecordedTypes.ts";
import type { ThreadManagementService } from "../../orchestration-v2/ThreadManagementService.ts";
import { isPublicStoredOrchestrationEvent } from "../../orchestration-v2/WireProjection.ts";
import * as M from "./RecordedMetadata.ts";
import type { LegacyBootstrapRecords } from "./RetainedRecords.ts";

// Test composition only. Public fields stay from the public reader; only sparse legacy fields
// come from the Jones reader, and a public run without its retained record fails closed.
export const overlayRetainedRecords = Effect.fn("LegacyRetainedRecords.overlay")(function* (
  projection: OrchestrationV2ThreadProjection,
  records: LegacyBootstrapRecords,
) {
  if (records.thread.id !== projection.thread.id)
    return yield* Effect.die(new Error("Retained thread belongs to another projection"));
  const runs: RecordedRun[] = [];
  for (const run of projection.runs) {
    const retained = records.runs.find((entry) => entry.id === run.id);
    if (retained === undefined)
      return yield* Effect.die(new Error(`Retained run ${run.id} is unavailable`));
    runs.push({ ...run, ...M.runMetadata(retained) });
  }
  return {
    ...projection,
    thread: { ...projection.thread, ...M.threadMetadata(records.thread) },
    runs,
  } satisfies RecordedThreadProjection;
});

export const readRetainedProjection = Effect.fn("LegacyRetainedRecords.readProjection")(function* (
  threads: ThreadManagementService["Service"],
  threadId: ThreadId,
) {
  const projection = yield* threads.getThreadProjection(threadId);
  const records = yield* threads.getLegacyBootstrapRecords(threadId);
  return yield* overlayRetainedRecords(projection, records);
});

// Public thread stream with each event's exact retained metadata from its command's lifecycle read.
// Events whose command has no Jones record keep their public payload.
export const readRetainedThreadEvents = Effect.fn("LegacyRetainedRecords.readThreadEvents")(
  function* (store: EventStoreV2["Service"], sink: EventSinkV2["Service"], threadId: ThreadId) {
    const raw = Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect)).filter(
      isPublicStoredOrchestrationEvent,
    );
    const retained = new Map<string, RecordedStoredLifecycleEvent>();
    const commandIds = new Set(
      raw.flatMap((stored) => (stored.commandId === null ? [] : [stored.commandId])),
    );
    for (const commandId of commandIds) {
      for (const stored of yield* sink
        .readLifecycleByCommandId({ commandId })
        .pipe(Stream.runCollect))
        retained.set(stored.event.id, stored);
    }
    return raw.map((stored) => {
      const merged = retained.get(stored.event.id);
      return merged !== undefined && merged.sequence === stored.sequence ? merged : stored;
    });
  },
);

// One streamed event with its exact retained metadata; the same event at the same sequence only.
export const readRetainedEvent = Effect.fn("LegacyRetainedRecords.readEvent")(function* (
  sink: EventSinkV2["Service"],
  stored: RecordedStoredLifecycleEvent,
) {
  const commandId = stored.commandId;
  if (commandId === null) return stored;
  const merged = Array.from(
    yield* sink.readLifecycleByCommandId({ commandId }).pipe(Stream.runCollect),
  ).find((entry) => entry.event.id === stored.event.id && entry.sequence === stored.sequence);
  return merged ?? stored;
});
