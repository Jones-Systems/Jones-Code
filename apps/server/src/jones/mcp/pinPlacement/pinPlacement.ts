import {
  NonNegativeInt,
  OrchestratorMcpFailure,
  ThreadId,
  TrimmedNonEmptyString,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { planPinnedReorder, sortPinnedThreadsByOrderKey } from "@t3tools/shared/jones/pinOrder";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { dispatchFailure, newCommandId, unavailable } from "../../../mcp/threadAccess.ts";
import {
  DispatchModeLimit,
  exceededDispatchModeLimit,
} from "../../../orchestration-v2/DispatchModeLimit.ts";
import type * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";

/**
 * Where `t3_thread_organize` pins a thread in the pinned order. `exact` takes
 * an `orderKey` read earlier from the thread's own metadata, so a pin recorded
 * before settlement can be restored to its original key.
 */
export const PinPlacement = Schema.Struct({
  position: Schema.Literals(["top", "bottom", "above", "below", "exact"]),
  anchorThreadId: Schema.optional(ThreadId),
  orderKey: Schema.optional(TrimmedNonEmptyString),
});
export type PinPlacement = typeof PinPlacement.Type;

const PinOrderReadback = Schema.Struct({
  threadId: ThreadId,
  pinOrderKey: Schema.NullOr(Schema.String),
});

export const PinPlacementResult = Schema.Struct({
  sequence: NonNegativeInt,
  pinOrderKey: Schema.NullOr(Schema.String),
  reorderedThreads: Schema.Array(PinOrderReadback),
});
export type PinPlacementResult = typeof PinPlacementResult.Type;

const invalid = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });

/**
 * The caller's mode limit, which the target already passed, applied to another
 * thread this call reads as an anchor or rewrites. The orchestrator re-checks
 * every write; checking here refuses before anything is written.
 */
const refuseAboveLimit = Effect.fn("jones.mcp.pinPlacement.refuseAboveLimit")(function* (
  thread: OrchestrationV2ThreadShell,
  role: string,
) {
  const limit = yield* DispatchModeLimit;
  const exceeded = limit === undefined ? undefined : exceededDispatchModeLimit(limit, thread);
  if (exceeded === undefined) return;
  return yield* new OrchestratorMcpFailure({
    code:
      exceeded === "runtime"
        ? "runtime_mode_escalation_denied"
        : "interaction_mode_escalation_denied",
    message: `${role} ${thread.id} runs in ${thread.runtimeMode}/${thread.interactionMode} mode, above this caller's.`,
  });
});

interface PinnedRow {
  readonly id: ThreadId;
  readonly createdAt: string;
  readonly pinOrderKey: string | null;
}

/**
 * The pinned order with `targetId` placed, as the ids the sidebar would show.
 * "Below" an anchor lands after every thread sharing the anchor's key (delegated
 * child threads have been seen sharing their parent's key), and "above" before
 * all of them.
 */
function placeInPinnedOrder(
  pinned: ReadonlyArray<PinnedRow>,
  targetId: ThreadId,
  placement: PinPlacement,
): ReadonlyArray<ThreadId> | undefined {
  const rows = sortPinnedThreadsByOrderKey(pinned.filter((row) => row.id !== targetId));
  let index = placement.position === "top" ? 0 : rows.length;
  if (placement.position === "above" || placement.position === "below") {
    const anchor = rows.findIndex((row) => row.id === placement.anchorThreadId);
    if (anchor === -1) return undefined;
    const key = rows[anchor]!.pinOrderKey;
    const group = rows.flatMap((row, at) => (key !== null && row.pinOrderKey === key ? [at] : []));
    index = placement.position === "above" ? (group[0] ?? anchor) : (group.at(-1) ?? anchor) + 1;
  }
  const ids = rows.map((row) => row.id);
  return [...ids.slice(0, index), targetId, ...ids.slice(index)];
}

/**
 * Pins (or moves) the target to a caller-supplied key, rewriting no other
 * thread. A key another pinned thread already holds is refused, so a restored
 * key never silently joins a different group; place it with an anchor instead.
 */
const pinWithExactKey = Effect.fn("jones.mcp.pinWithExactKey")(function* (input: {
  readonly threads: ThreadManagement.ThreadManagementService["Service"];
  readonly target: { readonly id: ThreadId; readonly pinnedAt?: unknown };
  readonly pinned: ReadonlyArray<PinnedRow>;
  readonly orderKey: string;
}) {
  const { threads, target, orderKey } = input;
  const holders = input.pinned.filter(
    (row) => row.id !== target.id && row.pinOrderKey === orderKey,
  );
  if (holders.length > 0) {
    return yield* invalid(
      `orderKey is already held by pinned thread ${holders.map((row) => row.id).join(", ")}.`,
    );
  }
  const common = { commandId: yield* newCommandId(), threadId: target.id, orderKey };
  const result = yield* threads
    .dispatch(
      target.pinnedAt != null
        ? { ...common, type: "thread.pin.reorder" }
        : { ...common, type: "thread.pin" },
    )
    .pipe(Effect.mapError(dispatchFailure));
  const shell = yield* threads.getThreadShell(target.id).pipe(Effect.mapError(unavailable));
  return {
    sequence: result.sequence,
    pinOrderKey: shell?.pinOrderKey ?? null,
    reorderedThreads: [],
  } satisfies PinPlacementResult;
});

/**
 * Pins (or moves) one thread to a slot in the environment's pinned order.
 * Keys come from the sidebar's own planner, so keyless neighbors are
 * materialized with spread keys exactly as a sidebar drag would; those
 * rewrites dispatch first as reorders, then the target's pin or reorder.
 */
export const pinWithPlacement = Effect.fn("jones.mcp.pinWithPlacement")(function* (input: {
  readonly threads: ThreadManagement.ThreadManagementService["Service"];
  readonly action: string;
  readonly target: { readonly id: ThreadId; readonly pinnedAt?: unknown };
  readonly placement: PinPlacement;
}) {
  const { threads, target, placement } = input;
  if (input.action !== "pin") return yield* invalid("placement applies only to action pin.");
  const anchored = placement.position === "above" || placement.position === "below";
  if (anchored !== (placement.anchorThreadId !== undefined)) {
    return yield* invalid(
      anchored
        ? `placement ${placement.position} requires anchorThreadId.`
        : `placement ${placement.position} does not take anchorThreadId.`,
    );
  }
  const exact = placement.position === "exact";
  if (exact !== (placement.orderKey !== undefined)) {
    return yield* invalid(
      exact
        ? "placement exact requires orderKey."
        : `placement ${placement.position} does not take orderKey.`,
    );
  }
  if (placement.anchorThreadId !== undefined) {
    if (placement.anchorThreadId === target.id) {
      return yield* invalid("anchorThreadId must be a different thread than the one being pinned.");
    }
    const anchor = yield* threads
      .getThreadShell(placement.anchorThreadId)
      .pipe(Effect.mapError(unavailable));
    if (anchor === null || anchor.deletedAt !== null) {
      return yield* new OrchestratorMcpFailure({
        code: "thread_not_found",
        message: "The anchor thread was not found.",
      });
    }
    yield* refuseAboveLimit(anchor, "Anchor thread");
  }
  const snapshot = yield* threads
    .getShellSnapshot({ location: "active" })
    .pipe(Effect.mapError(unavailable));
  const shells = new Map<ThreadId, OrchestrationV2ThreadShell>(
    snapshot.threads.map((thread) => [thread.id, thread]),
  );
  const pinned = snapshot.threads.flatMap((thread): PinnedRow[] =>
    thread.pinnedAt != null && thread.archivedAt === null && thread.deletedAt === null
      ? [
          {
            id: thread.id,
            createdAt: DateTime.formatIso(thread.createdAt),
            pinOrderKey: thread.pinOrderKey ?? null,
          },
        ]
      : [],
  );
  if (placement.orderKey !== undefined) {
    return yield* pinWithExactKey({ threads, target, pinned, orderKey: placement.orderKey });
  }
  const orderedIds = placeInPinnedOrder(pinned, target.id, placement);
  if (orderedIds === undefined) {
    return yield* invalid(
      `Anchor thread ${placement.anchorThreadId} is not pinned. Pin it first or choose a pinned anchor.`,
    );
  }
  const keysById = new Map<string, string | null>(pinned.map((row) => [row.id, row.pinOrderKey]));
  // The target's own key never counts: it always receives the planned slot.
  keysById.set(target.id, null);
  const assignments = planPinnedReorder({ orderedIds, keysById, movedId: target.id });
  const targetKey = assignments.find(({ id }) => id === target.id)?.orderKey;
  if (targetKey === undefined) return yield* unavailable();
  const neighbors = assignments.filter(({ id }) => id !== target.id);
  // Refuse before any write rather than stop partway through a rewrite.
  for (const { id } of neighbors) {
    const shell = shells.get(ThreadId.make(id));
    if (shell !== undefined) yield* refuseAboveLimit(shell, "Rewriting the pin order of thread");
  }
  for (const { id, orderKey } of neighbors) {
    yield* threads
      .dispatch({
        type: "thread.pin.reorder",
        commandId: yield* newCommandId(),
        threadId: ThreadId.make(id),
        orderKey,
      })
      .pipe(Effect.mapError(dispatchFailure));
  }
  const common = { commandId: yield* newCommandId(), threadId: target.id, orderKey: targetKey };
  const result = yield* threads
    .dispatch(
      target.pinnedAt != null
        ? { ...common, type: "thread.pin.reorder" }
        : { ...common, type: "thread.pin" },
    )
    .pipe(Effect.mapError(dispatchFailure));
  const readKey = (threadId: ThreadId) =>
    threads.getThreadShell(threadId).pipe(
      Effect.map((shell) => shell?.pinOrderKey ?? null),
      Effect.mapError(unavailable),
    );
  const reorderedThreads: Array<typeof PinOrderReadback.Type> = [];
  for (const { id } of neighbors) {
    const threadId = ThreadId.make(id);
    reorderedThreads.push({ threadId, pinOrderKey: yield* readKey(threadId) });
  }
  return {
    sequence: result.sequence,
    pinOrderKey: yield* readKey(target.id),
    reorderedThreads,
  } satisfies PinPlacementResult;
});
