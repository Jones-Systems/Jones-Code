import type { ThreadTokenUsageSnapshot } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

export class HandoffCompactionError extends Schema.TaggedError<HandoffCompactionError>()(
  "HandoffCompactionError",
  { reason: Schema.Literals(["ineligible", "cancelled", "failed", "ambiguous", "timeout"]) },
) {
  override get message() {
    return "Automatic context compaction could not be confirmed; the pending request was not sent.";
  }
}

interface Binding {
  readonly runtimeGeneration: string;
  readonly nativeThreadId: string;
}
interface AttemptBinding extends Binding {
  readonly runId: string;
  readonly attemptId: string;
}
interface OwnedCompaction extends Binding {
  readonly compactionItemId: string;
}
interface Request extends AttemptBinding {
  readonly done: Deferred.Deferred<ThreadTokenUsageSnapshot, HandoffCompactionError>;
  nativeTurnId?: string;
  compactionItemId?: string;
  dispatched: boolean;
  acknowledged: boolean;
  compacted: boolean;
  completed: boolean;
  failed: boolean;
  ambiguous: boolean;
  restorationPending: boolean;
  usage?: ThreadTokenUsageSnapshot;
}

const matchesBinding = (owner: Binding | undefined, binding: Binding) =>
  owner?.runtimeGeneration === binding.runtimeGeneration &&
  owner.nativeThreadId === binding.nativeThreadId;

// Never register maintenance as a root turn: it would attribute unsent user input to that native turn.
// Cancellation never revives it; acknowledged terminal evidence may release its native fence.
// After restoration dispatch, require restoration acknowledgement or producer retirement;
// the earlier compaction terminal cannot settle the later effect.
export function makeCodexHandoffCompaction() {
  const consumed = new Set<string>();
  // At most one entry per consumed attempt. Only matching late maintenance
  // frames may pass a subsequent start's rejection guard.
  const ownedTurns = new Map<string, OwnedCompaction>();
  let pending: Request | undefined;
  const matches = (binding: Binding) => matchesBinding(pending, binding);
  const canRestore = (request: Request) =>
    pending === request && !request.failed && request.restorationPending;
  const clear = (request: Request) => {
    if (request.nativeTurnId !== undefined && request.compactionItemId !== undefined)
      ownedTurns.set(request.nativeTurnId, {
        runtimeGeneration: request.runtimeGeneration,
        nativeThreadId: request.nativeThreadId,
        compactionItemId: request.compactionItemId,
      });
    pending = undefined;
  };
  const reconcile = (request: Request) => {
    if (
      pending === request &&
      request.failed &&
      request.acknowledged &&
      request.completed &&
      request.compactionItemId !== undefined &&
      !request.ambiguous &&
      !request.restorationPending
    )
      clear(request);
  };
  const finish = (request: Request) => {
    reconcile(request);
    return request.acknowledged &&
      request.compacted &&
      request.completed &&
      request.usage &&
      !request.failed
      ? Deferred.succeed(request.done, request.usage).pipe(Effect.asVoid)
      : Effect.void;
  };
  const cancelRequest = (request: Request, reason: HandoffCompactionError["reason"]) =>
    Effect.suspend(() => {
      request.failed = true;
      if (reason === "ambiguous") request.ambiguous = true;
      reconcile(request);
      return Deferred.fail(request.done, new HandoffCompactionError({ reason })).pipe(
        Effect.asVoid,
      );
    });
  const cancel = (reason: HandoffCompactionError["reason"]) =>
    Effect.suspend(() => (pending ? cancelRequest(pending, reason) : Effect.void));
  return {
    get blocked() {
      return pending !== undefined;
    },
    owns(binding: Binding) {
      return matches(binding);
    },
    canRestore(binding: AttemptBinding) {
      return (
        pending !== undefined &&
        matches(binding) &&
        pending.runId === binding.runId &&
        pending.attemptId === binding.attemptId &&
        canRestore(pending)
      );
    },
    ownsTurn(binding: Binding, nativeTurnId: string) {
      return (
        (matches(binding) && pending?.nativeTurnId === nativeTurnId) ||
        matchesBinding(ownedTurns.get(nativeTurnId), binding)
      );
    },
    ownsItem(binding: Binding, nativeTurnId: string, itemId: string) {
      return (
        matches(binding) &&
        pending?.nativeTurnId === nativeTurnId &&
        pending.compactionItemId === itemId
      );
    },
    observeActivity(binding: Binding, nativeTurnId: string, method: string, payload: object) {
      const item =
        (method === "item/started" || method === "item/completed") &&
        Predicate.hasProperty(payload, "item") &&
        Predicate.isObject(payload.item) &&
        payload.item.type === "contextCompaction" &&
        typeof payload.item.id === "string"
          ? payload.item.id
          : undefined;
      const started = method === "turn/started";
      const turnEvidence =
        started ||
        method === "thread/tokenUsage/updated" ||
        method === "turn/completed" ||
        method === "thread/compacted";
      const owned = ownedTurns.get(nativeTurnId);
      if (
        owned !== undefined &&
        matchesBinding(owned, binding) &&
        (turnEvidence || item === owned.compactionItemId)
      )
        return true;
      const request = pending;
      if (request === undefined || !matches(binding) || !request.dispatched) return false;
      // Compact's ACK carries no turn ID. Hold the candidate until its own
      // compaction item, terminal, usage and ACK agree. Every other frame
      // naming unresolved work on this binding remains opposing evidence.
      if (
        (request.nativeTurnId === undefined || request.nativeTurnId === nativeTurnId) &&
        (started ||
          item !== undefined ||
          (request.nativeTurnId === nativeTurnId && turnEvidence)) &&
        (item === undefined ||
          request.compactionItemId === undefined ||
          request.compactionItemId === item)
      ) {
        request.nativeTurnId ??= nativeTurnId;
        if (item !== undefined) request.compactionItemId ??= item;
        return true;
      }
      request.failed = true;
      request.ambiguous = true;
      Deferred.doneUnsafe(
        request.done,
        Effect.fail(new HandoffCompactionError({ reason: "ambiguous" })),
      );
      return false;
    },
    nativeTurnId(binding: Binding) {
      return matches(binding) && pending?.compactionItemId !== undefined
        ? pending.nativeTurnId
        : undefined;
    },
    cancel,
    retire(binding: Binding) {
      return Effect.sync(() => {
        if (matches(binding)) pending = undefined;
        for (const [id, owned] of ownedTurns)
          if (matchesBinding(owned, binding)) ownedTurns.delete(id);
      });
    },
    begin: Effect.fn("CodexHandoffCompaction.begin")(function* (binding: AttemptBinding) {
      const key = [
        binding.runtimeGeneration,
        binding.nativeThreadId,
        binding.runId,
        binding.attemptId,
      ]
        .map((part) => `${part.length}:${part}`)
        .join("");
      if (pending || consumed.has(key) || consumed.size >= 1_024)
        return yield* new HandoffCompactionError({ reason: "ineligible" });
      const request: Request = {
        ...binding,
        done: yield* Deferred.make<ThreadTokenUsageSnapshot, HandoffCompactionError>(),
        dispatched: false,
        acknowledged: false,
        compacted: false,
        completed: false,
        failed: false,
        ambiguous: false,
        restorationPending: false,
      };
      consumed.add(key);
      pending = request;
      return {
        cancel: cancelRequest(request, "cancelled"),
        // Only an unsent reservation can be discarded without native evidence.
        // Keep its consumed attempt so cancellation cannot replay the same input.
        cancelIfUndispatched: Effect.suspend(() => {
          if (
            pending === request &&
            !request.dispatched &&
            !request.acknowledged &&
            request.nativeTurnId === undefined &&
            !request.restorationPending
          )
            pending = undefined;
          return cancelRequest(request, "cancelled");
        }),
        canDispatch: () => pending === request && !request.failed && !request.dispatched,
        markDispatched: () => {
          request.dispatched = true;
        },
        assertActive: Effect.suspend(() =>
          request.failed
            ? Effect.fail(new HandoffCompactionError({ reason: "cancelled" }))
            : Effect.void,
        ),
        wait: Deferred.await(request.done),
        acknowledge: Effect.suspend(() => {
          request.acknowledged = true;
          return finish(request);
        }),
        beginRestoration: Effect.suspend(() => {
          if (pending !== request || request.failed)
            return Effect.fail(new HandoffCompactionError({ reason: "cancelled" }));
          request.restorationPending = true;
          return Effect.void;
        }),
        canRestore: () => canRestore(request),
        acknowledgeRestoration: Effect.sync(() => {
          request.restorationPending = false;
          reconcile(request);
        }),
        release: (canRelease: () => boolean) =>
          Effect.sync(() => {
            if (
              pending === request &&
              !request.failed &&
              !request.restorationPending &&
              request.acknowledged &&
              request.compacted &&
              request.completed &&
              request.usage !== undefined &&
              canRelease()
            ) {
              clear(request);
              return true;
            }
            return false;
          }),
      };
    }),
    compacted(binding: Binding, nativeTurnId: string) {
      return Effect.suspend(() => {
        if (!matches(binding) || !pending || pending.nativeTurnId !== nativeTurnId)
          return Effect.void;
        pending.compacted = true;
        return finish(pending);
      });
    },
    usage(binding: Binding, nativeTurnId: string, usage: ThreadTokenUsageSnapshot) {
      return Effect.suspend(() => {
        if (!matches(binding) || !pending || pending.nativeTurnId !== nativeTurnId)
          return Effect.void;
        if (
          !Number.isFinite(usage.usedTokens) ||
          usage.usedTokens < 0 ||
          !usage.maxTokens ||
          usage.maxTokens <= 0
        )
          return cancel("failed");
        pending.usage = usage;
        return finish(pending);
      });
    },
    completed(binding: Binding, nativeTurnId: string, succeeded: boolean) {
      return Effect.suspend(() => {
        if (!matches(binding) || !pending || pending.nativeTurnId !== nativeTurnId)
          return Effect.void;
        pending.completed = true;
        if (!succeeded) return cancel("failed");
        return finish(pending);
      });
    },
  };
}
