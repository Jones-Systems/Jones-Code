import type { ThreadTokenUsageSnapshot } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
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
interface Request extends Binding {
  readonly done: Deferred.Deferred<ThreadTokenUsageSnapshot, HandoffCompactionError>;
  nativeTurnId?: string;
  acknowledged: boolean;
  compacted: boolean;
  completed: boolean;
  failed: boolean;
  ambiguous: boolean;
  restorationPending: boolean;
  usage?: ThreadTokenUsageSnapshot;
}

// Maintenance never registers a root provider turn: that would falsely attribute
// the still-unsent user input to this native turn. Cancellation never revives it;
// acknowledged terminal evidence can still release its native admission fence.
// Once restoration is dispatched, its own acknowledgement or producer retirement
// is required: the earlier compaction terminal cannot settle that later effect.
export function makeCodexHandoffCompaction() {
  const consumed = new Set<string>();
  let pending: Request | undefined;
  const matches = (binding: Binding) =>
    pending?.runtimeGeneration === binding.runtimeGeneration &&
    pending.nativeThreadId === binding.nativeThreadId;
  const reconcile = (request: Request) => {
    if (
      pending === request &&
      request.failed &&
      request.acknowledged &&
      request.completed &&
      !request.ambiguous &&
      !request.restorationPending
    )
      pending = undefined;
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
    nativeTurnId(binding: Binding) {
      return matches(binding) ? pending?.nativeTurnId : undefined;
    },
    cancel,
    retire(binding: Binding) {
      return Effect.sync(() => {
        if (matches(binding)) pending = undefined;
      });
    },
    begin(binding: Binding & { readonly runId: string; readonly attemptId: string }) {
      return Effect.gen(function* () {
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
          acknowledgeRestoration: Effect.sync(() => {
            request.restorationPending = false;
            reconcile(request);
          }),
          release: Effect.sync(() => {
            if (pending === request && !request.failed && !request.restorationPending)
              pending = undefined;
          }),
        };
      });
    },
    started(binding: Binding, nativeTurnId: string) {
      return Effect.suspend(() => {
        if (!matches(binding) || !pending) return Effect.void;
        if (pending.nativeTurnId && pending.nativeTurnId !== nativeTurnId)
          return cancel("ambiguous");
        pending.nativeTurnId = nativeTurnId;
        return Effect.void;
      });
    },
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
