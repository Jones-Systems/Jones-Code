import {
  ModelSelection,
  type NativeCommandIdentityV2,
  type OrchestrationDispatchTargetV2,
  type OrchestrationV2ServerCommand,
  type ThreadTurnDispatchGuardV2,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { makeCommandObservationQuery } from "./CommandObservation.ts";
import type { NativeCommandFactsV2, NativeCommandAuthorityReadV2 } from "./EventSink.ts";
import { nativeCreationSha256 } from "./NativeCreationPreparation.ts";

const sameModelSelection = Schema.toEquivalence(ModelSelection);

export class DispatchGuardRejectedError extends Schema.TaggedError<DispatchGuardRejectedError>()(
  "DispatchGuardRejectedError",
  {
    commandType: Schema.String,
    reason: Schema.Literals([
      "unsupported_operation", "future_snapshot", "stale_target", "missing_target",
      "binding_mismatch", "busy", "unknown_evidence", "identity_conflict", "unbound_receipt",
    ]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `dispatch_guard_rejected: ${this.reason}: ${this.detail}`;
  }
}

// Type tags preserve omitted keys, explicit undefined/null, array order and decoded UTC dates.
export function nativeCommandCanonicalJsonV2(value: unknown): string {
  const encode = (child: unknown): unknown => {
    if (child === undefined) return ["undefined"];
    if (child === null) return ["null"];
    if (DateTime.isDateTime(child)) return ["utc", DateTime.formatIso(child)];
    if (Array.isArray(child)) return ["array", child.map(encode)];
    switch (typeof child) {
      case "string": return ["string", child];
      case "boolean": return ["boolean", child];
      case "number":
        if (!Number.isFinite(child)) throw new Error("Native command identity requires finite numbers.");
        return ["number", Object.is(child, -0) ? "-0" : child];
      case "object":
        if (Object.getPrototypeOf(child) !== Object.prototype && Object.getPrototypeOf(child) !== null)
          throw new Error("Native command identity requires decoded command values.");
        return ["object", Object.keys(child).sort().map((key) => [key, encode(Reflect.get(child, key))])];
      default: throw new Error("Native command identity requires decoded command values.");
    }
  };
  return JSON.stringify(encode(value));
}

export function makeGuardedCommandIdentityV2(
  command: Extract<OrchestrationV2ServerCommand, { readonly type: "message.dispatch" }>,
  guard: ThreadTurnDispatchGuardV2,
  binding: Readonly<Record<string, unknown>>,
): NativeCommandIdentityV2 {
  return {
    kind: "guarded_message_dispatch",
    version: 2,
    commandId: command.commandId,
    commandType: command.type,
    aggregateKind: "thread",
    aggregateId: command.threadId,
    normalizedCommandDigest: nativeCreationSha256(nativeCommandCanonicalJsonV2({ command, guard })),
    bindingDigest: nativeCreationSha256(nativeCommandCanonicalJsonV2(binding)),
  };
}

// Check native ownership before receipt status or current guard state, including ordinary retries.
export const assertNativeCommandReplayV2 = Effect.fn("DispatchGuard.assertNativeCommandReplayV2")(
  function* (facts: NativeCommandFactsV2, expected?: NativeCommandIdentityV2) {
    const reject = (reason: "identity_conflict" | "unbound_receipt", detail: string) =>
      new DispatchGuardRejectedError({ commandType: expected?.commandType ?? facts.receipt?.commandType ?? "unknown", reason, detail });
    if (facts.identity !== null && (facts.receipt === null || facts.identity.commandId !== facts.commandId ||
      facts.identity.aggregateId !== facts.threadId || facts.identity.commandType !== facts.receipt.commandType ||
      facts.receipt.threadId !== facts.threadId))
      return yield* reject("identity_conflict", "native identity has no matching original receipt");
    if (facts.identity !== null &&
      (expected === undefined || nativeCommandCanonicalJsonV2(facts.identity) !== nativeCommandCanonicalJsonV2(expected)))
      return yield* reject("identity_conflict", "native command identity differs");
    if (expected !== undefined && facts.receipt !== null && facts.identity === null)
      return yield* reject("unbound_receipt", "ordinary receipt has no original native identity");
    if (expected !== undefined &&
      (expected.commandId !== facts.commandId || expected.aggregateId !== facts.threadId ||
        (facts.receipt !== null && (facts.receipt.threadId !== expected.aggregateId || facts.receipt.commandType !== expected.commandType))))
      return yield* reject("identity_conflict", "receipt command or aggregate differs");
  },
);

export const validateDispatchGuardTargetV2 = Effect.fn("DispatchGuard.validateTargetV2")(
  function* (
    command: OrchestrationV2ServerCommand,
    guard: ThreadTurnDispatchGuardV2,
    current: { readonly facts: NativeCommandFactsV2; readonly target: OrchestrationDispatchTargetV2 | null; readonly runtimeReason?: string },
  ) {
    const reject = (reason: DispatchGuardRejectedError["reason"], detail: string) =>
      new DispatchGuardRejectedError({ commandType: command.type, reason, detail });
    if (command.type !== "message.dispatch" || command.dispatchMode.type !== "start_immediately")
      return yield* reject("unsupported_operation", "only an existing-thread immediate message is supported");
    const { target, facts } = current;
    if (target === null) return yield* reject("missing_target", "target is missing");
    if (guard.observedSnapshotSequence > facts.snapshotSequence)
      return yield* reject("future_snapshot", "observation is newer than the global watermark");
    if (facts.targetEventSequence > guard.observedSnapshotSequence)
      return yield* reject("stale_target", "target changed after observation");
    if (
      facts.threadId !== command.threadId || target.incarnation === null ||
      target.incarnation.eventId !== guard.expectedIncarnation.eventId ||
      target.incarnation.sequence !== guard.expectedIncarnation.sequence ||
      !sameModelSelection(target.modelSelection, guard.expectedModelSelection) ||
      (command.modelSelection !== undefined && command.modelSelection.instanceId !== guard.expectedModelSelection.instanceId) ||
      target.activeRunId !== guard.expectedActiveRunId ||
      target.latestRunId !== guard.expectedLatestRunId ||
      target.activeRunAttemptId !== guard.expectedActiveRunAttemptId ||
      target.activeProviderThreadId !== guard.expectedActiveProviderThreadId ||
      target.providerSessionId !== guard.expectedProviderSessionId ||
      target.providerSessionStatus !== guard.expectedProviderSessionStatus ||
      target.runtimeGeneration !== guard.expectedRuntimeGeneration
    ) return yield* reject("binding_mismatch", "target binding changed");
    if (!target.complete || target.blockers.includes("unknown_evidence"))
      return yield* reject("unknown_evidence", current.runtimeReason ?? "target evidence is incomplete");
    if (!target.idle) return yield* reject("busy", target.blockers.join(", "));
    return current;
  },
);

export const makeDispatchGuard = Effect.fn("makeDispatchGuard")(function* () {
  const query = yield* makeCommandObservationQuery();
  return Effect.fn("DispatchGuard.validate")(function* (
    command: OrchestrationV2ServerCommand,
    guard: ThreadTurnDispatchGuardV2,
    authority?: NativeCommandAuthorityReadV2,
  ) {
    if (command.type !== "message.dispatch")
      return yield* new DispatchGuardRejectedError({ commandType: command.type, reason: "unsupported_operation", detail: "only an existing-thread immediate message is supported" });
    const current = yield* query.getTarget(command.threadId, command.commandId, authority);
    return yield* validateDispatchGuardTargetV2(command, guard, current);
  });
});
