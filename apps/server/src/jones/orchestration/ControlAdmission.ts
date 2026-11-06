import type { ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

export class ControlAdmissionHeld extends Schema.TaggedError<ControlAdmissionHeld>()(
  "ControlAdmissionHeld",
  { instanceId: Schema.String },
) {
  override get message(): string {
    return "A previous provider request has an unknown outcome for this conversation; new work is held until it is reconciled.";
  }
}

export interface ControlProducerKey {
  readonly instanceId: string;
  readonly providerSessionId: string;
  readonly runtimeGeneration: string;
}

export interface ControlAdmissionTarget {
  readonly instanceId: string;
  readonly threadId: string | null;
  readonly nativeThreadId: string | null;
  readonly producer: ControlProducerKey | null;
}

declare const startupHoldBrand: unique symbol;
export interface StartupHold {
  readonly [startupHoldBrand]: true;
}

export interface ControlDispatchBinding extends ControlProducerKey {
  readonly threadId: string;
  readonly providerThreadId: string;
  readonly providerTurnId: string;
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
}

export interface ControlDispatch {
  readonly binding: ControlDispatchBinding;
}

export const targetFromDispatch = (binding: ControlDispatchBinding): ControlAdmissionTarget => ({
  instanceId: binding.instanceId,
  threadId: binding.threadId,
  nativeThreadId: binding.nativeThreadId,
  producer: binding,
});

export function isCodexMutation(method: unknown, params: unknown): boolean {
  if (method === "thread/goal/set") {
    return !(
      typeof params === "object" &&
      params !== null &&
      "status" in params &&
      params.status === "paused" &&
      Object.keys(params).every((key) => key === "threadId" || key === "status")
    );
  }
  return (
    typeof method === "string" &&
    [
      "thread/start",
      "thread/resume",
      "thread/fork",
      "thread/unarchive",
      "thread/revert",
      "thread/rollback",
      "thread/inject_items",
      "thread/compact/start",
      "thread/goal/clear",
      "turn/start",
      "turn/steer",
    ].includes(method)
  );
}

const sameProducer = (left: ControlProducerKey | null, right: ControlProducerKey | null) =>
  left !== null &&
  right !== null &&
  left.instanceId === right.instanceId &&
  left.providerSessionId === right.providerSessionId &&
  left.runtimeGeneration === right.runtimeGeneration;
const validTarget = (target: ControlAdmissionTarget) => {
  const selector = (id: string | null) =>
    id === null || (typeof id === "string" && id.trim().length > 0);
  if (!selector(target.threadId) || !selector(target.nativeThreadId)) return false;
  const producer = target.producer;
  if (
    producer !== null &&
    (producer.instanceId !== target.instanceId ||
      producer.providerSessionId.trim().length === 0 ||
      producer.runtimeGeneration.trim().length === 0)
  ) return false;
  return target.threadId !== null || target.nativeThreadId !== null || producer !== null;
};
const conflicts = (held: ControlAdmissionTarget, target: ControlAdmissionTarget) =>
  (held.threadId !== null && held.threadId === target.threadId) ||
  (held.nativeThreadId !== null && held.nativeThreadId === target.nativeThreadId) ||
  // Unknown creation retains both its logical alias and its exact issuing process.
  ((held.nativeThreadId === null || (target.threadId === null && target.nativeThreadId === null)) &&
    sameProducer(held.producer, target.producer));

export class ControlAdmission extends Context.Service<
  ControlAdmission,
  {
    readonly assertOpen: (target: ControlAdmissionTarget) => Effect.Effect<void, ControlAdmissionHeld>;
    readonly begin: (
      binding: ControlDispatchBinding,
    ) => Effect.Effect<ControlDispatch, ControlAdmissionHeld>;
    readonly markUnknown: (dispatch: ControlDispatch) => Effect.Effect<void>;
    readonly acknowledge: (
      dispatch: ControlDispatch,
      binding: ControlDispatchBinding,
    ) => Effect.Effect<boolean>;
    readonly rejectNonApplying: (dispatch: ControlDispatch) => Effect.Effect<void>;
    readonly holdStartup: (target: ControlAdmissionTarget) => Effect.Effect<StartupHold>;
    readonly identifyStartup: (
      hold: StartupHold,
      nativeThreadId: string,
    ) => Effect.Effect<void, ControlAdmissionHeld>;
    readonly resolveStartup: (
      hold: StartupHold,
      resolution:
        | { readonly kind: "not_applied" }
        | { readonly kind: "bound"; readonly binding: ProviderRuntimeBinding },
    ) => Effect.Effect<void, ControlAdmissionHeld>;
    // Only verified physical exit of this full producer key retires its uncertainty.
    readonly retireGeneration: (producer: ControlProducerKey) => Effect.Effect<void>;
    readonly snapshot: (
      instanceId: string,
    ) => Effect.Effect<ReadonlyArray<{
      readonly binding: ControlDispatchBinding;
      readonly unknown: boolean;
    }>>;
  }
>()("t3/jones/orchestration/ControlAdmission") {}

export const layer = Layer.sync(ControlAdmission, () => {
  const dispatches = new Map<ControlDispatch, boolean>();
  const startups = new Map<StartupHold, ControlAdmissionTarget>();
  const blocked = (target: ControlAdmissionTarget) =>
    !validTarget(target) ||
    Array.from(dispatches).some(
      ([dispatch, unknown]) => unknown && conflicts(targetFromDispatch(dispatch.binding), target),
    ) ||
    Array.from(startups.values()).some((held) => conflicts(held, target));
  const fail = (instanceId: string) => Effect.fail(new ControlAdmissionHeld({ instanceId }));
  return ControlAdmission.of({
    assertOpen: (target) =>
      Effect.suspend(() => blocked(target) ? fail(target.instanceId) : Effect.void),
    begin: (binding) =>
      Effect.suspend(() => {
        if (blocked(targetFromDispatch(binding))) return fail(binding.instanceId);
        const dispatch: ControlDispatch = { binding: { ...binding } };
        dispatches.set(dispatch, false);
        return Effect.succeed(dispatch);
      }),
    markUnknown: (dispatch) =>
      Effect.sync(() => {
        if (dispatches.has(dispatch)) dispatches.set(dispatch, true);
      }),
    acknowledge: (dispatch, binding) =>
      Effect.sync(() => {
        if (!dispatches.has(dispatch)) return false;
        for (const key of Object.keys(dispatch.binding) as Array<keyof ControlDispatchBinding>) {
          if (dispatch.binding[key] !== binding[key]) return false;
        }
        dispatches.delete(dispatch);
        return true;
      }),
    rejectNonApplying: (dispatch) =>
      Effect.sync(() => {
        dispatches.delete(dispatch);
      }),
    holdStartup: (target) =>
      Effect.sync(() => {
        const hold = Object.freeze({}) as StartupHold;
        // A missing native ID must retain a logical owner and producer, never an account veto.
        if (
          !validTarget(target) ||
          (target.nativeThreadId === null && (target.threadId === null || target.producer === null))
        ) throw new Error("Startup uncertainty requires its logical thread and exact producer.");
        startups.set(hold, {
          ...target,
          producer: target.producer === null ? null : { ...target.producer },
        });
        return hold;
      }),
    identifyStartup: (hold, nativeThreadId) =>
      Effect.suspend(() => {
        const target = startups.get(hold);
        if (
          target === undefined ||
          nativeThreadId.trim().length === 0 ||
          (target.nativeThreadId !== null && target.nativeThreadId !== nativeThreadId)
        )
          return fail(target?.instanceId ?? "unknown");
        startups.set(hold, { ...target, nativeThreadId });
        return Effect.void;
      }),
    resolveStartup: (hold, resolution) =>
      Effect.suspend(() => {
        const target = startups.get(hold);
        if (target === undefined) return fail("unknown");
        if (resolution.kind === "bound") {
          const binding = resolution.binding;
          if (
            target.nativeThreadId !== binding.nativeThreadId ||
            target.threadId !== binding.threadId ||
            !sameProducer(target.producer, {
              instanceId: binding.providerInstanceId,
              providerSessionId: binding.providerSessionId,
              runtimeGeneration: binding.runtimeGeneration,
            })
          )
            return fail(target.instanceId);
        }
        startups.delete(hold);
        return Effect.void;
      }),
    retireGeneration: (producer) =>
      Effect.sync(() => {
        for (const dispatch of dispatches.keys()) {
          if (sameProducer(dispatch.binding, producer)) dispatches.delete(dispatch);
        }
        for (const [hold, target] of startups) {
          if (sameProducer(target.producer, producer)) startups.delete(hold);
        }
      }),
    snapshot: (instanceId) =>
      Effect.sync(() => Array.from(dispatches).flatMap(([dispatch, unknown]) =>
        dispatch.binding.instanceId === instanceId
          ? [{ binding: { ...dispatch.binding }, unknown }]
          : [],
      )),
  });
});
