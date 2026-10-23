import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

export class ControlAdmissionHeld extends Schema.TaggedError<ControlAdmissionHeld>()(
  "ControlAdmissionHeld",
  { instanceId: Schema.String },
) {
  override get message(): string {
    return "A previous provider control request has an unknown outcome; new work is held until it is reconciled.";
  }
}

export interface ControlDispatchBinding {
  readonly instanceId: string;
  readonly providerSessionId: string;
  readonly providerThreadId: string;
  readonly providerTurnId: string;
  readonly runtimeGeneration: string;
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
}

export interface ControlDispatch {
  readonly binding: ControlDispatchBinding;
}

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

export class ControlAdmission extends Context.Service<
  ControlAdmission,
  {
    readonly assertOpen: (instanceId: string) => Effect.Effect<void, ControlAdmissionHeld>;
    readonly begin: (
      binding: ControlDispatchBinding,
    ) => Effect.Effect<ControlDispatch, ControlAdmissionHeld>;
    readonly markUnknown: (dispatch: ControlDispatch) => Effect.Effect<void>;
    readonly acknowledge: (
      dispatch: ControlDispatch,
      binding: ControlDispatchBinding,
    ) => Effect.Effect<boolean>;
    readonly rejectNonApplying: (dispatch: ControlDispatch) => Effect.Effect<void>;
    // Only the owner of verified physical teardown may retire a generation.
    // Adapter replacement, missing sessions and native turn terminals are insufficient.
    readonly retireGeneration: (binding: {
      readonly instanceId: string;
      readonly runtimeGeneration: string;
    }) => Effect.Effect<void>;
    readonly snapshot: (
      instanceId: string,
    ) => Effect.Effect<
      ReadonlyArray<{ readonly binding: ControlDispatchBinding; readonly unknown: boolean }>
    >;
  }
>()("t3/jones/orchestration/ControlAdmission") {}

export const layer = Layer.sync(ControlAdmission, () => {
  const dispatches = new Map<ControlDispatch, boolean>();
  const blocked = (instanceId: string) =>
    Array.from(dispatches).some(
      ([dispatch, unknown]) => unknown && dispatch.binding.instanceId === instanceId,
    );
  return ControlAdmission.of({
    assertOpen: (instanceId) =>
      Effect.suspend(() =>
        blocked(instanceId) ? Effect.fail(new ControlAdmissionHeld({ instanceId })) : Effect.void,
      ),
    begin: (binding) =>
      Effect.suspend(() => {
        if (blocked(binding.instanceId))
          return Effect.fail(new ControlAdmissionHeld({ instanceId: binding.instanceId }));
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
    retireGeneration: (binding) =>
      Effect.sync(() => {
        for (const dispatch of dispatches.keys()) {
          if (
            dispatch.binding.instanceId === binding.instanceId &&
            dispatch.binding.runtimeGeneration === binding.runtimeGeneration
          )
            dispatches.delete(dispatch);
        }
      }),
    snapshot: (instanceId) =>
      Effect.sync(() =>
        Array.from(dispatches).flatMap(([dispatch, unknown]) =>
          dispatch.binding.instanceId === instanceId
            ? [{ binding: { ...dispatch.binding }, unknown }]
            : [],
        ),
      ),
  });
});
