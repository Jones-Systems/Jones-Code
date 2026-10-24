import * as CodexErrors from "effect-codex-app-server/errors";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ControlAdmission from "./ControlAdmission.ts";

export const CODEX_STEER_TIMEOUT_MS = 30_000;

export const verifyCodexProcessExit = (
  handle: Pick<ChildProcessSpawner.ChildProcessHandle, "exitCode" | "isRunning">,
) =>
  Effect.gen(function* () {
    // A signal exit rejects exitCode. The captured Node handle's isRunning reads
    // its native exit-event Deferred, independently of the protocol's scope.
    yield* handle.exitCode.pipe(Effect.catch(() => Effect.void));
    if (yield* handle.isRunning)
      return yield* Effect.fail(
        new CodexErrors.CodexAppServerTransportError({
          operation: "read-process-exit-status",
          cause: "The captured child has no confirmed exit.",
        }),
      );
  }).pipe(
    Effect.timeout("3 seconds"),
    Effect.mapError(
      (cause) =>
        new CodexErrors.CodexAppServerTransportError({
          operation: "read-process-exit-status",
          cause,
        }),
    ),
  );

export class SteerNotDispatched extends Schema.TaggedError<SteerNotDispatched>()(
  "SteerNotDispatched",
  { instanceId: Schema.String },
) {
  override get message(): string {
    return "The steering deadline expired before the native request was sent.";
  }
}

export class SteerOutcomeUnknown extends Schema.TaggedError<SteerOutcomeUnknown>()(
  "SteerOutcomeUnknown",
  {
    instanceId: Schema.String,
    nativeThreadId: Schema.String,
    nativeTurnId: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message(): string {
    return "The steering request was sent but its outcome is unknown; it will not be sent again.";
  }
}

export function findSteerFailure(
  error: unknown,
): SteerNotDispatched | SteerOutcomeUnknown | ControlAdmission.ControlAdmissionHeld | undefined {
  const visited = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !visited.has(current)) {
    visited.add(current);
    if (
      Schema.is(SteerNotDispatched)(current) ||
      Schema.is(SteerOutcomeUnknown)(current) ||
      Schema.is(ControlAdmission.ControlAdmissionHeld)(current)
    )
      return current;
    if (Cause.isCause(current)) {
      const failure = Cause.findErrorOption(current);
      current = Option.isSome(failure) ? failure.value : undefined;
    } else current = "cause" in current ? current.cause : undefined;
  }
  return undefined;
}

// These protocol validation errors reject the request before the method applies.
// Decode failures, transport loss and arbitrary server errors provide no such proof.
const isNonApplyingRejection = (error: CodexErrors.CodexAppServerError) =>
  error._tag === "CodexAppServerRequestError" &&
  error.operation === "receive-response" &&
  error.method === "turn/steer" &&
  (error.code === -32600 || error.code === -32601 || error.code === -32602);

export const boundedSteer = <E, R>(input: {
  readonly instanceId: string;
  readonly prepare: Effect.Effect<
    {
      readonly binding: ControlAdmission.ControlDispatchBinding;
      readonly producerScope: Scope.Scope;
      readonly send: Effect.Effect<{ readonly turnId: string }, CodexErrors.CodexAppServerError>;
    },
    E,
    R
  >;
}) =>
  Effect.gen(function* () {
    const admission = yield* ControlAdmission.ControlAdmission;
    let dispatch: ControlAdmission.ControlDispatch | undefined;
    const unknown = (cause?: unknown) =>
      new SteerOutcomeUnknown({
        instanceId: input.instanceId,
        nativeThreadId: dispatch!.binding.nativeThreadId,
        nativeTurnId: dispatch!.binding.nativeTurnId,
        ...(cause === undefined ? {} : { cause }),
      });
    return yield* Effect.gen(function* () {
      yield* admission.assertOpen(input.instanceId);
      const prepared = yield* input.prepare;
      // Begin and fork cannot be interrupted between recording ownership and sending.
      const pending = yield* Effect.uninterruptibleMask(() =>
        Effect.gen(function* () {
          const owned = yield* admission.begin(prepared.binding);
          dispatch = owned;
          return yield* prepared.send.pipe(
            Effect.interruptible,
            Effect.flatMap((response) =>
              admission
                .acknowledge(owned, {
                  ...prepared.binding,
                  nativeTurnId: response.turnId,
                })
                .pipe(
                  Effect.flatMap((matched) => (matched ? Effect.void : Effect.fail(unknown()))),
                ),
            ),
            Effect.catch(
              (
                error,
              ): Effect.Effect<void, CodexErrors.CodexAppServerError | SteerOutcomeUnknown> => {
                if (error._tag !== "SteerOutcomeUnknown" && isNonApplyingRejection(error))
                  return admission
                    .rejectNonApplying(owned)
                    .pipe(Effect.andThen(Effect.fail(error)));
                return admission
                  .markUnknown(owned)
                  .pipe(Effect.andThen(Effect.fail(unknown(error))));
              },
            ),
            // Keep the original waiter alive after the caller times out or is cancelled.
            Effect.forkIn(prepared.producerScope, { startImmediately: true }),
          );
        }),
      );
      yield* Fiber.join(pending);
    }).pipe(
      Effect.timeoutOrElse({
        duration: CODEX_STEER_TIMEOUT_MS,
        orElse: () =>
          Effect.fail(
            dispatch === undefined
              ? new SteerNotDispatched({ instanceId: input.instanceId })
              : unknown(),
          ),
      }),
      Effect.onExit((exit) =>
        Exit.isFailure(exit) && dispatch !== undefined
          ? admission.markUnknown(dispatch)
          : Effect.void,
      ),
    );
  });
