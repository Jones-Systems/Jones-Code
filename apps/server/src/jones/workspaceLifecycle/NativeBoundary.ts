import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import type { IntentCommand } from "../ordinaryCheckout/CheckoutIntent.ts";
import { digest } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { LifecycleConfiguration } from "./NativeSource.ts";
import { LifecycleError } from "./Protocol.ts";
import { WorkspaceLifecycle, type NativeTransition } from "./WorkspaceLifecycle.ts";

const decodeScope = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.String,
      projectId: Schema.String,
      worktreePath: Schema.NullOr(Schema.String),
    }),
  ),
);
const isLifecycleError = Schema.is(LifecycleError);
const managedThread = Effect.fn("WorkspaceLifecycle.managedThread")(
  function* (threadId: string) {
    const configured = yield* Effect.serviceOption(LifecycleConfiguration);
    if (Option.isNone(configured)) return true;
    const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(sql)) return yield* new LifecycleError({ reason: "unavailable" });
    const rows = yield* sql.value<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${threadId}`;
    if (!rows[0]) return yield* new LifecycleError({ reason: "identity" });
    const thread = yield* decodeScope(rows[0].payload_json);
    if (thread.id !== threadId) return yield* new LifecycleError({ reason: "identity" });
    const prior =
      yield* sql.value`SELECT worktree FROM jones_workspace_lifecycle_bindings WHERE thread_id=${threadId} LIMIT 1`;
    if (prior.length > 0) return true;
    if (thread.worktreePath === null) return false;
    if (configured.value.projects.has(thread.projectId)) return true;
    const bound =
      yield* sql.value`SELECT worktree FROM jones_workspace_lifecycle_bindings WHERE worktree=${thread.worktreePath}`;
    return bound.length > 0;
  },
  Effect.mapError((cause) =>
    isLifecycleError(cause) ? cause : new LifecycleError({ reason: "storage", cause }),
  ),
);

const originalIsManaged = Effect.fn("WorkspaceLifecycle.originalIsManaged")(
  function* (id: string) {
    if (Option.isNone(yield* Effect.serviceOption(LifecycleConfiguration))) return false;
    const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(sql)) return yield* new LifecycleError({ reason: "unavailable" });
    const rows =
      yield* sql.value`SELECT transition_id FROM jones_workspace_lifecycle_transitions WHERE transition_id=${id}`;
    return rows.length > 0;
  },
  Effect.mapError((cause) =>
    isLifecycleError(cause) ? cause : new LifecycleError({ reason: "storage", cause }),
  ),
);

export class OriginalNativeTransition extends Context.Service<
  OriginalNativeTransition,
  { readonly id: string; readonly requestDigest: string }
>()("t3/jones/workspaceLifecycle/NativeBoundary/OriginalNativeTransition") {}

export const withOriginalTransition = <A, E, R>(
  id: string,
  request: unknown,
  effect: Effect.Effect<A, E, R>,
) =>
  effect.pipe(
    Effect.provideService(OriginalNativeTransition, { id, requestDigest: digest(request) }),
  );

export const inheritOriginalTransition = Effect.fn("WorkspaceLifecycle.inheritOriginalTransition")(
  function* <A, E, R>(id: string, request: unknown, effect: Effect.Effect<A, E, R>) {
    const current = yield* Effect.serviceOption(OriginalNativeTransition);
    return yield* Option.isSome(current) ? effect : withOriginalTransition(id, request, effect);
  },
);

export const beforeUse = Effect.fn("WorkspaceLifecycle.beforeNativeUse")(function* (input: {
  readonly threadId: string;
  readonly boundary: NativeTransition["boundary"];
  readonly fallback?: { readonly id: string; readonly request: unknown };
}) {
  const inherited = yield* Effect.serviceOption(OriginalNativeTransition);
  const original =
    Option.getOrUndefined(inherited) ??
    (input.fallback === undefined
      ? undefined
      : {
          id: input.fallback.id,
          requestDigest: digest(input.fallback.request),
        });
  if (
    !(yield* managedThread(input.threadId)) &&
    (original === undefined || !(yield* originalIsManaged(`${original.id}:${input.boundary}`)))
  )
    return;
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  const configured = yield* Effect.serviceOption(LifecycleConfiguration);
  if (Option.isNone(service)) {
    if (Option.isSome(configured)) return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  if (original === undefined) return yield* new LifecycleError({ reason: "identity" });
  yield* service.value.resume({
    transitionId: `${original.id}:${input.boundary}`,
    requestDigest: original.requestDigest,
    threadId: input.threadId,
    boundary: input.boundary,
  });
});

export const beforeCleanup = Effect.fn("WorkspaceLifecycle.beforeNativeCleanup")(function* (input: {
  readonly threadId: string;
  readonly boundary: "cleanup" | "settle-script";
  readonly id: string;
}) {
  if (!(yield* managedThread(input.threadId))) return;
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  const configured = yield* Effect.serviceOption(LifecycleConfiguration);
  if (Option.isNone(service)) {
    if (Option.isSome(configured)) return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  yield* service.value.cleanupVeto({
    transitionId: input.id,
    requestDigest: digest(input),
    threadId: input.threadId,
    boundary: input.boundary,
  });
});

export const beforeCleanupPath = Effect.fn("WorkspaceLifecycle.beforeCleanupPath")(function* (
  worktree: string,
) {
  const configured = yield* Effect.serviceOption(LifecycleConfiguration);
  if (Option.isSome(configured)) {
    const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(sql)) return yield* new LifecycleError({ reason: "unavailable" });
    // Cleanup already groups paths across threads; an unassigned orphan stays held.
    const rows = yield* sql.value<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.worktreePath')=${worktree}`.pipe(
      Effect.mapError((cause) => new LifecycleError({ reason: "storage", cause })),
    );
    const threads = yield* Effect.forEach(rows, (row) => decodeScope(row.payload_json)).pipe(
      Effect.mapError((cause) => new LifecycleError({ reason: "storage", cause })),
    );
    const bound =
      yield* sql.value`SELECT worktree FROM jones_workspace_lifecycle_bindings WHERE worktree=${worktree}`.pipe(
        Effect.mapError((cause) => new LifecycleError({ reason: "storage", cause })),
      );
    if (
      bound.length === 0 &&
      threads.length > 0 &&
      threads.every((thread) => !configured.value.projects.has(thread.projectId))
    )
      return;
  }
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  if (Option.isSome(service)) return yield* service.value.cleanupPathVeto(worktree);
  if (Option.isSome(yield* Effect.serviceOption(LifecycleConfiguration)))
    return yield* new LifecycleError({ reason: "unavailable" });
});

const commandTransitions = (
  command: IntentCommand,
  events: readonly OrchestrationV2DomainEvent[],
): readonly NativeTransition[] =>
  events.flatMap((event) =>
    event.type !== "thread.unsettled"
      ? []
      : [
          {
            transitionId: `command:${command.commandId}:${command.type === "thread.settlement.restore" ? "restore" : "unsettle"}`,
            requestDigest: digest(command),
            threadId: event.threadId,
            boundary:
              command.type === "thread.settlement.restore"
                ? ("restore" as const)
                : ("unsettle" as const),
          },
        ],
  );

export const beforeCommand = Effect.fn("WorkspaceLifecycle.beforeCommand")(function* (
  command: IntentCommand,
  events: readonly OrchestrationV2DomainEvent[],
) {
  const transitions = [];
  for (const transition of commandTransitions(command, events))
    if (
      (yield* managedThread(transition.threadId)) ||
      (yield* originalIsManaged(transition.transitionId))
    )
      transitions.push(transition);
  if (transitions.length === 0) return;
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  if (Option.isNone(service)) {
    if (Option.isSome(yield* Effect.serviceOption(LifecycleConfiguration)))
      return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  for (const transition of transitions) yield* service.value.resume(transition);
});

// EventSink calls only this SQL validation inside its transaction; external registry I/O belongs to beforeCommand.
export const validateCommand = Effect.fn("WorkspaceLifecycle.validateCommand")(function* (
  command: IntentCommand | undefined,
  events: readonly OrchestrationV2DomainEvent[],
) {
  if (!events.some((e) => e.type === "thread.unsettled")) return;
  if (
    Option.isNone(yield* Effect.serviceOption(WorkspaceLifecycle)) &&
    Option.isNone(yield* Effect.serviceOption(LifecycleConfiguration))
  )
    return;
  if (!command) {
    for (const event of events)
      if (event.type === "thread.unsettled" && (yield* managedThread(event.threadId)))
        return yield* new LifecycleError({ reason: "identity" });
    return;
  }
  const transitions = [];
  for (const transition of commandTransitions(command, events))
    if (
      (yield* managedThread(transition.threadId)) ||
      (yield* originalIsManaged(transition.transitionId))
    )
      transitions.push(transition);
  if (transitions.length === 0) return;
  const service = yield* Effect.serviceOption(WorkspaceLifecycle);
  if (Option.isNone(service)) {
    if (Option.isSome(yield* Effect.serviceOption(LifecycleConfiguration)))
      return yield* new LifecycleError({ reason: "unavailable" });
    return;
  }
  for (const transition of transitions) yield* service.value.validate(transition);
});
