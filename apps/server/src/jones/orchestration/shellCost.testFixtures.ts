import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import type * as Duration from "effect/Duration";
import * as Tracer from "effect/Tracer";
import * as Statement from "effect/sql/Statement";
import {
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";

export type Query = { text: string; parameters?: ReadonlyArray<unknown> };
export type Measurement = { operation: string; milliseconds: number; statements: Query[] };

export function measure<A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
  measurements: Measurement[],
): Effect.Effect<A, E, R> {
  return Effect.suspend(() => {
    const statements: Query[] = [];
    const bindings = new WeakMap<Tracer.Span, Query>();
    const capture: Statement.Transformer = (statement, _sql, _fiber, span) => {
      const [text, parameters] = statement.compile();
      bindings.set(span, { text, parameters: [...parameters] });
      return Effect.succeed(statement);
    };
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          const text = span.attributes.get("db.query.text");
          if (typeof text === "string") {
            const captured = bindings.get(span);
            statements.push(captured?.text === text ? captured : { text });
          }
        };
        return span;
      },
    });
    const start = performance.now();
    return effect.pipe(
      Effect.provideService(Statement.CurrentTransformer, capture),
      Effect.withTracer(tracer),
      Effect.ensuring(
        Effect.sync(() => {
          measurements.push({ operation, milliseconds: performance.now() - start, statements });
        }),
      ),
    );
  });
}

// Owns each diagnostic phase origin so observations use the same native monotonic clock.
export class NativeTimeline {
  readonly origins = new Map<string, number>();
  mark(phase: string) {
    this.origins.set(phase, performance.now());
  }
  elapsed(phase: string) {
    const origin = this.origins.get(phase);
    if (origin === undefined) throw new Error(`Diagnostic phase ${phase} has not started`);
    return performance.now() - origin;
  }
}

// Only the watchdog uses wall time; the operation retains its controlled stream clock.
export function withNativeWatchdog<A, E, R>(
  operation: Effect.Effect<A, E, R>,
  deadline: Duration.Input,
  failure: () => Error,
): Effect.Effect<A, E, R> {
  return operation.pipe(
    Effect.raceFirst(
      Effect.sleep(deadline).pipe(
        Effect.andThen(() => Effect.die(failure())),
        Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
      ),
    ),
  );
}

// A real projection makes inherited shell changes observable without changing the child's row.
export const seedForkShell = Effect.fn("shellCost.seedForkShell")(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sourceId = ThreadId.make("shell-composed:source");
  const threadId = ThreadId.make("shell-composed:fork");
  const runId = RunId.make("shell-composed:run");
  const now = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
  const instanceId = ProviderInstanceId.make("codex");
  const modelSelection = { instanceId, model: "synthetic" };
  for (const id of [sourceId, threadId]) {
    yield* projections.apply({
      id: EventId.make(`${id}:created`),
      type: "thread.created",
      threadId: id,
      occurredAt: now,
      payload: {
        id,
        projectId: ProjectId.make("shell-composed:project"),
        title: id,
        providerInstanceId: instanceId,
        modelSelection,
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        forkedFrom: id === threadId ? { type: "run", threadId: sourceId, runId } : null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        deletedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
      },
    });
  }
  yield* projections.apply({
    id: EventId.make("shell-composed:run-created"),
    type: "run.created",
    threadId: sourceId,
    occurredAt: now,
    payload: {
      id: runId,
      threadId: sourceId,
      ordinal: 1,
      providerInstanceId: instanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: MessageId.make("shell-composed:user"),
      rootNodeId: NodeId.make("shell-composed:node"),
      activeAttemptId: null,
      status: "completed",
      requestedAt: now,
      startedAt: now,
      completedAt: now,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  const appendInheritedItem = Effect.fn("shellCost.appendInheritedItem")(function* (index: number) {
    yield* projections.apply({
      id: EventId.make(`shell-composed:item:${index}`),
      type: "turn-item.updated",
      threadId: sourceId,
      occurredAt: now,
      payload: {
        id: TurnItemId.make(`shell-composed:item:${index}`),
        threadId: sourceId,
        runId,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: index,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "assistant_message",
        messageId: MessageId.make(`shell-composed:message:${index}`),
        text: "inherited",
        streaming: false,
      },
    });
  });
  yield* appendInheritedItem(1);
  return { threadId, appendInheritedItem };
});
