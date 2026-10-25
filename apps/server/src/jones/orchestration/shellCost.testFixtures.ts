import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import type * as Duration from "effect/Duration";
import * as Tracer from "effect/Tracer";
import * as Statement from "effect/sql/Statement";
import * as SqlClient from "effect/sql/SqlClient";
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

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export type Query = {
  text: string;
  parameters?: ReadonlyArray<unknown>;
  spanMilliseconds?: number;
};
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
        const started = performance.now();
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          const text = span.attributes.get("db.query.text");
          if (typeof text === "string") {
            const captured = bindings.get(span);
            statements.push({
              ...(captured?.text === text ? captured : { text }),
              spanMilliseconds: performance.now() - started,
            });
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

// Replays are outside all measured phases. Cardinality is returned rows, not
// SQLite row visits; SQL spans include driver work and cannot isolate row copy.
export const queryEvidence = Effect.fn("shellCost.queryEvidence")(function* (
  measurements: ReadonlyArray<Measurement>,
) {
  const sql = yield* SqlClient.SqlClient;
  const distinct = new Map<
    string,
    { query: Query; occurrences: Array<{ operation: string; spanMilliseconds: number | null }> }
  >();
  for (const { operation, statements } of measurements) {
    for (const query of statements) {
      const key = encodeJson([query.text, query.parameters]);
      const entry = distinct.get(key) ?? { query, occurrences: [] };
      entry.occurrences.push({ operation, spanMilliseconds: query.spanMilliseconds ?? null });
      distinct.set(key, entry);
    }
  }
  return yield* Effect.forEach([...distinct.values()], ({ query, occurrences }) =>
    Effect.gen(function* () {
      const evidence = {
        text: query.text,
        parameters: query.parameters,
        occurrences,
        timingScope: "instrumented SQL span; admission/execution/transfer may overlap",
        driverExecutionMilliseconds: null,
        rowCopyMilliseconds: null,
        unavailable: "driver execution and row-copy timing are not independently exposed",
      };
      if (!/^\s*SELECT\b/i.test(query.text) || query.parameters === undefined) {
        return { ...evidence, observation: "not replayed: non-SELECT or bindings unavailable" };
      }
      const started = performance.now();
      const rows = yield* sql.unsafe(query.text, query.parameters);
      const replayMilliseconds = performance.now() - started;
      const plan = yield* sql.unsafe(`EXPLAIN QUERY PLAN ${query.text}`, query.parameters);
      const hypotheses = [
        ...(query.text.includes("AS item_count") ? ["H1:item-counts"] : []),
        ...(query.text.includes("latest_user_authored_message_at")
          ? ["H2:latest-user-authored"]
          : []),
        ...(query.text.includes("completed_at IS NULL DESC") ? ["H3:blocked-run-ordering"] : []),
        ...(query.text.includes("failure_payload_json") ? ["H4:failure-lookups"] : []),
        ...(query.text.includes("owning_provider_instance_id") ? ["H6:pending-items"] : []),
      ];
      return {
        ...evidence,
        hypotheses,
        observation:
          "post-timing replay; replay includes SQL/driver/row transfer, excludes shell decode",
        replayMilliseconds,
        resultCardinality: rows.length,
        plan,
      };
    }),
  );
});

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
