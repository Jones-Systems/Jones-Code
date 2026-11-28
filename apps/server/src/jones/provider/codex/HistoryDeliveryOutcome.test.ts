import { assert, describe, it } from "@effect/vitest";
import {
  ContextHandoffId,
  EventId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2ContextHandoff,
} from "@t3tools/contracts";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../../persistence/EventLogPruneBoundary.ts";
import {
  HistoryNotApplied,
  PromptNotApplied,
  historyInjectionRejection,
  historyWasNotApplied,
  makeCodexHistoryDeliveryPersistence,
  promptWasNotApplied,
} from "./HistoryDeliveryOutcome.ts";

describe("history injection evidence", () => {
  it.each([
    {
      code: -32600,
      operation: "receive-response",
      method: "thread/inject_items",
      expected: "not_applied",
    },
    {
      code: -32602,
      operation: "receive-response",
      method: "thread/inject_items",
      expected: "not_applied",
    },
    {
      code: -32601,
      operation: "receive-response",
      method: "thread/inject_items",
      expected: "unsupported",
    },
    {
      code: -32602,
      operation: "encode-payload",
      method: "thread/inject_items",
      expected: "not_applied",
    },
    {
      code: -32602,
      operation: "decode-payload",
      method: "thread/inject_items",
      expected: "ambiguous",
    },
    {
      code: -32602,
      operation: "handle-request",
      method: "thread/inject_items",
      expected: "ambiguous",
    },
    { code: -32601, operation: undefined, method: undefined, expected: "ambiguous" },
    { code: -32602, operation: "receive-response", method: "turn/start", expected: "ambiguous" },
    {
      code: -32603,
      operation: "receive-response",
      method: "thread/inject_items",
      expected: "ambiguous",
    },
  ] as const)("requires attributed non-applying evidence: %j", ({ expected, ...input }) => {
    const error = new CodexErrors.CodexAppServerRequestError({
      code: input.code,
      errorMessage: "Synthetic rejection",
      ...(input.method === undefined ? {} : { method: input.method }),
      ...(input.operation === undefined ? {} : { operation: input.operation }),
    });
    assert.equal(historyInjectionRejection(error), expected);
    assert.isFalse(historyWasNotApplied(error));
  });

  it.each([
    { operation: "encode-payload", code: -32600, expected: true },
    { operation: "encode-payload", code: -32602, expected: true },
    { operation: "receive-response", code: -32600, expected: true },
    { operation: "receive-response", code: -32602, expected: true },
    { operation: "decode-payload", code: -32602, expected: false },
    { operation: "receive-response", code: -32603, expected: false },
  ] as const)(
    "attributes inline prompt refusals without admitting ambiguous failures: %j",
    ({ expected, ...input }) => {
      const cause = new CodexErrors.CodexAppServerRequestError({
        ...input,
        method: "turn/start",
        errorMessage: "Synthetic rejection",
      });
      assert.equal(promptWasNotApplied({ cause }), expected);
      assert.equal(
        promptWasNotApplied({ cause }, { beforeDispatchOnly: true }),
        input.operation === "encode-payload",
      );
    },
  );

  it("recognizes the private startup deadline's precluded-dispatch proof", () => {
    const cause = Object.assign(new Error("Local deadline"), { _tag: "CodexStartupNotDispatched" });
    assert.isTrue(promptWasNotApplied({ cause }, { beforeDispatchOnly: true }));
    assert.isTrue(promptWasNotApplied(new PromptNotApplied({ cause: new Error("Local refusal") })));
    assert.isFalse(
      historyWasNotApplied(new PromptNotApplied({ cause: new Error("Local refusal") })),
    );
    assert.isFalse(promptWasNotApplied(new Error("Timed out waiting for response")));
  });

  it("does not treat untyped or nested response data as pre-dispatch proof", () => {
    const untyped = {
      _tag: "CodexAppServerRequestError",
      method: "turn/start",
      operation: "encode-payload",
      code: -32602,
      errorMessage: "Unverified request error",
    };
    const wireError = new CodexErrors.CodexAppServerRequestError({
      method: "turn/start",
      operation: "receive-response",
      code: -32603,
      errorMessage: "Unverified response",
      cause: { _tag: "CodexStartupNotDispatched", cause: untyped },
    });
    assert.isFalse(promptWasNotApplied({ cause: untyped }, { beforeDispatchOnly: true }));
    assert.isFalse(promptWasNotApplied({ cause: wireError }, { beforeDispatchOnly: true }));
  });

  it("preserves the immediate cause and handles cyclic wrappers without inventing evidence", () => {
    const cause = new Error("Synthetic local admission refusal");
    const proof = new HistoryNotApplied({ cause });
    assert.strictEqual(proof.cause, cause);
    assert.isTrue(historyWasNotApplied({ cause: proof }));
    const cycle: { cause?: unknown } = {};
    cycle.cause = cycle;
    assert.isFalse(historyWasNotApplied(cycle));
    assert.equal(historyInjectionRejection(new Error("Transport lost")), "ambiguous");
  });
});

const persistenceLayer = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(EventStore.layer, ProjectionStore.layer)),
  Layer.provideMerge(Sqlite.layerMemory),
);

describe("definitively unsent history restoration", () => {
  it.effect.each([
    "latest marker",
    "statement rollback",
    "superseded after rollback",
    "pruned",
    "dropped inline",
    "superseded dropped inline",
  ])("compares the exact receipt for %s", (scenario) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("unsent-history");
      const runId = RunId.make("unsent-attempt-run");
      const original: OrchestrationV2ContextHandoff = {
        id: ContextHandoffId.make("unsent-handoff"),
        threadId,
        targetRunId: runId,
        fromProviderThreadIds: [],
        toProviderThreadId: ProviderThreadId.make("unsent-provider-thread"),
        coveredRunOrdinals: { from: 1, to: 1 },
        strategy: "full_thread_summary",
        status: "ready",
        summaryMessageId: null,
        summaryText: "Synthetic history",
        createdByProviderInstanceId: null,
        createdAt: now,
        updatedAt: now,
      };
      const pending = {
        ...original,
        delivery: { nativeThreadId: "known-conversation", status: "pending", itemIds: [] },
      } satisfies OrchestrationV2ContextHandoff;
      let id = 0;
      const write = (payload: OrchestrationV2ContextHandoff) =>
        Effect.suspend(() =>
          sink.write({
            events: [
              {
                id: EventId.make(`history-write-${++id}`),
                type: "context-handoff.updated",
                threadId,
                runId,
                occurredAt: now,
                payload,
              },
            ],
          }),
        );
      const latest = sql<{ readonly sequence: number; readonly payload: string }>`
          SELECT sequence, payload_json AS payload FROM orchestration_events
          WHERE event_type = 'context-handoff.updated' ORDER BY sequence DESC LIMIT 1
        `.pipe(Effect.map(([row]) => row!));
      let rollbacks = 0;
      let batches = 0;
      const retryingBatch: EventSink.EventSinkV2Shape["withProviderBatch"] = (body) =>
        Effect.sync(() => {
          batches += 1;
        }).pipe(
          Effect.andThen(sink.withProviderBatch(body)),
          Effect.tapError(() =>
            Effect.gen(function* () {
              rollbacks += 1;
              yield* sql`DROP TRIGGER fail_unsent_restore`.pipe(Effect.orDie);
              if (scenario === "superseded after rollback")
                yield* write(pending).pipe(Effect.orDie);
            }),
          ),
        );
      const older = makeCodexHistoryDeliveryPersistence(write, sink.withProviderBatch);
      const owner = makeCodexHistoryDeliveryPersistence(write, retryingBatch);
      yield* write(original);
      yield* older.persist(pending);
      const olderMarker = yield* latest;
      yield* owner.persist(pending);
      const ownedMarker = yield* latest;
      assert.equal(ownedMarker.payload, olderMarker.payload);
      assert.isAbove(ownedMarker.sequence, olderMarker.sequence);
      yield* older.restore(original);
      assert.deepEqual(yield* latest, ownedMarker);

      const withRollback =
        scenario === "statement rollback" || scenario === "superseded after rollback";
      if (withRollback) {
        yield* sql`CREATE TEMP TRIGGER fail_unsent_restore BEFORE INSERT ON orchestration_events
            WHEN NEW.event_type = 'context-handoff.updated'
              AND json_extract(NEW.payload_json, '$.delivery') IS NULL
            BEGIN SELECT RAISE(ABORT, 'synthetic restore failure'); END`;
      }
      if (scenario === "pruned") {
        yield* sql`INSERT INTO orchestration_v2_projection_metadata
            (projection_name, schema_version, last_sequence, updated_at)
            VALUES (${EVENT_LOG_PRUNED_THROUGH_PROJECTION}, 1, ${ownedMarker.sequence}, ${DateTime.formatIso(now)})`;
      }
      const dropped = scenario.endsWith("dropped inline");
      const superseded = scenario === "superseded dropped inline";
      if (dropped) {
        const delivered = {
          ...pending,
          delivery: { ...pending.delivery, status: "inline" as const },
        };
        yield* owner.persist(delivered);
        if (superseded) yield* write(delivered);
      }
      const beforeRestore = yield* latest;
      yield* dropped ? owner.restoreDropped(original) : owner.restore(original);
      const finalMarker = yield* latest;
      assert.equal(rollbacks, withRollback ? 1 : 0);
      assert.equal(batches, withRollback || dropped ? 2 : 1);
      if (scenario === "pruned" || superseded) assert.deepEqual(finalMarker, beforeRestore);
      else {
        assert.isAbove(finalMarker.sequence, beforeRestore.sequence);
        if (scenario === "superseded after rollback")
          assert.equal(finalMarker.payload, ownedMarker.payload);
        else assert.notProperty(JSON.parse(finalMarker.payload), "delivery");
      }
    }).pipe(Effect.provide(persistenceLayer)),
  );
});
