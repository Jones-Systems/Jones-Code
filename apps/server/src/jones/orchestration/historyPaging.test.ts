import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  buildBoundedThreadProjection,
  decodeThreadHistoryCursor,
  encodeThreadHistoryCursor,
  projectedRowEncodedBytes,
  THREAD_HISTORY_PAGE_POLICY,
} from "../../orchestration-v2/threadHistoryPaging.ts";
import { projectTurnItemForWire } from "../../orchestration-v2/WireProjection.ts";

const layer = Layer.mergeAll(
  ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
  SqlitePersistence.layerMemory,
);

type ObservedQuery = {
  query: string;
  parameters: ReadonlyArray<unknown> | undefined;
  outcome: "success" | "failure";
  rows: number | null;
};

const observeMetadata = () => {
  const queries: ObservedQuery[] = [];
  const bindings = new WeakMap<
    Tracer.Span,
    {
      query: string;
      parameters: ReadonlyArray<unknown>;
    }
  >();
  const capture: Statement.Transformer = (statement, _sql, _fiber, span) => {
    const [query, parameters] = statement.compile();
    bindings.set(span, { query, parameters: [...parameters] });
    return Effect.succeed(statement);
  };
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (endTime, exit) => {
        end(endTime, exit);
        const query = span.attributes.get("db.query.text");
        if (typeof query === "string") {
          const captured = bindings.get(span);
          queries.push({
            query,
            parameters: captured?.query === query ? captured.parameters : undefined,
            outcome: Exit.isSuccess(exit) ? "success" : "failure",
            rows: Exit.isSuccess(exit) && Array.isArray(exit.value) ? exit.value.length : null,
          });
        }
      };
      return span;
    },
  });
  const observe = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(Statement.CurrentTransformer, capture),
      Effect.withTracer(tracer),
    );
  return { queries, observe };
};

const selectionMetadata = (queries: ReadonlyArray<ObservedQuery>) =>
  queries.filter(({ query }) => query.includes("AS input_intent"));

const seedHistory = Effect.fn("historyPaging.test.seedHistory")(function* (
  suffix: string,
  mode: "human" | "notification" | "tools",
) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const now = yield* DateTime.now;
  const iso = DateTime.formatIso(now);
  const threadId = ThreadId.make(`thread:history-page:${suffix}`);
  const providerInstanceId = ProviderInstanceId.make("codex");
  const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };
  const thread = {
    id: threadId,
    projectId: ProjectId.make(`project:history-page:${suffix}`),
    title: "History page selection",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user" as const,
    creationSource: "web" as const,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* store.apply({
    id: EventId.make(`event:history-page:${suffix}`),
    type: "thread.created",
    threadId,
    occurredAt: now,
    payload: thread,
  });
  const runId = (run: number) => `run:history-page:${suffix}:${run}`;
  const itemId = (ordinal: number) => `item:history-page:${suffix}:${ordinal}`;
  const runs = Array.from({ length: 260 }, (_, index) => ({
    id: runId(index),
    threadId,
    ordinal: index + 1,
    providerInstanceId,
    modelSelection,
    providerThreadId: null,
    userMessageId: `message:history-page:${suffix}:${index}`,
    rootNodeId: `node:history-page:${suffix}:${index}`,
    activeAttemptId: null,
    status: "completed",
    requestedAt: iso,
    startedAt: iso,
    completedAt: iso,
    checkpointId: null,
    contextHandoffId: null,
  }));
  yield* sql`INSERT INTO orchestration_v2_projection_runs
    (run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at, completed_at, payload_json)
    SELECT json_extract(value, '$.id'), ${threadId}, json_extract(value, '$.ordinal'), 'codex',
      NULL, 'completed', ${iso}, ${iso}, value FROM json_each(${JSON.stringify(runs)})`;
  const attempts = runs.map((run) => ({
    id: `attempt:history-page:${suffix}:${run.ordinal}:completed`,
    runId: run.id,
    attemptOrdinal: 2,
    rootNodeId: run.rootNodeId,
    providerInstanceId,
    providerThreadId: `provider-thread:history-page:${suffix}`,
    providerTurnId: null,
    reason: "initial",
    status: "completed",
    startedAt: iso,
    completedAt: iso,
  }));
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
    (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
      provider_instance_id, provider_thread_id, provider_turn_id, status, payload_json)
    SELECT json_extract(value, '$.id'), ${threadId}, json_extract(value, '$.runId'),
      2, json_extract(value, '$.rootNodeId'), 'codex', 'codex',
      json_extract(value, '$.providerThreadId'), NULL, 'completed', value
    FROM json_each(${JSON.stringify(attempts)})`;
  const items = runs.flatMap((run, index) =>
    Array.from({ length: 4 }, (_, part) => {
      const ordinal = index * 4 + part + 1;
      const user = mode !== "tools" && part === 1 && (mode === "human" || index === 0);
      const notification = mode === "notification" && index > 0 && (part === 1 || part === 2);
      return {
        id: itemId(ordinal),
        threadId,
        runId: run.id,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        title: null,
        parentItemId: null,
        ordinal,
        status: "completed",
        startedAt: iso,
        completedAt: iso,
        updatedAt: iso,
        ...(user
          ? {
              type: "user_message",
              createdBy: "user",
              creationSource: "web",
              inputIntent: "turn_start",
              messageId: run.userMessageId,
              text: `Turn ${index}`,
              attachments: [],
            }
          : notification
            ? {
                type: "notification",
                source: { kind: "monitor" },
                outcome: "completed",
                summary: `Wake or steer ${index}`,
              }
            : {
                type: "command_execution",
                title: `Command ${ordinal}`,
                input: mode === "tools" && ordinal > 880 ? "x".repeat(20_000) : "echo fixture",
                output: "ok",
                exitCode: 0,
              }),
      };
    }),
  );
  yield* sql`INSERT INTO orchestration_v2_projection_turn_items
    (turn_item_id, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
      parent_item_id, ordinal, type, status, updated_at, payload_json)
    SELECT json_extract(value, '$.id'), ${threadId}, json_extract(value, '$.runId'), NULL, NULL, NULL,
      NULL, json_extract(value, '$.ordinal'), json_extract(value, '$.type'), 'completed', ${iso}, value
    FROM json_each(${JSON.stringify(items)})`;
  return { threadId, thread, itemId, runId, now, iso };
});

it.layer(layer)("direct history-page metadata selection", (it) => {
  it.effect(
    "caps a long human-started history window and pages every row with fallback parity",
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const { threadId, itemId } = yield* seedHistory("long-human", "tools");
        yield* sql`UPDATE orchestration_v2_projection_turn_items
        SET type = 'user_message',
          payload_json = json_set(payload_json, '$.type', 'user_message',
            '$.createdBy', 'user', '$.creationSource', 'web', '$.inputIntent', 'turn_start',
            '$.messageId', 'message:long-human', '$.text', 'Continue', '$.attachments', json('[]'))
        WHERE turn_item_id = ${itemId(2)}`;
        const snapshot = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 77 });
        const initial = buildBoundedThreadProjection(snapshot);
        assert.isAtMost(initial.projection.visibleTurnItems.length, 75);
        assert.isNotNull(initial.historyCursor);
        let cursor = initial.historyCursor;
        let ids = initial.projection.visibleTurnItems.map((row) => String(row.sourceItemId));
        let pages = 0;
        while (cursor !== null) {
          const observation = observeMetadata();
          const page = yield* store
            .getThreadHistoryPage(threadId, cursor)
            .pipe(observation.observe);
          const fallback = yield* store.getThreadHistoryPage(threadId, cursor, "entry:absent");
          assert.strictEqual(JSON.stringify(page), JSON.stringify(fallback));
          assert.isAtMost(page.items.length, 75);
          assert.isAtMost(
            selectionMetadata(observation.queries).filter(({ query }) =>
              query.includes("ORDER BY ordinal DESC"),
            ).length,
            7,
          );
          ids = [...page.items.map((row) => String(row.sourceItemId)), ...ids];
          cursor = page.nextCursor;
          pages += 1;
          assert.isAtMost(pages, 20);
        }
        assert.deepEqual(
          ids,
          Array.from({ length: 1040 }, (_, index) => itemId(index + 1)),
        );
      }),
  );

  it.effect(
    "bounds metadata from a relative snapshot cursor and preserves the 150 raw-turn ceiling",
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const { threadId, itemId } = yield* seedHistory("raw-turns", "notification");
        const snapshot = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 76 });
        const initial = buildBoundedThreadProjection(snapshot);
        assert.isNotNull(initial.historyCursor);
        const cursor = initial.historyCursor!;
        assert.isBelow(decodeThreadHistoryCursor(cursor).p, 76);
        const observation = observeMetadata();
        const page = yield* store.getThreadHistoryPage(threadId, cursor).pipe(observation.observe);
        // An absent through-entry keeps selection unchanged and forces the original full-index path.
        const fallback = yield* store.getThreadHistoryPage(threadId, cursor, "entry:absent");
        assert.strictEqual(JSON.stringify(page), JSON.stringify(fallback));
        assert.lengthOf(page.items, 150 * 4);
        assert.isAbove(decodeThreadHistoryCursor(page.nextCursor!).p, 0);
        const metadata = selectionMetadata(observation.queries);
        // Count materialized rows before inspecting SQL shape so the unchanged full-index path fails here.
        assert.isAtMost(
          metadata.reduce((sum, query) => sum + (query.rows ?? 0), 0),
          1 + 8 * 77,
        );
        const chunks = metadata.filter(({ query }) => query.includes("ORDER BY ordinal DESC"));
        assert.isNotEmpty(chunks);
        assert.isTrue(
          chunks.every(
            ({ rows }) => rows !== null && rows <= THREAD_HISTORY_PAGE_POLICY.maxItems + 2,
          ),
        );
        const selects = observation.queries.filter(({ query }) =>
          /^\s*(SELECT|WITH)\b/i.test(query),
        );
        assert.isTrue(
          selects.every(
            ({ outcome, rows, parameters }) =>
              outcome === "success" && rows !== null && parameters !== undefined,
          ),
        );
        const plans = yield* Effect.forEach(selects, (query) =>
          sql
            .unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query.query}`, query.parameters)
            .pipe(Effect.map((plan) => ({ ...query, plan: plan.map(({ detail }) => detail) }))),
        );
        yield* Effect.logInfo(
          "history-page SQL work",
          JSON.stringify({
            domain: { items: 1040, runs: 260, attempts: 260 },
            queries: plans,
          }),
        ).pipe(
          Effect.provideService(References.MinimumLogLevel, "Info"),
          Effect.withLogger(Logger.consoleLogFmt),
        );
        const chunkPlans = plans.filter(({ query }) => query.includes("ORDER BY ordinal DESC"));
        assert.isTrue(
          chunkPlans.every(({ plan }) =>
            plan.some(
              (detail) =>
                detail.includes("SEARCH") &&
                detail.includes("thread_ordinal_idx") &&
                detail.includes("thread_id=?") &&
                detail.includes("(ordinal,turn_item_id)<(?,?)"),
            ),
          ),
          JSON.stringify(chunkPlans),
        );
        assert.isTrue(
          chunkPlans.every(({ plan }) => !plan.some((detail) => detail.includes("TEMP B-TREE"))),
        );
        const prefix = plans.filter(({ query }) => query.includes("SELECT COUNT(*) AS count"));
        assert.lengthOf(prefix, 1);
        assert.isTrue(
          prefix[0]!.plan.some(
            (detail) =>
              detail.includes("COVERING INDEX") &&
              detail.includes("thread_ordinal_idx") &&
              detail.includes("thread_id=?") &&
              detail.includes("(ordinal,turn_item_id)<(?,?)"),
          ),
        );
        const correction = plans.filter(({ query }) => query.includes("WITH hidden AS"));
        assert.lengthOf(correction, 1);
        assert.isTrue(
          correction[0]!.plan.some(
            (detail) => detail.includes("SEARCH attempt") && detail.includes("thread_id=?"),
          ),
        );
        const runFacts = plans.filter(({ query }) => query.includes("wanted.value AS run_id"));
        assert.isNotEmpty(runFacts);
        assert.isTrue(
          runFacts.every(({ plan }) =>
            plan.some((detail) => detail.includes("SEARCH run") && detail.includes("run_id=?")),
          ),
        );
        const payloads = plans.filter(
          ({ query }) =>
            !query.includes("AS input_intent") &&
            query.includes("payload_json") &&
            query.includes("wanted") &&
            !query.includes("wanted.value AS run_id"),
        );
        assert.isNotEmpty(payloads);
        // Plans and returned rows expose each cost family, not SQLite row visits:
        // prefix counts remain O(N); hidden correction may inspect all 260 attempts;
        // user discovery and per-run fact scans can cross long spans.

        const decoded = decodeThreadHistoryCursor(cursor);
        assert.notEqual(decoded.si, itemId(1));
        const older = yield* store.getThreadHistoryPage(threadId, page.nextCursor!);
        const olderFallback = yield* store.getThreadHistoryPage(
          threadId,
          page.nextCursor!,
          "entry:absent",
        );
        assert.strictEqual(JSON.stringify(older), JSON.stringify(olderFallback));
        assert.isFalse(older.hasMoreHistory);
        assert.deepEqual(
          [...older.items, ...page.items, ...initial.projection.visibleTurnItems].map((row) =>
            String(row.sourceItemId),
          ),
          Array.from({ length: 1040 }, (_, index) => itemId(index + 1)),
        );
        yield* sql`INSERT INTO orchestration_v2_projection_turn_items
        (turn_item_id, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
          parent_item_id, ordinal, type, status, updated_at, payload_json)
        SELECT ${itemId(1041)}, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
          parent_item_id, 1041, type, status, updated_at,
          json_set(payload_json, '$.id', ${itemId(1041)}, '$.ordinal', 1041)
        FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${itemId(1040)}`;
        assert.strictEqual(
          JSON.stringify(yield* store.getThreadHistoryPage(threadId, cursor)),
          JSON.stringify(page),
        );
      }),
  );

  it.effect(
    "corrects hidden prefix positions and preserves tied order, cancellation and paired interrupts",
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const { threadId, itemId, runId, iso } = yield* seedHistory("visibility", "human");
        yield* sql`UPDATE orchestration_v2_projection_runs
        SET status = 'rolled_back', payload_json = json_set(payload_json, '$.status', 'rolled_back')
        WHERE run_id IN (${runId(2)}, ${runId(244)})`;
        yield* sql`UPDATE orchestration_v2_projection_runs
        SET status = 'cancelled', payload_json = json_set(payload_json, '$.status', 'cancelled')
        WHERE run_id = ${runId(245)}`;
        yield* sql`UPDATE orchestration_v2_projection_runs
        SET status = 'failed', payload_json = json_set(payload_json, '$.status', 'failed')
        WHERE run_id = ${runId(239)}`;
        yield* sql`UPDATE orchestration_v2_projection_turn_items
        SET payload_json = json_set(payload_json, '$.inputIntent', 'queued_turn')
        WHERE turn_item_id = ${itemId(982)}`;
        for (const run of [230, 232]) {
          const nodeId = `node:history-page:visibility:${run}`;
          const attemptId = `attempt:history-page:visibility:${run}`;
          yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
          (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
            provider_instance_id, provider_thread_id, provider_turn_id, status, payload_json)
          VALUES (${attemptId}, ${threadId}, ${runId(run)}, 1, ${nodeId}, 'codex', 'codex',
            'provider-thread:history-page:visibility', NULL, 'superseded', ${JSON.stringify({
              id: attemptId,
              runId: runId(run),
              attemptOrdinal: 1,
              rootNodeId: nodeId,
              providerInstanceId: "codex",
              providerThreadId: "provider-thread:history-page:visibility",
              providerTurnId: null,
              reason: "initial",
              status: "superseded",
              startedAt: iso,
              completedAt: iso,
            })})`;
          yield* sql`UPDATE orchestration_v2_projection_turn_items
          SET type = 'run_interrupt_result', node_id = ${nodeId},
            payload_json = json_set(payload_json, '$.type', 'run_interrupt_result',
              '$.nodeId', ${nodeId}, '$.message', 'Interrupted')
          WHERE turn_item_id = ${itemId(run * 4 + 3)}`;
        }
        // The paired request precedes the retained page and still keeps its result visible.
        yield* sql`UPDATE orchestration_v2_projection_turn_items
        SET type = 'run_interrupt_request', run_id = ${runId(230)},
          payload_json = json_set(payload_json, '$.type', 'run_interrupt_request',
            '$.runId', ${runId(230)}, '$.message', 'Interrupt requested')
        WHERE turn_item_id = ${itemId(20)}`;
        // Put one peer on the anchor ordinal so keyset comparison must include the item ID.
        yield* sql`UPDATE orchestration_v2_projection_turn_items
        SET ordinal = 1001, payload_json = json_set(payload_json, '$.ordinal', 1001)
        WHERE turn_item_id = ${itemId(1000)}`;
        const cursor = encodeThreadHistoryCursor({
          snapshotSequence: 0,
          sourceThreadId: threadId,
          sourceItemId: itemId(1001),
          position: 1,
        });
        for (const conversationOnly of [false, true]) {
          const page = yield* store.getThreadHistoryPage(
            threadId,
            cursor,
            undefined,
            conversationOnly,
          );
          const fallback = yield* store.getThreadHistoryPage(
            threadId,
            cursor,
            "entry:absent",
            conversationOnly,
          );
          assert.strictEqual(JSON.stringify(page), JSON.stringify(fallback));
          const oldest = decodeThreadHistoryCursor(page.nextCursor!);
          const projection = yield* store.getThreadProjection(threadId);
          assert.strictEqual(
            oldest.p,
            projection.visibleTurnItems.findIndex((row) => row.sourceItemId === oldest.si),
          );
          assert.isFalse(
            page.items.some((row) => [runId(244), runId(2)].includes(String(row.item.runId))),
          );
          assert.isFalse(page.items.some((row) => row.sourceItemId === itemId(982)));
          assert.isFalse(page.items.some((row) => row.sourceItemId === itemId(931)));
          assert.isTrue(page.items.some((row) => row.sourceItemId === itemId(923)));
          assert.isTrue(page.items.some((row) => row.item.runId === runId(239)));
        }
        const observation = observeMetadata();
        const page = yield* store.getThreadHistoryPage(threadId, cursor).pipe(observation.observe);
        assert.isAtMost(
          selectionMetadata(observation.queries).reduce((sum, query) => sum + (query.rows ?? 0), 0),
          1 + 2 * 77,
        );
        const selects = observation.queries.filter(({ query }) =>
          /^\s*(SELECT|WITH)\b/i.test(query),
        );
        assert.isTrue(
          selects.every(
            ({ outcome, rows, parameters }) =>
              outcome === "success" && rows !== null && parameters !== undefined,
          ),
        );
        const plans = yield* Effect.forEach(selects, (query) =>
          sql
            .unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query.query}`, query.parameters)
            .pipe(Effect.map((plan) => ({ ...query, plan: plan.map(({ detail }) => detail) }))),
        );
        const lazyAttempts = plans.filter(({ query }) =>
          query.includes(
            "SELECT run_id, root_node_id FROM orchestration_v2_projection_run_attempts",
          ),
        );
        assert.isNotEmpty(lazyAttempts);
        assert.isTrue(
          lazyAttempts.every(({ plan }) =>
            plan.some(
              (detail) =>
                detail.includes("SEARCH") &&
                detail.includes("thread_id=?") &&
                detail.includes("run_id=?"),
            ),
          ),
        );
        yield* Effect.logInfo(
          "history-page hidden and interrupt SQL work",
          JSON.stringify({
            domain: { items: 1040, runs: 260, attempts: 262 },
            queries: plans,
          }),
        ).pipe(
          Effect.provideService(References.MinimumLogLevel, "Info"),
          Effect.withLogger(Logger.consoleLogFmt),
        );
        assert.isTrue(page.items.some((row) => row.sourceItemId === itemId(1000)));
      }),
  );

  it.effect(
    "retains second-stage byte limits and full fallback for missing, hidden, foreign, filtered and fork anchors",
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const { threadId, thread, itemId, runId, now } = yield* seedHistory("fallback", "tools");
        const cursor = encodeThreadHistoryCursor({
          snapshotSequence: 0,
          sourceThreadId: threadId,
          sourceItemId: itemId(1000),
          position: 0,
        });
        const page = yield* store.getThreadHistoryPage(threadId, cursor);
        const fallback = yield* store.getThreadHistoryPage(threadId, cursor, "entry:absent");
        assert.strictEqual(JSON.stringify(page), JSON.stringify(fallback));
        assert.isAbove(page.items.length, 0);
        assert.isBelow(page.items.length, THREAD_HISTORY_PAGE_POLICY.maxItems);
        const count = page.items.length;
        assert.deepEqual(
          page.items.map((row) => row.sourceItemId),
          Array.from({ length: count }, (_, index) => itemId(1000 - count + index)),
        );
        assert.strictEqual(decodeThreadHistoryCursor(page.nextCursor!).p, 999 - count);
        const canonicalRows = page.items.map((row, index) => ({
          ...row,
          position: 999 - count + index,
        }));
        const bytes = canonicalRows.reduce((sum, row) => sum + projectedRowEncodedBytes(row), 0);
        assert.isAtMost(bytes, THREAD_HISTORY_PAGE_POLICY.maxEncodedBytes);
        const projection = yield* store.getThreadProjection(threadId);
        const preceding = projection.visibleTurnItems.find(
          (row) => row.sourceItemId === itemId(999 - count),
        );
        assert.isDefined(preceding);
        assert.isAbove(
          bytes +
            projectedRowEncodedBytes({
              ...preceding!,
              item: projectTurnItemForWire(preceding!.item),
            }),
          THREAD_HISTORY_PAGE_POLICY.maxEncodedBytes,
        );
        assert.isTrue(page.hasMoreHistory);
        assert.isAbove(decodeThreadHistoryCursor(page.nextCursor!).p, 900);
        yield* sql`UPDATE orchestration_v2_projection_runs
        SET status = 'rolled_back', payload_json = json_set(payload_json, '$.status', 'rolled_back')
        WHERE run_id = ${runId(249)}`;
        const forkThreadId = ThreadId.make("thread:history-page:fork");
        const forkedFrom: OrchestrationV2AppThread["forkedFrom"] = {
          type: "run",
          threadId,
          runId: RunId.make(runId(250)),
        };
        yield* store.apply({
          id: EventId.make("event:history-page:fork"),
          type: "thread.created",
          threadId: forkThreadId,
          occurredAt: now,
          payload: {
            ...thread,
            id: forkThreadId,
            forkedFrom,
            lineage: {
              parentThreadId: threadId,
              relationshipToParent: "fork",
              rootThreadId: threadId,
            },
          },
        });
        for (const shape of [
          { target: threadId, source: threadId, anchor: "item:missing", filter: undefined },
          { target: threadId, source: threadId, anchor: itemId(1000), filter: undefined },
          { target: threadId, source: "thread:foreign", anchor: itemId(950), filter: undefined },
          { target: threadId, source: threadId, anchor: itemId(950), filter: "message:absent" },
          { target: forkThreadId, source: threadId, anchor: itemId(950), filter: undefined },
        ]) {
          const shapeCursor = encodeThreadHistoryCursor({
            snapshotSequence: 0,
            sourceThreadId: shape.source,
            sourceItemId: shape.anchor,
            position: 500,
          });
          const observation = observeMetadata();
          const actual = yield* store
            .getThreadHistoryPage(shape.target, shapeCursor, shape.filter)
            .pipe(observation.observe);
          const expected = yield* store.getThreadHistoryPage(
            shape.target,
            shapeCursor,
            "entry:absent",
          );
          assert.strictEqual(JSON.stringify(actual), JSON.stringify(expected));
          assert.isTrue(
            selectionMetadata(observation.queries).some(
              ({ rows }) => rows !== null && rows >= 1000,
            ),
          );
        }
        const invalid = yield* store.getThreadHistoryPage(threadId, "invalid").pipe(Effect.flip);
        assert.strictEqual(invalid._tag, "ProjectionStoreReadError");
        const missing = yield* store
          .getThreadHistoryPage(ThreadId.make("thread:missing"), cursor)
          .pipe(Effect.flip);
        assert.strictEqual(missing._tag, "ProjectionStoreThreadNotFoundError");
      }),
  );
});
