import { assert, it } from "@effect/vitest";
import { RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  OfflinePreparationError,
  classifyPreparation,
  createContinuationPlan,
  offlinePreparationLegacy,
  preparationClock,
  preparationLedgerDefinition,
  readPreparationBasis,
  readPreparationRows,
  type PreparationBasis,
  type PreparationOutboxRow,
} from "./offlinePreparationCore.ts";
import { offlinePreparationPlanLimits } from "./offlinePreparationPlan.ts";

const timestamp = "2020-02-03T04:05:06.007Z";
const creation = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  timestamp,
  snapshotSha256: "a".repeat(64),
  settingsSha256: "b".repeat(64),
  preparationIds: ["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"],
};
const makeBasis = () =>
  ({
    selections: ["one", "two"].map((id) => ({
      threadId: ThreadId.make("thread:" + id),
      runId: RunId.make("run:" + id),
      existingId: null,
      projectionSha256: "c".repeat(64),
    })),
    rows: [
      {
        effect_id: "effect:preserved",
        command_id: "command:ordinary",
        thread_id: "thread:other",
        effect_type: "provider-turn.start",
        payload_json: "opaque-unrelated-payload",
        status: "succeeded" as const,
        attempt_count: 1,
        available_at: timestamp,
        created_at: timestamp,
        updated_at: timestamp,
        lease_owner: null,
        lease_expires_at: null,
        completed_at: timestamp,
        last_error: null,
      },
    ],
  }) satisfies PreparationBasis;
const appliedBasis = (plan: ReturnType<typeof createContinuationPlan>): PreparationBasis => ({
  rows: [...plan.preservedRows, ...plan.expectedRows],
  selections: plan.selections.map((selection) => ({
    ...selection,
    existingId: plan.expectedRows.find((row) => row.thread_id === selection.threadId)!.effect_id,
  })),
});

it("creates detached immutable plans with canonical rows and the supplied timestamp", () => {
  const basis = makeBasis();
  const plan = createContinuationPlan(basis, creation);
  assert.equal(plan.source, "bd20d2d2fc0ba89797698192b877a7f1312f8fe3");
  assert.deepEqual(plan.expectedRows[0], {
    effect_id: "effect:server-update-continuation:" + creation.preparationIds[0],
    command_id: "command:server-update-prepare:" + creation.preparationIds[0],
    thread_id: "thread:one",
    effect_type: "provider-runtime.continue",
    payload_json: JSON.stringify({
      type: "provider-runtime.continue",
      sourceRunId: "run:one",
      preparedForRestart: true,
      continueWithoutPreference: false,
    }),
    status: "pending",
    attempt_count: 0,
    available_at: timestamp,
    created_at: timestamp,
    updated_at: timestamp,
    lease_owner: null,
    lease_expires_at: null,
    completed_at: null,
    last_error: null,
  });
  assert.isFalse(Object.isFrozen(basis));
  assert.isFalse(Object.isFrozen(basis.rows[0]));
  basis.rows[0]!.attempt_count = 99;
  basis.selections[0]!.projectionSha256 = "d".repeat(64);
  assert.equal(plan.preservedRows[0]!.attempt_count, 1);
  assert.equal(plan.selections[0]!.projectionSha256, "c".repeat(64));
  for (const value of [
    plan,
    plan.selections,
    plan.expectedRows,
    plan.preservedRows,
    ...plan.selections,
    ...plan.expectedRows,
    ...plan.preservedRows,
  ])
    assert.isTrue(Object.isFrozen(value));
});

it("rejects missing, duplicate, and preserved-row-colliding preparation IDs", () => {
  assert.throws(
    () => createContinuationPlan(makeBasis(), { ...creation, preparationIds: [] }),
    /preparation-id-count/,
  );
  assert.throws(
    () =>
      createContinuationPlan(makeBasis(), {
        ...creation,
        preparationIds: [creation.preparationIds[0]!, creation.preparationIds[0]!],
      }),
    /relationships/,
  );
  const basis = makeBasis();
  basis.rows[0]!.effect_id = "effect:server-update-continuation:" + creation.preparationIds[0];
  assert.throws(() => createContinuationPlan(basis, creation), /relationships/);
});

it.effect("fixes every Effect clock read and rejects maintenance sleeps", () =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock;
    const millis = Date.parse(timestamp);
    const nanos = BigInt(millis) * 1_000_000n;
    assert.equal(DateTime.formatIso(yield* DateTime.now), timestamp);
    assert.equal(clock.currentTimeMillisUnsafe(), millis);
    assert.equal(yield* clock.currentTimeMillis, millis);
    assert.equal(clock.currentTimeNanosUnsafe(), nanos);
    assert.equal(yield* clock.currentTimeNanos, nanos);
    assert.equal(clock.monotonicTimeNanosUnsafe(), nanos);
    assert.equal(yield* clock.monotonicTimeNanos, nanos);
    const exit = yield* Effect.exit(clock.sleep(Duration.millis(1)));
    assert.equal(exit._tag, "Failure");
    if (exit._tag === "Failure") {
      const failure = Cause.squash(exit.cause);
      assert.instanceOf(failure, OfflinePreparationError);
      assert.equal((failure as OfflinePreparationError).reason, "unexpected-maintenance-sleep");
    }
  }).pipe(Effect.provideService(Clock.Clock, preparationClock(timestamp))),
);

it("distinguishes exact original and applied logical state without a physical authority claim", () => {
  const basis = makeBasis();
  const plan = createContinuationPlan(basis, creation);
  assert.equal(classifyPreparation(plan, basis), "not-applied");
  assert.equal(classifyPreparation(plan, appliedBasis(plan)), "applied");
  const empty = { selections: [], rows: basis.rows };
  const noNewRows = createContinuationPlan(empty, { ...creation, preparationIds: [] });
  assert.equal(classifyPreparation(noNewRows, empty), "applied");
});

it("holds drift in each of the fourteen persisted outbox fields", () => {
  const plan = createContinuationPlan(makeBasis(), creation);
  const current = appliedBasis(plan);
  const changes: ReadonlyArray<Partial<PreparationOutboxRow>> = [
    { effect_id: "changed" },
    { command_id: "changed" },
    { thread_id: "changed" },
    { effect_type: "changed" },
    { payload_json: "{}" },
    { status: "running" },
    { attempt_count: 1 },
    { available_at: "2021-01-01T00:00:00.000Z" },
    { created_at: "2021-01-01T00:00:00.000Z" },
    { updated_at: "2021-01-01T00:00:00.000Z" },
    { lease_owner: "worker" },
    { lease_expires_at: "2021-01-01T00:00:00.000Z" },
    { completed_at: "2021-01-01T00:00:00.000Z" },
    { last_error: "failure" },
  ];
  assert.lengthOf(changes, 14);
  for (const change of changes) {
    assert.equal(
      classifyPreparation(plan, {
        ...current,
        rows: current.rows.map((row) =>
          row.effect_id === plan.expectedRows[0]!.effect_id ? { ...row, ...change } : row,
        ),
      }),
      "held",
    );
  }
});

it("holds partial writes, extra rows, and projection or marker drift on either baseline", () => {
  const basis = makeBasis();
  const plan = createContinuationPlan(basis, creation);
  const current = appliedBasis(plan);
  assert.equal(classifyPreparation(plan, { ...current, rows: current.rows.slice(0, -1) }), "held");
  assert.equal(
    classifyPreparation(plan, {
      ...current,
      rows: [...current.rows, { ...current.rows[0]!, effect_id: "unexpected" }],
    }),
    "held",
  );
  for (const observed of [basis, current]) {
    assert.equal(
      classifyPreparation(plan, {
        ...observed,
        selections: observed.selections.map((selection) => ({
          ...selection,
          projectionSha256: "d".repeat(64),
        })),
      }),
      "held",
    );
    assert.equal(
      classifyPreparation(plan, {
        ...observed,
        selections: observed.selections.map((selection) => ({
          ...selection,
          existingId: "foreign-marker",
        })),
      }),
      "held",
    );
  }
});

function rowQueryService(
  size: { row_count: number; byte_count: number },
  rows: ReadonlyArray<PreparationOutboxRow>,
) {
  const queries: Array<{ statement: string; parameters: ReadonlyArray<unknown> }> = [];
  const query = (parts: TemplateStringsArray, ...parameters: ReadonlyArray<unknown>) => {
    const statement = parts.join("?");
    queries.push({ statement, parameters });
    return Effect.succeed(statement.includes("COUNT(*)") ? [size] : rows);
  };
  return { queries, sql: query as unknown as SqlClient.SqlClient };
}

it.effect("refuses cardinality and byte overflow before fetching outbox payload arrays", () =>
  Effect.gen(function* () {
    const maximum =
      offlinePreparationPlanLimits.preservedRows + offlinePreparationPlanLimits.selections;
    for (const size of [
      { row_count: maximum + 1, byte_count: 1 },
      { row_count: 1, byte_count: offlinePreparationPlanLimits.bytes + 1 },
    ]) {
      const mock = rowQueryService(size, []);
      const exit = yield* Effect.exit(
        readPreparationRows.pipe(Effect.provideService(SqlClient.SqlClient, mock.sql)),
      );
      assert.equal(exit._tag, "Failure");
      assert.lengthOf(mock.queries, 1);
      assert.include(mock.queries[0]!.statement, "COUNT(*)");
      if (exit._tag === "Failure")
        assert.equal(
          (Cause.squash(exit.cause) as OfflinePreparationError).reason,
          "outbox-read-limit",
        );
    }
  }),
);

it.effect("bounds the payload query and refuses a count mismatch instead of truncating", () =>
  Effect.gen(function* () {
    const mock = rowQueryService({ row_count: 2, byte_count: 1 }, makeBasis().rows);
    const exit = yield* Effect.exit(
      readPreparationRows.pipe(Effect.provideService(SqlClient.SqlClient, mock.sql)),
    );
    assert.equal(exit._tag, "Failure");
    assert.lengthOf(mock.queries, 2);
    assert.include(mock.queries[1]!.statement, "LIMIT ?");
    assert.deepEqual(mock.queries[1]!.parameters, [
      offlinePreparationPlanLimits.preservedRows + offlinePreparationPlanLimits.selections + 1,
    ]);
    if (exit._tag === "Failure")
      assert.equal(
        (Cause.squash(exit.cause) as OfflinePreparationError).reason,
        "outbox-read-limit",
      );
  }),
);

function boundedProjectionServices(candidateCount: number, overflowingTable?: string) {
  let selected = false;
  let projectionRead = false;
  const observedTables: string[] = [];
  const query = (
    parts: TemplateStringsArray | string,
    ...parameters: ReadonlyArray<unknown>
  ): unknown => {
    if (typeof parts === "string") return parts;
    const statement = parts.join("?");
    const table = parameters[0];
    if (statement.includes("FROM orchestration_v2_effect_outbox"))
      return Effect.succeed(
        statement.includes("COUNT(*)") ? [{ row_count: 0, byte_count: 0 }] : [],
      );
    const ledger =
      table === "effect_sql_migrations"
        ? offlinePreparationLegacy.ledgers.upstream
        : offlinePreparationLegacy.ledgers.jones;
    if (statement.includes("COUNT(*)"))
      return Effect.succeed([
        {
          row_count: statement.includes("deleted_at IS NULL")
            ? candidateCount
            : statement.includes("sqlite_master")
              ? Object.keys(offlinePreparationLegacy.tables).length + 2
              : ledger.length,
          byte_count: 0,
        },
      ]);
    if (statement.includes("SELECT sql FROM sqlite_master"))
      return Effect.succeed([
        {
          sql: preparationLedgerDefinition(
            table as "effect_sql_migrations" | "jones_sql_migrations",
          ),
        },
      ]);
    if (statement.includes("SELECT migration_id"))
      return Effect.succeed(ledger.map(([migration_id, name]) => ({ migration_id, name })));
    if (statement.includes("SELECT name, sql"))
      return Effect.succeed(
        Object.entries(offlinePreparationLegacy.tables).map(([name, sql]) => ({ name, sql })),
      );
    if (statement.includes("type = 'trigger'")) return Effect.succeed([]);
    return Effect.die(new Error("Unexpected payload read before the read bound."));
  };
  const sql = Object.assign(query, {
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect,
    unsafe: (statement: string) => {
      observedTables.push(statement);
      return Effect.succeed([
        {
          row_count: 1,
          byte_count:
            overflowingTable !== undefined &&
            statement.includes("FROM " + overflowingTable + " WHERE ")
              ? offlinePreparationPlanLimits.bytes + 1
              : 1,
        },
      ]);
    },
  }) as unknown as SqlClient.SqlClient;
  const store = {
    getRecoveryThreadIds: () => {
      selected = true;
      return Effect.succeed([ThreadId.make("thread:one")]);
    },
    getRuntimeRecoveryProjection: () => {
      projectionRead = true;
      return Effect.die(new Error("Unbounded historical payload read."));
    },
  } as unknown as ProjectionStore.ProjectionStoreV2["Service"];
  return { sql, store, observedTables, observed: () => ({ selected, projectionRead }) };
}

it.effect("refuses an oversized candidate source before invoking the historical selector", () =>
  Effect.gen(function* () {
    const mock = boundedProjectionServices(offlinePreparationPlanLimits.selections + 1);
    const exit = yield* Effect.exit(
      readPreparationBasis(undefined).pipe(
        Effect.provideService(SqlClient.SqlClient, mock.sql),
        Effect.provideService(ProjectionStore.ProjectionStoreV2, mock.store),
      ),
    );
    assert.equal(exit._tag, "Failure");
    assert.deepEqual(mock.observed(), { selected: false, projectionRead: false });
    if (exit._tag === "Failure")
      assert.equal(
        (Cause.squash(exit.cause) as OfflinePreparationError).reason,
        "selection-read-limit",
      );
  }),
);

it.effect("bounds every historical projection field before the full reader materializes it", () =>
  Effect.gen(function* () {
    const fields = [
      "threads",
      "runs",
      "run_attempts",
      "nodes",
      "subagents",
      "provider_sessions",
      "provider_threads",
      "provider_turns",
      "runtime_requests",
      "messages",
      "turn_items",
    ];
    assert.lengthOf(fields, 11);
    for (const field of fields) {
      const table = "orchestration_v2_projection_" + field;
      const mock = boundedProjectionServices(1, table);
      const exit = yield* Effect.exit(
        readPreparationBasis(undefined).pipe(
          Effect.provideService(SqlClient.SqlClient, mock.sql),
          Effect.provideService(ProjectionStore.ProjectionStoreV2, mock.store),
        ),
      );
      assert.equal(exit._tag, "Failure");
      assert.deepEqual(mock.observed(), { selected: true, projectionRead: false });
      assert.isTrue(
        mock.observedTables.some((query) => query.includes("FROM " + table + " WHERE ")),
      );
      if (exit._tag === "Failure")
        assert.equal(
          (Cause.squash(exit.cause) as OfflinePreparationError).reason,
          "projection-read-limit",
        );
    }
  }),
);
