import { expect, it } from "@effect/vitest";
import {
  ContinuityReceipt,
  type ContinuityOwner,
  type ContinuityIntent,
  type ContinuitySnapshot,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../../persistence/Migrations/134_JonesGoalContinuity.ts";
import migration124 from "../../persistence/Migrations/124_JonesTaskContinuations.ts";
import * as Store from "./GoalContinuityStore.ts";
import {
  canonicalJson,
  checkpointContinuity,
  closeContinuity,
  continuityDigest,
  handoffContinuity,
  initializeContinuity,
} from "./model.ts";

const owner = {
  environmentId: "synthetic-env",
  threadId: "synthetic-thread",
  providerInstanceId: "codex",
  providerThreadId: "synthetic-provider-thread",
  driver: "codex",
  nativeThreadId: "synthetic-native",
} as ContinuityOwner;
const intent: ContinuityIntent = {
  goalId: "goal",
  revision: "1",
  northStar: "Preserve accepted work",
  preservedQualities: [],
  finishItems: [{ id: "finish", text: "Prove completion" }],
  source: { type: "owner_request", identity: "synthetic-owner", reference: "synthetic-request" },
  derivation: null,
  initializationMode: "direct",
  predecessor: null,
  startEvidence: [{ id: "start", reference: "synthetic-request", assertion: "Explicit request" }],
};
function initial(): ContinuitySnapshot {
  const result = initializeContinuity({ owner, intent, current: null });
  if (result.outcome !== "applied") throw new Error(result.reason);
  return result.snapshot;
}
const database = NodeSqliteClient.layer({ filename: ":memory:" });
const setup = Layer.effectDiscard(migration).pipe(Layer.provideMerge(database));
const layer = Store.layer.pipe(Layer.provideMerge(setup));
const expected = (snapshot: ContinuitySnapshot) => ({
  goalId: snapshot.intent.goalId,
  revision: snapshot.intent.revision,
  generation: snapshot.generation,
  owner: snapshot.owner,
});

it.effect(
  "competing stale CAS cannot overwrite the winner and rollback preserves its current pointer",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.GoalContinuityStore;
      const snapshot = initial();
      const record = yield* store.transaction(store.save(null, snapshot));
      const first = checkpointContinuity(snapshot, expected(snapshot), { focus: "first" });
      const second = checkpointContinuity(snapshot, expected(snapshot), { focus: "second" });
      if (first.outcome !== "applied" || second.outcome !== "applied")
        throw new Error("Invalid fixture");
      const outcomes = yield* Effect.all(
        [
          store.transaction(store.save(record, first.snapshot)).pipe(Effect.result),
          store.transaction(store.save(record, second.snapshot)).pipe(Effect.result),
        ],
        { concurrency: 2 },
      );
      expect(outcomes.filter((item) => item._tag === "Success")).toHaveLength(1);
      expect(outcomes.filter((item) => item._tag === "Failure")).toHaveLength(1);
      const current = yield* store.readOnly(store.current(owner.environmentId, owner.threadId));
      expect(current!.snapshot.generation).toBe("1");
      expect(["first", "second"]).toContain(current!.snapshot.focus);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "snapshot and durable refusal receipt commit together without changing refused-close bytes",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.GoalContinuityStore;
      const snapshot = initial();
      yield* store.transaction(store.save(null, snapshot));
      const result = closeContinuity(snapshot, expected(snapshot), ["start"]);
      expect(result).toEqual({ outcome: "refused", reason: "uncovered_finish" });
      const request = {
        action: "close",
        clientRequestId: "close-1",
        expected: expected(snapshot),
        completionEvidenceIds: ["start"],
      };
      const receipt: ContinuityReceipt = {
        clientRequestId: "close-1",
        requestDigest: continuityDigest(request),
        environmentId: owner.environmentId,
        threadId: owner.threadId,
        actorNamespace: "owner",
        actorId: "synthetic-owner",
        targetOwner: owner,
        witness: null,
        previousGeneration: "0",
        attemptedGeneration: "0",
        result,
      };
      const key: Store.ContinuityRowKey = {
        ...owner,
        kind: "mutation",
        namespace: "owner",
        key: "close-1",
      };
      yield* store.transaction(store.writeRow(key, null, receipt));
      const row = yield* store.readOnly(store.row(key));
      expect(
        Schema.decodeUnknownSync(Schema.fromJsonString(ContinuityReceipt))(row!.payload),
      ).toEqual(receipt);
      const current = yield* store.readOnly(store.current(owner.environmentId, owner.threadId));
      expect(canonicalJson(current!.snapshot)).toBe(canonicalJson(snapshot));
      expect(current!.snapshot.generation).toBe("0");
      const rewrite = yield* store
        .transaction(store.writeRow(key, row, { ...receipt, requestDigest: "a".repeat(64) }))
        .pipe(Effect.flip);
      expect(rewrite.reason).toBe("stale");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "a receipt failure rolls back the snapshot CAS and the reader refuses corrupt state",
  () =>
    Effect.gen(function* () {
      const store = yield* Store.GoalContinuityStore;
      const sql = yield* SqlClient.SqlClient;
      const snapshot = initial();
      const record = yield* store.transaction(store.save(null, snapshot));
      const patch = checkpointContinuity(snapshot, expected(snapshot), { focus: "must roll back" });
      if (patch.outcome !== "applied") throw new Error("Invalid fixture");
      yield* store
        .transaction(
          Effect.gen(function* () {
            yield* store.save(record, patch.snapshot);
            return yield* Effect.fail(new Error("synthetic receipt failure"));
          }),
        )
        .pipe(Effect.flip);
      expect((yield* store.current(owner.environmentId, owner.threadId))!.digest).toBe(
        record.digest,
      );
      yield* sql`UPDATE jones_goal_continuity SET snapshot_digest=${"f".repeat(64)} WHERE record_id=${record.recordId}`;
      const error = yield* store
        .readOnly(store.current(owner.environmentId, owner.threadId))
        .pipe(Effect.flip);
      expect(error.reason).toBe("corrupt_snapshot");
    }).pipe(Effect.provide(layer)),
);

it.effect("migration creates only sparse Jones tables without upstream foreign keys", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const tables = yield* sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`;
    expect(tables.map((row) => row.name)).toEqual([
      "jones_goal_continuity",
      "jones_goal_continuity_receipts",
    ]);
    expect(yield* sql`PRAGMA foreign_key_list(jones_goal_continuity)`).toEqual([]);
    expect(yield* sql`PRAGMA foreign_key_list(jones_goal_continuity_receipts)`).toEqual([]);
    expect(yield* sql`SELECT 1 FROM jones_goal_continuity`).toEqual([]);
  }).pipe(Effect.provide(setup)),
);

it.effect("persistence retains decimal generations above JavaScript's safe-number range", () =>
  Effect.gen(function* () {
    const store = yield* Store.GoalContinuityStore;
    const snapshot = { ...initial(), generation: "9007199254740993" };
    const record = yield* store.transaction(store.save(null, snapshot));
    const changed = checkpointContinuity(snapshot, expected(snapshot), { focus: "exact integer" });
    if (changed.outcome !== "applied") throw new Error("Invalid fixture");
    yield* store.transaction(store.save(record, changed.snapshot));
    expect(
      (yield* store.readOnly(store.current(owner.environmentId, owner.threadId)))!.snapshot
        .generation,
    ).toBe("9007199254740994");
  }).pipe(Effect.provide(layer)),
);

it.effect("explicit handoff back to an earlier conversation preserves the historical record", () =>
  Effect.gen(function* () {
    const store = yield* Store.GoalContinuityStore;
    const sql = yield* SqlClient.SqlClient;
    const original = initial();
    const record = yield* store.transaction(store.save(null, original));
    const outbound = handoffContinuity(
      original,
      expected(original),
      { ...owner, nativeThreadId: "explicit-other-conversation" },
      ["start"],
    );
    if (outbound.outcome !== "applied") throw new Error("Invalid fixture");
    const away = yield* store.transaction(store.save(record, outbound.snapshot));
    const inbound = handoffContinuity(outbound.snapshot, expected(outbound.snapshot), owner, [
      "start",
    ]);
    if (inbound.outcome !== "applied") throw new Error("Invalid fixture");
    const returned = yield* store.transaction(store.save(away, inbound.snapshot));
    expect(returned.recordId).not.toBe(record.recordId);
    expect(returned.snapshot.generation).toBe("2");
    const history = yield* sql<{
      snapshot_json: string;
    }>`SELECT snapshot_json FROM jones_goal_continuity WHERE record_id=${record.recordId}`;
    expect(history[0]!.snapshot_json).toBe(canonicalJson(original));
  }).pipe(Effect.provide(layer)),
);

it.effect("an aborted 134 transaction preserves migration124 witness rows and upstream state", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migration124;
    yield* sql`CREATE TABLE synthetic_upstream(id TEXT PRIMARY KEY,payload TEXT)`;
    yield* sql`INSERT INTO synthetic_upstream VALUES ('upstream','untouched')`;
    yield* sql`INSERT INTO jones_task_continuation_witnesses VALUES
    ('attempt','message','command','root-run','run','thread','instance','provider-thread','native','generation','2026-01-01T00:00:00Z')`;
    const before = yield* sql`SELECT * FROM jones_task_continuation_witnesses`;
    yield* sql
      .withTransaction(migration.pipe(Effect.andThen(Effect.fail(new Error("synthetic abort")))))
      .pipe(Effect.flip);
    expect(yield* sql`SELECT * FROM jones_task_continuation_witnesses`).toEqual(before);
    expect(yield* sql`SELECT payload FROM synthetic_upstream WHERE id='upstream'`).toEqual([
      { payload: "untouched" },
    ]);
    expect(
      yield* sql`SELECT name FROM sqlite_master WHERE name IN ('jones_goal_continuity','jones_goal_continuity_receipts')`,
    ).toEqual([]);
  }).pipe(Effect.provide(database)),
);
