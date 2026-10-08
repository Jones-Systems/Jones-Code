import { TurnItemId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  historyExperimentLayer,
  historyMessageId,
  historyStatements,
  historyThreadId,
  mapHistoryRows,
  seedHistory,
} from "./historyExperiment.ts";

it.effect("V2 full/window reads and disjoint message SQL retain ordered decoded equivalence", () =>
  Effect.gen(function* () {
    yield* seedHistory(120);
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const full = yield* store.getThreadSnapshot(historyThreadId);
    assert.lengthOf(full.projection.messages, 120);
    assert.lengthOf(full.projection.visibleTurnItems, 120);
    for (const options of [
      { rowLimit: 25 },
      { rowLimit: 25, userTurnLimit: 10 },
      { rowLimit: 25, anchorItemId: TurnItemId.make("item-000059") },
      { rowLimit: 25, userTurnLimit: 10, anchorItemId: TurnItemId.make("item-000059") },
    ]) {
      const window = yield* store.getThreadSnapshotWindow(historyThreadId, options);
      const cohort = window.projection.visibleTurnItems.flatMap(({ item }) =>
        item.type === "user_message" ? [item.messageId] : [],
      );
      const expected = full.projection.messages.filter(
        (message) => cohort.includes(message.id) || message.runId === "active-run",
      );
      const positions = new Map(cohort.map((id, index) => [id, index]));
      const windowExpected = expected.toSorted(
        (a, b) =>
          (positions.get(a.id) ?? Infinity) - (positions.get(b.id) ?? Infinity) ||
          a.id.localeCompare(b.id),
      );
      assert.deepEqual(window.projection.messages, windowExpected);
      const variants = historyStatements(cohort);
      const results = yield* Effect.forEach(variants, (statement) =>
        sql.unsafe<{ payload_json: string }>(statement.text, statement.values),
      );
      assert.deepEqual(results[0], results[1]);
      assert.deepEqual(mapHistoryRows(results[0]!), expected);
      assert.deepEqual(mapHistoryRows(results[1]!), expected);
    }
    // Empty, overlapping, duplicate and missing IDs cannot duplicate active rows.
    for (const cohort of [
      [],
      [historyMessageId(0), historyMessageId(1), historyMessageId(0), "missing"],
    ]) {
      const results = yield* Effect.forEach(historyStatements(cohort), (statement) =>
        sql.unsafe<{ payload_json: string }>(statement.text, statement.values),
      );
      const expected = full.projection.messages.filter(
        (message) => cohort.includes(message.id) || message.runId === "active-run",
      );
      assert.deepEqual(results[0], results[1]);
      assert.deepEqual(mapHistoryRows(results[0]!), expected);
      assert.deepEqual(mapHistoryRows(results[1]!), expected);
    }
  }).pipe(Effect.provide(historyExperimentLayer)),
);
