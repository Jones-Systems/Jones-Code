import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as EffectStore from "./effectStore.ts";
import * as ReceiptStore from "./receiptStore.ts";
import * as HistoryStore from "./historyStore.ts";
import { scope, seed, testLayer, now, pending, terminal } from "./sql.testkit.ts";

it.effect(
  "pending receipts and intent reserve atomically, enforce backpressure and complete with history in one commit",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const effects = yield* EffectStore.EffectStore;
      const receipts = yield* ReceiptStore.ReceiptStore;
      const history = yield* HistoryStore.HistoryStore;
      const sql = yield* SqlClient.SqlClient;
      const request = pending("effect:first-000000");
      const reservation = {
        command_id: request.receipt.command_id,
        principal_id: "principal:test",
        native_reference_id: "ref:test",
        effect: "request_native_t3_settlement" as const,
        server_generation: 1,
        admitted_registry_version: 0,
        intent: {
          native_action: "settle",
          thread_id: "thread:test",
          exact_request: "recovery material",
        },
      };
      yield* effects.reserve(scope, request, reservation);
      const second = pending("effect:second-000000");
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            effects.reserve(scope, second, {
              ...reservation,
              command_id: second.receipt.command_id,
            }),
          ),
        ),
      ).toBe(true);
      expect(yield* receipts.read(scope, second.receipt.command_id)).toBe(null);
      yield* effects.saveResult(scope, request.receipt.command_id, {
        state: "committed",
        proof: "retained-result",
      });
      expect(yield* effects.active(scope)).toEqual([reservation]);
      const event = {
        event_id: "event:effect",
        command_id: request.receipt.command_id,
        registry_version: 0,
        occurred_at: now,
        workstream_ids: [],
        value: { result: "committed" },
      };
      yield* history.append(scope, { ...event, value: { result: "different" } });
      expect(
        Exit.isFailure(yield* Effect.exit(effects.complete(scope, terminal(request), 0, [event]))),
      ).toBe(true);
      expect((yield* receipts.read(scope, request.receipt.command_id))?.receipt.state).toBe(
        "pending",
      );
      expect(yield* effects.active(scope)).toHaveLength(1);
      yield* effects.complete(scope, terminal(request), 0, [
        { ...event, event_id: "event:completed" },
      ]);
      expect(yield* effects.active(scope)).toEqual([]);
      expect((yield* receipts.read(scope, request.receipt.command_id))?.receipt.state).toBe(
        "committed",
      );
      expect((yield* sql`SELECT state FROM jones_workstreams_external_reservations`)[0]).toEqual({
        state: "released",
      });
      expect(yield* effects.result(scope, request.receipt.command_id)).toEqual({
        state: "committed",
        proof: "retained-result",
      });
    }).pipe(Effect.provide(testLayer())),
);
