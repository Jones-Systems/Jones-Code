// @effect-diagnostics nodeBuiltinImport:off -- Native SQLite reopen proof uses one scoped, cleanup-owned temporary root.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeSql from "./sql.ts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as OperationStore from "./operationStore.ts";
import * as BindingStore from "./bindingStore.ts";
import * as EffectStore from "./effectStore.ts";
import * as ReceiptStore from "./receiptStore.ts";
import { scope, seed, testLayer, now, binding, pending } from "./sql.testkit.ts";

it.effect(
  "restart retains exact operation target, child IDs, request bytes and unresolved effect material past 24 hours",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const parent = NodePath.resolve("node_modules/.cache/nw-recorded-recovery/store-files");
        yield* Effect.promise(() => NodeFSP.mkdir(parent, { recursive: true }));
        const dir = yield* Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(parent, "store-")));
        // Retained synthetic SQLite evidence; root owns cleanup after delivery.
        const filename = NodePath.join(dir, "native.sqlite");
        const { state: _bindingState, ...destination } = binding;
        const input: OperationStore.OperationInput = {
          operation_id: "operation:restart",
          kind: "set_primary",
          thread_environment_id: "source:test",
          thread_id: "thread:test",
          origin: { kind: "queue", request_id: "queue:test" },
          destination,
          child_command_ids: ["verify:exact", "attach:exact"],
          request_body: '{ "keep": "these exact bytes" }',
          accepted_at: now,
        };
        const request = pending("effect:restart-0000");
        const reservation = {
          command_id: request.receipt.command_id,
          principal_id: "principal:test",
          native_reference_id: "ref:test",
          effect: "request_native_t3_settlement" as const,
          server_generation: 1,
          admitted_registry_version: 0,
          intent: {
            exact_target: "thread:test",
            native_action: "settle",
            request_body: "exact provider request",
          },
        };
        yield* Effect.gen(function* () {
          yield* seed;
          const bindings = yield* BindingStore.BindingStore;
          const operations = yield* OperationStore.OperationStore;
          const effects = yield* EffectStore.EffectStore;
          yield* bindings.bind(binding, null);
          yield* operations.accept(input);
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                operations.accept({ ...input, operation_id: "operation:overlap" }),
              ),
            ),
          ).toBe(true);
          yield* operations.advance(input.operation_id, "accepted", "dispatching", now, null);
          yield* operations.advance(
            input.operation_id,
            "dispatching",
            "reconciling",
            "2026-02-01T00:00:00.000Z",
            null,
          );
          yield* effects.reserve(scope, request, reservation);
          yield* effects.saveResult(scope, reservation.command_id, {
            status: "committed",
            durable_proof: "provider-result",
          });
        }).pipe(Effect.provide(testLayer({ filename })));
        yield* Effect.gen(function* () {
          const operations = yield* OperationStore.OperationStore;
          const effects = yield* EffectStore.EffectStore;
          const receipts = yield* ReceiptStore.ReceiptStore;
          const bindings = yield* BindingStore.BindingStore;
          expect(yield* operations.open()).toHaveLength(1);
          const current = yield* operations.read(input.operation_id);
          expect(current).toMatchObject({ ...input, state: "reconciling" });
          expect(yield* operations.accept(input)).toEqual(current);
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                operations.accept({
                  ...input,
                  destination: { ...binding, home_environment_id: "other" },
                }),
              ),
            ),
          ).toBe(true);
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                bindings.bind({ ...binding, binding_version: 2, home_generation: 2 }, 1),
              ),
            ),
          ).toBe(true);
          expect(yield* effects.active(scope)).toEqual([reservation]);
          expect(yield* effects.result(scope, reservation.command_id)).toEqual({
            status: "committed",
            durable_proof: "provider-result",
          });
          expect((yield* receipts.read(scope, reservation.command_id))?.request_body).toBe(
            request.request_body,
          );
        }).pipe(Effect.provide(testLayer({ filename })));
      }),
    ),
);

it.effect(
  "versioned encrypted payloads leave historical admission absent and never backfill it on duplicate acceptance",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient,
        db = yield* NativeSql.NativeSql;
      yield* (yield* BindingStore.BindingStore).bind(binding, null);
      const operations = yield* OperationStore.OperationStore;
      const { state: _state, ...destination } = binding;
      const input: OperationStore.OperationInput = {
        operation_id: "command-new-format-001",
        kind: "command",
        thread_environment_id: null,
        thread_id: null,
        origin: { kind: "app" },
        destination,
        child_command_ids: ["command:child-00000"],
        request_body: '{ "exact": "protected-request-sentinel" }',
        accepted_at: now,
      };
      const saved = yield* operations.accept(input);
      expect(saved.recorded_caller).toBeNull();
      const [row] = yield* sql<
        Record<string, string | number | null>
      >`SELECT * FROM jones_workstreams_operations WHERE operation_id = ${input.operation_id}`;
      const payload = yield* db.openJson(
        {
          ...scope,
          key_scope: "local",
          purpose: "operation_request",
          record_id: input.operation_id,
          revision: 1,
        },
        String(row!.request_envelope),
      );
      expect(payload).toEqual({
        version: 2,
        request_body: input.request_body,
        principal_id: binding.principal_id,
        recorded_caller: null,
      });
      const id = "command-historical-001";
      const envelope = yield* db.sealJson(
        { ...scope, key_scope: "local", purpose: "operation_request", record_id: id, revision: 1 },
        { request_body: input.request_body, principal_id: binding.principal_id },
      );
      yield* sql`INSERT INTO jones_workstreams_operations ${sql.insert({ ...row!, operation_id: id, request_envelope: envelope })}`;
      const historical = yield* operations.read(id);
      expect(historical?.recorded_caller).toBeNull();
      expect(historical?.request_body).toBe(input.request_body);
      expect(yield* operations.accept({ ...input, operation_id: id })).toEqual(historical);
      expect(String(row!.request_envelope)).not.toContain("protected-request-sentinel");
    }).pipe(Effect.provide(testLayer())),
);
