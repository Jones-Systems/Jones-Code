import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as EffectStore from "./effectStore.ts";
import * as ReceiptStore from "./receiptStore.ts";
import * as HistoryStore from "./historyStore.ts";
import * as RegistryStore from "./registryStore.ts";
import * as NativeSql from "./sql.ts";
import { completeNativeExternalIntent } from "../domain/decider.ts";
import { nativeSettlementResult } from "../domain/externalIntents.ts";
import { createWorkstreamDomainState, type ExternalIntent } from "../domain/state.ts";
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

const recoveryFixture = (
  outcome: "committed" | "denied" | "unsupported" | "failed" = "committed",
) => {
  const command: ExternalIntent["command"] = {
    command_id: "effect:retained-native-000000",
    expected_server_generation: 1,
    expected_registry_version: 0,
    action: {
      operation: "request_native_t3_settlement",
      native_reference_id: "ref:test",
      expected_attestation_version: 1,
      native_action: "settle",
    },
  };
  const body = " \n" + JSON.stringify(command, null, 2) + "\n";
  const request = pending(command.command_id, body);
  const admitted = {
    ...request,
    receipt: { ...request.receipt, operation: command.action.operation },
  };
  const intent: ExternalIntent = {
    kind: "native_t3_settlement",
    command,
    owner_id: scope.owner_id,
    actor: admitted.receipt.actor,
    authorization: {
      owner_id: scope.owner_id,
      actor: admitted.receipt.actor,
      server_generation: 1,
      grant: {
        owner_id: scope.owner_id,
        principal_id: admitted.receipt.actor.principal_id,
        grant_id: "grant:test",
        grant_version: 1,
        permissions: ["workstreams:read", "workstreams:write"],
        state: "active",
      },
    },
    request_sha256: admitted.receipt.request_sha256,
    accepted_at: now,
    admitted_registry_version: 0,
    reference: {
      native_reference_id: "ref:test",
      owner_id: scope.owner_id,
      identity: {
        provider: "t3",
        source_instance_id: "source:test",
        resource_kind: "thread",
        id_kind: "internal",
        native_id: "thread:test",
        account_provenance: { kind: "not_account_scoped" },
      },
      pr_locator: null,
      registration: {
        state: "verification-pending",
        attestation_version: 0,
        attested_at: null,
        expires_at: null,
        evidence: null,
      },
      created_at: now,
      created_by: admitted.receipt.actor,
      created_registry_version: 1,
    },
    source_binding: {
      provider: "t3",
      source_instance_id: "source:test",
      authority_namespace: "source-authority",
      store_generation: 1,
      state: "active",
    },
    membership: null,
    previous_observation: null,
  };
  const reservation: EffectStore.Reservation = {
    command_id: command.command_id,
    principal_id: admitted.receipt.actor.principal_id,
    native_reference_id: "ref:test",
    effect: "request_native_t3_settlement",
    server_generation: 1,
    admitted_registry_version: 0,
    intent,
  };
  const result = nativeSettlementResult(intent, outcome);
  const state = createWorkstreamDomainState(scope.owner_id);
  state.registry_version = 1;
  state.receipts.set(command.command_id, admitted.receipt);
  state.external_intents.set(command.command_id, intent);
  const completed = completeNativeExternalIntent(
    state,
    intent,
    { next: () => "event:retained-native" },
    result,
    now,
    true,
  );
  return {
    request: admitted,
    reservation,
    result,
    terminal: { ...admitted, receipt: completed.receipt },
    events: completed.state.audit.map((value) => ({
      event_id: value.event_id,
      command_id: value.command_id,
      registry_version: value.registry_version,
      occurred_at: value.occurred_at,
      workstream_ids: value.workstream_versions.map((v) => v.workstream_id),
      value,
    })),
    expected: { server_generation: 1, registry_version: 1 },
  };
};
type RecoveryFixture = ReturnType<typeof recoveryFixture>;

const prepareRecovery = Effect.fn("EffectStoreTest.prepareRecovery")(function* (
  fixture: RecoveryFixture,
  retainResult = true,
) {
  yield* seed;
  const effects = yield* EffectStore.EffectStore;
  const registry = yield* RegistryStore.RegistryStore;
  yield* effects.reserve(scope, fixture.request, fixture.reservation);
  if (retainResult)
    yield* effects.saveResult(scope, fixture.request.receipt.command_id, fixture.result);
  yield* registry.advance(scope, { server_generation: 1, registry_version: 0 });
});
const completeRecovery = Effect.fn("EffectStoreTest.completeRecovery")(function* (
  fixture: RecoveryFixture,
) {
  const db = yield* NativeSql.NativeSql;
  const effects = yield* EffectStore.EffectStore;
  const registry = yield* RegistryStore.RegistryStore;
  yield* db.transaction(
    scope,
    Effect.gen(function* () {
      if (fixture.terminal.receipt.changed)
        yield* registry.advance(scope, { server_generation: 1, registry_version: 1 });
      yield* effects.completeRetainedNativeSettlement(
        scope,
        fixture.terminal,
        fixture.expected,
        fixture.events,
      );
    }),
  );
});
const persistedRecovery = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    registry: yield* sql`SELECT * FROM jones_workstreams_registries`,
    receipts: yield* sql`SELECT * FROM jones_workstreams_receipts`,
    reservations: yield* sql`SELECT * FROM jones_workstreams_external_reservations`,
    results: yield* sql`SELECT * FROM jones_workstreams_external_results`,
    history: yield* sql`SELECT * FROM jones_workstreams_history`,
    scopes: yield* sql`SELECT * FROM jones_workstreams_history_scopes`,
  };
});

it.effect.each(["committed", "denied", "unsupported", "failed"] as const)(
  "retained native %s completes at the current revision atomically",
  (outcome) =>
    Effect.gen(function* () {
      const fixture = recoveryFixture(outcome);
      yield* prepareRecovery(fixture);
      yield* completeRecovery(fixture);
      const effects = yield* EffectStore.EffectStore;
      expect(yield* effects.active(scope)).toEqual([]);
      expect(yield* effects.result(scope, fixture.request.receipt.command_id)).toEqual(
        fixture.result,
      );
      expect(
        yield* (yield* ReceiptStore.ReceiptStore).read(scope, fixture.request.receipt.command_id),
      ).toEqual(fixture.terminal);
      expect(
        (yield* (yield* HistoryStore.HistoryStore).page(scope, { limit: 100 })).events,
      ).toEqual(fixture.events.map((event) => ({ ...event, event_sequence: 1 })));
      expect((yield* (yield* RegistryStore.RegistryStore).context(scope))?.registry_version).toBe(
        outcome === "committed" ? 2 : 1,
      );
    }).pipe(Effect.provide(testLayer())),
);

const invalidRecoveries: ReadonlyArray<readonly [string, (f: RecoveryFixture) => RecoveryFixture]> =
  [
    [
      "actor",
      (f) => ({
        ...f,
        terminal: {
          ...f.terminal,
          receipt: { ...f.terminal.receipt, actor: { principal_id: "other" } },
        },
      }),
    ],
    [
      "digest",
      (f) => ({
        ...f,
        terminal: {
          ...f.terminal,
          receipt: { ...f.terminal.receipt, request_sha256: "b".repeat(64) },
        },
      }),
    ],
    [
      "exact request bytes",
      (f) => ({ ...f, terminal: { ...f.terminal, request_body: f.terminal.request_body + " " } }),
    ],
    [
      "request command association",
      (f) => {
        const intent = f.reservation.intent as ExternalIntent;
        const substituted: ExternalIntent = {
          ...intent,
          command: {
            ...intent.command,
            action: {
              operation: "request_native_t3_settlement",
              native_reference_id: "ref:test",
              expected_attestation_version: 1,
              native_action: "unsettle",
            },
          },
        };
        return {
          ...f,
          reservation: { ...f.reservation, intent: substituted },
          result: nativeSettlementResult(substituted, "committed"),
          terminal: {
            ...f.terminal,
            receipt: {
              ...f.terminal.receipt,
              effects: {
                ...f.terminal.receipt.effects,
                native_settlement: {
                  native_reference_id: "ref:test",
                  native_action: "unsettle",
                  outcome: "committed",
                },
              },
            },
          },
        };
      },
    ],
    [
      "home generation",
      (f) => ({
        ...f,
        terminal: { ...f.terminal, receipt: { ...f.terminal.receipt, server_generation: 2 } },
      }),
    ],
    [
      "current generation CAS",
      (f) => ({ ...f, expected: { ...f.expected, server_generation: 2 } }),
    ],
    ["current revision CAS", (f) => ({ ...f, expected: { ...f.expected, registry_version: 0 } })],
    [
      "reservation reference",
      (f) => ({ ...f, reservation: { ...f.reservation, native_reference_id: "ref:other" } }),
    ],
    [
      "reservation admission",
      (f) => ({
        ...f,
        reservation: {
          ...f.reservation,
          intent: { ...(f.reservation.intent as ExternalIntent), admitted_registry_version: 1 },
        },
      }),
    ],
    [
      "source generation association",
      (f) => {
        const intent = f.reservation.intent as ExternalIntent;
        return {
          ...f,
          reservation: {
            ...f.reservation,
            intent: {
              ...intent,
              source_binding: { ...intent.source_binding, store_generation: 2 },
            },
          },
        };
      },
    ],
    [
      "ordinary verification",
      (f) => ({ ...f, reservation: { ...f.reservation, effect: "verify_reference" } }),
    ],
    [
      "ordinary PR observation",
      (f) => ({ ...f, reservation: { ...f.reservation, effect: "refresh_linked_pr" } }),
    ],
    ["result outcome", (f) => ({ ...f, result: { ...f.result, native_outcome: "denied" } })],
    [
      "result actor",
      (f) => ({
        ...f,
        result: {
          ...f.result,
          native_evidence: { ...f.result.native_evidence!, principal_id: "other" },
        },
      }),
    ],
    [
      "result reservation digest",
      (f) => ({
        ...f,
        result: {
          ...f.result,
          native_evidence: { ...f.result.native_evidence!, reservation_sha256: "b".repeat(64) },
        },
      }),
    ],
    [
      "result digest",
      (f) => ({
        ...f,
        result: {
          ...f.result,
          native_evidence: { ...f.result.native_evidence!, result_sha256: "b".repeat(64) },
        },
      }),
    ],
    [
      "unresolved outcome",
      (f) => ({
        ...f,
        result: nativeSettlementResult(f.reservation.intent as ExternalIntent, "unresolved"),
      }),
    ],
    [
      "audit actor",
      (f) => ({
        ...f,
        events: f.events.map((e) => ({
          ...e,
          value: { ...e.value, actor: { principal_id: "other" } },
        })),
      }),
    ],
  ];
it.effect.each(invalidRecoveries.map(([name, invalidate]) => ({ name, invalidate })))(
  "retained native recovery rejects mismatched $name without any durable changes",
  ({ invalidate }) =>
    Effect.gen(function* () {
      const fixture = invalidate(recoveryFixture());
      yield* prepareRecovery(fixture);
      const before = yield* persistedRecovery;
      expect(Exit.isFailure(yield* Effect.exit(completeRecovery(fixture)))).toBe(true);
      expect(yield* persistedRecovery).toEqual(before);
    }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "retained native recovery requires a durable result and rolls back a late history conflict",
  () =>
    Effect.gen(function* () {
      const fixture = recoveryFixture();
      yield* prepareRecovery(fixture, false);
      const before = yield* persistedRecovery;
      expect(Exit.isFailure(yield* Effect.exit(completeRecovery(fixture)))).toBe(true);
      expect(yield* persistedRecovery).toEqual(before);
      yield* (yield* EffectStore.EffectStore).saveResult(
        scope,
        fixture.request.receipt.command_id,
        fixture.result,
      );
      yield* (yield* HistoryStore.HistoryStore).append(scope, {
        ...fixture.events[0]!,
        value: { conflicting: true },
      });
      const withHistory = yield* persistedRecovery;
      expect(Exit.isFailure(yield* Effect.exit(completeRecovery(fixture)))).toBe(true);
      expect(yield* persistedRecovery).toEqual(withHistory);
    }).pipe(Effect.provide(testLayer())),
);
it.effect.each(["verify_reference", "refresh_linked_pr", "request_native_t3_settlement"] as const)(
  "ordinary %s completion retains its admission-version fence",
  (effect) =>
    Effect.gen(function* () {
      const fixture = recoveryFixture();
      const request = {
        ...fixture.request,
        receipt: { ...fixture.request.receipt, operation: effect },
      };
      yield* prepareRecovery({
        ...fixture,
        request,
        reservation: { ...fixture.reservation, effect },
      });
      const before = yield* persistedRecovery;
      const db = yield* NativeSql.NativeSql;
      const registry = yield* RegistryStore.RegistryStore;
      const effects = yield* EffectStore.EffectStore;
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            db.transaction(
              scope,
              Effect.gen(function* () {
                yield* registry.advance(scope, { server_generation: 1, registry_version: 1 });
                yield* effects.complete(
                  scope,
                  {
                    ...fixture.terminal,
                    receipt: { ...fixture.terminal.receipt, operation: effect },
                  },
                  2,
                  fixture.events,
                );
              }),
            ),
          ),
        ),
      ).toBe(true);
      expect(yield* persistedRecovery).toEqual(before);
    }).pipe(Effect.provide(testLayer())),
);
