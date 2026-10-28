import * as Startup from "../runtime/startup.ts";
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
        const parent = NodePath.resolve(
          process.env.NW_WRITE_FOUNDATION_EVIDENCE_DIR ??
            "node_modules/.cache/nw-recorded-recovery/store-files",
        );
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

import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { WORKSTREAMS_NATIVE_FORWARDING_CONTRACT } from "@t3tools/contracts/jones/workstreamsForwarding";
import { MemberRecordedWriteCallerV2 } from "../runtime/memberAdmission.ts";
import migration125 from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";

const digest = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const decodeMemberCaller = Schema.decodeUnknownSync(MemberRecordedWriteCallerV2);
// This store fixture supplies a synthetic accepted caller; genuine M capture is exercised by memberAdmission.test.ts.
const memberInput = (operationId = "member-operation-0001"): OperationStore.MemberForwardInput => {
  const { state: _state, ...destination } = binding;
  const home = { ...scope, home_environment_id: binding.home_environment_id, server_generation: 1 };
  const source = {
    environment_id: "source:test",
    authority_namespace: "source-store",
    store_generation: 1,
  };
  const caller = decodeMemberCaller({
    schema: "jones-workstreams-member-recorded-write-caller/2",
    local: {
      schema: "jones-workstreams-member-local-write-caller/2",
      auth: {
        kind: "bearer",
        session_id: "original-session",
        subject: "protected-caller-sentinel",
        method: "bearer-access-token",
      },
      origin: { kind: "app" },
      environment_id: source.environment_id,
      request_namespace: null,
      invocation_issued_at: null,
      link_origin: null,
      link_session: null,
      session: { issued_at: 0, expires_at: 9999999999999 },
      captured_at: 1000,
      ceiling: {
        permissions: ["workstreams:read", "workstreams:write"],
        scopes: ["orchestration:read", "orchestration:operate"],
        capabilities: [],
        limits: { runtimeMode: "approval-required", interactionMode: "default" },
      },
      target: { kind: "thread", environment_id: source.environment_id, thread_id: "thread:test" },
    },
    association: {
      enrollment_id: "enrollment-original",
      enrollment_session_id: "provider-original",
      intent_sha256: "c".repeat(64),
      home,
      canonical_origin: "https://native.workstreams.invalid",
      principal_id: binding.principal_id,
      singleton_binding_version: 1,
      source_binding_version: 1,
      pin_bound_revision: 2,
      source,
      link_generation: "opaque-link-generation:1",
      member_pin_id: "member-pin",
      home_pin_id: "home-pin",
      home_pin_bound_revision: 2,
    },
    original_outbound_ceiling: {
      access: "full-access",
      runtime_mode: "approval-required",
      interaction_mode: "default",
      scopes: ["orchestration:read", "orchestration:operate"],
      permissions: ["workstreams:read", "workstreams:write"],
    },
  });
  const request = {
    contract: "workstream-operations/1.0.0",
    operation_id: operationId,
    origin: { kind: "app" },
    kind: "set_primary",
    thread: { environment_id: source.environment_id, thread_id: "thread:test" },
    target: {
      kind: "workstream",
      registry_id: scope.registry_instance_id,
      owner_id: scope.owner_id,
      workstream_id: "protected-request-sentinel",
    },
    expected: { kind: "any" },
  };
  const body = JSON.stringify(request, null, 2) + "\n";
  const identity = {
    forward_id: "member-forward-0001",
    source_operation_id: operationId,
    parent_operation_id: null,
    command_ids: ["member-child-command-0001"],
    request_sha256: digest(body),
    recorded_caller_sha256: digest(canonical(caller)),
    expected_home: home,
    canonical_origin: caller.association.canonical_origin,
    source_authority: source,
    source_enrollment_id: caller.association.enrollment_id,
    source_enrollment_session_id: caller.association.enrollment_session_id,
    principal_id: binding.principal_id,
    singleton_binding_version: 1,
    source_binding_version: 1,
    member_pin_id: "member-pin",
    member_pin_bound_revision: 2,
    outbound_link_generation: "opaque-link-generation:1",
  };
  const forwardBody =
    JSON.stringify(
      {
        contract: WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
        kind: "operation",
        identity,
        request: {
          expected_home: home,
          relay: {
            source_environment_id: source.environment_id,
            origin: request.origin,
            linked_origin_environment_id: null,
            hop: 1,
          },
          request,
          request_sha256: digest(body),
        },
        request_body: body,
      },
      null,
      2,
    ) + "\n";
  return {
    operation_id: operationId,
    kind: "set_primary",
    thread_environment_id: source.environment_id,
    thread_id: "thread:test",
    origin: { kind: "app" },
    destination,
    child_command_ids: identity.command_ids,
    request_body: body,
    accepted_at: now,
    member_forward: {
      schema: "jones-workstreams-member-forward-accepted/1",
      kind: "MEMBER-FORWARD",
      forward_request_body: forwardBody,
      forward_request_sha256: digest(forwardBody),
      recorded_caller: caller,
    },
  };
};
const seedMember = Effect.gen(function* () {
  yield* migration125;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('test-local',${scope.registry_instance_id},'local','active',${now})`;
  yield* (yield* BindingStore.BindingStore).bind(binding, null);
});
it.effect(
  "member intent survives file-backed restart with encrypted exact forward/caller/result and no home replicas",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const parent = NodePath.resolve(
          process.env.NW_WRITE_FOUNDATION_EVIDENCE_DIR ??
            "node_modules/.cache/nw-write-foundation/store-files",
        );
        yield* Effect.promise(() => NodeFSP.mkdir(parent, { recursive: true }));
        const dir = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(parent, "member-store-")),
        );
        const filename = NodePath.join(dir, "native.sqlite"),
          input = memberInput();
        const assertNoPlaintext = Effect.gen(function* () {
          for (const path of [filename, filename + "-wal"]) {
            const bytes = yield* Effect.promise(() => NodeFSP.readFile(path));
            for (const sentinel of [
              "protected-caller-sentinel",
              "protected-request-sentinel",
              "protected-result-sentinel",
            ])
              expect(bytes.includes(Buffer.from(sentinel))).toBe(false);
          }
        });
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA journal_mode=WAL`;
          yield* seedMember;
          const store = yield* OperationStore.OperationStore;
          const accepted = yield* store.acceptMemberForward(input);
          expect(accepted.member_forward).toEqual(input.member_forward);
          expect(accepted.recorded_caller).toBeNull();
          expect(yield* store.open()).toEqual([]);
          expect(yield* store.read(input.operation_id).pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
          });
          expect(
            yield* store
              .advance(input.operation_id, "accepted", "dispatching", now, null)
              .pipe(Effect.result),
          ).toMatchObject({ _tag: "Failure" });
          yield* store.advanceMemberForward(
            input.operation_id,
            "accepted",
            "dispatching",
            now,
            null,
          );
          yield* store.advanceMemberForward(
            input.operation_id,
            "dispatching",
            "reconciling",
            now,
            null,
            "protected-result-sentinel",
          );
          yield* assertNoPlaintext;
          for (const table of [
            "jones_workstreams_registries",
            "jones_workstreams_principals",
            "jones_workstreams_grants",
            "jones_workstreams_source_bindings",
          ])
            expect((yield* sql.unsafe(`SELECT * FROM ${table}`)).length).toBe(0);
        }).pipe(Effect.provide(testLayer({ filename })));
        yield* Effect.gen(function* () {
          const store = yield* OperationStore.OperationStore,
            db = yield* NativeSql.NativeSql,
            sql = yield* SqlClient.SqlClient;
          const current = yield* store.readMemberForward(input.operation_id);
          expect(current).toMatchObject({
            ...input,
            recorded_caller: null,
            state: "reconciling",
            result_body: "protected-result-sentinel",
          });
          expect(yield* store.acceptMemberForward(input)).toEqual(current);
          expect(yield* store.openMemberForwards()).toEqual([current]);
          const [row] = yield* sql<{
            request_envelope: string;
          }>`SELECT request_envelope FROM jones_workstreams_operations WHERE operation_id=${input.operation_id}`;
          expect(
            yield* db.openJson(
              {
                ...scope,
                key_scope: "local",
                purpose: "operation_request",
                record_id: input.operation_id,
                revision: 1,
              },
              row!.request_envelope,
            ),
          ).toMatchObject({
            version: 3,
            kind: "MEMBER-FORWARD",
            member_forward: input.member_forward,
          });
          expect(JSON.parse(row!.request_envelope).key_id).toBe("test-local");
          expect(
            yield* db
              .openJson(
                {
                  ...scope,
                  key_scope: "local",
                  purpose: "other-purpose",
                  record_id: input.operation_id,
                  revision: 1,
                },
                row!.request_envelope,
              )
              .pipe(Effect.result),
          ).toMatchObject({ _tag: "Failure" });
          yield* assertNoPlaintext;
        }).pipe(Effect.provide(testLayer({ filename })));
      }),
    ),
);
it.effect(
  "member acceptance is transactional, shares the one-open-thread guard, and cannot backfill home caller evidence",
  () =>
    Effect.gen(function* () {
      yield* seedMember;
      const store = yield* OperationStore.OperationStore,
        sql = yield* SqlClient.SqlClient;
      const input = memberInput();
      expect(
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* store.acceptMemberForward(input);
              return yield* new NativeSql.StoreError({ reason: "conflict" });
            }),
          )
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      expect(yield* store.readMemberForward(input.operation_id)).toBeNull();
      yield* store.acceptMemberForward(input);
      expect(
        yield* store
          .acceptMemberForward(memberInput("member-overlapping-0001"))
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      expect(yield* store.openMemberForwards()).toHaveLength(1);
      expect(yield* store.accept(input).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      const { member_forward: _member, ...homeInput } = input;
      expect(yield* store.accept(homeInput).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
      expect(
        yield* store
          .advanceMemberForward(input.operation_id, "dispatching", "reconciling", now, null)
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      yield* store.advanceMemberForward(input.operation_id, "accepted", "dispatching", now, null);
      yield* store.advanceMemberForward(input.operation_id, "dispatching", "committed", now, null);
      expect(
        yield* store
          .advanceMemberForward(input.operation_id, "committed", "dispatching", now, null)
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
    }).pipe(Effect.provide(testLayer())),
);
it.effect.each([
  "bytes",
  "caller",
  "destination",
  "generation",
  "link",
  "digest",
  "historical",
] as const)(
  "same member ID rejects changed %s and preserves the immutable accepted evidence",
  (change) =>
    Effect.gen(function* () {
      yield* seedMember;
      const store = yield* OperationStore.OperationStore,
        input = memberInput();
      const accepted = yield* store.acceptMemberForward(input);
      const altered =
        change === "bytes"
          ? { ...input, request_body: input.request_body + " " }
          : change === "destination"
            ? {
                ...input,
                destination: { ...input.destination, home_environment_id: "replacement-home" },
              }
            : change === "generation"
              ? { ...input, destination: { ...input.destination, home_generation: 2 } }
              : {
                  ...input,
                  member_forward: {
                    ...input.member_forward,
                    ...(change === "digest" ? { forward_request_sha256: "f".repeat(64) } : {}),
                    recorded_caller: {
                      ...input.member_forward.recorded_caller,
                      ...(change === "historical"
                        ? { schema: "jones-workstreams-member-recorded-caller/1" }
                        : {}),
                      ...(change === "caller"
                        ? {
                            local: {
                              ...input.member_forward.recorded_caller.local,
                              auth: {
                                kind: "bearer",
                                session_id: "replacement-session",
                                subject: "protected-caller-sentinel",
                                method: "bearer-access-token",
                              },
                            },
                          }
                        : {}),
                      ...(change === "link"
                        ? {
                            association: {
                              ...input.member_forward.recorded_caller.association,
                              link_generation: "relinked",
                            },
                          }
                        : {}),
                    },
                  },
                };
      expect(
        yield* store
          .acceptMemberForward(altered as OperationStore.MemberForwardInput)
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      expect(yield* store.readMemberForward(input.operation_id)).toEqual(accepted);
    }).pipe(Effect.provide(testLayer())),
);

import { canonicalJson as publicCanonicalJson } from "../../operations/encoding.ts";
import { WorkstreamsForwardResult } from "@t3tools/contracts/jones/workstreamsForwarding";
import { WorkstreamOperationResult } from "@t3tools/contracts/jones/workstreamOperations";
const decodePublicOperation = Schema.decodeUnknownSync(WorkstreamOperationResult);
const decodeNativeResult = Schema.decodeUnknownSync(WorkstreamsForwardResult);
it.effect(
  "raw reordered operation intents conflict while public v1 results retain the canonical decoded-request digest",
  () =>
    Effect.gen(function* () {
      yield* seedMember;
      const store = yield* OperationStore.OperationStore,
        input = memberInput();
      const decoded = JSON.parse(input.request_body);
      const reordered =
        JSON.stringify(Object.fromEntries(Object.entries(decoded).toReversed())) + "\n";
      expect(reordered).not.toBe(input.request_body);
      expect(digest(reordered)).not.toBe(digest(input.request_body));
      const canonicalDigest = digest(publicCanonicalJson(decoded));
      expect(digest(publicCanonicalJson(JSON.parse(reordered)))).toBe(canonicalDigest);
      expect(canonicalDigest).not.toBe(digest(input.request_body));
      const forward = JSON.parse(input.member_forward.forward_request_body);
      const result = decodePublicOperation({
        contract: "workstream-operations/1.0.0",
        operation_id: input.operation_id,
        origin: input.origin,
        kind: "set_primary",
        request_sha256: canonicalDigest,
        observed: null,
        children: [],
        accepted_at: now,
        updated_at: now,
        result: null,
        poll_after_ms: null,
        detail: null,
        state: "unavailable",
        reason: "offline",
      });
      const nativeResult = decodeNativeResult({
        contract: WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
        kind: "operation",
        identity: forward.identity,
        result,
        home: {
          registry: {
            ...scope,
            canonical_origin: "https://native.workstreams.invalid",
            home_environment_id: binding.home_environment_id,
            server_generation: 1,
            registry_version: 0,
          },
          principal_id: binding.principal_id,
          authorization_revision: 1,
          source_binding_version: 1,
          incoming_peer_pin_id: "home-pin",
          incoming_peer_pin_revision: 2,
        },
      });
      if (nativeResult.kind !== "operation") return yield* Effect.die("expected operation");
      expect(nativeResult.result.request_sha256).toBe(canonicalDigest);
      const accepted = yield* store.acceptMemberForward(input);
      const changedForward = {
        ...forward,
        identity: { ...forward.identity, request_sha256: digest(reordered) },
        request: { ...forward.request, request_sha256: digest(reordered) },
        request_body: reordered,
      };
      const bytes = JSON.stringify(changedForward);
      expect(
        yield* store
          .acceptMemberForward({
            ...input,
            request_body: reordered,
            member_forward: {
              ...input.member_forward,
              forward_request_body: bytes,
              forward_request_sha256: digest(bytes),
            },
          })
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure", failure: { reason: "idempotency_conflict" } });
      expect(yield* store.readMemberForward(input.operation_id)).toEqual(accepted);
    }).pipe(Effect.provide(testLayer())),
);

it("initializes the real member admission/startup/integration/store import composition before decoding member evidence", () => {
  expect(Startup.chooseLayer).toBeTypeOf("function");
  expect(decodeMemberCaller(memberInput().member_forward.recorded_caller).schema).toBe(
    "jones-workstreams-member-recorded-write-caller/2",
  );
});
