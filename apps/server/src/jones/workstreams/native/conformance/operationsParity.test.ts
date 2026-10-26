// @effect-diagnostics nodeBuiltinImport:off -- Synthetic SQLite fixtures stay under the task cache for the root cleanup owner.
import * as NodeFS from "node:fs";
import { expect, it } from "@effect/vitest";
import * as Recovery from "../runtime/recovery.ts";
import * as NativeRegistry from "../registry/service.ts";
import * as Effects from "../store/effectStore.ts";
import * as Receipts from "../store/receiptStore.ts";
import {
  Effect,
  Wire,
  Operations,
  OperationStore,
  Store,
  Entry,
  TestClock,
  DateTime,
  principal,
  registry,
  scope,
  enrollmentRequest,
  fresh,
  seed,
  app,
  harness,
  createOperation,
  assignOperation,
  canonicalJson,
  revokeGrant,
} from "./testkit.ts";

it.effect(
  "mounted operations persist original caller request children and pinned home across reopen beyond 24h",
  () =>
    Effect.gen(function* () {
      const f = fresh(),
        request = createOperation("durable-ingress-create-0001");
      let original: unknown,
        saved: OperationStore.Operation | null = null;
      yield* Effect.gen(function* () {
        const prepared = yield* seed(f);
        yield* Effect.gen(function* () {
          const operations = yield* Operations.WorkstreamOperations;
          original = yield* operations.submit({ request, wait_ms: 3000 });
          expect(original).toMatchObject({ state: "committed" });
          saved = yield* (yield* OperationStore.OperationStore).read(request.operation_id);
          expect(saved).toMatchObject({
            request_body: canonicalJson(request),
            destination: {
              registry_instance_id: registry.registry_instance_id,
              owner_id: registry.owner_id,
              home_environment_id: registry.home_environment_id,
              home_generation: 1,
              principal_id: enrollmentRequest.context.principal_id,
              binding_version: 1,
            },
            recorded_caller: {
              auth: { session_id: principal.sessionId },
              source: {
                enrollment_id: enrollmentRequest.enrollment_id,
                enrollment_session_id: enrollmentRequest.session.session_id,
              },
              ceiling: { permissions: ["workstreams:read", "workstreams:write"] },
            },
          });
        }).pipe(app, Effect.provide(prepared.services));
      }).pipe(Effect.provide(f.layer));
      yield* Effect.gen(function* () {
        const prepared = yield* seed(f, false);
        yield* TestClock.adjust(25 * 60 * 60 * 1000);
        yield* Effect.gen(function* () {
          const operations = yield* Operations.WorkstreamOperations;
          expect(yield* operations.read({ operation_id: request.operation_id })).toEqual({
            operation: original,
          });
          expect(yield* operations.submit({ request, wait_ms: 3000 })).toEqual(original);
          expect(yield* (yield* OperationStore.OperationStore).read(request.operation_id)).toEqual(
            saved,
          );
          expect((yield* (yield* harness).gateway.readMetadata()).items).toHaveLength(1);
          const conflict = yield* operations
            .submit({
              request: {
                ...request,
                workstream: {
                  name: "different bytes",
                  lifecycle: "active",
                  progress: { state: "unknown" },
                  sort_order: 0,
                },
              },
              wait_ms: 0,
            })
            .pipe(Effect.result);
          expect(conflict._tag).toBe("Failure");
          if (conflict._tag === "Failure")
            expect(conflict.failure).toMatchObject({ code: "operation_id_conflict" });
        }).pipe(app, Effect.provide(prepared.services));
      }).pipe(Effect.provide(f.layer));
      const sentinel = Buffer.from("Synthetic durable-ingress-create-0001");
      for (const suffix of ["", "-wal"])
        if (NodeFS.existsSync(f.database + suffix))
          expect(NodeFS.readFileSync(f.database + suffix).includes(sentinel)).toBe(false);
    }),
);

it.effect(
  "mounted operation assignment unassignment and concurrent primary writers preserve one primary",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          operations = yield* Operations.WorkstreamOperations,
          a = yield* h.create("assignment a"),
          b = yield* h.create("assignment b");
        const requests = [
          assignOperation("primary-ingress-a-000001", a),
          assignOperation("primary-ingress-b-000001", b),
        ];
        const results = yield* Effect.all(
          requests.map((request) =>
            operations.submit({ request, wait_ms: 3000 }).pipe(Effect.result),
          ),
          { concurrency: "unbounded" },
        );
        expect(
          results.filter((x) => x._tag === "Success" && x.success.state === "committed"),
          JSON.stringify(results),
        ).toHaveLength(1);
        const memberships = [
          ...(yield* h.gateway.readMemberships(a, { limit: 100 })).items,
          ...(yield* h.gateway.readMemberships(b, { limit: 100 })).items,
        ];
        expect(memberships.filter((x) => x.kind === "primary" && x.closed === null)).toHaveLength(
          1,
        );
        const assignment = yield* operations.read({
          thread: { environment_id: registry.home_environment_id, thread_id: "thread-synthetic" },
        });
        expect(assignment).toMatchObject({ assignment: { kind: "primary" } });
        const unassign = {
          ...assignOperation("primary-ingress-unassign-01", null),
          expected:
            "assignment" in assignment && assignment.assignment.kind === "primary"
              ? { kind: "primary" as const, workstream_id: assignment.assignment.workstream_id }
              : { kind: "unassigned" as const },
        };
        expect((yield* operations.submit({ request: unassign, wait_ms: 3000 })).state).toBe(
          "committed",
        );
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "mounted accepted raw verification is recovered with its original caller and no request-driven trigger",
  () =>
    Effect.gen(function* () {
      const f = fresh();
      let command!: Wire.WorkstreamCommand, ref!: string;
      yield* Effect.gen(function* () {
        const prepared = yield* seed(f);
        yield* Effect.gen(function* () {
          const h = yield* harness,
            entry = yield* Entry.Entry,
            registryService = yield* NativeRegistry.Registry;
          ref = yield* h.register();
          command = yield* h.command({
            operation: "verify_reference",
            native_reference_id: ref,
            expected_attestation_version: 0,
          });
          const pending = yield* entry.run("workstreams:write", "thread-synthetic", () =>
            registryService.submit(
              {
                principal_id: enrollmentRequest.context.principal_id,
                source_environment_id: registry.home_environment_id,
                binding_version: 1,
                origin: { kind: "app" },
              },
              canonicalJson(command),
            ),
          );
          expect(pending.receipt.state).toBe("pending");
          const active = yield* (yield* Effects.EffectStore).active(scope);
          expect(active).toHaveLength(1);
          expect(active[0]?.recovery?.recorded_caller).toMatchObject({
            auth: { session_id: principal.sessionId },
          });
        }).pipe(app, Effect.provide(prepared.services));
      }).pipe(Effect.provide(f.layer));
      yield* Effect.gen(function* () {
        const prepared = yield* seed(f, false);
        yield* Effect.gen(function* () {
          // The mounted startup worker's initial sweep and this named drain coalesce.
          yield* (yield* Recovery.NativeWorkstreamsRecovery).sweep();
          expect(yield* (yield* Effects.EffectStore).active(scope)).toHaveLength(0);
          const receipt = yield* (yield* Receipts.ReceiptStore).lookup(
            scope,
            command.command_id,
            1,
          );
          expect(receipt).toMatchObject({ kind: "present", receipt: { state: "committed" } });
          expect(
            (yield* (yield* harness).gateway.readReference(ref)).reference.registration.state,
          ).toBe("attested");
          expect(prepared.provider.calls).toHaveLength(0);
        }).pipe(app, Effect.provide(prepared.services));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "mounted source session grant and home generation revocations refuse before durable acceptance",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const operations = yield* Operations.WorkstreamOperations;
        yield* prepared.sessions.revoke({
          sessionId: enrollmentRequest.session.session_id,
          revokedAt: DateTime.makeUnsafe(enrollmentRequest.session.issued_at),
        });
        const request = assignOperation("revoked-source-ingress-01", "workstream:missing");
        expect((yield* operations.submit({ request, wait_ms: 0 }).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect(yield* (yield* OperationStore.OperationStore).read(request.operation_id)).toBeNull();
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each([
  "caller_session",
  "grant",
  "home_generation",
  "source_generation",
  "expired_session",
] as const)(
  "mounted admission refuses %s changes before accepting a new durable request",
  (changed) => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(
        f,
        true,
        changed === "expired_session" ? { callerExpiryMs: 1 } : {},
      );
      yield* Effect.gen(function* () {
        const store = yield* Store.RegistryStore;
        if (changed === "caller_session")
          yield* prepared.sessions.revoke({
            sessionId: principal.sessionId,
            revokedAt: DateTime.makeUnsafe(enrollmentRequest.session.issued_at),
          });
        if (changed === "grant") yield* revokeGrant();
        if (changed === "home_generation") {
          const current = (yield* store.context(scope))!;
          yield* store.advance(scope, current, { generation: true });
        }
        if (changed === "source_generation")
          yield* store.appendPlain(scope, "source_bindings", {
            provider: "t3",
            source_instance_id: registry.home_environment_id,
            binding_version: 2,
            authority_namespace: enrollmentRequest.context.authority_namespace,
            store_generation: 2,
            state: "active",
          });
        if (changed === "expired_session") yield* TestClock.adjust(2);
        const request = createOperation("fenced-ingress-" + changed + "-000001");
        const result = yield* (yield* Operations.WorkstreamOperations)
          .submit({ request, wait_ms: 0 })
          .pipe(Effect.result);
        expect(result._tag, canonicalJson(result)).toBe("Failure");
        expect(yield* (yield* OperationStore.OperationStore).read(request.operation_id)).toBeNull();
        expect(yield* store.list(scope, "records")).toHaveLength(0);
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(["committed", "denied"] as const)(
  "mounted source settlement %s remains distinct from coordination and exact replay dispatches once",
  (nativeOutcome) => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f, true, { nativeOutcome });
      yield* Effect.gen(function* () {
        const h = yield* harness,
          a = yield* h.create(),
          ref = yield* h.register();
        yield* h.verify(ref);
        const membership = (yield* h.attach(a, ref)).effects.membership_ids[0]!;
        const coordination = yield* h.committed({
          operation: "set_coordination_disposition",
          workstream_id: a,
          expected_version: 2,
          membership_id: membership,
          disposition: "completed",
          other_disposition: null,
        });
        expect(coordination.effects.native_settlement).toBeNull();
        expect(prepared.provider.calls).toHaveLength(0);
        const command = yield* h.command({
          operation: "request_native_t3_settlement",
          native_reference_id: ref,
          expected_attestation_version: 1,
          native_action: "settle",
        });
        const receipt = yield* h.gateway.submit(command);
        expect(receipt).toMatchObject({
          state: "committed",
          changed: nativeOutcome === "committed",
          effects: { native_settlement: { outcome: nativeOutcome } },
        });
        expect(receipt.command_id).not.toBe(coordination.command_id);
        expect((yield* h.gateway.readMemberships(a, { limit: 100 })).items[0]?.closed?.reason).toBe(
          "completed",
        );
        expect((yield* h.gateway.readDetail(a)).workstream.lifecycle).toBe("active");
        expect(yield* h.gateway.resubmit(command.command_id, canonicalJson(command))).toEqual({
          kind: "receipt",
          receipt,
        });
        expect(prepared.provider.calls).toHaveLength(1);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);
