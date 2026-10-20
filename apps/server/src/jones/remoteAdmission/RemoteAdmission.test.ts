import { expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  publicRemoteAdmission,
  type RemoteAdmissionEnvelope,
  type RemoteAdmissionRequest,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import migrate from "../persistence/Migrations/115_JonesRemoteAdmission.ts";
import * as Admission from "./RemoteAdmission.ts";

class SyntheticEvaluationError extends Schema.TaggedError<SyntheticEvaluationError>()(
  "SyntheticEvaluationError",
  {},
) {}

const memory = Layer.effectDiscard(migrate).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const policy: RemoteAdmissionEnvelope["receiverPolicy"] = {
  enabled: true,
  revision: "receiver-policy-1",
  modelSelection: { instanceId: ProviderInstanceId.make("claude"), model: "synthetic" },
};
const envelope: RemoteAdmissionEnvelope = {
  identity: { clientId: "client", linkId: "link" },
  operation: "task.create",
  payload: { message: "Add a settings toggle", mode: "default" },
  target: {
    projectId: "project",
    threadId: null,
    runtimeMode: "full-access",
    interactionMode: "default",
  },
  receiverPolicy: policy,
};
const request: RemoteAdmissionRequest = { idempotencyKey: "request", envelope };
const pass = { verdict: "pass", category: "none", reason: "Ordinary coding request." };
const hold = { verdict: "hold", category: "risky", reason: "Receiver-local review detail." };
const owner = { kind: "human-owner", subject: "owner-session" } as const;
const bind = (record: { id: string; bindingDigest: string }) => ({
  id: record.id,
  bindingDigest: record.bindingDigest,
});
const testLayer = (
  evaluate: Admission.RemoteAdmissionEvaluator = () => Effect.succeed(pass),
  timeoutMs = 1000,
) => Admission.layerWithEvaluator(evaluate, { timeoutMs }).pipe(Layer.provideMerge(memory));

it.effect(
  "persists the exact request before evaluating and canonicalizes duplicate object keys",
  () => {
    let evaluations = 0;
    const evaluate: Admission.RemoteAdmissionEvaluator = (captured) =>
      Effect.gen(function* () {
        evaluations++;
        if (evaluations === 1) expect(captured).toEqual(envelope);
        return pass;
      });
    return Effect.gen(function* () {
      const service = yield* Admission.RemoteAdmission;
      const sql = yield* SqlClient.SqlClient;
      const first = yield* service.request(request);
      expect(first.status).toBe("passed");
      expect(first.bindingDigest).toMatch(/^[a-f0-9]{64}$/);
      const duplicate = yield* service.request({
        ...request,
        envelope: {
          ...envelope,
          payload: { mode: "default", message: "Add a settings toggle" },
          receiverPolicy: { ...policy, revision: "changed-after-request" },
        },
      });
      expect(duplicate).toEqual(first);
      expect(evaluations).toBe(1);
      expect((yield* sql`SELECT id FROM jones_remote_admissions`).length).toBe(1);
      for (const changed of [
        { ...envelope, payload: { message: "Something different" } },
        { ...envelope, operation: "task.other" },
        { ...envelope, identity: { ...envelope.identity, linkId: "different-link" } },
        { ...envelope, target: { ...envelope.target, runtimeMode: "different-mode" } },
      ]) {
        const result = yield* service
          .request({ ...request, envelope: changed })
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure._tag).toBe("RemoteAdmissionKeyConflict");
      }
      const otherClient = yield* service.request({
        ...request,
        envelope: {
          ...envelope,
          identity: { clientId: "another-client", linkId: "link" },
        },
      });
      expect(otherClient.id).not.toBe(first.id);
    }).pipe(Effect.provide(testLayer(evaluate)));
  },
);

it.effect(
  "intent is committed while evaluation is blocked, and concurrent duplicate never re-evaluates",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const layer = Admission.layerWithEvaluator(() =>
        Effect.gen(function* () {
          calls++;
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
          return pass;
        }),
      );
      yield* Effect.gen(function* () {
        const service = yield* Admission.RemoteAdmission;
        const sql = yield* SqlClient.SqlClient;
        const fiber = yield* service.request(request).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const rows = yield* sql<{
          status: string;
          envelope_json: string;
        }>`SELECT status,envelope_json FROM jones_remote_admissions`;
        expect(rows).toHaveLength(1);
        expect(rows[0]?.status).toBe("evaluating");
        expect(JSON.parse(rows[0]!.envelope_json)).toEqual(envelope);
        expect((yield* service.request(request)).status).toBe("evaluating");
        expect(calls).toBe(1);
        yield* Deferred.succeed(release, undefined);
        expect((yield* Fiber.join(fiber)).status).toBe("passed");
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "hold reasons remain local and only an authenticated human-owner classification resolves",
  () =>
    Effect.gen(function* () {
      const service = yield* Admission.RemoteAdmission;
      const record = yield* service.request(request);
      expect(record.status).toBe("held");
      expect(publicRemoteAdmission(record)).not.toHaveProperty("reason");
      expect(publicRemoteAdmission(record)).not.toHaveProperty("envelope");
      expect(yield* service.listPending()).toEqual([record]);
      for (const kind of ["mcp", "link", "agent"] as const) {
        const result = yield* service
          .resolve({
            ...bind(record),
            decision: "approve",
            principal: { kind, subject: "owner-session" },
          })
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure._tag).toBe("RemoteAdmissionForbidden");
      }
      const mismatched = yield* service
        .resolve({
          ...bind(record),
          bindingDigest: "0".repeat(64),
          decision: "approve",
          principal: owner,
        })
        .pipe(Effect.result);
      expect(mismatched._tag).toBe("Failure");
      if (mismatched._tag === "Failure")
        expect(mismatched.failure._tag).toBe("RemoteAdmissionBindingMismatch");
      const approved = yield* service.resolve({
        ...bind(record),
        decision: "approve",
        principal: owner,
      });
      expect(approved.status).toBe("approved");
      expect(approved.resolvedBy).toBe(owner.subject);
      expect((yield* service.listPending()).map((pending) => pending.id)).toContain(approved.id);
      expect(
        (yield* service.resolve({ ...bind(record), decision: "approve", principal: owner })).status,
      ).toBe("approved");
      expect(
        (yield* service
          .resolve({ ...bind(record), decision: "deny", principal: owner })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(testLayer(() => Effect.succeed(hold)))),
);

it.effect(
  "errors, defects, missing models and malformed verdicts hold without silently passing",
  () =>
    Effect.gen(function* () {
      const results: ReadonlyArray<Effect.Effect<unknown, SyntheticEvaluationError>> = [
        Effect.fail(new SyntheticEvaluationError({})),
        Effect.die("provider defect"),
        Effect.succeed({ verdict: "pass", category: "risky", reason: "unsafe" }),
        Effect.succeed({ ...pass, reason: " " }),
        Effect.succeed({ ...pass, reason: "x".repeat(601) }),
        Effect.succeed({ verdict: "unknown" }),
      ];
      for (const [index, result] of results.entries()) {
        yield* Effect.gen(function* () {
          const service = yield* Admission.RemoteAdmission;
          const record = yield* service.request({ ...request, idempotencyKey: `failure-${index}` });
          expect(record.status).toBe("held");
          expect(record.category).toBe("evaluation_unavailable");
        }).pipe(Effect.provide(Admission.layerWithEvaluator(() => result)));
      }
      yield* Effect.gen(function* () {
        const service = yield* Admission.RemoteAdmission;
        const record = yield* service.request({
          ...request,
          idempotencyKey: "missing-model",
          envelope: {
            ...envelope,
            receiverPolicy: { ...policy, modelSelection: null },
          },
        });
        expect(record.status).toBe("held");
      }).pipe(Effect.provide(Admission.layerWithEvaluator(() => Effect.die("must not run"))));
    }).pipe(Effect.provide(memory)),
);

it.effect("evaluation times out into durable held state", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const layer = Admission.layerWithEvaluator(
      () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          return yield* Effect.never;
        }),
      { timeoutMs: 1000 },
    );
    yield* Effect.gen(function* () {
      const service = yield* Admission.RemoteAdmission;
      const fiber = yield* service.request(request).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* TestClock.adjust("2 seconds");
      const record = yield* Fiber.join(fiber);
      expect(record.status).toBe("held");
      expect((yield* service.get(record.id))?.status).toBe("held");
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(memory)),
);

it.effect("disabled receivers pass through without evaluator execution", () =>
  Effect.gen(function* () {
    const service = yield* Admission.RemoteAdmission;
    const disabled = { ...policy, enabled: false, modelSelection: null };
    const record = yield* service.request({
      ...request,
      envelope: { ...envelope, receiverPolicy: disabled },
    });
    expect(record.status).toBe("passed");
    expect(record.reason).toBeNull();
    expect(
      (yield* service.claimDispatch({
        ...bind(record),
        commandId: "normal-command",
        currentPolicy: disabled,
      })).claimed,
    ).toBe(true);
  }).pipe(Effect.provide(testLayer(() => Effect.die("disabled evaluator must not run")))),
);

it.effect(
  "dispatch is claimed once for bound payload and current policy before normal command execution",
  () =>
    Effect.gen(function* () {
      const service = yield* Admission.RemoteAdmission;
      const record = yield* service.request(request);
      for (const currentPolicy of [
        { ...policy, revision: "updated" },
        { ...policy, modelSelection: { ...policy.modelSelection!, model: "different-model" } },
        { ...policy, enabled: false },
      ]) {
        const result = yield* service
          .claimDispatch({ ...bind(record), commandId: "normal-command", currentPolicy })
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure._tag).toBe("RemoteAdmissionPolicyChanged");
      }
      const input = { ...bind(record), commandId: "normal-command", currentPolicy: policy };
      const claims = yield* Effect.all(
        [service.claimDispatch(input), service.claimDispatch(input)],
        { concurrency: 2 },
      );
      expect(claims.filter((claim) => claim.claimed)).toHaveLength(1);
      expect(claims.every((claim) => claim.record.status === "dispatching")).toBe(true);
      expect(claims[0]!.record.envelope).toEqual(envelope);
      expect(
        (yield* service
          .claimDispatch({ ...input, commandId: "replacement-command" })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* service.withdraw({ ...bind(record), clientId: "client" }).pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect((yield* service.markDispatched(input)).status).toBe("dispatched");
      expect((yield* service.claimDispatch(input)).claimed).toBe(false);
      expect((yield* service.markDispatched(input)).status).toBe("dispatched");
    }).pipe(Effect.provide(testLayer())),
);

it.effect("denial and authenticated withdrawal are durable and never dispatchable", () =>
  Effect.gen(function* () {
    const service = yield* Admission.RemoteAdmission;
    for (const decision of ["deny", "withdraw"] as const) {
      const record = yield* service.request({ ...request, idempotencyKey: decision });
      expect(
        (yield* service
          .claimDispatch({ ...bind(record), commandId: decision, currentPolicy: policy })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      if (decision === "deny") {
        expect(
          (yield* service.resolve({ ...bind(record), decision, principal: owner })).status,
        ).toBe("denied");
      } else {
        expect(
          (yield* service
            .withdraw({ ...bind(record), clientId: "foreign-client" })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect((yield* service.withdraw({ ...bind(record), clientId: "client" })).status).toBe(
          "cancelled",
        );
      }
      expect(
        (yield* service
          .claimDispatch({ ...bind(record), commandId: decision, currentPolicy: policy })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* service
          .resolve({ ...bind(record), decision: "approve", principal: owner })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
    }
  }).pipe(Effect.provide(testLayer(() => Effect.succeed(hold)))),
);

it.effect("withdrawal during evaluation wins over a late pass", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const service = yield* Admission.RemoteAdmission;
      const fiber = yield* service.request(request).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const record = yield* service.request(request);
      yield* service.withdraw({ ...bind(record), clientId: "client" });
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(fiber)).status).toBe("cancelled");
    }).pipe(
      Effect.provide(
        Admission.layerWithEvaluator(() =>
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            return pass;
          }),
        ),
      ),
    );
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "restart holds interrupted evaluations, preserves holds and never reclaims unknown dispatch",
  () =>
    Effect.gen(function* () {
      const ids = yield* Effect.gen(function* () {
        const service = yield* Admission.RemoteAdmission;
        const held = yield* service.request({ ...request, idempotencyKey: "held" });
        const dispatched = yield* service.request({ ...request, idempotencyKey: "dispatch" });
        yield* service.resolve({ ...bind(dispatched), decision: "approve", principal: owner });
        yield* service.claimDispatch({
          ...bind(dispatched),
          commandId: "normal-command",
          currentPolicy: policy,
        });
        const evaluating = yield* service.request({ ...request, idempotencyKey: "interrupted" });
        const sql = yield* SqlClient.SqlClient;
        // Simulate a process ending after intent commit and before evaluator completion.
        yield* sql`UPDATE jones_remote_admissions SET status = 'evaluating', reason = NULL, category = NULL WHERE id = ${evaluating.id}`;
        return { held: held.id, dispatched, evaluating: evaluating.id };
      }).pipe(Effect.provide(Admission.layerWithEvaluator(() => Effect.succeed(hold))));
      yield* Effect.gen(function* () {
        const service = yield* Admission.RemoteAdmission;
        expect((yield* service.get(ids.held))?.status).toBe("held");
        expect((yield* service.get(ids.evaluating))?.category).toBe("evaluation_interrupted");
        expect((yield* service.get(ids.dispatched.id))?.status).toBe("dispatching");
        expect(
          (yield* service.claimDispatch({
            ...bind(ids.dispatched),
            commandId: "normal-command",
            currentPolicy: policy,
          })).claimed,
        ).toBe(false);
        expect((yield* service.request({ ...request, idempotencyKey: "interrupted" })).status).toBe(
          "held",
        );
      }).pipe(
        Effect.provide(
          Admission.layerWithEvaluator(() => Effect.die("restart must not re-evaluate")),
        ),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect("migration has no upstream references and stored bindings cannot be changed", () =>
  Effect.gen(function* () {
    const service = yield* Admission.RemoteAdmission;
    const sql = yield* SqlClient.SqlClient;
    const record = yield* service.request(request);
    expect(yield* sql`PRAGMA foreign_key_list(jones_remote_admissions)`).toEqual([]);
    expect(
      (yield* sql`UPDATE jones_remote_admissions SET envelope_json = '{}' WHERE id = ${record.id}`.pipe(
        Effect.result,
      ))._tag,
    ).toBe("Failure");
    expect((yield* service.get(record.id))?.envelope).toEqual(envelope);
    yield* sql`DROP TABLE jones_remote_admissions`;
    const result = yield* service
      .request({ ...request, idempotencyKey: "no-database" })
      .pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure")
      expect(result.failure._tag).toBe("RemoteAdmissionPersistenceError");
  }).pipe(Effect.provide(testLayer())),
);
