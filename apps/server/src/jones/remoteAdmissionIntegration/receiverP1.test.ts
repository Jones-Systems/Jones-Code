import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import { classifyLaunchEvidence, recoveryIdentities } from "./LaunchRecovery.ts";
import { recordedLaunchResult, terminalLaunchResult } from "./recordedResult.ts";
import { awaitDispatched } from "./receiver.testkit.ts";
import {
  p1Owner,
  p1Scope,
  p1Services,
  receiverP1Layer,
  writeP1Creation,
} from "./receiverP1.testkit.ts";
const scratch = {
  clientRequestId: "scratch-launch",
  scratch: true,
  message: "Synthetic scratch work",
};
const ids = recoveryIdentities("remote", scratch.clientRequestId);
const target = () => Effect.succeed({ receiverResolvedTarget: "synthetic" });

it.effect.each([false, true])(
  "scratch creation binds exact receiver project and enforces managed sends with policy off (revoked before creation: %s)",
  (revokeBeforeCreation) =>
    Effect.gen(function* () {
      const { sql, intake, admissions, evidence, protocol } = yield* p1Services;
      const started = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let launches = 0,
        sends = 0;
      intake.register(
        "t3_thread_launch",
        () =>
          Effect.gen(function* () {
            launches++;
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            yield* writeP1Creation(sql, ids.hashed, "receiver-scratch-project");
            return { threadId: ids.hashed };
          }),
        target,
      );
      intake.register(
        "t3_thread_send",
        () =>
          Effect.sync(() => {
            sends++;
            return { accepted: true };
          }),
        target,
      );
      yield* intake.invoke("t3_thread_launch", scratch, p1Scope).pipe(Effect.flip);
      const record = (yield* intake.snapshot()).pending[0]!;
      expect(record.envelope.target.projectId).toBeNull();
      yield* intake.decide(
        { ...record, decision: "approve", grant: { durationSeconds: 86400 } },
        p1Owner,
      );
      yield* Deferred.await(started);
      const grant = (yield* sql<{
        grant_id: string;
        generation: number;
      }>`SELECT grant_id,generation FROM jones_remote_admission_grants WHERE admission_id=${record.id}`)[0]!;
      if (revokeBeforeCreation)
        yield* admissions.access.mutateGrant({
          grantId: grant.grant_id,
          expectedGeneration: grant.generation,
          action: "revoke",
          now: DateTime.formatIso(yield* DateTime.now),
        });
      yield* Deferred.succeed(release, undefined);
      const dispatched = yield* awaitDispatched(record.id);
      expect(launches).toBe(1);
      expect(classifyLaunchEvidence(yield* evidence.read(dispatched)).result).toMatchObject({
        outcome: "created",
        threadId: ids.hashed,
        stages: { availability: "available" },
      });
      expect(yield* admissions.access.getGrant(grant.grant_id)).toMatchObject({
        state: revokeBeforeCreation ? "revoked" : "active",
        destination_thread_id: ids.hashed,
      });
      expect(
        (yield* sql`SELECT * FROM jones_remote_managed_scopes WHERE destination_thread_id=${ids.hashed}`)
          .length,
      ).toBe(1);
      yield* sql`UPDATE jones_remote_admission_policy SET policy_json=json_set(policy_json,'$.enabled',json('false'))`;
      const sent = yield* intake
        .invoke(
          "t3_thread_send",
          {
            clientRequestId: "later-send",
            threadId: ids.hashed,
            message: "Later synthetic message",
          },
          p1Scope,
        )
        .pipe(Effect.result);
      expect(sent._tag).toBe(revokeBeforeCreation ? "Failure" : "Success");
      expect(sends).toBe(revokeBeforeCreation ? 0 : 1);
      if (revokeBeforeCreation)
        expect((yield* intake.snapshot()).pending[0]).toMatchObject({ status: "held" });
      // Policy-off observation also validates project against the exact immutable creation event.
      expect(
        (yield* protocol.observe(p1Scope, { clientRequestId: scratch.clientRequestId })).result
          ?.outcome,
      ).toBe("created");
    }).pipe(Effect.provide(receiverP1Layer)),
);

it.effect(
  "scratch recovery rejects missing, unrelated or conflicting creation project evidence",
  () =>
    Effect.gen(function* () {
      const { sql, intake, admissions, evidence, protocol } = yield* p1Services;
      intake.register("t3_thread_launch", () => Effect.die("held launch must not run"), target);
      yield* intake.invoke("t3_thread_launch", scratch, p1Scope).pipe(Effect.flip);
      const record = (yield* intake.snapshot()).pending[0]!;
      yield* writeP1Creation(sql, ids.hashed, "receiver-scratch-project");
      const classify = () => evidence.read(record).pipe(Effect.map(classifyLaunchEvidence));
      expect((yield* classify()).result.outcome).toBe("created");
      yield* sql`UPDATE orchestration_command_receipts SET result_sequence=0`;
      expect((yield* classify()).result.outcome).toBe("unknown");
      yield* sql`UPDATE orchestration_command_receipts SET result_sequence=(SELECT sequence FROM orchestration_events WHERE command_id=${ids.hashed})`;
      const original = (yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_events`)[0]!.payload_json;
      yield* sql`UPDATE orchestration_events SET payload_json=json_set(payload_json,'$.projectId','other-project')`;
      expect((yield* classify()).evidence.contradictions).toContain("project_mismatch");
      yield* sql`UPDATE orchestration_events SET payload_json=${original},command_id='unrelated-command'`;
      expect((yield* classify()).result.outcome).toBe("unknown");
      yield* sql`UPDATE orchestration_events SET command_id=${ids.hashed},payload_json=json_set(payload_json,'$.id','unrelated-thread')`;
      expect((yield* classify()).result.outcome).toBe("unknown");
      yield* sql`DELETE FROM orchestration_events`;
      expect((yield* classify()).result.outcome).toBe("unknown");
      expect(yield* sql`SELECT * FROM jones_remote_managed_scopes`).toEqual([]);
      // No-admission policy-off observation cannot self-validate the shell's asserted project either.
      yield* sql`DELETE FROM jones_remote_admissions`;
      expect(
        (yield* protocol.observe(p1Scope, { clientRequestId: scratch.clientRequestId })).result
          ?.outcome,
      ).toBe("unknown");
      expect(yield* admissions.get(record.id)).toBeNull();
    }).pipe(Effect.provide(receiverP1Layer)),
);

it.effect(
  "restart closes an abandoned dispatch as observable unknown, never re-enters the handler, and preserves the result on read/ack",
  () =>
    Effect.gen(function* () {
      const { sql, intake, protocol, review } = yield* p1Services;
      let calls = 0;
      intake.register(
        "t3_thread_launch",
        () =>
          Effect.sync(() => {
            calls++;
            return {};
          }),
        target,
      );
      yield* intake.invoke("t3_thread_launch", scratch, p1Scope).pipe(Effect.flip);
      const record = (yield* intake.snapshot()).pending[0]!;
      yield* sql`UPDATE jones_remote_admissions SET status='dispatching',command_id=${`admission:${record.id}`} WHERE id=${record.id}`;
      expect(
        (yield* protocol.observe(p1Scope, { clientRequestId: scratch.clientRequestId })).result,
      ).toBeNull();
      // This service did not claim the row: it models a new receiver process recovering durable state.
      yield* intake.resumeApproved();
      yield* awaitDispatched(record.id);
      expect(calls).toBe(0);
      const stored = yield* sql`SELECT * FROM jones_remote_admission_results`;
      const observed = yield* protocol.observe(p1Scope, {
        clientRequestId: scratch.clientRequestId,
      });
      expect(observed.result).toMatchObject({
        outcome: "unknown",
        threadId: null,
        stages: { creation: "unknown", availability: "unknown", initialMessage: "unconfirmed" },
      });
      expect((yield* review.item(record.id, p1Owner)).extension.result).toEqual(observed.result);
      expect((yield* protocol.status(p1Scope, {})).after).not.toBe("peer:remote:0");
      yield* protocol.ack(p1Scope, {
        clientRequestId: scratch.clientRequestId,
        disposition: "delivered",
      });
      yield* protocol.ack(p1Scope, {
        clientRequestId: scratch.clientRequestId,
        disposition: "delivered",
      });
      expect((yield* sql`SELECT * FROM jones_remote_admission_result_acks`).length).toBe(1);
      expect(yield* sql`SELECT * FROM jones_remote_admission_results`).toEqual(stored);
    }).pipe(Effect.provide(receiverP1Layer)),
);

it.effect.each(["deny", "withdraw"] as const)(
  "a held launch's %s projects a definite pre-dispatch refusal to both readers without inventing a receipt",
  (action) =>
    Effect.gen(function* () {
      const { sql, intake, protocol, review } = yield* p1Services;
      intake.register(
        "t3_thread_launch",
        () => Effect.die("refused launch must never dispatch"),
        target,
      );
      yield* intake.invoke("t3_thread_launch", scratch, p1Scope).pipe(Effect.flip);
      const record = (yield* intake.snapshot()).pending[0]!;
      const before = (yield* protocol.status(p1Scope, {})).after;
      if (action === "deny") yield* intake.decide({ ...record, decision: "deny" }, p1Owner);
      else yield* intake.withdraw(record, "remote");
      const terminal = (yield* protocol.status(p1Scope, {})).after;
      expect(terminal).not.toBe(before);
      if (action === "withdraw") {
        yield* intake.withdraw(record, "remote");
        expect((yield* protocol.status(p1Scope, {})).after).toBe(terminal);
      }
      const observed = yield* protocol.observe(p1Scope, {
        clientRequestId: scratch.clientRequestId,
      });
      expect(observed.result).toMatchObject({
        outcome: "refused",
        threadId: null,
        stages: { creation: "refused", providerProgress: "not_started" },
      });
      expect((yield* review.item(record.id, p1Owner)).extension.result).toEqual(observed.result);
      expect(yield* sql`SELECT * FROM jones_remote_admission_results`).toEqual([]);
      expect(yield* sql`SELECT * FROM orchestration_command_receipts`).toEqual([]);
    }).pipe(Effect.provide(receiverP1Layer)),
);

it.effect(
  "legacy generic terminal failure is unknown, live holds stay pending, and exact stored evidence wins unchanged",
  () =>
    Effect.gen(function* () {
      const { sql, intake, protocol, review } = yield* p1Services;
      intake.register("t3_thread_launch", () => Effect.die("not dispatched here"), target);
      yield* intake.invoke("t3_thread_launch", scratch, p1Scope).pipe(Effect.flip);
      const record = (yield* intake.snapshot()).pending[0]!;
      expect(yield* recordedLaunchResult(sql, record.id)).toBeNull();
      yield* sql`UPDATE jones_remote_admissions SET status='dispatched',command_id=${`admission:${record.id}`} WHERE id=${record.id}`;
      const legacy = JSON.stringify({
        ok: false,
        value: { code: "orchestration_error", message: "Interrupted" },
      });
      yield* sql`INSERT INTO jones_remote_admission_results VALUES(${record.id},${legacy})`;
      const unknown = yield* recordedLaunchResult(sql, record.id);
      expect(unknown?.outcome).toBe("unknown");
      expect((yield* review.item(record.id, p1Owner)).extension.result).toEqual(unknown);
      expect(
        (yield* protocol.observe(p1Scope, { clientRequestId: scratch.clientRequestId })).result,
      ).toEqual(unknown);
      expect(
        (yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM jones_remote_admission_results`)[0]?.result_json,
      ).toBe(legacy);
      // Fixture simulates a pre-existing positive immutable result, never a production rewrite.
      const positive = {
        ...unknown!,
        outcome: "created" as const,
        threadId: ids.hashed,
        stages: { ...unknown!.stages, creation: "confirmed" as const },
        reasonCode: "creation_receipt",
      };
      yield* sql`DELETE FROM jones_remote_admission_results WHERE admission_id=${record.id}`;
      const exact = JSON.stringify({ ok: true, value: {}, launchResult: positive });
      yield* sql`INSERT INTO jones_remote_admission_results VALUES(${record.id},${exact})`;
      expect(yield* recordedLaunchResult(sql, record.id)).toEqual(positive);
      expect(
        (yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM jones_remote_admission_results`)[0]?.result_json,
      ).toBe(exact);
    }).pipe(Effect.provide(receiverP1Layer)),
);

it("a denial carrying a dispatch claim cannot be projected as no-effect refusal", () => {
  expect(
    terminalLaunchResult({
      status: "denied",
      commandId: "admission:claimed",
      recordedAt: "2026-11-16T00:00:00.000Z",
      initialMessageRequested: true,
    }),
  ).toMatchObject({
    outcome: "unknown",
    threadId: null,
    stages: { creation: "unknown", initialMessage: "unconfirmed", providerProgress: "unknown" },
  });
});
