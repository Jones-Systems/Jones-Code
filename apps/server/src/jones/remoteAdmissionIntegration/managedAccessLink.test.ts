import * as Schema from "effect/Schema";
import { RemoteAdmissionAccessBinding } from "@t3tools/contracts/jones/remoteAdmissionReview";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import { managedAccessGrantId } from "./managedAccessLink.ts";
import { awaitDispatched } from "./receiver.testkit.ts";
import { p1Owner, p1Scope, p1Services, receiverP1Layer } from "./receiverP1.testkit.ts";
const target = () => Effect.succeed({ destination: "destination" });
const input = {
  clientRequestId: "grant-source",
  threadId: "destination",
  message: "Synthetic message",
};
const setup = Effect.gen(function* () {
  const services = yield* p1Services;
  const { intake, sql } = services;
  let calls = 0;
  intake.register(
    "t3_thread_send",
    () =>
      Effect.sync(() => {
        calls++;
        return {};
      }),
    target,
  );
  yield* intake.invoke("t3_thread_send", input, p1Scope).pipe(Effect.asVoid, Effect.flip);
  const source = (yield* intake.snapshot()).pending[0]!;
  yield* intake.decide(
    { ...source, decision: "approve", grant: { durationSeconds: 86400 } },
    p1Owner,
  );
  yield* awaitDispatched(source.id);
  const grant = (yield* sql<{
    grant_id: string;
    managed_scope_id: string;
  }>`SELECT grant_id,managed_scope_id FROM jones_remote_admission_grants WHERE admission_id=${source.id}`)[0]!;
  return { ...services, source, grant, calls: () => calls };
});
it.effect.each(["revoked", "expired"] as const)(
  "a newly %s-blocked message links the exact current grant head; Restore leaves it held",
  (state) =>
    Effect.gen(function* () {
      const { sql, intake, admissions, review, grant, calls } = yield* setup;
      const now = DateTime.formatIso(yield* DateTime.now);
      if (state === "revoked")
        yield* admissions.access.mutateGrant({
          grantId: grant.grant_id,
          expectedGeneration: 1,
          action: "revoke",
          now,
        });
      else
        yield* sql`UPDATE jones_remote_admission_grants SET expires_at=${DateTime.formatIso(DateTime.subtract(DateTime.makeUnsafe(now), { seconds: 1 }))} WHERE grant_id=${grant.grant_id}`;
      // A later unrelated grant with the same destination must never win scope selection.
      const original = (yield* admissions.access.getGrant(grant.grant_id))!;
      const otherBinding = {
        ...(yield* Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionAccessBinding))(original.binding_json)),
        originThreadId: "unrelated-origin",
      };
      yield* sql.withTransaction(
        admissions.access.createGrantInTransaction({
          admissionId: "unrelated-source",
          binding: otherBinding,
          durationSeconds: 86400,
          now,
        }),
      );
      yield* intake
        .invoke("t3_thread_send", { ...input, clientRequestId: "blocked-message" }, p1Scope)
        .pipe(Effect.asVoid, Effect.flip);
      const blocked = (yield* intake.snapshot()).pending[0]!;
      expect(blocked.status).toBe("held");
      const item = yield* review.item(blocked.id, p1Owner);
      expect(item.extension.grant).toBeNull();
      expect(item.extension.managedAccessGrantId).toBe(grant.grant_id);
      expect(item.extension.blocked?.reason).toBe(
        state === "revoked" ? "access_revoked" : "access_expired",
      );
      const pane = yield* review.grants({ grantId: item.extension.managedAccessGrantId! }, p1Owner);
      expect(pane.grants[0]?.binding.originThreadId).toBe("origin");
      if (state === "expired")
        yield* admissions.access.mutateGrant({
          grantId: grant.grant_id,
          expectedGeneration: 1,
          action: "revoke",
          now,
        });
      const restored = yield* review.restore(
        {
          grantId: grant.grant_id,
          expectedGeneration: 2,
          durationSeconds: 86400,
          commandKey: "restore-original",
        },
        p1Owner,
      );
      expect(restored.successor.binding.destinationThreadId).toBe("destination");
      expect((yield* review.item(blocked.id, p1Owner)).extension.managedAccessGrantId).toBe(
        restored.successor.grantId,
      );
      expect((yield* admissions.get(blocked.id))?.status).toBe("held");
      yield* review.mutateGrant(
        { grantId: restored.successor.grantId, expectedGeneration: 1 },
        "revoke",
        p1Owner,
      );
      const next = yield* review.restore(
        {
          grantId: restored.successor.grantId,
          expectedGeneration: 2,
          durationSeconds: 86400,
          commandKey: "restore-successor",
        },
        p1Owner,
      );
      expect((yield* review.item(blocked.id, p1Owner)).extension.managedAccessGrantId).toBe(
        next.successor.grantId,
      );
      // Same-admission rehold survives restoration and disabling the policy.
      yield* sql`UPDATE jones_remote_admission_policy SET policy_json=json_set(policy_json,'$.enabled',json('false'))`;
      yield* intake
        .invoke("t3_thread_send", { ...input, clientRequestId: "blocked-message" }, p1Scope)
        .pipe(Effect.asVoid, Effect.flip);
      expect((yield* admissions.get(blocked.id))?.status).toBe("held");
      expect(calls()).toBe(1);
      expect(
        (yield* sql`SELECT * FROM jones_remote_admission_blocked_marks WHERE admission_id=${blocked.id}`)
          .length,
      ).toBe(1);
    }).pipe(Effect.provide(receiverP1Layer)),
);
it.effect(
  "missing restore lineage never substitutes a predecessor or adopts an orphan successor",
  () =>
    Effect.gen(function* () {
      const { sql, admissions, review, source, grant } = yield* setup;
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* admissions.access.mutateGrant({
        grantId: grant.grant_id,
        expectedGeneration: 1,
        action: "revoke",
        now,
      });
      const restored = yield* review.restore(
        {
          grantId: grant.grant_id,
          expectedGeneration: 2,
          durationSeconds: 86400,
          commandKey: "restore",
        },
        p1Owner,
      );
      expect(yield* managedAccessGrantId(sql, source, "other-receiver")).toBeNull();
      // A pruned successor is unavailable; the predecessor's stale link must not be presented as current.
      yield* sql`DELETE FROM jones_remote_admission_grants WHERE grant_id=${restored.successor.grantId}`;
      expect(yield* managedAccessGrantId(sql, source, "receiver")).toBeNull();
      // A retained restore-shaped row without its predecessor is not adopted as a new root.
      const original = (yield* admissions.access.getGrant(grant.grant_id))!;
      yield* sql`INSERT INTO jones_remote_admission_grants(grant_id,admission_id,binding_json,managed_scope_id,
      destination_thread_id,session_id,state,generation,expires_at,created_at,updated_at)
      VALUES('orphan','restore:orphan',${original.binding_json},${original.managed_scope_id},
      ${original.destination_thread_id},${original.session_id},'active',1,${original.expires_at},${now},${now})`;
      yield* sql`DELETE FROM jones_remote_admission_grant_restores`;
      yield* sql`DELETE FROM jones_remote_admission_grants WHERE grant_id=${grant.grant_id}`;
      expect(yield* managedAccessGrantId(sql, source, "receiver")).toBeNull();
    }).pipe(Effect.provide(receiverP1Layer)),
);

import * as TestClock from "effect/testing/TestClock";
it.effect("the latest restored head wins even when its root predates another scope grant", () =>
  Effect.gen(function* () {
    const { sql, admissions, review, source, grant } = yield* setup;
    const original = (yield* admissions.access.getGrant(grant.grant_id))!;
    yield* TestClock.adjust("1 second");
    yield* sql.withTransaction(
      admissions.access.createGrantInTransaction({
        admissionId: "newer-independent-root",
        binding: yield* Schema.decodeEffect(Schema.fromJsonString(RemoteAdmissionAccessBinding))(original.binding_json),
        durationSeconds: 86400,
        now: DateTime.formatIso(yield* DateTime.now),
      }),
    );
    yield* TestClock.adjust("1 second");
    yield* admissions.access.mutateGrant({
      grantId: grant.grant_id,
      expectedGeneration: 1,
      action: "revoke",
      now: DateTime.formatIso(yield* DateTime.now),
    });
    const restored = yield* review.restore(
      {
        grantId: grant.grant_id,
        expectedGeneration: 2,
        durationSeconds: 86400,
        commandKey: "latest-restore",
      },
      p1Owner,
    );
    expect((yield* review.item(source.id, p1Owner)).extension.managedAccessGrantId).toBe(
      restored.successor.grantId,
    );
    expect(
      yield* managedAccessGrantId(
        sql,
        {
          ...source,
          envelope: {
            ...source.envelope,
            target: { ...source.envelope.target, threadId: "other-destination" },
          },
        },
        "receiver",
      ),
    ).toBeNull();
  }).pipe(Effect.provide(receiverP1Layer)),
);
