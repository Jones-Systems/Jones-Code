import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { RemoteAdmissionReviewItem } from "@t3tools/contracts/jones/remoteAdmissionReview";
import admissionMigration from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import reviewMigration from "../persistence/Migrations/157_JonesRemoteAdmissionReview.ts";
import * as Admission from "./RemoteAdmission.ts";

const memory = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* admissionMigration;
    yield* reviewMigration;
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const layer = Admission.layerWithEvaluator(() => Effect.die("policy disabled")).pipe(
  Layer.provideMerge(memory),
);

it.effect(
  "450 held requests page completely at 50/100 with exact counts, off-page lookup and revision boundaries",
  () =>
    Effect.gen(function* () {
      const service = yield* Admission.RemoteAdmission;
      const sql = yield* SqlClient.SqlClient;
      const seed = yield* service.request({
        idempotencyKey: "seed",
        envelope: {
          identity: { clientId: "client", linkId: null },
          operation: "t3_thread_send",
          payload: { text: "synthetic" },
          target: {
            projectId: null,
            threadId: "target",
            runtimeMode: "full-access",
            interactionMode: "default",
          },
          receiverPolicy: { enabled: false, revision: "p", modelSelection: null },
        },
      });
      yield* sql`UPDATE jones_remote_admissions SET status='held' WHERE id=${seed.id}`;
      for (let i = 1; i < 450; i++) {
        yield* sql`INSERT INTO jones_remote_admissions(id,client_id,idempotency_key,request_digest,binding_digest,envelope_json,status,created_at,updated_at)
      SELECT ${`r-${String(i).padStart(3, "0")}`},client_id,${`key-${i}`},request_digest,binding_digest,envelope_json,'held',created_at,updated_at
      FROM jones_remote_admissions WHERE id=${seed.id}`;
      }
      const protocol = yield* service.request({
        idempotencyKey: "protocol",
        envelope: { ...seed.envelope, operation: "t3_remote_admission_capabilities_register" },
      });
      yield* sql`UPDATE jones_remote_admissions SET status='held' WHERE id=${protocol.id}`;
      for (let i = 0; i < 210; i++) {
        yield* sql`INSERT INTO jones_remote_admissions(id,client_id,idempotency_key,request_digest,binding_digest,envelope_json,status,created_at,updated_at)
        SELECT ${`r-${String(i).padStart(3, "0")}-protocol`},client_id,${`protocol-${i}`},request_digest,binding_digest,envelope_json,'held',
          (SELECT created_at FROM jones_remote_admissions WHERE id=${seed.id}),updated_at
        FROM jones_remote_admissions WHERE id=${protocol.id}`;
      }
      expect(yield* service.listPending()).toHaveLength(200);
      expect((yield* service.listPending()).map((record) => record.envelope.operation)).toEqual(
        Array.from({ length: 200 }, () => "t3_thread_send"),
      );
      expect((yield* service.get(protocol.id))?.envelope.operation).toBe(
        "t3_remote_admission_capabilities_register",
      );
      const projectItem = (id: string) =>
        Effect.gen(function* () {
          const record = yield* service.get(id).pipe(Effect.orDie);
          if (!record) return yield* Effect.die("missing seeded record");
          return {
            record,
            extension: {
              grantable: false,
              grantUnavailableReason: "synthetic",
              grant: null,
              evaluator: null,
              blocked: null,
              accessBasis: null,
              attemptId: null,
              result: null,
              recoveredResult: null,
              originNotifiedAt: null,
              retryEligible: false,
              recheckEligible: false,
            },
          } satisfies RemoteAdmissionReviewItem;
        });
      const input = {
        view: "current" as const,
        authorizationScope: "owner",
        now: "2026-11-16T00:00:00.000Z",
        validLeaseIds: () => Effect.succeed([] as string[]),
        liveDestinations: () => Effect.succeed([] as string[]),
        projectItem,
      };
      for (const limit of [50, 100]) {
        const seen = new Set<string>();
        let cursor: string | undefined;
        let pages = 0;
        do {
          const page = yield* service.review.page({
            ...input,
            limit,
            ...(cursor ? { cursor } : {}),
          });
          expect(page.counts.currentHeldAdmissions).toBe(450);
          expect(page.counts.heldAdmissions).toBe(450);
          for (const item of page.items) {
            expect(seen.has(item.record.id)).toBe(false);
            seen.add(item.record.id);
          }
          cursor = page.nextCursor ?? undefined;
          pages++;
        } while (cursor);
        expect(seen.size).toBe(450);
        expect(pages).toBe(limit === 50 ? 9 : 5);
      }
      expect((yield* service.get("r-001"))?.status).toBe("held");
      yield* sql`UPDATE jones_remote_admissions SET status='cancelled' WHERE id IN (${protocol.id}, 'r-001')`;
      const done = yield* service.review.page({ ...input, view: "done" });
      expect(done.retainedRecordCount).toBe(1);
      expect(done.items.map((item) => item.record.id)).toEqual(["r-001"]);
      yield* sql`UPDATE jones_remote_admissions SET status='dispatched',command_id='failed-command' WHERE id='r-001'`;
      yield* sql`INSERT INTO jones_remote_admission_extensions(admission_id,outcome) VALUES('r-001','failure')`;
      const page = yield* service.review.page(input);
      expect(page.counts.currentHeldAdmissions).toBe(449);
      expect(page.counts.failedAdmissions).toBe(1);
      expect(page.retainedRecordCount).toBe(450);
      expect(page.nextCursor).not.toBeNull();
      expect(
        (yield* service.review
          .page({ ...input, authorizationScope: "other", cursor: page.nextCursor! })
          .pipe(Effect.flip))._tag,
      ).toBe("ReviewHistoryUnavailable");
      yield* sql.withTransaction(service.access.bumpInTransaction("admission", seed.id, input.now));
      expect(
        (yield* service.review.page({ ...input, cursor: page.nextCursor! }).pipe(Effect.flip))._tag,
      ).toBe("ReviewHistoryUnavailable");
      yield* sql`UPDATE jones_remote_admissions SET status='denied' WHERE status='held'`;
      const failedOnly = yield* service.review.page(input);
      expect(failedOnly.counts.currentHeldAdmissions).toBe(0);
      expect(failedOnly.items.map((item) => item.record.id)).toEqual(["r-001"]);
    }).pipe(Effect.provide(layer)),
);
