import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import core from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import integration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import review from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import { makeReceiverAccess } from "../remoteAdmission/ReceiverAccess.ts";
import { makeRecoveryStore, type ReceiverRecoveryEvidence } from "./RecoveryStore.ts";
import { recoveryIdentities, type LaunchEvidenceInput } from "./LaunchRecovery.ts";
const now = "2026-11-16T00:00:00.000Z";
const record: RemoteAdmissionRecord = {
  id: "predecessor",
  idempotencyKey: "old-key",
  requestDigest: "a".repeat(64),
  bindingDigest: "b".repeat(64),
  status: "dispatched",
  commandId: "admission:predecessor",
  reason: null,
  category: null,
  resolvedBy: null,
  resolvedAt: null,
  createdAt: now,
  updatedAt: now,
  envelope: {
    identity: { clientId: "session", linkId: "session" },
    operation: "t3_thread_launch",
    payload: {
      params: { clientRequestId: "original-key", projectId: "project" },
      invocation: { environmentId: "receiver", requestNamespace: "client:session" },
    },
    target: {
      projectId: "project",
      threadId: null,
      runtimeMode: "auto",
      interactionMode: "default",
    },
    receiverPolicy: { enabled: false, revision: "policy", modelSelection: null },
  },
};
const ids = recoveryIdentities("session", "original-key");
const proof = (): LaunchEvidenceInput => ({
  keyed: true,
  abandonedClaim: true,
  readsComplete: true,
  liveClaim: false,
  expectedProjectId: "project",
  initialMessageRequested: false,
  observedAt: now,
  hashed: {
    commandId: ids.hashed,
    creation: null,
    creationProjectId: null,
    message: null,
    shell: null,
    claimMatches: true,
  },
  legacy: {
    commandId: ids.legacy,
    creation: null,
    creationProjectId: null,
    message: null,
    shell: null,
    claimMatches: true,
  },
});
const memory = Layer.effectDiscard(
  core.pipe(Effect.andThen(integration), Effect.andThen(review)),
).pipe(Layer.provideMerge(Sqlite.layer({ filename: ":memory:" })));
const setup = (read: ReceiverRecoveryEvidence["Service"]["read"]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO jones_remote_admissions(id,client_id,idempotency_key,request_digest,binding_digest,envelope_json,status,command_id,created_at,updated_at)
    VALUES(${record.id},'session',${record.idempotencyKey},${record.requestDigest},${record.bindingDigest},${JSON.stringify(record.envelope)},'dispatched',${record.commandId},${now},${now})`;
    yield* sql`INSERT INTO jones_remote_admission_results VALUES(${record.id},'{"ok":false,"value":{"code":"orchestration_error","message":"unknown"}}')`;
    yield* sql`INSERT INTO jones_remote_admission_policy VALUES(1,${JSON.stringify(record.envelope.receiverPolicy)})`;
    yield* sql`INSERT INTO jones_remote_peer_capabilities VALUES('session','generation','{"retryObservation":"jones.remote-admission-retry-observation/v1","recoveredResultObservation":"jones.remote-admission-recovered-result/v1"}',${now})`;
    const store = makeRecoveryStore(
      sql,
      {
        access: makeReceiverAccess(sql),
        get: (id) => Effect.succeed(id === record.id ? record : null),
      },
      {
        read,
        authorize: () => Effect.succeed(true),
        root: () =>
          Effect.succeed({
            rootObservationKey: "root",
            returnRouteKey: "root",
            originThreadId: "origin",
          }),
      },
    );
    return { sql, store };
  });
it.effect(
  "retry preparation atomically fences predecessor and remains unable to claim before registration",
  () =>
    Effect.gen(function* () {
      const { sql, store } = yield* setup(() => Effect.succeed(proof()));
      const input = {
        id: record.id,
        bindingDigest: record.bindingDigest,
        commandKey: "retry-command",
      };
      const attempt = yield* store.retry(input);
      const replay = yield* store.retry(input);
      expect(replay).toEqual(attempt);
      expect(attempt.state).toBe("prepared");
      expect((yield* sql`SELECT * FROM jones_remote_admission_fences`).length).toBe(1);
      expect((yield* sql`SELECT * FROM jones_remote_admission_grants`).length).toBe(0);
      const child = {
        ...record,
        id: attempt.childAdmissionId,
        status: "approved" as const,
        commandId: null,
      };
      expect(yield* sql.withTransaction(store.guardChildInTransaction(child))).toBe(false);
      expect(
        (yield* store.retry({ ...input, commandKey: "different" }).pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(memory)),
);
it.effect(
  "positive evidence arriving after preparation closes the registered child without launch",
  () =>
    Effect.gen(function* () {
      let positive = false;
      const { sql, store } = yield* setup(() =>
        Effect.succeed(
          positive
            ? {
                ...proof(),
                hashed: {
                  ...proof().hashed,
                  creationProjectId: "project",
                  creation: {
                    commandId: ids.hashed,
                    threadId: ids.hashed,
                    commandType: "thread.create",
                    status: "accepted",
                  },
                },
              }
            : proof(),
        ),
      );
      const attempt = yield* store.retry({
        id: record.id,
        bindingDigest: record.bindingDigest,
        commandKey: "retry",
      });
      yield* sql`UPDATE jones_remote_admission_attempts SET state='origin_registered',observation_key='registered' WHERE attempt_id=${attempt.attemptId}`;
      positive = true;
      expect(
        yield* sql.withTransaction(
          store.guardChildInTransaction({
            ...record,
            id: attempt.childAdmissionId,
            status: "approved",
            commandId: null,
          }),
        ),
      ).toBe(false);
      expect(
        (yield* sql<{ state: string }>`SELECT state FROM jones_remote_admission_attempts`)[0]
          ?.state,
      ).toBe("refused");
    }).pipe(Effect.provide(memory)),
);
it.effect(
  "recheck crash rolls back evidence, notice and peer revision; replay appends once and preserves original result",
  () =>
    Effect.gen(function* () {
      const { sql, store } = yield* setup(() =>
        Effect.succeed({
          ...proof(),
          hashed: {
            ...proof().hashed,
            creationProjectId: "project",
            creation: {
              commandId: ids.hashed,
              threadId: ids.hashed,
              commandType: "thread.create",
              status: "accepted",
            },
          },
        }),
      );
      const before = yield* sql`SELECT * FROM jones_remote_admission_results`;
      const input = { id: record.id, bindingDigest: record.bindingDigest };
      yield* sql
        .withTransaction(store.recheck(input).pipe(Effect.andThen(Effect.fail("synthetic_crash"))))
        .pipe(Effect.result);
      expect(yield* sql`SELECT * FROM jones_remote_admission_recovered_notices`).toEqual([]);
      expect(yield* sql`SELECT * FROM jones_remote_admission_peer_revisions`).toEqual([]);
      const first = yield* store.recheck(input),
        second = yield* store.recheck(input);
      expect(second.revision).toBe(first.revision);
      expect((yield* sql`SELECT * FROM jones_remote_admission_recovered_notices`).length).toBe(1);
      expect(yield* sql`SELECT * FROM jones_remote_admission_results`).toEqual(before);
    }).pipe(Effect.provide(memory)),
);
