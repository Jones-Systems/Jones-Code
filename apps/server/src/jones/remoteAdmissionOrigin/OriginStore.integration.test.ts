import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { MessageId, ThreadId } from "@t3tools/contracts";
import type { RemoteAdmissionLaunchResult } from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/156_JonesRemoteAdmissionReview.ts";
import { admitOriginWake, intent, makeOriginStore, type ObservationRow } from "./OriginStore.ts";
import { cacheIsStale, STALE_MS } from "./OriginCoordinator.ts";

const timestamp = "2026-11-16T00:00:00.000Z";
const fixture = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* migration;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_thread_result_fences (origin_environment_id TEXT,operation_id TEXT,generation INTEGER)`;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads (thread_id TEXT,archived_at TEXT,deleted_at TEXT)`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES ('waiter',NULL,NULL)`;
    yield* sql`CREATE TABLE synthetic_message_receipts (id TEXT PRIMARY KEY)`;
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));
const row = (): ObservationRow => ({
  observation_key: "root",
  peer_environment_id: "receiver",
  link_generation: "generation-1",
  session_id: "session-1",
  client_request_id: "stable-key",
  receiver_namespace: "client:session-1",
  kind: "launch",
  root_observation_key: "root",
  predecessor_observation_key: null,
  admission_id: null,
  attempt_id: null,
  recovered_revision: null,
  return_binding_json: JSON.stringify({
    rootObservationKey: "root",
    originThreadId: "waiter",
    returnRouteKey: "route",
    waiterGeneration: 0,
    waiterRunId: null,
  }),
  intent_json: JSON.stringify({
    phase: "prepared",
    fingerprint: "digest",
    registered: true,
    acknowledged: false,
  }),
  result_json: null,
  state: "intent",
  created_at: timestamp,
  updated_at: timestamp,
  terminal_at: null,
});
const result: RemoteAdmissionLaunchResult = {
  outcome: "unknown",
  stages: {
    creation: "unknown",
    availability: "unknown",
    initialMessage: "unconfirmed",
    preparation: "unknown",
    providerProgress: "unknown",
  },
  threadId: null,
  recordedAt: timestamp,
  reasonCode: "lost_response",
};
const wake = {
  kind: "remote_admission_origin" as const,
  dependencyId: "root",
  observationKey: "root",
  generation: 0,
  waiterThreadId: ThreadId.make("waiter"),
  messageId: MessageId.make("wake"),
  linkGeneration: "generation-1",
  sessionId: "session-1",
  committedAt: timestamp,
};

it.effect(
  "intent survives reload and a lost response retains exactly the same key and receiver namespace",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = makeOriginStore(sql);
      yield* sql.withTransaction(store.prepareInTransaction(row()));
      yield* sql.withTransaction(store.pending("root", "sent_unknown", timestamp));
      const reloaded = makeOriginStore(sql);
      const old = yield* sql.withTransaction(reloaded.prepareInTransaction(row()));
      expect(old.state).toBe("pending");
      expect(intent(old).phase).toBe("sent_unknown");
      expect(old.client_request_id).toBe("stable-key");
      expect(old.receiver_namespace).toBe("client:session-1");
      expect((yield* reloaded.list()).length).toBe(1);
    }).pipe(Effect.provide(fixture)),
);

it.effect("replaced link cannot rebind retained intent or its return anchor", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOriginStore(sql);
    yield* sql.withTransaction(store.prepareInTransaction(row()));
    const rejected = yield* sql
      .withTransaction(store.prepareInTransaction({ ...row(), link_generation: "replacement" }))
      .pipe(Effect.flip());
    expect(rejected._tag).toBe("OrchestratorMcpFailure");
    expect((yield* store.get("root"))?.link_generation).toBe("generation-1");
  }).pipe(Effect.provide(fixture)),
);

it.effect("synchronous release has no wake and retains root for notices beyond thirty days", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOriginStore(sql);
    yield* sql.withTransaction(store.prepareInTransaction(row()));
    yield* store.release("root", timestamp);
    yield* store.resolve("root", result, "admission", "2027-01-20T00:00:00.000Z");
    expect((yield* store.get("root"))?.state).toBe("released");
    expect(yield* store.anchor("root")).toBeDefined();
    yield* sql.withTransaction(admitOriginWake(sql, wake)).pipe(Effect.flip());
    expect((yield* sql`SELECT * FROM synthetic_message_receipts`).length).toBe(0);
  }).pipe(Effect.provide(fixture)),
);

it.effect("wake state and receipt roll back together on crash; successful retry commits once", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOriginStore(sql);
    yield* sql.withTransaction(store.prepareInTransaction(row()));
    yield* store.resolve("root", result, "admission", timestamp);
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO synthetic_message_receipts VALUES ('wake')`;
          yield* admitOriginWake(sql, wake);
          return yield* Effect.fail("crash");
        }),
      )
      .pipe(Effect.flip());
    expect((yield* store.get("root"))?.state).toBe("resolved");
    expect((yield* sql`SELECT * FROM synthetic_message_receipts`).length).toBe(0);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO synthetic_message_receipts VALUES ('wake')`;
        yield* admitOriginWake(sql, wake);
      }),
    );
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO synthetic_message_receipts VALUES ('second')`;
          yield* admitOriginWake(sql, wake);
        }),
      )
      .pipe(Effect.flip());
    expect((yield* store.get("root"))?.state).toBe("delivered");
    expect((yield* sql`SELECT * FROM synthetic_message_receipts`).length).toBe(1);
  }).pipe(Effect.provide(fixture)),
);

it.effect("Stop records suppression and new recovered notice never changes old delivery", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOriginStore(sql);
    yield* sql.withTransaction(store.prepareInTransaction(row()));
    yield* store.release("root", timestamp);
    yield* sql`INSERT INTO jones_thread_result_fences VALUES ('local-waiter','waiter',1)`;
    const notice = {
      ...row(),
      observation_key: "notice:admission:2",
      client_request_id: "notice:admission:2",
      kind: "recovered_notice" as const,
      admission_id: "admission",
      recovered_revision: 2,
      state: "resolved" as const,
      result_json: JSON.stringify(result),
    };
    yield* sql.withTransaction(store.prepareInTransaction(notice));
    expect(
      yield* sql.withTransaction(store.suppressIfStoppedInTransaction(notice, timestamp)),
    ).toBe(true);
    expect((yield* store.get("root"))?.state).toBe("released");
    expect((yield* store.get(notice.observation_key))?.state).toBe("suppressed");
    expect((yield* store.anchor("root"))?.stopped_at).toBe(timestamp);
  }).pipe(Effect.provide(fixture)),
);

it.effect("offline pure reads neither register attempts nor acknowledge notices", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = makeOriginStore(sql);
    yield* sql.withTransaction(store.prepareInTransaction(row()));
    const before = yield* sql`SELECT * FROM jones_remote_origin_observations`;
    yield* store.list();
    yield* store.get("root");
    yield* store.anchor("root");
    expect(yield* sql`SELECT * FROM jones_remote_origin_observations`).toEqual(before);
    expect((yield* sql`SELECT * FROM jones_remote_peer_capabilities`).length).toBe(0);
  }).pipe(Effect.provide(fixture)),
);

it("marks advisory cache stale at fifteen minutes", () => {
  expect(cacheIsStale(timestamp, Date.parse(timestamp) + STALE_MS - 1)).toBe(false);
  expect(cacheIsStale(timestamp, Date.parse(timestamp) + STALE_MS)).toBe(true);
});

it.effect(
  "retry registration needs durable root and predecessor; their immutable states survive preparation",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = makeOriginStore(sql);
      const retry = {
        ...row(),
        observation_key: "retry",
        client_request_id: "attempt",
        kind: "retry" as const,
        predecessor_observation_key: "root",
        attempt_id: "attempt",
        receiver_namespace: "admission-retry:admission",
        admission_id: "child",
      };
      expect(
        (yield* sql.withTransaction(store.prepareInTransaction(retry)).pipe(Effect.flip()))._tag,
      ).toBe("OrchestratorMcpFailure");
      yield* sql.withTransaction(store.prepareInTransaction(row()));
      yield* store.release("root", timestamp);
      yield* sql.withTransaction(store.prepareInTransaction(retry));
      expect((yield* store.get("root"))?.state).toBe("released");
      expect((yield* store.get("retry"))?.predecessor_observation_key).toBe("root");
      // A crash before receiver registration leaves a durable new row, never a launch replay.
      expect((yield* makeOriginStore(sql).get("retry"))?.receiver_namespace).toBe(
        "admission-retry:admission",
      );
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "notice identity and unacknowledged disposition survive reload without another wake",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = makeOriginStore(sql);
      yield* sql.withTransaction(store.prepareInTransaction(row()));
      yield* store.release("root", timestamp);
      const notice = {
        ...row(),
        observation_key: "notice",
        client_request_id: "notice",
        kind: "recovered_notice" as const,
        admission_id: "admission",
        recovered_revision: 1,
        state: "resolved" as const,
        result_json: JSON.stringify(result),
      };
      yield* sql.withTransaction(store.prepareInTransaction(notice));
      yield* sql.withTransaction(
        admitOriginWake(sql, { ...wake, observationKey: "notice", dependencyId: "notice" }),
      );
      const reloaded = yield* sql.withTransaction(
        makeOriginStore(sql).prepareInTransaction(notice),
      );
      expect(reloaded.state).toBe("delivered");
      expect(intent(reloaded).acknowledged).toBe(false);
      yield* sql.withTransaction(store.updateIntent("notice", { acknowledged: true }, timestamp));
      expect((yield* store.get("root"))?.state).toBe("released");
      expect(intent((yield* store.get("notice"))!).acknowledged).toBe(true);
    }).pipe(Effect.provide(fixture)),
);
