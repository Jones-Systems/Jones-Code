import { assert, it } from "@effect/vitest";
import { ProviderTurnId, RunAttemptId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as StartRefusal from "./Store.ts";
import * as Fixture from "./integration.testkit.ts";
import type { RefusalReason } from "./model.ts";

const settle = Effect.fnUntraced(function* (
  f: Effect.Success<typeof Fixture.seed>,
  reason?: RefusalReason,
  activeAttemptId = f.run.activeAttemptId!,
  breakTransaction = false,
  consumer?: StartRefusal.CertificationObserver["Service"],
) {
  const projection = yield* f.orchestrator.getThreadProjection(f.threadId);
  const attempt = projection.attempts.find((a) => a.id === f.run.activeAttemptId)!;
  const now = yield* DateTime.now;
  const events = [
    yield* f.event({
      type: "run.updated",
      threadId: f.threadId,
      runId: f.run.id,
      payload: { ...f.run, status: "failed", completedAt: now },
    }),
    yield* f.event({
      type: "run-attempt.updated",
      threadId: f.threadId,
      runId: f.run.id,
      payload: { ...attempt, status: "failed", completedAt: now },
    }),
  ];
  if (breakTransaction) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TEMP TRIGGER fail_certification BEFORE UPDATE ON jones_start_refusal_attempts
      WHEN NEW.state = 'refused' BEGIN SELECT RAISE(ABORT, 'synthetic certification failure'); END`;
  }
  const write = f.sink.writeIfRunCurrent({
    threadId: f.threadId,
    runId: f.run.id,
    activeAttemptId,
    expectedStatus: "starting",
    ...(reason === undefined || consumer !== undefined ? {} : { startRefusalReason: reason }),
    events,
    effects: [],
  });
  if (consumer === undefined) return yield* write;
  const sql = yield* SqlClient.SqlClient;
  const store = yield* StartRefusal.StartRefusals;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const result = yield* write;
      if (result.committed && reason !== undefined)
        yield* store.certify(
          { threadId: f.threadId, runId: f.run.id, attemptId: activeAttemptId },
          reason,
          result.storedEvents,
          consumer,
        );
      return result;
    }),
  );
});
const proof = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ state: string; reason: string | null; settlement_event_id: string | null }>`
    SELECT state, reason, settlement_event_id FROM jones_start_refusal_attempts`;
});

it.effect.each([
  "restart_admission",
  "continuity_unavailable",
  "continuity_held",
  "control_held",
] as const)("certifies %s in the successful settlement transaction", (reason) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    assert.deepEqual(
      (yield* proof).map((p) => p.state),
      ["eligible"],
    );
    assert.isTrue((yield* settle(f, reason)).committed);
    const p = (yield* proof)[0]!;
    assert.equal(p.state, "refused");
    assert.equal(p.reason, reason);
    assert.isNotNull(p.settlement_event_id);
    assert.isTrue(yield* (yield* StartRefusal.StartRefusals).qualifies(f.threadId, f.run.id));
    assert.equal(
      yield* (yield* StartRefusal.StartRefusals).classify(f.threadId, f.run.id),
      "qualified",
    );
  }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect.each(["stale", "transaction_failure", "untyped_failure"] as const)(
  "%s creates no certificate",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const outcome = yield* settle(
        f,
        mode === "untyped_failure" ? undefined : "control_held",
        mode === "stale" ? RunAttemptId.make("replaced") : f.run.activeAttemptId!,
        mode === "transaction_failure",
      ).pipe(Effect.result);
      if (mode === "transaction_failure") assert.equal(outcome._tag, "Failure");
      else if (mode === "stale" && outcome._tag === "Success")
        assert.isFalse(outcome.success.committed);
      const p = (yield* proof)[0]!;
      assert.equal(p.state, "eligible");
      assert.equal(p.settlement_event_id, null);
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect(
  "same-attempt crossing is irreversible across execution retry and repeated creation callbacks",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const store = yield* StartRefusal.StartRefusals;
      const key = { threadId: f.threadId, runId: f.run.id, attemptId: f.run.activeAttemptId! };
      yield* store.cross(key);
      yield* store.establish(key, "delegated_completion", 999);
      yield* settle(f, "control_held");
      assert.equal((yield* proof)[0]!.state, "crossed");
      assert.isFalse(yield* store.qualifies(f.threadId, f.run.id));
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect.each([
  "missing_row",
  "extra_attempt",
  "empty_attempts",
  "provider_null_ref",
  "provider_unknown_attempt",
  "provider_settlement",
  "started",
  "started_unknown",
  "missing_event",
] as const)("rejects incomplete or native proof: %s", (mode) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* StartRefusal.StartRefusals;
    yield* settle(f, "control_held");
    const projection = yield* f.orchestrator.getThreadProjection(f.threadId);
    if (mode === "missing_row") yield* sql`DELETE FROM jones_start_refusal_attempts`;
    if (mode === "empty_attempts")
      yield* sql`DELETE FROM orchestration_v2_projection_run_attempts WHERE run_id = ${f.run.id}`;
    if (mode === "missing_event")
      yield* sql`DELETE FROM orchestration_events
      WHERE event_id = (SELECT settlement_event_id FROM jones_start_refusal_attempts LIMIT 1)`;
    if (mode === "provider_settlement")
      yield* sql`UPDATE orchestration_v2_projection_run_attempts
      SET payload_json = json_set(payload_json, '$.providerSettlement', json('{}'))
      WHERE attempt_id = ${f.run.activeAttemptId}`;
    if (mode === "started")
      yield* sql`UPDATE orchestration_v2_projection_runs
      SET payload_json = json_set(payload_json, '$.startedAt', '2026-11-12T15:00:00Z') WHERE run_id = ${f.run.id}`;
    if (mode === "started_unknown")
      yield* sql`UPDATE orchestration_v2_projection_runs
        SET payload_json = json_remove(payload_json, '$.startedAt') WHERE run_id = ${f.run.id}`;
    const attempt = projection.attempts.find((a) => a.id === f.run.activeAttemptId)!;
    if (mode === "extra_attempt")
      yield* f.sink.write({
        events: [
          yield* f.event({
            type: "run-attempt.updated",
            threadId: f.threadId,
            runId: f.run.id,
            payload: { ...attempt, id: RunAttemptId.make("legacy-attempt"), attemptOrdinal: 2 },
          }),
        ],
      });
    if (mode === "provider_null_ref" || mode === "provider_unknown_attempt")
      yield* f.sink.write({
        events: [
          yield* f.event({
            type: "provider-turn.updated",
            threadId: f.threadId,
            runId: f.run.id,
            driver: Fixture.driver,
            payload: {
              id: ProviderTurnId.make("turn:preack"),
              providerThreadId: attempt.providerThreadId,
              nodeId: attempt.rootNodeId,
              runAttemptId: mode === "provider_unknown_attempt" ? null : attempt.id,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: null,
              completedAt: null,
            },
          }),
        ],
      });
    assert.isFalse(yield* store.qualifies(f.threadId, f.run.id));
    assert.equal(
      yield* store.classify(f.threadId, f.run.id),
      mode === "missing_event" ? "unknown" : "ordinary",
    );
  }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

it.effect("refused is terminal even if an outbox callback later crosses or certifies again", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const store = yield* StartRefusal.StartRefusals;
    yield* settle(f, "control_held");
    const before = yield* proof;
    yield* store.cross({
      threadId: f.threadId,
      runId: f.run.id,
      attemptId: f.run.activeAttemptId!,
    });
    yield* settle(f, "continuity_held");
    assert.deepEqual(yield* proof, before);
  }).pipe(Effect.provide(Fixture.runtimeWithoutProbe)),
);

const observationLayer = Layer.effect(
  StartRefusal.CertificationObserver,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_test_certification_observer (attempt_id TEXT PRIMARY KEY, scope TEXT NOT NULL)`;
    return StartRefusal.CertificationObserver.of({
      certified: (input) =>
        sql`
    INSERT INTO jones_test_certification_observer VALUES (${input.attemptId}, ${input.scope})`.pipe(
          Effect.asVoid,
        ),
    });
  }),
).pipe(Layer.provide(Fixture.database));

it.effect(
  "certification observers commit once with the terminal settlement and carry the eligibility scope",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const sql = yield* SqlClient.SqlClient;
      yield* settle(f, "control_held");
      yield* settle(f, "control_held");
      const rows = yield* sql`SELECT * FROM jones_test_certification_observer`;
      assert.deepEqual(rows, [
        { attempt_id: f.run.activeAttemptId, scope: "delegated_completion" },
      ]);
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe.pipe(Layer.provide(observationLayer)))),
);

it.effect(
  "a failed certification observer rolls back the terminal settlement and certificate",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TEMP TRIGGER fail_observer BEFORE INSERT ON jones_test_certification_observer
      BEGIN SELECT RAISE(ABORT, 'synthetic observer failure'); END`;
      assert.equal((yield* settle(f, "control_held").pipe(Effect.result))._tag, "Failure");
      assert.equal((yield* proof)[0]!.state, "eligible");
      const projection = yield* f.orchestrator.getThreadProjection(f.threadId);
      assert.equal(projection.runs.find((run) => run.id === f.run.id)!.status, "starting");
      assert.lengthOf(yield* sql`SELECT * FROM jones_test_certification_observer`, 0);
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe.pipe(Layer.provide(observationLayer)))),
);

it.effect.each([false, true])(
  "per-call certification consumer is transactional and idempotent (failure=%s)",
  (fail) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* StartRefusal.StartRefusals;
      yield* sql`CREATE TABLE jones_test_certification_consumer (attempt_id TEXT PRIMARY KEY, scope TEXT NOT NULL)`;
      if (fail)
        yield* sql`CREATE TEMP TRIGGER fail_consumer BEFORE INSERT ON jones_test_certification_consumer
      BEGIN SELECT RAISE(ABORT, 'synthetic consumer failure'); END`;
      const consumer = StartRefusal.CertificationObserver.of({
        certified: (input) =>
          Effect.gen(function* () {
            assert.equal(
              (yield* sql`SELECT state FROM jones_start_refusal_attempts`)[0]!.state,
              "refused",
            );
            assert.lengthOf(yield* sql`SELECT * FROM jones_test_certification_observer`, 1);
            assert.equal(input.reason, "control_held");
            assert.equal(input.settlement.event.type, "run.updated");
            yield* sql`INSERT INTO jones_test_certification_consumer VALUES (${input.attemptId}, ${input.scope})`;
          }),
      });
      const result = yield* settle(f, "control_held", f.run.activeAttemptId!, false, consumer).pipe(
        Effect.result,
      );
      assert.equal(result._tag, fail ? "Failure" : "Success");
      assert.equal((yield* proof)[0]!.state, fail ? "eligible" : "refused");
      assert.lengthOf(yield* sql`SELECT * FROM jones_test_certification_observer`, fail ? 0 : 1);
      assert.lengthOf(yield* sql`SELECT * FROM jones_test_certification_consumer`, fail ? 0 : 1);
      const projection = yield* f.orchestrator.getThreadProjection(f.threadId);
      assert.equal(
        projection.runs.find((r) => r.id === f.run.id)!.status,
        fail ? "starting" : "failed",
      );
      if (result._tag === "Success") {
        yield* store.certify(
          { threadId: f.threadId, runId: f.run.id, attemptId: f.run.activeAttemptId! },
          "control_held",
          result.success.storedEvents,
          consumer,
        );
        assert.lengthOf(yield* sql`SELECT * FROM jones_test_certification_consumer`, 1);
        assert.lengthOf(yield* sql`SELECT * FROM jones_test_certification_observer`, 1);
      }
    }).pipe(Effect.provide(Fixture.runtimeWithoutProbe.pipe(Layer.provide(observationLayer)))),
);
