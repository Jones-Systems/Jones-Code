import { expect, it } from "@effect/vitest";
import { AuthOrchestrationReadScope, AuthOrchestrationOperateScope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Callers from "./recordedCaller.ts";
import * as Recovery from "./recovery.ts";
import { captureLogs } from "./supervision.testkit.ts";
import * as Entries from "../operations/durableEntries.ts";
import * as Operations from "../../operations/service.ts";
const record: Callers.RecordedCallerV1 = {
  version: 1,
  origin: { kind: "agent" },
  auth: {
    kind: "mcp_client",
    session_id: "caller-session-sentinel",
    subject: "mcp-client",
    method: "bearer-access-token",
    approved_access: "full-access",
    requested_limits: { runtimeMode: "auto-accept-edits", interactionMode: "default" },
  },
  source_environment_id: "source",
  registry_principal_id: "principal",
  singleton_binding_version: 2,
  grant_version: 3,
  captured_at: 0,
  source: {
    environment_id: "source",
    authority_namespace: "source-namespace-sentinel",
    store_generation: 4,
    binding_version: 5,
    enrollment_id: "native-enrollment",
    enrollment_session_id: "provider-session",
  },
  destination: {
    registry_instance_id: "registry",
    owner_id: "owner",
    home_environment_id: "home",
    home_generation: 6,
    principal_id: "principal",
    binding_version: 2,
  },
  ceiling: {
    permissions: ["workstreams:read", "workstreams:write"],
    scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
    capabilities: ["orchestration"],
    limits: { runtimeMode: "auto-accept-edits", interactionMode: "default" },
  },
};
const current = (admission = record): Callers.CurrentCallerState => ({
  admission,
  registry_active: true,
  singleton_active: true,
  source_binding_active: true,
  grant: {
    active: true,
    version: admission.grant_version,
    permissions: ["workstreams:read", "workstreams:write"],
  },
  enrollment: {
    enrollment_id: admission.source.enrollment_id,
    session_id: admission.source.enrollment_session_id,
    active: true,
    issued_at: 0,
    expires_at: 1_000_000_000,
  },
  session: {
    session_id: "caller-session-sentinel",
    subject: "mcp-client",
    method: "bearer-access-token",
    scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
    issued_at: 0,
    expires_at: 1_000_000_000,
    revoked: false,
  },
  credential: null,
  run: null,
  trusted_cli_authority_id: null,
});

const entries = Layer.succeed(Entries.DurableEntries, {
  open: () => Effect.succeed([]),
  read: () => Effect.succeed(null),
  accept: () => Effect.die("unexpected acceptance"),
  save: () => Effect.die("unexpected checkpoint"),
});
const operations = Layer.succeed(Operations.WorkstreamOperations, {
  resumeRecorded: () => Effect.die("unexpected operation recovery"),
  submit: () => Effect.die("unexpected submit"),
  read: () => Effect.succeed({ operations: [] }),
  setupStatus: () => Effect.die("unexpected status"),
});
const callers = Callers.layer.pipe(
  Layer.provide(
    Layer.succeed(Callers.CallerAuthority, {
      capture: () => Effect.die("background sweep must not capture a new caller"),
      refresh: (record) => Effect.succeed(current(record)),
      checkTarget: () => Effect.void,
    }),
  ),
);
const worker = (raw: Recovery.RawRecovery["Service"]) =>
  Recovery.layer.pipe(
    Layer.provide(entries),
    Layer.provide(operations),
    Layer.provide(callers),
    Layer.provide(Layer.succeed(Recovery.RawRecovery, raw)),
  );
it.effect(
  "initial raw sweep admits each selected native item; cadence respects due times and never dispatches imported or old evidence",
  () =>
    Effect.gen(function* () {
      const first = yield* Deferred.make<void>(),
        second = yield* Deferred.make<void>();
      const calls: string[] = [];
      const base = { origin: "native" as const, recorded_caller: record, next_poll_at: null };
      let list: Parameters<Recovery.RawRecovery["Service"]["pending"]> extends []
        ? {
            command_id: string;
            origin: "native" | "legacy_open";
            recorded_caller: Callers.RecordedCallerV1 | null;
            next_poll_at: string | null;
          }[]
        : never = [
        { ...base, command_id: "first" },
        { ...base, command_id: "due", next_poll_at: "1970-01-01T00:01:00.000Z" },
        { ...base, command_id: "imported", origin: "legacy_open" },
        { ...base, command_id: "old", recorded_caller: null },
        { ...base, command_id: "invalid-due", next_poll_at: "invalid" },
      ];
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Recovery.NativeWorkstreamsRecovery;
          yield* Deferred.await(first);
          expect(calls).toEqual(["first"]);
          yield* TestClock.adjust(30_000);
          expect(calls).toEqual(["first"]);
          yield* TestClock.adjust(30_000);
          yield* Deferred.await(second);
          expect(calls).toEqual(["first", "due"]);
        }).pipe(
          Effect.provide(
            worker({
              pending: () => Effect.succeed(list),
              recoverOne: Effect.fn("test.recoverOne")(function* (id) {
                expect(yield* Callers.RecordedAdmission).toEqual(record);
                const caller = yield* Callers.RecoveryCaller;
                yield* caller
                  .check("workstreams:write", null)
                  .pipe(
                    Effect.mapError(() => new Callers.RecordedCallerFence({ code: "not_granted" })),
                  );
                calls.push(id);
                list = list.filter((item) => item.command_id !== id);
                yield* Deferred.succeed(id === "first" ? first : second, undefined);
              }),
            }),
          ),
        ),
      );
    }),
);
it.effect(
  "overlapping sweeps coalesce and scope cancellation interrupts only the owned worker",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let calls = 0,
        interrupted = false;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const worker = yield* Recovery.NativeWorkstreamsRecovery;
          yield* Deferred.await(started);
          const results = yield* Effect.all(
            Array.from({ length: 8 }, () => worker.sweep()),
            { concurrency: "unbounded" },
          );
          expect(results.every((r) => r.attempted === 0)).toBe(true);
          expect(calls).toBe(1);
        }).pipe(
          Effect.provide(
            worker({
              pending: () =>
                Effect.succeed([
                  {
                    command_id: "one",
                    origin: "native",
                    next_poll_at: null,
                    recorded_caller: record,
                  },
                ]),
              recoverOne: () =>
                Effect.gen(function* () {
                  calls++;
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(release);
                }).pipe(
                  Effect.onInterrupt(() =>
                    Effect.sync(() => {
                      interrupted = true;
                    }),
                  ),
                ),
            }),
          ),
        ),
      );
      expect(interrupted).toBe(true);
      expect(calls).toBe(1);
    }),
);

it.effect(
  "tagged PEER recovery uses its lookup-only port without a fabricated HOME caller; imports remain fenced",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const done = yield* Deferred.make<void>();
        const calls: string[] = [];
        let pending = [
          {
            command_id: "peer-original",
            origin: "native" as const,
            recorded_caller: null,
            peer_recovery: true,
            next_poll_at: null,
          },
          {
            command_id: "imported-peer",
            origin: "legacy_open" as const,
            recorded_caller: null,
            peer_recovery: true,
            next_poll_at: null,
          },
        ];
        yield* Effect.gen(function* () {
          yield* Recovery.NativeWorkstreamsRecovery;
          yield* Deferred.await(done);
          expect(calls).toEqual(["peer-original"]);
        }).pipe(
          Effect.provide(
            worker({
              pending: () => Effect.succeed(pending),
              recoverOne: () => Effect.die("PEER must not enter HOME readmission"),
              recoverPeer: (id) =>
                Effect.gen(function* () {
                  expect(yield* Callers.RecordedAdmission).toBeNull();
                  calls.push(id);
                  pending = [];
                  yield* Deferred.succeed(done, undefined);
                }),
            }),
          ),
        );
      }),
    ),
);
it.effect("tagged PEER recovery remains fenced if the lookup-only owner is absent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const workerService = yield* Recovery.NativeWorkstreamsRecovery;
      const result = yield* workerService.sweep();
      expect(result).toEqual({ attempted: 0, fenced: 1, deferred: 0 });
    }).pipe(
      Effect.provide(
        worker({
          pending: () =>
            Effect.succeed([
              {
                command_id: "peer-no-owner",
                origin: "native",
                recorded_caller: null,
                peer_recovery: true,
                next_poll_at: null,
              },
            ]),
          recoverOne: () => Effect.die("Missing PEER owner cannot become HOME recovery"),
        }),
      ),
    ),
  ),
);

it.effect(
  "a failing or defecting sweep is logged with its failure class and the loop keeps sweeping",
  () => {
    const capture = captureLogs();
    let calls = 0;
    const tail = () => capture.logs.slice(-1)[0]!;
    return Effect.scoped(
      Effect.gen(function* () {
        yield* Recovery.NativeWorkstreamsRecovery;
        // Let the forked loop run its first sweep before observing it.
        yield* TestClock.adjust(0);
        // The first sweep fails with a typed error: it is logged, never swallowed.
        expect(calls).toBe(1);
        // Control: the capture reaches the loop fiber, so a missing warning means none was logged.
        expect(capture.at("Info").map((entry) => entry.message)).toEqual([
          ["synthetic sweep marker"],
        ]);
        expect(capture.at("Warn")).toHaveLength(1);
        expect(capture.at("Warn")[0]!.annotations).toEqual({
          loop: "recovery",
          failure: "typed",
          code: "reservations",
          consecutiveFailures: 1,
          retryInMs: 30_000,
        });
        expect(capture.at("Warn")[0]!.cause.reasons.length).toBeGreaterThan(0);
        // A defect used to end the loop silently; it is logged and the next cadence still runs.
        yield* TestClock.adjust(30_000);
        expect(calls).toBe(2);
        expect(capture.at("Warn")).toHaveLength(2);
        expect(capture.at("Warn")[1]!.annotations).toEqual({
          loop: "recovery",
          failure: "defect",
          consecutiveFailures: 2,
          retryInMs: 60_000,
        });
        // Repeated failure backs off; the next attempt is not due at the base cadence.
        yield* TestClock.adjust(59_999);
        expect(calls).toBe(2);
        yield* TestClock.adjust(1);
        expect(calls).toBe(3);
        expect(capture.at("Warn")).toHaveLength(2);
        expect(tail().level).toBe("Info");
        expect(tail().annotations).toEqual({ loop: "recovery", afterFailures: 2 });
        // Success restores the base cadence.
        yield* TestClock.adjust(29_999);
        expect(calls).toBe(3);
        yield* TestClock.adjust(1);
        expect(calls).toBe(4);
      }).pipe(
        Effect.provide(
          worker({
            pending: () =>
              Effect.gen(function* () {
                calls++;
                yield* Effect.logInfo("synthetic sweep marker");
                if (calls === 1)
                  return yield* new Recovery.RecoveryError({ stage: "reservations" });
                if (calls === 2) return yield* Effect.die(new Error("synthetic recovery defect"));
                return [];
              }),
            recoverOne: () => Effect.die("no item is selected"),
          }).pipe(Layer.provide(capture.layer)),
        ),
      ),
    );
  },
);
it.effect("a changed fenced count is logged once per change, not once per sweep", () => {
  const capture = captureLogs();
  let pending: Array<{
    command_id: string;
    origin: "native";
    next_poll_at: null;
    recorded_caller: null;
  }> = [
    { command_id: "old", origin: "native", next_poll_at: null, recorded_caller: null },
    { command_id: "older", origin: "native", next_poll_at: null, recorded_caller: null },
  ];
  const markers = () =>
    capture.at("Info").filter((entry) => entry.message[0] === "synthetic sweep marker");
  const fenced = () =>
    capture.logs.filter(
      (entry) => entry.annotations.loop === "recovery" && "fenced" in entry.annotations,
    );
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Recovery.NativeWorkstreamsRecovery;
      yield* TestClock.adjust(0);
      // Control: the capture reaches the loop fiber, so a missing warning means none was logged.
      expect(markers()).toHaveLength(1);
      expect(fenced()).toHaveLength(1);
      expect(fenced()[0]!.level).toBe("Warn");
      expect(fenced()[0]!.annotations).toEqual({
        loop: "recovery",
        fenced: 2,
        attempted: 0,
        deferred: 0,
      });
      // An unchanged count stays quiet on every later sweep.
      yield* TestClock.adjust(120_000);
      expect(fenced()).toHaveLength(1);
      pending = [];
      yield* TestClock.adjust(30_000);
      expect(fenced()).toHaveLength(2);
      expect(fenced()[1]!.level).toBe("Info");
      expect(fenced()[1]!.annotations).toEqual({
        loop: "recovery",
        fenced: 0,
        attempted: 0,
        deferred: 0,
      });
    }).pipe(
      Effect.provide(
        worker({
          pending: () => Effect.logInfo("synthetic sweep marker").pipe(Effect.as(pending)),
          recoverOne: () => Effect.die("no item is selected"),
        }).pipe(Layer.provide(capture.layer)),
      ),
    ),
  );
});
