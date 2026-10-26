import { expect, it } from "@effect/vitest";
import { AuthOrchestrationReadScope, AuthOrchestrationOperateScope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Callers from "./recordedCaller.ts";
import * as Recovery from "./recovery.ts";
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
