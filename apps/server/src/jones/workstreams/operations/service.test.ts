import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import * as TestClock from "effect/testing/TestClock";
import type { WorkstreamCommand } from "@t3tools/contracts";
import { parseWorkstreamOperationResult } from "@t3tools/contracts/jones/workstreamOperations";
import { WorkstreamOperations } from "./service.ts";
import { canonicalJson } from "./encoding.ts";
import {
  fakeControlPlane,
  request,
  createRequest,
  childId,
  hash,
  offline,
} from "./testFixtures.ts";

const withCP = <A, E, R>(
  run: (cp: ReturnType<typeof fakeControlPlane>) => Effect.Effect<A, E, R | WorkstreamOperations>,
) => {
  const cp = fakeControlPlane();
  return run(cp).pipe(Effect.provide(cp.layer));
};
const command = (
  id: string,
  version: number,
  action: WorkstreamCommand["action"],
): WorkstreamCommand => ({
  command_id: id,
  expected_registry_version: version,
  expected_server_generation: 5,
  action,
});
const attach = {
  operation: "attach_primary",
  workstream_id: "target",
  expected_version: 1,
  native_reference_id: "reference",
} as const;

describe("workstream operation state machine", () => {
  it.effect("registers before assigning, caches only settled results, and reads from memory", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        const service = yield* WorkstreamOperations;
        const result = yield* service.submit({ request, wait_ms: 3000 });
        expect(parseWorkstreamOperationResult(result).state).toBe("committed");
        expect(cp.state.writes.map((body) => JSON.parse(body).action.operation)).toEqual([
          "register_reference",
          "attach_primary",
        ]);
        const reads = cp.state.reads;
        expect(yield* service.submit({ request, wait_ms: 0 })).toEqual(result);
        expect(yield* service.read({ operation_id: request.operation_id })).toEqual({
          operation: result,
        });
        expect(cp.state.reads).toBe(reads);
        expect(cp.state.membershipCommits).toBe(1);
      }),
    ),
  );

  it.effect(
    "adopts a lost reply after an owner move using atomic resubmit and exact bytes (r2 1,2,12)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.state.afterSend = (sent) =>
            Effect.suspend(() => {
              if (sent.action.operation === "attach_primary") {
                cp.state.primary = "other";
                cp.state.version++;
                return Effect.fail(offline());
              }
              return Effect.void;
            });
          const service = yield* WorkstreamOperations;
          const result = yield* service.submit({ request, wait_ms: 3000 });
          expect(result.state).toBe("committed");
          expect(result.observed).toMatchObject({ workstream_id: "other" });
          expect(cp.state.membershipCommits).toBe(1);
          expect(cp.state.writes.filter((body) => body.includes('"attach_primary"'))).toHaveLength(
            1,
          );
        }),
      ),
  );

  it.effect("recovers committed membership after process loss and later move (r2 4)", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        cp.seedRegistration();
        cp.decide(command(childId(request, "mem"), cp.state.version, attach));
        cp.state.primary = "other";
        cp.state.version++;
        cp.state.exists = false;
        const service = yield* WorkstreamOperations;
        const result = yield* service.submit({ request, wait_ms: 3000 });
        expect(result.state).toBe("committed");
        expect(result.observed).toMatchObject({ workstream_id: "other" });
        expect(cp.state.writes).toEqual([]);
      }),
    ),
  );

  it.effect(
    "pins create to the version before scanning when an old executor commits (r2 5,6)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          const old = command(childId(createRequest, "cre"), cp.state.version, {
            operation: "create_workstream",
            ...createRequest.workstream,
          });
          cp.state.beforeSend = () =>
            Effect.sync(() => {
              cp.state.beforeSend = undefined;
              cp.decide(old);
            });
          const service = yield* WorkstreamOperations;
          const result = yield* service.submit({ request: createRequest, wait_ms: 3000 });
          expect(result.state).toBe("committed");
          expect(cp.state.creates).toBe(1);
          expect(cp.state.writes).toHaveLength(1);
          expect(JSON.parse(cp.state.writes[0]!).expected_registry_version).toBe(
            old.expected_registry_version,
          );
        }),
      ),
  );

  it.effect(
    "uses the first absent index and resolves an intervening different-body receipt (r2 7)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.decide(
            command(childId(createRequest, "cre", 1), 0, {
              operation: "create_workstream",
              ...createRequest.workstream,
            }),
          );
          cp.decide(
            command(childId(createRequest, "cre", 2), 0, {
              operation: "create_workstream",
              ...createRequest.workstream,
            }),
          );
          cp.state.beforeSend = (sent) =>
            Effect.sync(() => {
              cp.state.beforeSend = undefined;
              cp.decide({ ...sent, expected_registry_version: 0 });
            });
          const retrying = yield* Deferred.make<void>();
          cp.state.onSleep = (ms) =>
            ms === 250 ? Deferred.succeed(retrying, undefined).pipe(Effect.asVoid) : Effect.void;
          const service = yield* WorkstreamOperations;
          const submitted = yield* service
            .submit({ request: createRequest, wait_ms: 3000 })
            .pipe(Effect.forkChild);
          yield* Deferred.await(retrying);
          yield* TestClock.adjust(1000);
          expect((yield* Fiber.join(submitted)).state).toBe("committed");
          expect(JSON.parse(cp.state.writes[0]!).command_id).toBe(childId(createRequest, "cre", 3));
          expect(cp.state.creates).toBe(1);
          expect(JSON.parse(cp.state.writes[1]!).command_id).toBe(childId(createRequest, "cre", 4));
        }),
      ),
  );

  it.effect("exhausts nine rejected indices without submitting (r2 8)", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        for (let n = 1; n <= 9; n++)
          cp.decide(
            command(childId(createRequest, "cre", n), 0, {
              operation: "create_workstream",
              ...createRequest.workstream,
            }),
          );
        const service = yield* WorkstreamOperations;
        expect(yield* service.submit({ request: createRequest, wait_ms: 3000 })).toMatchObject({
          state: "unavailable",
          reason: "contention",
        });
        expect(cp.state.writes).toEqual([]);
      }),
    ),
  );

  it.effect(
    "never treats unreadable receipts as absence or caches CONFIG failures (r2 10, r3 5)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.state.offline = true;
          const service = yield* WorkstreamOperations;
          expect(yield* service.submit({ request: createRequest, wait_ms: 3000 })).toMatchObject({
            state: "unavailable",
            reason: "offline",
          });
          expect(cp.state.writes).toEqual([]);
          cp.state.offline = false;
          cp.state.writable = false;
          expect(yield* service.submit({ request: createRequest, wait_ms: 3000 })).toMatchObject({
            state: "rejected",
            reason: "permission_denied",
          });
          cp.state.writable = true;
          expect((yield* service.submit({ request: createRequest, wait_ms: 3000 })).state).toBe(
            "committed",
          );
          expect(cp.state.creates).toBe(1);
        }),
      ),
  );

  it.effect(
    "seals a quiet old attempt only after an unrelated write, without submitting (r3 1,5)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.seedRegistration();
          cp.state.verified = false;
          cp.state.verificationPending = true;
          // Existing verify is pending without advancing the registry since the old mem plan.
          cp.decide(
            command(childId(request, "ver"), cp.state.version, {
              operation: "verify_reference",
              native_reference_id: "reference",
              expected_attestation_version: 0,
            }),
          );
          const service = yield* WorkstreamOperations;
          const submitted = yield* service
            .submit({ request, wait_ms: 3000 })
            .pipe(Effect.forkChild);
          yield* TestClock.adjust(30_000);
          yield* Fiber.join(submitted);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "reconciling", poll_after_ms: 30_000 },
          });
          expect(cp.state.writes).toEqual([]);
          cp.state.version++;
          yield* TestClock.adjust(30_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "unavailable", reason: "verification_pending" },
          });
          expect(cp.state.writes).toEqual([]);
        }),
      ),
  );

  it.effect(
    "closing scan adopts an old membership commit during pending verification (r2 11, r3 1)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.seedRegistration();
          cp.state.verified = false;
          cp.state.verificationPending = true;
          cp.decide(
            command(childId(request, "ver"), cp.state.version, {
              operation: "verify_reference",
              native_reference_id: "reference",
              expected_attestation_version: 0,
            }),
          );
          const old = command(childId(request, "mem"), cp.state.version, attach);
          const service = yield* WorkstreamOperations;
          yield* service.submit({ request, wait_ms: 0 });
          yield* TestClock.adjust(30_000);
          cp.decide(old);
          yield* TestClock.adjust(30_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "committed" },
          });
          expect(cp.state.membershipCommits).toBe(1);
        }),
      ),
  );

  it.effect("retries a sealed failure, then replays success with no calls (r3 2)", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        cp.state.exists = false;
        const service = yield* WorkstreamOperations;
        expect(yield* service.submit({ request, wait_ms: 3000 })).toMatchObject({
          state: "rejected",
          reason: "thread_not_found",
        });
        cp.state.exists = true;
        expect((yield* service.submit({ request, wait_ms: 3000 })).state).toBe("committed");
        const reads = cp.state.reads;
        yield* service.submit({ request, wait_ms: 0 });
        expect(cp.state.reads).toBe(reads);
      }),
    ),
  );

  it.effect("wait budget does not cancel the background child, and conflicts are precise", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        cp.state.beforeSend = () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)));
        const service = yield* WorkstreamOperations;
        const waiting = yield* service.submit({ request, wait_ms: 1000 }).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* TestClock.adjust(1000);
        expect((yield* Fiber.join(waiting)).state).toBe("in_progress");
        expect(
          yield* service
            .submit({ request: { ...request, operation_id: "another-operation-001" }, wait_ms: 0 })
            .pipe(Effect.flip),
        ).toMatchObject({
          code: "thread_operation_in_progress",
          open_operation_id: request.operation_id,
        });
        expect(
          yield* service
            .submit({ request: { ...request, origin: { kind: "cli" } }, wait_ms: 0 })
            .pipe(Effect.flip),
        ).toMatchObject({ code: "operation_id_conflict" });
        const reads = cp.state.reads;
        yield* service.read({ open: true });
        yield* service.read({ thread: request.thread });
        expect(cp.state.reads).toBe(reads);
        yield* Deferred.succeed(release, undefined);
        expect((yield* service.submit({ request, wait_ms: 3000 })).state).toBe("committed");
        expect(cp.state.membershipCommits).toBe(1);
      }),
    ),
  );

  it.effect(
    "an unrelated version advance stores a permanent rejection before replanning (r2 3)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.seedRegistration();
          let first = true;
          cp.state.beforeSend = () =>
            Effect.sync(() => {
              if (first) {
                first = false;
                cp.state.version++;
              }
            });
          const retrying = yield* Deferred.make<void>();
          cp.state.onSleep = (ms) =>
            ms === 250 ? Deferred.succeed(retrying, undefined).pipe(Effect.asVoid) : Effect.void;
          const service = yield* WorkstreamOperations;
          const submitted = yield* service
            .submit({ request, wait_ms: 3000 })
            .pipe(Effect.forkChild);
          yield* Deferred.await(retrying);
          yield* TestClock.adjust(1000);
          expect((yield* Fiber.join(submitted)).state).toBe("committed");
          const original = cp.state.writes[0]!;
          const late = yield* cp.ports.gateway.resubmit(childId(request, "mem"), original);
          expect(late).toMatchObject({
            kind: "receipt",
            receipt: { state: "rejected", error: { code: "version_conflict" } },
          });
          expect(cp.state.membershipCommits).toBe(1);
        }),
      ),
  );

  it.effect(
    "restarts a pass when V-prime differs and never substitutes the later version (r2 9)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.seedRegistration();
          let sessions = 0;
          cp.state.beforeSession = () => {
            if (++sessions === 3) cp.state.version++;
          };
          const service = yield* WorkstreamOperations;
          const submitted = yield* service
            .submit({ request, wait_ms: 3000 })
            .pipe(Effect.forkChild);
          yield* TestClock.adjust(1000);
          expect((yield* Fiber.join(submitted)).state).toBe("committed");
          expect(cp.state.writes).toHaveLength(1);
          expect(JSON.parse(cp.state.writes[0]!).expected_registry_version).toBe(3);
        }),
      ),
  );

  it.effect(
    "a fresh pending verify seals through absent reg-1, then the same operation retries (r3 2)",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.state.registered = true;
          cp.state.verified = false;
          cp.state.verificationPending = true;
          const service = yield* WorkstreamOperations;
          yield* service.submit({ request, wait_ms: 0 });
          yield* TestClock.adjust(30_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "unavailable", reason: "verification_pending" },
          });
          cp.state.verified = true;
          cp.state.verificationPending = false;
          expect((yield* service.submit({ request, wait_ms: 3000 })).state).toBe("committed");
          const reads = cp.state.reads;
          yield* service.submit({ request, wait_ms: 0 });
          expect(cp.state.reads).toBe(reads);
        }),
      ),
  );

  it.effect(
    "unknown children retain identical bytes and exclude a second child beyond sixty seconds",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.seedRegistration();
          cp.state.beforeSend = () =>
            Effect.sync(() => {
              cp.state.offline = true;
            });
          const service = yield* WorkstreamOperations;
          yield* service.submit({ request, wait_ms: 0 });
          yield* TestClock.adjust(65_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "reconciling", poll_after_ms: 30_000 },
          });
          expect(new Set(cp.state.writes).size).toBe(1);
          expect(cp.state.membershipCommits).toBe(0);
          const reads = cp.state.reads;
          const writes = cp.state.writes.length;
          expect(yield* service.submit({ request, wait_ms: 0 })).toMatchObject({
            state: "reconciling",
          });
          expect(cp.state.reads).toBe(reads);
          expect(cp.state.writes.length).toBe(writes);
          cp.state.beforeSend = undefined;
          cp.state.offline = false;
          yield* TestClock.adjust(30_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "committed" },
          });
          expect(cp.state.membershipCommits).toBe(1);
        }),
      ),
  );

  it.effect("sealing releases the thread lock and a second operation seals the first", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        cp.seedRegistration();
        cp.state.exists = false;
        const service = yield* WorkstreamOperations;
        const first = yield* service.submit({ request, wait_ms: 0 });
        yield* TestClock.adjust(1);
        expect(yield* service.read({ operation_id: first.operation_id })).toMatchObject({
          operation: { state: "reconciling" },
        });
        cp.state.exists = true;
        expect(
          (yield* service.submit({
            request: { ...request, operation_id: "second-assignment-001" },
            wait_ms: 3000,
          })).state,
        ).toBe("committed");
        const writes = cp.state.writes.length;
        yield* TestClock.adjust(30_000);
        expect(yield* service.read({ operation_id: first.operation_id })).toMatchObject({
          operation: { state: "rejected", reason: "thread_not_found" },
        });
        expect(cp.state.writes.length).toBe(writes);
      }),
    ),
  );

  it.effect(
    "only an explicit same-ID resubmit restarts a sealing operation after thread recovery",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.seedRegistration();
          cp.state.exists = false;
          const service = yield* WorkstreamOperations;
          yield* service.submit({ request, wait_ms: 0 });
          yield* TestClock.adjust(1);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "reconciling", poll_after_ms: 30_000 },
          });
          const version = cp.state.version;
          cp.state.exists = true;
          yield* TestClock.adjust(30_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
            operation: { state: "reconciling" },
          });
          expect(cp.state.version).toBe(version);
          expect(cp.state.writes).toEqual([]);

          const retried = yield* service.submit({ request, wait_ms: 3000 }).pipe(Effect.forkChild);
          yield* TestClock.adjust(3000);
          const result = yield* Fiber.join(retried);
          expect(result.state).toBe("committed");
          expect(result.children).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                command_id: childId(request, "reg"),
                outcome: "committed",
              }),
              expect.objectContaining({
                command_id: childId(request, "mem"),
                outcome: "committed",
              }),
            ]),
          );
          expect(cp.state.writes.map((body) => JSON.parse(body).command_id)).toEqual([
            childId(request, "mem"),
          ]);
          expect(cp.state.membershipCommits).toBe(1);
          const reads = cp.state.reads;
          expect(yield* service.submit({ request, wait_ms: 0 })).toEqual(result);
          expect(cp.state.reads).toBe(reads);
        }),
      ),
  );

  it.effect("sealing resubmits reacquire the thread lock and join a single executor", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        cp.seedRegistration();
        cp.state.exists = false;
        const service = yield* WorkstreamOperations;
        yield* service.submit({ request, wait_ms: 0 });
        yield* TestClock.adjust(1);
        cp.state.exists = true;
        const release = yield* Deferred.make<void>();
        cp.state.beforeSend = () => Deferred.await(release);
        const other = { ...request, operation_id: "second-assignment-001" };
        yield* service.submit({ request: other, wait_ms: 0 });
        yield* TestClock.adjust(1);
        expect(yield* service.submit({ request, wait_ms: 0 }).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { code: "thread_operation_in_progress", open_operation_id: other.operation_id },
        });
        cp.state.exists = false;
        yield* Deferred.succeed(release, undefined);
        yield* TestClock.adjust(1);
        expect(yield* service.read({ operation_id: other.operation_id })).toMatchObject({
          operation: { state: "rejected", reason: "thread_not_found" },
        });

        cp.state.exists = true;
        const resume = yield* Deferred.make<void>();
        cp.state.beforeSend = () => Deferred.await(resume);
        const writes = cp.state.writes.length;
        yield* Effect.all(
          [service.submit({ request, wait_ms: 0 }), service.submit({ request, wait_ms: 0 })],
          { concurrency: "unbounded" },
        );
        yield* TestClock.adjust(1);
        expect(cp.state.writes.length).toBe(writes + 1);
        const blocked = yield* service.submit({ request: other, wait_ms: 0 }).pipe(Effect.result);
        expect(blocked).toMatchObject({
          _tag: "Failure",
          failure: {
            code: "thread_operation_in_progress",
            open_operation_id: request.operation_id,
          },
        });
        yield* Deferred.succeed(resume, undefined);
        yield* TestClock.adjust(1);
        expect(yield* service.read({ operation_id: request.operation_id })).toMatchObject({
          operation: { state: "committed" },
        });
        expect(cp.state.membershipCommits).toBe(1);
      }),
    ),
  );

  it.effect(
    "bounds destination history to ten pages and keeps terminal operations for 24 hours",
    () =>
      withCP((cp) =>
        Effect.gen(function* () {
          cp.state.pages = 11;
          const service = yield* WorkstreamOperations;
          expect(yield* service.submit({ request, wait_ms: 3000 })).toMatchObject({
            state: "unavailable",
            reason: "destination_scan_bound",
          });
          expect(cp.state.membershipCommits).toBe(0);
          yield* TestClock.adjust(86_400_000);
          expect(yield* service.read({ operation_id: request.operation_id })).toEqual({
            operation: null,
          });
        }),
      ),
  );

  it.effect("hashes request only, caches setup ten seconds, and expires memory assignment", () =>
    withCP((cp) =>
      Effect.gen(function* () {
        cp.state.expiresAt = "1970-01-01T00:02:00.000Z";
        const service = yield* WorkstreamOperations;
        const result = yield* service.submit({ request, wait_ms: 3000 });
        expect(result.request_sha256).toBe(hash(canonicalJson(request)));
        const setup = yield* service.setupStatus();
        const reads = cp.state.reads;
        expect(yield* service.setupStatus()).toEqual(setup);
        expect(cp.state.reads).toBe(reads);
        yield* TestClock.adjust(10_000);
        yield* service.setupStatus();
        expect(cp.state.reads).toBeGreaterThan(reads);
        yield* TestClock.adjust(120_000);
        const before = cp.state.reads;
        expect(yield* service.read({ thread: request.thread })).toEqual({
          assignment: { kind: "unverified" },
          open_operation: null,
        });
        expect(cp.state.reads).toBe(before);
        expect(JSON.stringify(setup)).not.toContain("secret-value");
      }),
    ),
  );
});

it.effect.each([
  "descriptor_absent",
  "build_unavailable",
  "contract_mismatch",
  "source_mismatch",
  "authority_mismatch",
  "generation_mismatch",
  "authority_fenced",
  "authority_unavailable",
] as const)("preserves availability reason %s in submit and setup status", (reason) =>
  withCP((cp) =>
    Effect.gen(function* () {
      cp.state.availability = reason;
      const service = yield* WorkstreamOperations;
      expect(yield* service.submit({ request, wait_ms: 3000 })).toMatchObject({
        state: "unavailable",
        reason: "registration_unavailable",
        detail: { availability: reason },
      });
      expect(yield* service.setupStatus()).toMatchObject({
        authority_effect: "none",
        registry: { registry_id: "registry-test" },
        t3_source: { state: "unavailable", reason },
      });
      expect(cp.state.writes).toEqual([]);
    }),
  ),
);

it.effect("a missing target is rejected only after a complete live coherent list", () =>
  withCP((cp) =>
    Effect.gen(function* () {
      cp.state.groups.delete("target");
      const service = yield* WorkstreamOperations;
      expect(yield* service.submit({ request, wait_ms: 3000 })).toMatchObject({
        state: "rejected",
        reason: "workstream_not_found",
      });
      expect(cp.state.membershipCommits).toBe(0);
    }),
  ),
);

it.effect("serializes writes and prioritizes app/CLI/agent ahead of queued writes", () =>
  withCP((cp) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const third = yield* Deferred.make<void>();
      let calls = 0;
      cp.state.beforeSend = () =>
        Effect.gen(function* () {
          if (++calls === 1) {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
          if (calls === 3) yield* Deferred.succeed(third, undefined);
        });
      const service = yield* WorkstreamOperations;
      const queueRequest = {
        ...createRequest,
        operation_id: "queued-create-00001",
        origin: { kind: "queue" as const, request_id: "queue-request" },
      };
      const cliRequest = {
        ...createRequest,
        operation_id: "cli-create-00000001",
        origin: { kind: "cli" as const },
      };
      yield* service.submit({ request: createRequest, wait_ms: 0 });
      yield* Deferred.await(entered);
      yield* service.submit({ request: queueRequest, wait_ms: 0 });
      yield* service.submit({ request: cliRequest, wait_ms: 0 });
      yield* TestClock.adjust(1);
      expect(cp.state.writes).toHaveLength(1);
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(third);
      expect(cp.state.writes.slice(0, 3).map((body) => JSON.parse(body).command_id)).toEqual([
        childId(createRequest, "cre"),
        childId(cliRequest, "cre"),
        childId(queueRequest, "cre"),
      ]);
      yield* TestClock.adjust(1000);
    }),
  ),
);
