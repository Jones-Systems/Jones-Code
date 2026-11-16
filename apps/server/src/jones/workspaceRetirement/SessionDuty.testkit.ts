import * as NodeAssert from "node:assert/strict";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { TestClock } from "effect/testing";
import migration104 from "../persistence/Migrations/104_JonesDeletionAdmission.ts";
import migration138 from "../persistence/Migrations/138_JonesWorkspaceRetirement.ts";
import migration156 from "../persistence/Migrations/156_JonesWorkspaceDutyLifetimes.ts";
import { createRuntimeUseSchema } from "../cleanup/RuntimeUseSchema.ts";
import { makeRuntimeUseStore } from "../cleanup/RuntimeUseStore.ts";
import { canonical, digest, type RuntimeUseBinding } from "../cleanup/RuntimeUseProtocol.ts";
import { bindOwner, linkLifetime, retiredLifetime } from "./DutyLifetimeLink.ts";
import { ClaimAbsence, reconcile, type PersistedDuty } from "./ClaimReconciliation.ts";
import { Receipts, layer, RetirementError } from "./Receipts.ts";
import { NativeDutyOwnerJson } from "./DurableCodecs.ts";
import {
  captureNativeOwner,
  observeNativeOwner,
  unresolvedOwner,
  type NativeDutyOwner,
  type NativeOwnerIO,
} from "./NativeDutyOwner.ts";
import * as DutyRelease from "./DutyRelease.ts";
import * as Duty from "./Duty.ts";
import migration108 from "../persistence/Migrations/108_JonesPhysicalProcessEvidence.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const fixture = <A, E, R>(effect: Effect.Effect<A, E, R | SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    yield* migration104;
    yield* migration138;
    yield* migration156;
    yield* createRuntimeUseSchema;
    return yield* effect;
  }).pipe(Effect.provide(memory));
const decodeOwnerJson = Schema.decodeSync(NativeDutyOwnerJson);
const decodeOwnerReason = Schema.decodeSync(
  Schema.fromJsonString(Schema.Struct({ reason: Schema.String })),
);
const encodeOwnerJson = Schema.encodeEffect(NativeDutyOwnerJson);
const ownerType = (json: string) => decodeOwnerJson(json).type;
const ownerReason = (json: string) => decodeOwnerReason(json).reason;
const host = {
  host: "fixture",
  boot_id: "boot",
  pid: 200,
  process_start: "start",
  process_namespace: "namespace",
};
const native: NativeDutyOwner = {
  schema: "workspace-duty-owner/v1",
  type: "native-session",
  host,
  pid: 201,
  start: "child-start",
  session: "child-session",
};
const claim = (id = "claim"): PersistedDuty => ({
  claim_id: id,
  worktree: "/synthetic/checkout",
  kind: "session",
  owner: "session",
  holder_json: canonical({ nonce: id }),
  native_owner_json: null,
});
const binding = (useId = "open"): RuntimeUseBinding => ({
  schema: "jones.workspace-runtime-use/v1",
  useId,
  canonicalPath: "/synthetic/checkout",
  threadId: "thread",
  directoryIdentity: { version: 1, device: "device", inode: "inode", birth: "birth" },
  actor: {
    kind: "provider_open",
    provider: { sessionId: "session", instanceId: "codex", driver: "codex" },
  },
  original: {
    commandId: null,
    runId: null,
    attemptId: null,
    outboxReference: null,
    nativeReference: null,
    claim: null,
  },
});
const insert = Effect.fn("SessionDuty.testInsert")(function* (c: PersistedDuty) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_workspace_duty_claims VALUES (${c.claim_id},${c.worktree},NULL,${c.kind},${c.owner},${c.holder_json},'2026-11-16T00:00:00Z',NULL)`;
  yield* sql`INSERT INTO jones_workspace_duty_identity VALUES (${c.claim_id},'{}',${yield* encodeOwnerJson(unresolvedOwner("session_native_launch_capture_pending", host))})`;
});
const handle = (c: PersistedDuty) => ({ id: c.claim_id, holder: c.holder_json, owner: c.owner });
const notInvoked = (b: RuntimeUseBinding) => ({
  schema: "jones.workspace-runtime-retirement/v1",
  useId: b.useId,
  bindingSha256: digest(b),
  outcome: "not_invoked",
  observationCount: 0,
});
const proof = (
  b: RuntimeUseBinding,
  producers: readonly { producerId: string; generation: string; threadId: string }[],
) => ({
  schema: "jones.workspace-runtime-retirement/v1",
  useId: b.useId,
  bindingSha256: digest(b),
  outcome: "producers_exited",
  observationCount: producers.length,
  nativeBindings: [],
  producers: producers.map((p) => ({
    ...p,
    observationSha256: digest({
      ...p,
      schema: "jones.workspace-runtime-producer/v1",
      useId: b.useId,
      bindingSha256: digest(b),
      writerSetSha256: digest([]),
      coverage: "complete",
    }),
    driver: "codex",
    instanceId: "codex",
    providerSessionId: "session",
    pid: 201,
    scopeUnit: "captured.scope",
    writerBindingsSha256: digest([]),
  })),
});
export const sessionDutyCases = {
  "exact old and reopened claims never borrow binding or retirement; renewal permanently downgrades ownership":
    async () =>
      Effect.runPromise(
        fixture(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const old = claim("old"),
              next = claim("next");
            yield* insert(old);
            yield* insert(next);
            yield* bindOwner(handle(old), native);
            yield* bindOwner(handle(old), { ...native, pid: 202, start: "next-start" });
            yield* bindOwner(handle(old), native);
            const rows = yield* sql<{
              claim_id: string;
              native_owner_json: string;
            }>`SELECT * FROM jones_workspace_duty_identity ORDER BY claim_id`;
            NodeAssert.equal(
              ownerReason(rows[0]!.native_owner_json),
              "session_native_launch_capture_pending",
            );
            NodeAssert.equal(
              ownerReason(rows[1]!.native_owner_json),
              "multiple_producer_generations",
            );
            const store = yield* makeRuntimeUseStore;
            const b = binding();
            yield* store.reserve(b);
            yield* linkLifetime(handle(old), b);
            yield* store.recordRetirement(notInvoked(b));
            const receipts = yield* Receipts.pipe(Effect.provide(layer));
            const result = yield* reconcile(sql, receipts).pipe(
              Effect.provideService(ClaimAbsence, {
                observe: (c) =>
                  Effect.succeed({ holder: c.holder_json, status: "unknown" as const }),
              }),
            );
            NodeAssert.equal(result.released, 1);
            NodeAssert.deepEqual(
              (yield* receipts.claims(old.worktree)).map((c) => c.claim_id),
              ["next"],
            );
          }),
        ),
      ),
  "native lifecycle binds the immutable open handle and distinct generations never overwrite predecessors":
    async () =>
      Effect.runPromise(
        fixture(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* migration108;
            const duty = yield* Duty.hooks.pipe(
              Effect.provideService(Duty.NativeOwnerCapture, { capture: async () => native }),
            );
            const old = yield* duty.hold(
              "/synthetic/nonexistent-session-duty",
              "session",
              "logical-session",
            );
            const reopened = yield* duty.hold(
              "/synthetic/nonexistent-session-duty",
              "session",
              "logical-session",
            );
            const physical = {
              status: "observed",
              runtimeGeneration: "generation-1",
              launcherPid: 201,
              responder: {
                platform: "linux",
                bootId: "boot",
                pidNamespace: "namespace",
                pid: 201,
                startTime: "1",
                startTimeUnit: "linux-clock-ticks-since-boot",
                executableIdentity: "synthetic",
                build: "unknown",
              },
              source: "codex.server/diagnostics+local-os",
            };
            yield* sql`INSERT INTO jones_physical_process_evidence (provider_thread_id,thread_id,runtime_generation,observed_at,observation_json) VALUES ('provider-thread','thread','generation-1','2026-11-16',${canonical(physical)})`;
            yield* duty.bindSession(old, "provider-thread", "generation-1");
            const owners = yield* sql<{
              native_owner_json: string;
            }>`SELECT native_owner_json FROM jones_workspace_duty_identity ORDER BY rowid`;
            NodeAssert.equal(ownerType(owners[0]!.native_owner_json), "native-session");
            NodeAssert.equal(ownerType(owners[1]!.native_owner_json), "unresolved");
            // Renewal has no usable physical observation. It must still invalidate the
            // predecessor's single-process shortcut, including later old callbacks.
            yield* duty.bindSession(old, "provider-thread", "generation-2");
            yield* duty.bindSession(old, "provider-thread", "generation-1");
            const after = yield* sql<{
              native_owner_json: string;
            }>`SELECT native_owner_json FROM jones_workspace_duty_identity ORDER BY rowid`;
            NodeAssert.equal(
              ownerReason(after[0]!.native_owner_json),
              "multiple_producer_generations",
            );
            yield* old;
            const remaining = yield* sql<{
              claim_id: string;
            }>`SELECT claim_id FROM jones_workspace_duty_claims WHERE released_at IS NULL`;
            NodeAssert.equal(remaining.length, 1);
            NodeAssert.equal(duty.claimed(reopened), true);
            yield* reopened;
          }),
        ),
      ),
  "restart consumes only an exact validated journal and never falls back to weaker owner absence":
    async () =>
      Effect.runPromise(
        fixture(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const journal = yield* makeRuntimeUseStore;
            // Corruption fixtures deliberately bypass only the two SQL admission guards
            // in this disposable in-memory database; production read validation remains.
            yield* sql`DROP TRIGGER jones_workspace_runtime_retirement_coverage`;
            yield* sql`DROP TRIGGER jones_workspace_runtime_events_immutable`;
            const modes = [
              "not-invoked",
              "all-producers",
              "pending",
              "holder",
              "binding",
              "owner",
              "zero",
              "corrupt",
              "missing",
              "shared-sibling",
              "recreated",
            ];
            for (const mode of modes) {
              const c = claim(mode),
                b = binding(mode);
              yield* insert(c);
              yield* journal.reserve(b);
              yield* linkLifetime(handle(c), b);
              if (mode === "all-producers" || mode === "shared-sibling") {
                const producers = ["predecessor", "successor"].map((id) => ({
                  producerId: id,
                  generation: id,
                  threadId: id,
                }));
                for (const p of producers)
                  yield* journal.observe({
                    ...p,
                    schema: "jones.workspace-runtime-producer/v1",
                    useId: b.useId,
                    bindingSha256: digest(b),
                    writerSetSha256: digest([]),
                    coverage: "complete",
                  });
                if (mode === "all-producers") yield* journal.recordRetirement(proof(b, producers));
              } else if (!["pending", "zero", "missing", "recreated"].includes(mode))
                yield* journal.recordRetirement(notInvoked(b));
              if (mode === "holder")
                yield* sql`UPDATE jones_workspace_duty_lifetimes SET holder_sha256=${"0".repeat(64)} WHERE claim_id=${mode}`;
              if (mode === "binding")
                yield* sql`UPDATE jones_workspace_duty_lifetimes SET binding_sha256=${"0".repeat(64)} WHERE claim_id=${mode}`;
              if (mode === "owner")
                yield* sql`UPDATE jones_workspace_duty_claims SET owner='different' WHERE claim_id=${mode}`;
              if (mode === "corrupt")
                yield* sql`UPDATE jones_workspace_runtime_use_events SET evidence_sha256=${"0".repeat(64)} WHERE use_id=${mode}`;
              if (mode === "missing")
                yield* sql`UPDATE jones_workspace_duty_lifetimes SET use_id='absent' WHERE claim_id=${mode}`;
              if (mode === "zero") {
                const zero = proof(b, []);
                // Bypass the write validator to model corrupt historical SQL.
                yield* sql`INSERT INTO jones_workspace_runtime_use_events VALUES (${mode},1,'retired',${digest(b)},${canonical(zero)},${digest(zero)})`;
              }
              if (mode === "recreated")
                yield* sql`UPDATE jones_workspace_duty_claims SET holder_json='"replacement-holder"' WHERE claim_id=${mode}`;
            }
            const receipts = yield* Receipts.pipe(Effect.provide(layer));
            const result = yield* reconcile(sql, receipts).pipe(
              Effect.provideService(ClaimAbsence, {
                observe: () => Effect.die("Linked claims must bypass single-PID observation"),
              }),
            );
            NodeAssert.equal(result.released, 2);
            NodeAssert.equal(
              (yield* receipts.claims("/synthetic/checkout")).length,
              modes.length - 2,
            );
          }),
        ),
      ),
  "failed link remains conservative and write-once links cannot be replaced": async () =>
    Effect.runPromise(
      fixture(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const c = claim();
          yield* insert(c);
          const b = binding();
          const store = yield* makeRuntimeUseStore;
          yield* store.reserve(b);
          yield* sql`CREATE TRIGGER fail_link BEFORE INSERT ON jones_workspace_duty_lifetimes BEGIN SELECT RAISE(FAIL,'synthetic failure'); END`;
          NodeAssert.equal(Exit.isFailure(yield* Effect.exit(linkLifetime(handle(c), b))), true);
          NodeAssert.equal(yield* retiredLifetime(c), null);
          yield* sql`DROP TRIGGER fail_link`;
          yield* linkLifetime(handle(c), b);
          yield* linkLifetime(handle(c), binding("different"));
          NodeAssert.equal(yield* retiredLifetime(c), false);
          yield* store.recordRetirement(notInvoked(b));
          NodeAssert.equal(yield* retiredLifetime(c), true);
        }),
      ),
    ),
  "unresolved host codec is compatible and only a verified new boot proves absence": async () => {
    const old = unresolvedOwner("forwarder_or_unknown_launch");
    const current = unresolvedOwner("multiple_producer_generations", host);
    const encode = Schema.encodeSync(NativeDutyOwnerJson),
      decode = Schema.decodeSync(NativeDutyOwnerJson);
    NodeAssert.deepEqual(decode(encode(old)), old);
    NodeAssert.deepEqual(decode(encode(current)), current);
    // The previous Struct codec ignores additive fields when reading new records.
    const legacy = Schema.fromJsonString(
      Schema.Struct({
        schema: Schema.Literal("workspace-duty-owner/v1"),
        type: Schema.Literal("unresolved"),
        reason: Schema.String,
        nextAction: Schema.String,
      }),
    );
    NodeAssert.equal(
      Schema.decodeSync(legacy)(encode(current)).reason,
      "multiple_producer_generations",
    );
    const io: NativeOwnerIO = {
      host: async () => host,
      processes: async () => {
        throw Error("never use PID absence for unresolved");
      },
      linuxIdentity: async () => ({ bootId: "boot", startTime: "1" }),
    };
    NodeAssert.equal(await observeNativeOwner(old, io), "unknown");
    NodeAssert.equal(await observeNativeOwner(current, io), "unknown");
    NodeAssert.equal(
      await observeNativeOwner(current, { ...io, host: async () => ({ ...host, boot_id: "new" }) }),
      "verified_absent",
    );
    NodeAssert.equal(
      await observeNativeOwner(current, {
        ...io,
        host: async () => ({ ...host, boot_id: "new", host: "foreign" }),
      }),
      "unknown",
    );
    NodeAssert.equal(
      await observeNativeOwner(current, {
        ...io,
        host: async () => ({ ...host, boot_id: "new", process_namespace: "foreign" }),
      }),
      "unknown",
    );
    NodeAssert.equal(
      await observeNativeOwner(native, {
        ...io,
        processes: async () => "201 reused-session reused-start",
      }),
      "unknown",
    );
    NodeAssert.equal(
      (await captureNativeOwner(201, { bootId: "boot", startTime: "different" }, io)).type,
      "unresolved",
    );
  },
  "interrupted acquisition registers the exact handle before the open failure handler runs":
    async () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const acquired = yield* Deferred.make<void>(),
              resume = yield* Deferred.make<void>();
            const registered = new Set<object>();
            const exactHandle = {};
            let released = false,
              invoked = false;
            const opening = yield* DutyRelease.withRegisteredOpen(
              Deferred.succeed(acquired, undefined).pipe(
                Effect.andThen(Deferred.await(resume)),
                Effect.as(exactHandle),
              ),
              (handle) => {
                registered.add(handle);
                return Effect.sync(() => {
                  invoked = true;
                });
              },
              (handle) =>
                Effect.sync(() => {
                  NodeAssert.equal(registered.has(handle), true);
                  registered.delete(handle);
                  released = true;
                }),
            ).pipe(Effect.forkChild);
            yield* Deferred.await(acquired);
            const interrupting = yield* Fiber.interrupt(opening).pipe(Effect.forkChild);
            yield* Effect.yieldNow;
            yield* Deferred.succeed(resume, undefined);
            yield* Fiber.join(interrupting);
            NodeAssert.equal(released, true);
            NodeAssert.equal(invoked, false);
            NodeAssert.equal(registered.size, 0);
          }),
        ),
      ),
  "manager shutdown interrupts persistence backoff inherited from an uninterruptible close":
    async () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.uninterruptible(
            Effect.gen(function* () {
              const releases = yield* DutyRelease.make;
              const handle = {};
              const attempted = yield* Deferred.make<void>();
              releases.register(
                handle,
                Deferred.succeed(attempted, undefined).pipe(
                  Effect.andThen(
                    new RetirementError({ target: "fixture", reason: "sql_unavailable" }),
                  ),
                ),
              );
              yield* releases.drained([handle]);
              yield* Deferred.await(attempted);
            }),
          ),
        ),
      ),
  "SQL release failures retry independently after a timed-out native close without repeating close":
    async () =>
      Effect.runPromise(
        fixture(
          Effect.scoped(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              const receipts = yield* Receipts.pipe(Effect.provide(layer));
              const releases = yield* DutyRelease.make;
              const first = {},
                sibling = {},
                reopened = {};
              yield* insert(claim("first"));
              yield* insert(claim("sibling"));
              yield* insert(claim("reopened"));
              yield* sql`CREATE TRIGGER fail_first BEFORE UPDATE ON jones_workspace_duty_claims WHEN OLD.claim_id='first' BEGIN SELECT RAISE(FAIL,'synthetic SQL outage'); END`;
              let firstAttempts = 0,
                siblingAttempts = 0,
                reopenedAttempts = 0;
              const attempted = yield* Deferred.make<void>(),
                done = yield* Deferred.make<void>(),
                siblingDone = yield* Deferred.make<void>();
              releases.register(
                first,
                Effect.sync(() => {
                  firstAttempts++;
                }).pipe(
                  Effect.andThen(receipts.releaseClaim("first")),
                  Effect.tapError(() => Deferred.succeed(attempted, undefined)),
                  Effect.tap(() => Deferred.succeed(done, undefined)),
                ),
              );
              releases.register(
                sibling,
                receipts.releaseClaim("sibling").pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      siblingAttempts++;
                    }),
                  ),
                  Effect.tap(() => Deferred.succeed(siblingDone, undefined)),
                ),
              );
              releases.register(
                reopened,
                receipts.releaseClaim("reopened").pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      reopenedAttempts++;
                    }),
                  ),
                ),
              );
              const nativeDrain = yield* Deferred.make<void>();
              let closeCount = 0;
              const closing = yield* Effect.sync(() => {
                closeCount++;
              }).pipe(
                Effect.andThen(Deferred.await(nativeDrain)),
                Effect.tap(() => releases.drained([first, sibling])),
                Effect.forkChild,
              );
              // This is the manager's non-cancelling timeout: the native close continues.
              const waiting = yield* Fiber.join(closing).pipe(
                Effect.timeoutOption("5 seconds"),
                Effect.forkChild,
              );
              yield* TestClock.adjust("5 seconds");
              NodeAssert.equal((yield* Fiber.join(waiting))._tag, "None");
              yield* releases.retry;
              NodeAssert.equal(firstAttempts, 0);
              yield* Deferred.succeed(nativeDrain, undefined);
              yield* Fiber.join(closing);
              yield* Deferred.await(attempted);
              yield* Deferred.await(siblingDone);
              NodeAssert.equal(siblingAttempts, 1);
              NodeAssert.equal(reopenedAttempts, 0);
              yield* sql`DROP TRIGGER fail_first`;
              yield* releases.retry;
              yield* TestClock.adjust("250 millis");
              yield* Deferred.await(done);
              yield* releases.drained([first, sibling]);
              yield* releases.retry;
              NodeAssert.equal(closeCount, 1);
              NodeAssert.equal(firstAttempts, 2);
              NodeAssert.equal(siblingAttempts, 1);
              NodeAssert.equal(reopenedAttempts, 0);
              NodeAssert.deepEqual(
                (yield* receipts.claims("/synthetic/checkout")).map((c) => c.claim_id),
                ["reopened"],
              );
            }),
          ),
        ).pipe(Effect.provide(TestClock.layer())),
      ),
};
