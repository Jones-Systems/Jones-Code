import { expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Semaphore from "effect/Semaphore";
import {
  makeRuntimeOperationPermit,
  unavailableResidentOperationPermit,
  revalidateRuntimeOperationStartAdmission,
  RuntimePermitError,
  type RuntimeOperationIdentity,
  type ScopeObservation,
} from "./RuntimeOperationPermit.ts";

function fixture(nativeEffect: Effect.Effect<void, string> = Effect.void) {
  let current = true;
  let invoked = 0;
  let starts = 0;
  let readHook: Effect.Effect<void> = Effect.void;
  let observation: ScopeObservation = { state: "active", coverage: "complete" };
  const identity: RuntimeOperationIdentity = {
    issuerId: "issuer",
    operationId: "operation",
    admissionId: "admission",
    participantId: "participant",
    binding: {
      threadId: ThreadId.make("thread"),
      providerThreadId: ProviderThreadId.make("provider-thread"),
      providerSessionId: ProviderSessionId.make("session"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      driver: ProviderDriverKind.make("codex"),
      nativeThreadId: "native",
      runtimeGeneration: "generation",
    },
    evidenceRevision: 7,
    evidenceReference: "evidence",
    writerReferences: ["writer-one"],
  };
  const result = makeRuntimeOperationPermit({
    mode: "exclusive-operation",
    identity,
    isCurrent: Effect.sync(() => current),
    withCurrent: <V, F, R>(effect: Effect.Effect<V, F, R>) =>
      Effect.suspend((): Effect.Effect<V, F | RuntimePermitError, R> =>
        current ? effect : Effect.fail(new RuntimePermitError({ reason: "not_current" })),
      ),
    invoke: Effect.sync(() => {
      invoked += 1;
    }).pipe(Effect.andThen(nativeEffect)),
    observe: Effect.suspend(() => readHook.pipe(Effect.andThen(Effect.sync(() => observation)))),
  });
  if (result.status !== "available") throw new Error("fixture must be supported");
  return {
    permit: result.permit,
    identity,
    commit: Effect.sync(() => {
      starts += 1;
    }),
    counts: () => ({ invoked, starts }),
    onRead: (hook: Effect.Effect<void>) => {
      readHook = hook;
    },
    expire: () => {
      current = false;
    },
    observe: (next: ScopeObservation) => {
      observation = next;
    },
  };
}

it.effect("revoke before durable start proves only this participant not invoked", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.revoke;
    expect(Exit.isFailure(yield* f.permit.invoke(f.commit).pipe(Effect.exit))).toBe(true);
    expect(f.counts()).toEqual({ invoked: 0, starts: 0 });
    expect(yield* f.permit.observe).toMatchObject({
      outcome: "not_invoked",
      identity: { participantId: "participant", writerReferences: ["writer-one"] },
    });
  }),
);

it.effect("committed start followed by revoke is unknown, never root not_invoked", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.invoke(f.commit.pipe(Effect.andThen(f.permit.revoke))).pipe(Effect.exit);
    expect(f.counts()).toEqual({ invoked: 0, starts: 1 });
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
  }),
);

it.effect("failed durable-start attempt remains unknown and cannot be retried", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.invoke(Effect.fail("commit response lost")).pipe(Effect.exit);
    yield* f.permit.revoke;
    yield* f.permit.invoke(f.commit).pipe(Effect.exit);
    expect(f.counts()).toEqual({ invoked: 0, starts: 0 });
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
  }),
);

it.effect("confirmed invocation is single-use and active only while current", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.invoke(f.commit);
    expect(yield* f.permit.observe).toMatchObject({ outcome: "active", coverage: "complete" });
    expect(Exit.isFailure(yield* f.permit.invoke(f.commit).pipe(Effect.exit))).toBe(true);
    expect(f.counts()).toEqual({ invoked: 1, starts: 1 });
    f.expire();
    expect(Exit.isFailure(yield* f.permit.assertCurrent.pipe(Effect.exit))).toBe(true);
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
  }),
);

it.effect("retained complete quiescence survives permit expiry without granting invocation", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.invoke(f.commit);
    f.expire();
    yield* f.permit.revoke;
    f.observe({ state: "quiescent", coverage: "complete" });
    expect(yield* f.permit.observe).toMatchObject({ outcome: "quiescent", coverage: "complete" });
    expect(Exit.isFailure(yield* f.permit.assertCurrent.pipe(Effect.exit))).toBe(true);
  }),
);

it.effect("partial and escaped-descendant observations never retire a participant", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.invoke(f.commit);
    for (const state of ["active", "quiescent"] as const) {
      f.observe({ state, coverage: "partial" });
      expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown", coverage: "partial" });
    }
    f.observe({ state: "unknown" });
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
  }),
);

it.effect("interrupted or failed native invocation is unknown", () =>
  Effect.gen(function* () {
    for (const native of [Effect.interrupt, Effect.fail("native response lost")]) {
      const f = fixture(native);
      yield* f.permit.invoke(f.commit).pipe(Effect.exit);
      expect(f.counts()).toEqual({ invoked: 1, starts: 1 });
      expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
    }
  }),
);

it.effect("owner currentness is rechecked after durable start, before invocation", () =>
  Effect.gen(function* () {
    const f = fixture();
    yield* f.permit.invoke(f.commit.pipe(Effect.andThen(Effect.sync(f.expire)))).pipe(Effect.exit);
    expect(f.counts()).toEqual({ invoked: 0, starts: 1 });
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
  }),
);

it("warm, unsupported and restarted owners cannot issue current permits", () => {
  for (const mode of ["warm_runtime", "unsupported", "restart"] as const)
    expect(makeRuntimeOperationPermit({ mode })).toEqual({ status: "unavailable", reason: mode });
});

it("freezes exact coverage and binding; JSON retains no callable capability", () => {
  const f = fixture();
  expect(Object.isFrozen(f.permit.identity)).toBe(true);
  expect(Object.isFrozen(f.permit.identity.binding)).toBe(true);
  expect(Object.isFrozen(f.permit.identity.writerReferences)).toBe(true);
  expect(f.permit.identity.writerReferences).not.toBe(f.identity.writerReferences);
  expect(JSON.parse(JSON.stringify(f.permit)).invoke).toBeUndefined();
});

it.effect("an empty scope cannot retire an invocation that may still start", () =>
  Effect.gen(function* () {
    const f = fixture();
    f.observe({ state: "quiescent", coverage: "complete" });
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
    yield* f.permit.invoke(
      Effect.gen(function* () {
        expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
        yield* f.permit.invoke(f.commit).pipe(Effect.exit);
        expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
        yield* f.commit;
      }),
    );
    expect(f.counts()).toEqual({ invoked: 1, starts: 1 });
  }),
);

it.effect("an observation crossing invocation cannot retire from a stale empty scope", () =>
  Effect.gen(function* () {
    const f = fixture();
    f.observe({ state: "quiescent", coverage: "complete" });
    f.onRead(f.permit.invoke(f.commit).pipe(Effect.ignoreCause));
    expect(yield* f.permit.observe).toMatchObject({ outcome: "unknown" });
    expect(f.counts()).toEqual({ invoked: 1, starts: 1 });
  }),
);

it.effect("resident Codex and Claude never grant an operation writer capability", () =>
  Effect.sync(() => {
    for (const driver of ["codex", "claudeAgent"])
      expect(unavailableResidentOperationPermit(driver)).toEqual({
        status: "unavailable",
        reason: "warm_runtime",
      });
    for (const driver of [undefined, "grok", "opencode", "pi", "cursor", "antigravity"])
      expect(unavailableResidentOperationPermit(driver)).toEqual({
        status: "unavailable",
        reason: "unsupported",
      });
  }),
);

it.effect("in-lock admission preserves prior denial and rechecks expiry after it", () =>
  Effect.gen(function* () {
    const f = fixture();
    const rejected = yield* revalidateRuntimeOperationStartAdmission(
      f.permit,
      f.identity,
      Effect.fail("prior denial"),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(rejected)).toBe(true);
    const expired = yield* revalidateRuntimeOperationStartAdmission(
      f.permit,
      f.identity,
      Effect.sync(f.expire),
    ).pipe(Effect.exit);
    expect(Exit.isFailure(expired)).toBe(true);
    expect(f.counts()).toEqual({ invoked: 0, starts: 0 });
  }),
);

it.effect("admission rejects mismatched issuer, operation, generation, evidence and coverage", () =>
  Effect.gen(function* () {
    const f = fixture();
    const mismatches = [
      { ...f.identity, issuerId: "other" },
      { ...f.identity, operationId: "other" },
      { ...f.identity, admissionId: "other" },
      { ...f.identity, participantId: "other" },
      { ...f.identity, evidenceRevision: 8 },
      { ...f.identity, evidenceReference: "other" },
      { ...f.identity, writerReferences: ["other"] },
      { ...f.identity, binding: { ...f.identity.binding, runtimeGeneration: "other" } },
    ];
    let previousCalls = 0;
    for (const identity of mismatches) {
      const result = yield* revalidateRuntimeOperationStartAdmission(
        f.permit,
        identity,
        Effect.sync(() => {
          previousCalls += 1;
        }),
      ).pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
    }
    expect(previousCalls).toBe(0);
    yield* revalidateRuntimeOperationStartAdmission(f.permit, f.identity, Effect.void);
    yield* f.permit.revoke;
    expect(
      Exit.isFailure(
        yield* revalidateRuntimeOperationStartAdmission(f.permit, f.identity, Effect.void).pipe(
          Effect.exit,
        ),
      ),
    ).toBe(true);
  }),
);

it.effect("exclusive executor revalidates inside its existing lock without reacquiring it", () =>
  Effect.gen(function* () {
    const f = fixture();
    const lock = yield* Semaphore.make(1);
    let inside = false;
    let invoked = 0;
    const admission = revalidateRuntimeOperationStartAdmission(
      f.permit,
      f.identity,
      Effect.sync(() => {
        expect(inside).toBe(true);
      }),
    );
    yield* lock.withPermits(1)(
      Effect.gen(function* () {
        inside = true;
        yield* admission;
        invoked += 1;
        inside = false;
      }),
    );
    expect(invoked).toBe(1);
    yield* f.permit.revoke;
    inside = true;
    expect(Exit.isFailure(yield* lock.withPermits(1)(admission.pipe(Effect.exit)))).toBe(true);
    expect(f.counts()).toEqual({ invoked: 0, starts: 0 });
  }),
);
