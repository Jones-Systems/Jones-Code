import { assert, describe, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";

import type { ProviderAdapterV2Event } from "../../orchestration-v2/ProviderAdapter.ts";
import * as ProviderEventOrigin from "./ProviderEventOrigin.ts";

const driver = ProviderDriverKind.make("codex");
const currentCheck = (): Effect.Effect<void, ProviderEventOrigin.ProviderEventOriginStaleError> =>
  Effect.sync(() => undefined);
const owner = () => ({
  binding: {
    threadId: ThreadId.make("app-thread"),
    providerThreadId: ProviderThreadId.make("provider-thread"),
    providerSessionId: ProviderSessionId.make("provider-session"),
    providerInstanceId: ProviderInstanceId.make("codex-personal"),
    driver,
    runtimeGeneration: "native-incarnation",
    nativeThreadId: "native-thread",
  },
  runId: RunId.make("run"),
  attemptId: RunAttemptId.make("attempt"),
  providerTurnId: ProviderTurnId.make("provider-turn"),
});
const origin = () => ({
  producer: {
    token: { closed: false },
    driver,
    instanceId: ProviderInstanceId.make("codex-personal"),
    providerSessionId: ProviderSessionId.make("provider-session"),
    runtimeGeneration: "native-incarnation",
    revalidateCurrent: currentCheck(),
  },
  turn: owner(),
});
const event = () =>
  ({
    type: "turn.terminal",
    driver,
    providerThreadId: ProviderThreadId.make("provider-thread"),
    providerTurnId: ProviderTurnId.make("provider-turn"),
    runOrdinal: 1,
    status: "completed",
    failure: null,
    threadDisposition: "reusable",
  }) as const satisfies ProviderAdapterV2Event;

describe("provider event origin", () => {
  const runtime = {
    driver,
    instanceId: ProviderInstanceId.make("codex-personal"),
    providerSessionId: ProviderSessionId.make("provider-session"),
    eventOriginMode: "captured" as const,
  };

  it.effect("requires captured origins only for adopted runtimes", () =>
    Effect.gen(function* () {
      const raw = event();
      assert.equal(
        (yield* ProviderEventOrigin.revalidateProviderEventOrigin(raw, runtime).pipe(Effect.flip))._tag,
        "ProviderEventOriginStaleError",
      );
      yield* ProviderEventOrigin.revalidateProviderEventOrigin(raw, {
        driver: runtime.driver,
        instanceId: runtime.instanceId,
        providerSessionId: runtime.providerSessionId,
      });
    }),
  );

  it.effect("allows captured terminals to drain but rejects a replaced producer with the same generation", () =>
    Effect.gen(function* () {
      const identity = { ...runtime, runtimeGeneration: "native-incarnation" };
      const original = ProviderEventOrigin.makeProviderEventProducer(identity);
      const terminal = ProviderEventOrigin.stampProviderEvent(event(), { producer: original.origin });
      const effect = original.origin.revalidateCurrent;
      original.drain();
      assert.isFalse(original.accepting);
      yield* ProviderEventOrigin.revalidateProviderEventOrigin(terminal, runtime);
      original.retire();
      original.drain();
      const replacement = ProviderEventOrigin.makeProviderEventProducer(identity);
      assert.notStrictEqual(original.origin.token, replacement.origin.token);
      assert.strictEqual(original.origin.revalidateCurrent, effect);
      assert.equal(
        (yield* ProviderEventOrigin.revalidateProviderEventOrigin(terminal, runtime).pipe(Effect.flip))._tag,
        "ProviderEventOriginStaleError",
      );
      const fresh = ProviderEventOrigin.stampProviderEvent(event(), { producer: replacement.origin });
      yield* ProviderEventOrigin.revalidateProviderEventOrigin(fresh, runtime);
    }),
  );

  it.effect("rejects captured identity mismatches and mismatched wire evidence", () =>
    Effect.gen(function* () {
      const captured = origin();
      const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
      for (const changed of [
        { ...runtime, driver: ProviderDriverKind.make("claude") },
        { ...runtime, instanceId: ProviderInstanceId.make("other-instance") },
        { ...runtime, providerSessionId: ProviderSessionId.make("other-session") },
      ]) {
        assert.equal(
          (yield* ProviderEventOrigin.revalidateProviderEventOrigin(terminal, changed).pipe(Effect.flip))._tag,
          "ProviderEventOriginStaleError",
        );
      }
      const wrongEvidence = ProviderEventOrigin.stampProviderEvent({
        ...event(),
        runtimeEvidence: { ...captured.turn.binding, runtimeGeneration: "replacement" },
      }, captured);
      assert.equal(
        (yield* ProviderEventOrigin.revalidateProviderEventOrigin(wrongEvidence, runtime).pipe(Effect.flip))._tag,
        "ProviderEventOriginStaleError",
      );
    }),
  );
  it("preserves frozen event identity, own properties and serialized wire data", () => {
    const terminal = Object.freeze(event());
    const before = JSON.stringify(terminal);
    const keys = Reflect.ownKeys(terminal);
    assert.strictEqual(ProviderEventOrigin.stampProviderEvent(terminal, origin()), terminal);
    assert.deepEqual(Reflect.ownKeys(terminal), keys);
    assert.strictEqual(JSON.stringify(terminal), before);
    assert.isDefined(ProviderEventOrigin.readProviderEventOrigin(terminal));
    assert.isUndefined(ProviderEventOrigin.readProviderEventOrigin(JSON.parse(before)));
  });

  it("does not implicitly transfer provenance to a shallow clone", () => {
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), origin());
    const clone = { ...terminal };
    assert.isUndefined(ProviderEventOrigin.readProviderEventOrigin(clone));
    assert.strictEqual(ProviderEventOrigin.copyProviderEventOrigin(terminal, clone), clone);
    const originalOrigin = ProviderEventOrigin.readProviderEventOrigin(terminal)!;
    const copiedOrigin = ProviderEventOrigin.readProviderEventOrigin(clone)!;
    assert.strictEqual(copiedOrigin.producer.token, originalOrigin.producer.token);
    assert.strictEqual(
      copiedOrigin.producer.revalidateCurrent,
      originalOrigin.producer.revalidateCurrent,
    );
    assert.deepEqual(copiedOrigin.turn, originalOrigin.turn);
  });

  it("does not invent a source when filtering an unstamped event", () => {
    const raw = event();
    const filtered = { ...raw };
    assert.strictEqual(ProviderEventOrigin.copyProviderEventOrigin(raw, filtered), filtered);
    assert.isUndefined(ProviderEventOrigin.readProviderEventOrigin(filtered));
  });

  it("allows repeated identical association without replacing the original snapshot", () => {
    const captured = origin();
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    const snapshot = ProviderEventOrigin.readProviderEventOrigin(terminal);
    assert.strictEqual(
      ProviderEventOrigin.stampProviderEvent(terminal, {
        producer: { ...captured.producer },
        turn: { ...captured.turn, binding: { ...captured.turn.binding } },
      }),
      terminal,
    );
    assert.strictEqual(ProviderEventOrigin.readProviderEventOrigin(terminal), snapshot);
    assert.strictEqual(ProviderEventOrigin.copyProviderEventOrigin(terminal, terminal), terminal);
  });

  it("freezes identity and turn copies without freezing producer-owned token or Effect internals", () => {
    const captured = origin();
    const effectWasFrozen = Object.isFrozen(captured.producer.revalidateCurrent);
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    const snapshot = ProviderEventOrigin.readProviderEventOrigin(terminal)!;
    captured.producer.runtimeGeneration = "later-incarnation";
    captured.turn.binding.nativeThreadId = "later-native-thread";
    captured.turn.runId = RunId.make("later-run");
    captured.producer.token.closed = true;
    assert.strictEqual(snapshot.producer.runtimeGeneration, "native-incarnation");
    assert.strictEqual(snapshot.turn?.binding.nativeThreadId, "native-thread");
    assert.strictEqual(snapshot.turn?.runId, RunId.make("run"));
    assert.strictEqual(snapshot.producer.token, captured.producer.token);
    assert.isTrue(captured.producer.token.closed);
    assert.isFalse(Object.isFrozen(captured.producer.token));
    assert.strictEqual(Object.isFrozen(captured.producer.revalidateCurrent), effectWasFrozen);
    assert.isTrue(Object.isFrozen(snapshot));
    assert.isTrue(Object.isFrozen(snapshot.producer));
    assert.isTrue(Object.isFrozen(snapshot.turn));
    assert.isTrue(Object.isFrozen(snapshot.turn?.binding));
  });

  it("rejects a replacement source even if its public generation and identity are unchanged", () => {
    const captured = origin();
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        ProviderEventOrigin.stampProviderEvent(terminal, {
          ...captured,
          producer: { ...captured.producer, token: {} },
        }),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
    assert.strictEqual(
      ProviderEventOrigin.readProviderEventOrigin(terminal)?.producer.token,
      captured.producer.token,
    );
  });

  it("rejects a different generation on the same producer token", () => {
    const captured = origin();
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        ProviderEventOrigin.stampProviderEvent(terminal, {
          ...captured,
          producer: { ...captured.producer, runtimeGeneration: "replacement" },
        }),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
  });

  it("rejects different turn ownership without silently replacing the captured attempt", () => {
    const captured = origin();
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        ProviderEventOrigin.stampProviderEvent(terminal, {
          ...captured,
          turn: { ...captured.turn, attemptId: RunAttemptId.make("replacement-attempt") },
        }),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
    assert.strictEqual(
      ProviderEventOrigin.readProviderEventOrigin(terminal)?.turn?.attemptId,
      captured.turn.attemptId,
    );
  });

  it.each([
    { field: "driver", binding: { ...owner().binding, driver: ProviderDriverKind.make("claude") } },
    {
      field: "providerInstanceId",
      binding: { ...owner().binding, providerInstanceId: ProviderInstanceId.make("replacement") },
    },
  ])("rejects changed $field in the captured turn binding", ({ binding }) => {
    const captured = origin();
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        ProviderEventOrigin.stampProviderEvent(terminal, {
          ...captured,
          turn: { ...captured.turn, binding },
        }),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
    assert.deepEqual(
      ProviderEventOrigin.readProviderEventOrigin(terminal)?.turn?.binding,
      captured.turn.binding,
    );
  });

  it("rejects substituting another currentness Effect on an already queued event", () => {
    const captured = origin();
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        ProviderEventOrigin.stampProviderEvent(terminal, {
          ...captured,
          producer: {
            ...captured.producer,
            revalidateCurrent: Effect.fail(new ProviderEventOrigin.ProviderEventOriginStaleError({})),
          },
        }),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
  });

  it("keeps runless source metadata without inventing a turn or runtime generation", () => {
    const captured = origin();
    const runless: ProviderEventOrigin.ProviderEventOrigin = {
      producer: {
        token: captured.producer.token,
        driver,
        instanceId: captured.producer.instanceId,
        providerSessionId: captured.producer.providerSessionId,
        revalidateCurrent: captured.producer.revalidateCurrent,
      },
    };
    const terminal = ProviderEventOrigin.stampProviderEvent(event(), runless);
    const snapshot = ProviderEventOrigin.readProviderEventOrigin(terminal)!;
    assert.isFalse("turn" in snapshot);
    assert.isFalse("runtimeGeneration" in snapshot.producer);
    assert.throws(
      () => ProviderEventOrigin.stampProviderEvent(terminal, { ...runless, turn: captured.turn }),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
  });

  it("rejects copying over a derived event that already belongs to another source", () => {
    const source = ProviderEventOrigin.stampProviderEvent(event(), origin());
    const other = origin();
    const derived = ProviderEventOrigin.stampProviderEvent({ ...source }, other);
    assert.throws(
      () => ProviderEventOrigin.copyProviderEventOrigin(source, derived),
      ProviderEventOrigin.ProviderEventOriginConflictError,
    );
    assert.strictEqual(
      ProviderEventOrigin.readProviderEventOrigin(derived)?.producer.token,
      other.producer.token,
    );
  });

  it.effect("does not run the captured currentness check when stamping, reading or copying", () =>
    Effect.gen(function* () {
      const captured = origin();
      let checkCount = 0;
      captured.producer.revalidateCurrent = Effect.sync(() => {
        checkCount += 1;
      });
      const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
      const snapshot = ProviderEventOrigin.readProviderEventOrigin(terminal)!;
      const clone = ProviderEventOrigin.copyProviderEventOrigin(terminal, { ...terminal });
      assert.strictEqual(
        ProviderEventOrigin.readProviderEventOrigin(clone)?.producer.revalidateCurrent,
        captured.producer.revalidateCurrent,
      );
      assert.equal(checkCount, 0);
      yield* snapshot.producer.revalidateCurrent;
      assert.equal(checkCount, 1);
    }),
  );

  it.effect("retains one event object and captured source across independent fanout queues", () =>
    Effect.gen(function* () {
      const first = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const second = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const terminal = ProviderEventOrigin.stampProviderEvent(event(), origin());
      yield* Queue.offer(first, terminal);
      yield* Queue.offer(second, terminal);
      const firstReceived = yield* Queue.take(first);
      const secondReceived = yield* Queue.take(second);
      assert.strictEqual(firstReceived, terminal);
      assert.strictEqual(secondReceived, terminal);
      assert.strictEqual(
        ProviderEventOrigin.readProviderEventOrigin(firstReceived),
        ProviderEventOrigin.readProviderEventOrigin(secondReceived),
      );
    }),
  );

  it.effect("rejects an old event queued across two boundaries after actual source supersession", () =>
    Effect.gen(function* () {
      const adapterQueue = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const managerQueue = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const old = origin();
      let currentSource: object = old.producer.token;
      old.producer.revalidateCurrent = Effect.suspend(() =>
        currentSource === old.producer.token
          ? Effect.void
          : Effect.fail(new ProviderEventOrigin.ProviderEventOriginStaleError({})),
      );
      const terminal = ProviderEventOrigin.stampProviderEvent(event(), old);
      yield* Queue.offer(adapterQueue, terminal);
      const managerReceived = yield* Queue.take(adapterQueue);
      yield* Queue.offer(managerQueue, managerReceived);
      currentSource = {};
      const consumerReceived = yield* Queue.take(managerQueue);
      const captured = ProviderEventOrigin.readProviderEventOrigin(consumerReceived)!;
      assert.strictEqual(consumerReceived, terminal);
      assert.strictEqual(captured.producer.token, old.producer.token);
      assert.isTrue(Exit.isFailure(yield* captured.producer.revalidateCurrent.pipe(Effect.exit)));
      const fresh = ProviderEventOrigin.stampProviderEvent(event(), {
        producer: {
          ...old.producer,
          token: currentSource,
          runtimeGeneration: "fresh-incarnation",
          revalidateCurrent: Effect.void,
        },
      });
      assert.isTrue(
        Exit.isSuccess(
          yield* ProviderEventOrigin.readProviderEventOrigin(fresh)!.producer.revalidateCurrent.pipe(
            Effect.exit,
          ),
        ),
      );
    }),
  );

  it.effect("allows terminal draining after normal producer exit while its captured source remains current", () =>
    Effect.gen(function* () {
      const captured = origin();
      const actualSource = captured.producer.token;
      let currentSource: object = actualSource;
      captured.producer.revalidateCurrent = Effect.suspend(() =>
        currentSource === actualSource
          ? Effect.void
          : Effect.fail(new ProviderEventOrigin.ProviderEventOriginStaleError({})),
      );
      const terminal = ProviderEventOrigin.stampProviderEvent(event(), captured);
      actualSource.closed = true;
      assert.isTrue(
        Exit.isSuccess(
          yield* ProviderEventOrigin.readProviderEventOrigin(terminal)!.producer.revalidateCurrent.pipe(
            Effect.exit,
          ),
        ),
      );
      currentSource = {};
      assert.isTrue(
        Exit.isFailure(
          yield* ProviderEventOrigin.readProviderEventOrigin(terminal)!.producer.revalidateCurrent.pipe(
            Effect.exit,
          ),
        ),
      );
    }),
  );
});
