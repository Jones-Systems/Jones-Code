import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";

import type { ProviderAdapterV2Event } from "./ProviderAdapter.ts";
import {
  copyProviderEventOrigin,
  ProviderEventOriginConflictError,
  readProviderEventOrigin,
  stampProviderEvent,
  type ProviderEventOrigin,
} from "./ProviderEventOrigin.ts";

const driver = ProviderDriverKind.make("codex");
const currentCheck = (): Effect.Effect<void, string> => Effect.sync(() => {});
const owner = () => ({
  binding: {
    threadId: ThreadId.make("app-thread"),
    providerThreadId: ProviderThreadId.make("provider-thread"),
    providerSessionId: ProviderSessionId.make("provider-session"),
    instanceId: ProviderInstanceId.make("codex-personal"),
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

const derivationOrigin = () => {
  const captured = origin();
  const claude = ProviderDriverKind.make("claudeAgent");
  const producer = {
    ...captured.producer,
    driver: claude,
    instanceId: ProviderInstanceId.make("claude-personal"),
  };
  const historical = {
    ...producer,
    token: { closed: true },
    runtimeGeneration: "historical-query",
    revalidateCurrent: Effect.fail("The historical source is obsolete."),
  };
  const result = stampProviderEvent({ type: "result" }, { producer });
  const notification = stampProviderEvent({ type: "task_notification" }, { producer: historical });
  return {
    producer,
    derivation: {
      kind: "claude_buffered_subagent_completion" as const,
      executor: {
        ...captured.turn,
        binding: { ...captured.turn.binding, instanceId: producer.instanceId },
      },
      result: { token: result, producer, nativeThreadId: "native-thread" },
      notification: {
        token: notification,
        producer: historical,
        nativeThreadId: "native-thread",
        nativeTaskId: "known-task",
        toolUseId: "known-tool",
        summary: "The historical task completed.",
        status: "completed" as const,
      },
      subject: {
        subagentId: NodeId.make("known-subagent"),
        parentThreadId: ThreadId.make("app-thread"),
        runId: RunId.make("historical-run"),
        parentNodeId: NodeId.make("historical-parent"),
        providerThreadId: ProviderThreadId.make("provider-thread"),
        childThreadId: ThreadId.make("known-child"),
        childRootNodeId: NodeId.make("known-child-root"),
        nativeTaskRef: {
          driver: claude,
          nativeId: "known-task",
          strength: "strong" as const,
          fingerprint: "task-source",
          ordinal: 1,
        },
        startedAt: DateTime.makeUnsafe(1_000),
        expectedUpdatedAt: DateTime.makeUnsafe(2_000),
      },
      childResult: {
        messageId: MessageId.make("known-result-message"),
        turnItemId: TurnItemId.make("known-result-item"),
        nativeItemRef: {
          driver: claude,
          nativeId: "known-result",
          strength: "strong" as const,
          fingerprint: "result-source",
          ordinal: 2,
        },
      },
      revalidateDerivation: currentCheck(),
    },
  } satisfies ProviderEventOrigin;
};

describe("provider event origin", () => {
  it("preserves frozen event identity, own properties and serialized wire data", () => {
    const terminal = Object.freeze(event());
    const before = JSON.stringify(terminal);
    const keys = Reflect.ownKeys(terminal);
    assert.strictEqual(stampProviderEvent(terminal, origin()), terminal);
    assert.deepEqual(Reflect.ownKeys(terminal), keys);
    assert.strictEqual(JSON.stringify(terminal), before);
    assert.isDefined(readProviderEventOrigin(terminal));
    assert.isUndefined(readProviderEventOrigin(JSON.parse(before)));
  });

  it("does not implicitly transfer provenance to a shallow clone", () => {
    const terminal = stampProviderEvent(event(), origin());
    const clone = { ...terminal };
    assert.isUndefined(readProviderEventOrigin(clone));
    assert.strictEqual(copyProviderEventOrigin(terminal, clone), clone);
    const originalOrigin = readProviderEventOrigin(terminal)!;
    const copiedOrigin = readProviderEventOrigin(clone)!;
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
    assert.strictEqual(copyProviderEventOrigin(raw, filtered), filtered);
    assert.isUndefined(readProviderEventOrigin(filtered));
  });

  it("allows repeated identical association without replacing the original snapshot", () => {
    const captured = origin();
    const terminal = stampProviderEvent(event(), captured);
    const snapshot = readProviderEventOrigin(terminal);
    assert.strictEqual(
      stampProviderEvent(terminal, {
        producer: { ...captured.producer },
        turn: { ...captured.turn, binding: { ...captured.turn.binding } },
      }),
      terminal,
    );
    assert.strictEqual(readProviderEventOrigin(terminal), snapshot);
    assert.strictEqual(copyProviderEventOrigin(terminal, terminal), terminal);
  });

  it("freezes identity and turn copies without freezing producer-owned token or Effect internals", () => {
    const captured = origin();
    const effectWasFrozen = Object.isFrozen(captured.producer.revalidateCurrent);
    const terminal = stampProviderEvent(event(), captured);
    const snapshot = readProviderEventOrigin(terminal)!;
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
    const terminal = stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        stampProviderEvent(terminal, {
          ...captured,
          producer: { ...captured.producer, token: {} },
        }),
      ProviderEventOriginConflictError,
    );
    assert.strictEqual(readProviderEventOrigin(terminal)?.producer.token, captured.producer.token);
  });

  it("rejects a different generation on the same producer token", () => {
    const captured = origin();
    const terminal = stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        stampProviderEvent(terminal, {
          ...captured,
          producer: { ...captured.producer, runtimeGeneration: "replacement" },
        }),
      ProviderEventOriginConflictError,
    );
  });

  it("rejects different turn ownership without silently replacing the captured attempt", () => {
    const captured = origin();
    const terminal = stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        stampProviderEvent(terminal, {
          ...captured,
          turn: { ...captured.turn, attemptId: RunAttemptId.make("replacement-attempt") },
        }),
      ProviderEventOriginConflictError,
    );
    assert.strictEqual(readProviderEventOrigin(terminal)?.turn?.attemptId, captured.turn.attemptId);
  });

  it("rejects substituting another currentness Effect on an already queued event", () => {
    const captured = origin();
    const terminal = stampProviderEvent(event(), captured);
    assert.throws(
      () =>
        stampProviderEvent(terminal, {
          ...captured,
          producer: { ...captured.producer, revalidateCurrent: Effect.fail("different-check") },
        }),
      ProviderEventOriginConflictError,
    );
  });

  it("keeps runless source metadata without inventing a turn or runtime generation", () => {
    const captured = origin();
    const runless: ProviderEventOrigin = {
      producer: {
        token: captured.producer.token,
        driver,
        instanceId: captured.producer.instanceId,
        providerSessionId: captured.producer.providerSessionId,
        revalidateCurrent: captured.producer.revalidateCurrent,
      },
    };
    const terminal = stampProviderEvent(event(), runless);
    const snapshot = readProviderEventOrigin(terminal)!;
    assert.isFalse("turn" in snapshot);
    assert.isFalse("runtimeGeneration" in snapshot.producer);
    assert.throws(
      () => stampProviderEvent(terminal, { ...runless, turn: captured.turn }),
      ProviderEventOriginConflictError,
    );
  });

  it("rejects copying over a derived event that already belongs to another source", () => {
    const source = stampProviderEvent(event(), origin());
    const other = origin();
    const derived = stampProviderEvent({ ...source }, other);
    assert.throws(() => copyProviderEventOrigin(source, derived), ProviderEventOriginConflictError);
    assert.strictEqual(readProviderEventOrigin(derived)?.producer.token, other.producer.token);
  });

  it.effect("retains one event object and captured source across independent fanout queues", () =>
    Effect.gen(function* () {
      const first = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const second = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const terminal = stampProviderEvent(event(), origin());
      yield* Queue.offer(first, terminal);
      yield* Queue.offer(second, terminal);
      const firstReceived = yield* Queue.take(first);
      const secondReceived = yield* Queue.take(second);
      assert.strictEqual(firstReceived, terminal);
      assert.strictEqual(secondReceived, terminal);
      assert.strictEqual(
        readProviderEventOrigin(firstReceived),
        readProviderEventOrigin(secondReceived),
      );
    }),
  );

  it.effect(
    "rejects an old event queued across two boundaries after actual source supersession",
    () =>
      Effect.gen(function* () {
        const adapterQueue = yield* Queue.unbounded<ProviderAdapterV2Event>();
        const managerQueue = yield* Queue.unbounded<ProviderAdapterV2Event>();
        const old = origin();
        let currentSource: object = old.producer.token;
        old.producer.revalidateCurrent = Effect.suspend(() =>
          currentSource === old.producer.token ? Effect.void : Effect.fail("superseded"),
        );
        const terminal = stampProviderEvent(event(), old);
        yield* Queue.offer(adapterQueue, terminal);
        const managerReceived = yield* Queue.take(adapterQueue);
        yield* Queue.offer(managerQueue, managerReceived);
        currentSource = {};
        const consumerReceived = yield* Queue.take(managerQueue);
        const captured = readProviderEventOrigin(consumerReceived)!;
        assert.strictEqual(consumerReceived, terminal);
        assert.strictEqual(captured.producer.token, old.producer.token);
        assert.isTrue(Exit.isFailure(yield* captured.producer.revalidateCurrent.pipe(Effect.exit)));
        const fresh = stampProviderEvent(event(), {
          producer: {
            ...old.producer,
            token: currentSource,
            runtimeGeneration: "fresh-incarnation",
            revalidateCurrent: Effect.void,
          },
        });
        assert.isTrue(
          Exit.isSuccess(
            yield* readProviderEventOrigin(fresh)!.producer.revalidateCurrent.pipe(Effect.exit),
          ),
        );
      }),
  );

  it.effect(
    "allows terminal draining after normal producer exit while its captured source remains current",
    () =>
      Effect.gen(function* () {
        const captured = origin();
        const actualSource = captured.producer.token;
        let currentSource: object = actualSource;
        captured.producer.revalidateCurrent = Effect.suspend(() =>
          currentSource === actualSource ? Effect.void : Effect.fail("superseded"),
        );
        const terminal = stampProviderEvent(event(), captured);
        actualSource.closed = true;
        assert.isTrue(
          Exit.isSuccess(
            yield* readProviderEventOrigin(terminal)!.producer.revalidateCurrent.pipe(Effect.exit),
          ),
        );
        currentSource = {};
        assert.isTrue(
          Exit.isFailure(
            yield* readProviderEventOrigin(terminal)!.producer.revalidateCurrent.pipe(Effect.exit),
          ),
        );
      }),
  );

  it("snapshots the complete child derivation without freezing raw SDK tokens or Effects", () => {
    const captured = derivationOrigin();
    const raw = Object.freeze({ childResult: "historical task summary" });
    const wire = JSON.stringify(raw);
    const effectFrozen = Object.isFrozen(captured.derivation.revalidateDerivation);
    stampProviderEvent(raw, captured);
    const snapshot = readProviderEventOrigin(raw)!;
    const derived = snapshot.derivation!;
    captured.derivation.executor.binding.runtimeGeneration = "replacement";
    captured.derivation.subject.nativeTaskRef.nativeId = "another-task";
    captured.derivation.subject.expectedUpdatedAt = DateTime.makeUnsafe(9_000);
    captured.derivation.childResult.nativeItemRef.fingerprint = "changed-result";
    assert.equal(derived.executor.binding.runtimeGeneration, "native-incarnation");
    assert.equal(derived.subject.runId, RunId.make("historical-run"));
    assert.notEqual(derived.subject.runId, derived.executor.runId);
    assert.equal(derived.subject.nativeTaskRef.nativeId, "known-task");
    assert.equal(DateTime.toEpochMillis(derived.subject.expectedUpdatedAt), 2_000);
    assert.equal(derived.childResult?.nativeItemRef.fingerprint, "result-source");
    assert.equal(DateTime.formatIso(derived.subject.expectedUpdatedAt), "1970-01-01T00:00:02.000Z");
    for (const value of [
      derived,
      derived.executor,
      derived.executor.binding,
      derived.result,
      derived.result.producer,
      derived.notification,
      derived.notification.producer,
      derived.subject,
      derived.subject.nativeTaskRef,
      derived.subject.startedAt,
      derived.subject.expectedUpdatedAt,
      derived.childResult,
      derived.childResult?.nativeItemRef,
    ])
      assert.isTrue(Object.isFrozen(value));
    assert.strictEqual(derived.result.token, captured.derivation.result.token);
    assert.strictEqual(derived.notification.token, captured.derivation.notification.token);
    assert.strictEqual(derived.revalidateDerivation, captured.derivation.revalidateDerivation);
    assert.isFalse(Object.isFrozen(derived.result.token));
    assert.isFalse(Object.isFrozen(derived.notification.token));
    assert.strictEqual(Object.isFrozen(derived.revalidateDerivation), effectFrozen);
    assert.isFalse("turn" in snapshot);
    assert.equal(JSON.stringify(raw), wire);
  });

  it("preserves a derivation across filtering without reassigning its original notification", () => {
    const captured = derivationOrigin();
    const historical = readProviderEventOrigin(captured.derivation.notification.token)!;
    const original = stampProviderEvent({ text: "Full child result." }, captured);
    const filtered = copyProviderEventOrigin(original, { text: "Filtered child result." });
    const copied = readProviderEventOrigin(filtered)!;
    assert.strictEqual(
      copied.derivation?.notification.token,
      captured.derivation.notification.token,
    );
    assert.strictEqual(copied.derivation?.result.token, captured.derivation.result.token);
    assert.strictEqual(copied.producer.token, captured.producer.token);
    assert.strictEqual(readProviderEventOrigin(captured.derivation.notification.token), historical);
    assert.notEqual(historical.producer.token, copied.producer.token);
    assert.isUndefined(readProviderEventOrigin(JSON.parse(JSON.stringify(filtered))));
    assert.throws(
      () => stampProviderEvent(captured.derivation.notification.token, captured),
      ProviderEventOriginConflictError,
    );
  });

  it("allows an identical derivation with equivalent copied UTC timestamps", () => {
    const captured = derivationOrigin();
    const raw = stampProviderEvent({}, captured);
    const snapshot = readProviderEventOrigin(raw);
    stampProviderEvent(raw, {
      ...captured,
      derivation: {
        ...captured.derivation,
        subject: {
          ...captured.derivation.subject,
          startedAt: DateTime.makeUnsafe(1_000),
          expectedUpdatedAt: DateTime.makeUnsafe(2_000),
        },
      },
    });
    assert.strictEqual(readProviderEventOrigin(raw), snapshot);
  });

  it("rejects replacing any captured child derivation correlation", () => {
    const captured = derivationOrigin();
    const raw = stampProviderEvent({}, captured);
    const derived = captured.derivation;
    const changes: ReadonlyArray<ProviderEventOrigin> = [
      { producer: captured.producer },
      {
        ...captured,
        derivation: {
          ...derived,
          executor: { ...derived.executor, attemptId: RunAttemptId.make("another-attempt") },
        },
      },
      { ...captured, derivation: { ...derived, result: { ...derived.result, token: {} } } },
      {
        ...captured,
        derivation: {
          ...derived,
          result: {
            ...derived.result,
            producer: { ...derived.result.producer, runtimeGeneration: "new-query" },
          },
        },
      },
      {
        ...captured,
        derivation: { ...derived, notification: { ...derived.notification, token: {} } },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          notification: {
            ...derived.notification,
            producer: { ...derived.notification.producer, token: {} },
          },
        },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          notification: { ...derived.notification, nativeTaskId: "unknown-task" },
        },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          notification: { ...derived.notification, nativeThreadId: "another-conversation" },
        },
      },
      {
        ...captured,
        derivation: { ...derived, notification: { ...derived.notification, toolUseId: null } },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          notification: { ...derived.notification, summary: "Another summary." },
        },
      },
      {
        ...captured,
        derivation: { ...derived, notification: { ...derived.notification, status: "failed" } },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          subject: { ...derived.subject, childThreadId: ThreadId.make("another-child") },
        },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          subject: { ...derived.subject, expectedUpdatedAt: DateTime.makeUnsafe(2_001) },
        },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          subject: {
            ...derived.subject,
            nativeTaskRef: { ...derived.subject.nativeTaskRef, fingerprint: "another-source" },
          },
        },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          childResult: { ...derived.childResult, messageId: MessageId.make("another-message") },
        },
      },
      {
        ...captured,
        derivation: {
          ...derived,
          childResult: {
            ...derived.childResult,
            nativeItemRef: { ...derived.childResult.nativeItemRef, ordinal: 3 },
          },
        },
      },
      { ...captured, derivation: { ...derived, revalidateDerivation: currentCheck() } },
    ];
    const snapshot = readProviderEventOrigin(raw);
    for (const changed of changes)
      assert.throws(() => stampProviderEvent(raw, changed), ProviderEventOriginConflictError);
    assert.strictEqual(readProviderEventOrigin(raw), snapshot);
  });

  it.effect(
    "reads historical notification evidence without running its obsolete-source check",
    () =>
      Effect.gen(function* () {
        const captured = derivationOrigin();
        const raw = stampProviderEvent({}, captured);
        const derived = readProviderEventOrigin(raw)!.derivation!;
        assert.isTrue(
          Exit.isFailure(yield* derived.notification.producer.revalidateCurrent.pipe(Effect.exit)),
        );
        assert.isTrue(
          Exit.isSuccess(yield* derived.result.producer.revalidateCurrent.pipe(Effect.exit)),
        );
        assert.isTrue(Exit.isSuccess(yield* derived.revalidateDerivation.pipe(Effect.exit)));
        assert.strictEqual(
          readProviderEventOrigin(derived.notification.token)?.producer.token,
          derived.notification.producer.token,
        );
      }),
  );
});
