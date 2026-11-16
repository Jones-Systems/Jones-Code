import { assert, it } from "@effect/vitest";
import {
  CommandId, EventId, MessageId, ProviderDriverKind, ProviderInstanceId, ProviderSessionId, ProviderThreadId,
  RunAttemptId, RunId, ThreadId, type OrchestrationV2Run, type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { runtimeBinding } from "@t3tools/provider-core/server/ProviderAdapter";
import * as StartAdmission from "../provider/StartAdmission.ts";
import * as Admission from "./NativeStartAdmission.ts";
import { CurrentEffectClaim, EffectClaimLost } from "../workspaceLifecycle/ActiveEffectClaim.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Control from "../orchestration/ControlAdmission.ts";
import * as Refusals from "../startRefusal/Store.ts";

const threadId = ThreadId.make("thread:admission");
const runId = RunId.make("run:admission");
const attemptId = RunAttemptId.make("attempt:admission");
const providerThreadId = ProviderThreadId.make("provider:admission");
const instanceId = ProviderInstanceId.make("codex");
const sourceId = RunId.make("run:source");
const sourceAttemptId = RunAttemptId.make("attempt:source");
const provider = {
  id: providerThreadId, providerInstanceId: instanceId, driver: ProviderDriverKind.make("codex"),
  providerSessionId: ProviderSessionId.make("session:admission"),
  nativeThreadRef: { driver: ProviderDriverKind.make("codex"), nativeId: "original-native", strength: "strong" },
} as OrchestrationV2ProviderThread;
const run = { id: runId, threadId, providerThreadId, providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "synthetic" }, activeAttemptId: attemptId,
  status: "starting", userMessageId: MessageId.make("message:admission"), manualContinuationOfRunId: sourceId,
} as OrchestrationV2Run;

function fixture(options: { originalNativeId?: string; prefix?: "refused" | "crossed" | "unknown"; unclaimed?: number; pruned?: boolean; generation?: string; chained?: boolean } = {}) {
  let lost = false;
  let stopped = false;
  let crosses = 0;
  const initialProvider = { ...provider, appThreadId: threadId,
    ...(options.generation === undefined ? {} : { runtimeIdentity: { runtimeGeneration: options.generation } }),
  } as OrchestrationV2ProviderThread;
  let currentProvider = initialProvider;
  const now = DateTime.makeUnsafe("2026-11-16T20:00:00Z");
  const effect: Outbox.OrchestrationEffectV2 = {
    id: "effect:admission", commandId: CommandId.make("command:admission"), threadId,
    request: { type: "provider-turn.start", runId }, status: "running", attemptCount: 1,
    availableAt: DateTime.formatIso(now), createdAt: DateTime.formatIso(now), updatedAt: DateTime.formatIso(now),
    leaseOwner: "original-worker", completedAt: null, lastError: null,
    payloadIdentity: JSON.stringify({ type: "provider-turn.start", runId }),
  };
  const source = { ...run, id: sourceId, activeAttemptId: sourceAttemptId,
    manualContinuationOfRunId: options.chained ? RunId.make("run:earlier-source") : undefined,
    status: "failed", startedAt: null } as OrchestrationV2Run;
  const sourceAttempt = { id: sourceAttemptId, runId: sourceId, providerThreadId,
    nativeThreadId: options.originalNativeId, providerTurnId: "turn:source", reason: "initial", attemptOrdinal: 1,
    providerSettlement: options.unclaimed === undefined ? { runAttemptId: sourceAttemptId, providerTurnId: "turn:source", status: "failed", completedAt: now } : null,
  };
  const sourceCommandId = CommandId.make("command:source");
  const sourceEffect = { ...effect, commandId: sourceCommandId,
    id: `effect:${sourceCommandId}:provider-turn.start:${sourceId}`,
    request: { type: "provider-turn.start" as const, runId: sourceId },
    status: "cancelled" as const, leaseOwner: null, attemptCount: options.unclaimed ?? 0,
    payloadIdentity: JSON.stringify({ type: "provider-turn.start", runId: sourceId }),
  };
  const sourceEvents = [
    { sequence: 1, commandId: sourceCommandId, event: { id: EventId.make("event:source-born"), type: "run.created", threadId,
      payload: { ...source, activeAttemptId: sourceAttemptId } } },
    { sequence: 2, commandId: sourceCommandId, event: { id: EventId.make("event:source-attempt"), type: "run-attempt.created", threadId,
      payload: { ...sourceAttempt, startedAt: null } } },
  ] as unknown as ReadonlyArray<import("@t3tools/contracts").OrchestrationV2StoredEvent>;
  const check = Effect.suspend(() => lost ? Effect.fail(new EffectClaimLost({ effectId: effect.id })) : Effect.void);
  const dependencies = Layer.mergeAll(
    Layer.mock(Outbox.EffectOutboxV2)({
      listByThreadId: () => Effect.succeed(options.unclaimed === undefined ? [] : [sourceEffect]),
      get: () => Effect.succeed(Option.some(sourceEffect)),
    }),
    Layer.mock(Receipts.CommandReceiptStoreV2)({ getByCommandId: (commandId) => Effect.succeed(Option.some({
      commandId, threadId, commandType: "message.dispatch", acceptedAt: now,
      resultSequence: commandId === sourceCommandId ? 2 : 1, status: "accepted", error: null,
    })) }),
    Layer.mock(Sink.EventSinkV2)({
      assertRuntimeStopStartAllowed: () => Effect.suspend(() => stopped
        ? Effect.fail(new Sink.EventSinkWriteError({ eventCount: 0 })) : Effect.void),
      readByCommandId: () => Stream.fromArray(options.pruned ? [] : sourceEvents),
    }),
    Layer.mock(Projections.ProjectionStoreV2)({
      getTurnStartContext: () => Effect.sync(() => ({ thread: { archivedAt: null, deletedAt: null },
        runs: [run], attempts: [{ id: attemptId, runId, providerThreadId }], providerThreads: [currentProvider],
      } as unknown as Effect.Success<ReturnType<Projections.ProjectionStoreV2["Service"]["getTurnStartContext"]>>)),
      getThreadRecords: () => Effect.succeed({ runs: [source], attempts: [sourceAttempt], providerThreads: [provider] } as never),
    }),
    Layer.mock(Control.ControlAdmission)({ assertOpen: () => Effect.void }),
    Layer.mock(Refusals.StartRefusals)({
      readPrefix: () => Effect.succeed({ state: options.prefix ?? "unknown" } as never),
      cross: () => Effect.sync(() => { crosses++; }),
    }),
  );
  const claim = Layer.mock(CurrentEffectClaim)({ nativeCustody: false, check,
    use: (operation) => check.pipe(Effect.andThen(Effect.suspend(() => operation(effect)))),
  });
  return { layer: Admission.layer.pipe(Layer.provide(dependencies), Layer.provideMerge(claim)),
    provider: initialProvider,
    replace: (generation: string, nativeId = "original-native") => {
      currentProvider = { ...initialProvider, runtimeIdentity: { ...initialProvider.runtimeIdentity, runtimeGeneration: generation },
        nativeThreadRef: { ...initialProvider.nativeThreadRef!, nativeId },
      } as OrchestrationV2ProviderThread;
      return runtimeBinding(currentProvider, generation)!;
    },
    stop: () => { stopped = true; }, lose: () => { lost = true; }, crosses: () => crosses };
}

it.effect("capture and repeated validation do not cross a prefix and observe a later Stop", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const owner = yield* StartAdmission.StartAdmission;
    const capture = yield* owner.capture({ threadId, run, attemptId, providerThread: provider });
    yield* capture.revalidate;
    assert.equal(f.crosses(), 0);
    f.stop();
    assert.isTrue(Exit.isFailure(yield* capture.revalidate.pipe(Effect.exit)));
  }).pipe(Effect.provide(f.layer));
});

for (const boundary of ["missing", "stale", "exact", "replacement-native"] as const) {
  it.effect(`runtime generation adoption requires the manager's exact successor: ${boundary}`, () => {
    const f = fixture({ generation: "generation:original" });
    return Effect.gen(function* () {
      const owner = yield* StartAdmission.StartAdmission;
      const capture = yield* owner.capture({ threadId, run, attemptId, providerThread: f.provider });
      const binding = f.replace("generation:successor", boundary === "replacement-native" ? "other-native" : undefined);
      const result = yield* capture.revalidate.pipe(
        Effect.provideService(StartAdmission.CurrentBindingPublication, boundary === "missing" ? undefined : {
          previousGeneration: boundary === "stale" ? "generation:unrelated" : "generation:original", binding,
        }),
        Effect.exit,
      );
      assert.equal(Exit.isSuccess(result), boundary === "exact");
      if (boundary === "exact") {
        // The admitted successor is retained for subsequent calls without context.
        yield* capture.revalidate;
        f.replace("generation:unowned");
        assert.isTrue(Exit.isFailure(yield* capture.revalidate.pipe(Effect.exit)));
      }
      assert.equal(f.crosses(), 0);
    }).pipe(Effect.provide(f.layer));
  });
}

it.effect("a lost original worker claim cannot be replaced by current projection state", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const owner = yield* StartAdmission.StartAdmission;
    const capture = yield* owner.capture({ threadId, run, attemptId, providerThread: provider });
    f.lose();
    assert.isTrue(Exit.isFailure(yield* capture.revalidate.pipe(Effect.exit)));
  }).pipe(Effect.provide(f.layer));
});

for (const originalNativeId of [undefined, "replacement-native", "original-native"]) {
  it.effect(`Resume uses original attempt identity: ${originalNativeId ?? "unknown"}`, () => {
    const f = fixture({ ...(originalNativeId === undefined ? {} : { originalNativeId }) });
    return Effect.gen(function* () {
      const owner = yield* StartAdmission.StartAdmission;
      const capture = yield* owner.capture({ threadId, run, attemptId, providerThread: provider });
      assert.equal(yield* capture.classifyResume, originalNativeId === "original-native" ? "native_resume" : "held");
    }).pipe(Effect.provide(f.layer));
  });
}

for (const [count, pruned, expected] of [[0, false, "unsent"], [1, false, "held"], [0, true, "held"]] as const) {
  it.effect(`original cancelled effect count=${count}, pruned=${pruned} classifies ${expected}`, () => {
    const f = fixture({ unclaimed: count, pruned });
    return Effect.gen(function* () {
      const owner = yield* StartAdmission.StartAdmission;
      const capture = yield* owner.capture({ threadId, run, attemptId, providerThread: provider });
      assert.equal(yield* capture.classifyResume, expected);
    }).pipe(Effect.provide(f.layer));
  });
}

it.effect("a chained source stays held even when its direct attempt has a refusal certificate", () => {
  const f = fixture({ prefix: "refused", chained: true });
  return Effect.gen(function* () {
    const owner = yield* StartAdmission.StartAdmission;
    const capture = yield* owner.capture({ threadId, run, attemptId, providerThread: provider });
    assert.equal(yield* capture.classifyResume, "held");
  }).pipe(Effect.provide(f.layer));
});
