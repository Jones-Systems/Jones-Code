import { assert, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  RunAttemptId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { requirementFor, loadedSessionMatches } from "./guard.ts";
import { fixture } from "./testkit.ts";
import type { TaskContinuationStore } from "./TaskContinuationStore.ts";
import { TaskContinuationError } from "./sourceBinding.ts";

it.effect(
  "ordinary restart ancestry permits an approved owner selection and provider handoff",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      for (const target of [
        {
          modelSelection: {
            ...f.run.modelSelection,
            model: "owner-model",
            options: [{ id: "effort", value: "max" }],
          },
        },
        {
          providerInstanceId: ProviderInstanceId.make("owner-provider"),
          providerThreadId: ProviderThreadId.make("owner-thread"),
          modelSelection: {
            instanceId: ProviderInstanceId.make("owner-provider"),
            model: "owner-model",
          },
        },
      ]) {
        const successor = {
          ...f.run,
          ...target,
          id: RunId.make("ordinary-restart"),
          restartContinuationOfRunId: f.run.id,
        };
        assert.isUndefined(
          yield* requirementFor(
            undefined,
            { ...f.child, runs: [f.run, successor] },
            successor,
            f.attempt,
          ),
        );
      }
    }),
);

it.effect(
  "native roots stay guarded through ordinary restart messages and reject owner tuple or task changes",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      const root = {
        ...f.run,
        id: f.row.runId,
        userMessageId: f.row.messageId,
        delegatedTaskId: f.row.taskId,
      };
      const first = {
        ...root,
        id: RunId.make("first-restart"),
        userMessageId: MessageId.make("message:restart-continuation:first"),
        restartContinuationOfRunId: root.id,
      };
      const last = {
        ...first,
        id: RunId.make("last-restart"),
        userMessageId: MessageId.make("message:restart-continuation:last"),
        activeAttemptId: RunAttemptId.make("last-attempt"),
        restartContinuationOfRunId: first.id,
      };
      const attempt = { ...f.attempt, id: last.activeAttemptId, runId: last.id };
      const child = { ...f.child, runs: [root, first, last] };
      const store: TaskContinuationStore["Service"] = {
        get: () => Effect.succeed(f.row),
        insert: () => Effect.void,
        witnesses: () => Effect.succeed([]),
        confirm: () => Effect.void,
      };
      assert.equal(
        (yield* requirementFor(store, child, last, attempt))?.nativeSessionId,
        "original-native",
      );
      assert.equal(
        (yield* Effect.result(requirementFor(undefined, child, last, attempt)))._tag,
        "Failure",
      );
      const { delegatedTaskId: _task, ...withoutTask } = last;
      for (const changed of [
        { ...last, modelSelection: { ...last.modelSelection, model: "owner-model" } },
        {
          ...last,
          modelSelection: { ...last.modelSelection, options: [{ id: "effort", value: "high" }] },
        },
        { ...last, providerInstanceId: ProviderInstanceId.make("owner-provider") },
        { ...last, providerThreadId: ProviderThreadId.make("owner-thread") },
        { ...last, delegatedTaskId: NodeId.make("different-task") },
        withoutTask,
      ]) {
        const result = yield* Effect.result(
          requirementFor(store, { ...child, runs: [root, first, changed] }, changed, attempt),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.reason, "ancestry_invalid");
      }
      const modelSelection = { ...last.modelSelection, model: "owner-model" };
      const changedChain = [root, first, last].map((run) => ({ ...run, modelSelection }));
      assert.equal(
        (yield* Effect.result(
          requirementFor(store, { ...child, runs: changedChain }, changedChain[2]!, attempt),
        ))._tag,
        "Failure",
      );
    }),
);

it.effect("fails closed for a continuation root or crash successor without storage", () =>
  Effect.gen(function* () {
    const f = fixture();
    const root = {
      ...f.run,
      id: f.row.runId,
      userMessageId: f.row.messageId,
      delegatedTaskId: f.row.taskId,
    };
    const successor = {
      ...root,
      id: RunId.make("crash-successor"),
      restartContinuationOfRunId: root.id,
    };
    const child = { ...f.child, runs: [root, successor] };
    assert.equal(
      (yield* Effect.result(requirementFor(undefined, child, successor, f.attempt)))._tag,
      "Failure",
    );
    assert.isUndefined(yield* requirementFor(undefined, f.child, f.run, f.attempt));
  }),
);

it.effect("binds confirmation to the current guarded run and attempt and refuses moved refs", () =>
  Effect.gen(function* () {
    const f = fixture();
    let confirmations = 0;
    const run = {
      ...f.run,
      id: f.row.runId,
      userMessageId: f.row.messageId,
      delegatedTaskId: f.row.taskId,
      activeAttemptId: RunAttemptId.make("continued-attempt"),
    };
    const attempt = { ...f.attempt, id: run.activeAttemptId, runId: run.id };
    const store: TaskContinuationStore["Service"] = {
      get: () => Effect.succeed(f.row),
      insert: () => Effect.void,
      witnesses: () => Effect.succeed([]),
      confirm: (_row, input) =>
        Effect.sync(() => {
          assert.equal(input.runId, run.id);
          confirmations++;
        }),
    };
    const child = { ...f.child, runs: [run] };
    const required = yield* requirementFor(store, child, run, attempt);
    assert.isDefined(required);
    yield* required!.confirm({
      runId: run.id,
      attemptId: attempt.id,
      nativeSessionId: "original-native",
      runtimeGeneration: "query-1",
    });
    assert.equal(confirmations, 1);
    const bad = yield* Effect.result(
      required!.confirm({
        runId: RunId.make("wrong"),
        attemptId: attempt.id,
        nativeSessionId: "original-native",
        runtimeGeneration: "query-1",
      }),
    );
    assert.equal(bad._tag, "Failure");
    assert.equal(confirmations, 1);
    assert.isTrue(loadedSessionMatches(required!, f.provider, f.provider));
    assert.isFalse(
      loadedSessionMatches(required!, f.provider, { ...f.provider, nativeThreadRef: null }),
    );
    assert.equal(
      (yield* Effect.result(requirementFor(store, { ...child, providerThreads: [] }, run, attempt)))
        ._tag,
      "Failure",
    );
  }),
);

it.effect(
  "retains the accepted guard across a crash successor and refuses missing or unreadable records",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      const root = {
        ...f.run,
        id: f.row.runId,
        userMessageId: f.row.messageId,
        delegatedTaskId: f.row.taskId,
      };
      const successor = {
        ...root,
        id: RunId.make("crash-successor"),
        ordinal: 3,
        activeAttemptId: RunAttemptId.make("successor-attempt"),
        restartContinuationOfRunId: root.id,
      };
      const attempt = { ...f.attempt, id: successor.activeAttemptId, runId: successor.id };
      const child = { ...f.child, runs: [root, successor] };
      const store: TaskContinuationStore["Service"] = {
        get: () => Effect.succeed(f.row),
        insert: () => Effect.void,
        witnesses: () => Effect.succeed([]),
        confirm: () => Effect.void,
      };
      const required = yield* requirementFor(store, child, successor, attempt);
      assert.equal(required?.runId, successor.id);
      assert.equal(required?.attemptId, attempt.id);
      assert.equal(required?.nativeSessionId, "original-native");
      assert.equal(
        (yield* Effect.result(
          requirementFor({ ...store, get: () => Effect.succeed(null) }, child, successor, attempt),
        ))._tag,
        "Failure",
      );
      const unreadable = {
        ...store,
        get: () => Effect.fail(new TaskContinuationError({ reason: "storage_unavailable" })),
      };
      assert.equal(
        (yield* Effect.result(requirementFor(unreadable, child, successor, attempt)))._tag,
        "Failure",
      );
      assert.isUndefined(yield* requirementFor(unreadable, f.child, f.run, f.attempt));
      assert.equal(
        (yield* Effect.result(
          requirementFor(store, { ...child, runs: [successor] }, successor, attempt),
        ))._tag,
        "Failure",
      );
    }),
);
