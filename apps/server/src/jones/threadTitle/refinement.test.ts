import { assert, describe, it } from "@effect/vitest";
import { CommandId, MessageId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as SqlClient from "effect/sql/SqlClient";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as Worker from "../../orchestration-v2/ThreadTitleRegenerationService.ts";
import * as TitleStore from "./store.ts";
import { createThread, dispatchUserMessage, makeHarness } from "./testkit.ts";

const snapshot = Effect.fn("test.title.snapshot")(function* (
  threadId: Parameters<
    ThreadManagement.ThreadManagementService["Service"]["getThreadProjection"]
  >[0],
) {
  const threads = yield* ThreadManagement.ThreadManagementService;
  return yield* threads.getThreadProjection(threadId);
});
const stateOf = Effect.fn("test.title.state")(function* (
  threadId: Parameters<
    ThreadManagement.ThreadManagementService["Service"]["getThreadProjection"]
  >[0],
) {
  return yield* (yield* TitleStore.make).read(threadId);
});
const execute = Effect.fn("test.title.execute")(function* (
  threadId: Parameters<
    ThreadManagement.ThreadManagementService["Service"]["getThreadProjection"]
  >[0],
) {
  const state = yield* stateOf(threadId);
  assert.isOk(state?.pending);
  const pending = state!.pending!;
  yield* (yield* Worker.ThreadTitleRegenerationService).execute({
    threadId,
    requestId: pending.requestId,
    kind:
      state!.covered.length === 0
        ? { type: "initial", messageId: pending.evidence[0]! }
        : { type: "regenerate" },
  });
});

describe("bounded automatic thread titles", () => {
  it.effect(
    "does not spend title evidence on maintenance and preserves first-owner generation after it",
    () => {
      const harness = makeHarness();
      return Effect.gen(function* () {
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        yield* dispatchUserMessage({ command: "maintenance", threadId, text: "/compact" });
        assert.isNull(yield* stateOf(threadId));
        yield* dispatchUserMessage({
          command: "one",
          threadId,
          text: "Goal: choose installation options",
          seed: false,
        });
        assert.deepEqual((yield* stateOf(threadId))?.accepted, [MessageId.make("one:message")]);
        yield* execute(threadId);
        assert.notInclude(harness.generateThreadTitle.mock.calls[0]![0]!.message, "/compact");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "stops after a sufficient initial title, even with newer messages already accepted",
    () => {
      const harness = makeHarness({
        generateTitle: () =>
          Effect.succeed({ title: "Installation choices", assessment: "sufficient" }),
      });
      return Effect.gen(function* () {
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        yield* dispatchUserMessage({
          command: "one",
          threadId,
          text: "Choose installation options",
        });
        yield* dispatchUserMessage({ command: "two", threadId, text: "Include storage" });
        yield* execute(threadId);
        assert.equal((yield* stateOf(threadId))?.status, "sufficient");
        assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
        assert.equal(harness.generateThreadTitle.mock.calls.length, 1);
        assert.notInclude(
          harness.generateThreadTitle.mock.calls[0]![0]!.message,
          "Include storage",
        );
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "unknown assessments consume increasing opportunities without erasing known insufficiency",
    () => {
      let calls = 0;
      const harness = makeHarness({
        generateTitle: () =>
          Effect.succeed(
            ++calls === 1
              ? { title: "Vague title", assessment: "insufficient" }
              : { title: "Still unclear" },
          ),
      });
      return Effect.gen(function* () {
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        for (const command of ["one", "two", "three"]) {
          yield* dispatchUserMessage({ command, threadId, text: command });
          yield* execute(threadId);
          assert.isTrue((yield* stateOf(threadId))?.knownInsufficient);
          assert.isNull((yield* stateOf(threadId))?.pending);
        }
        assert.equal(calls, 3);
        assert.equal((yield* stateOf(threadId))?.status, "capped");
        yield* dispatchUserMessage({ command: "four", threadId, text: "fourth" });
        assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("refines insufficient evidence 1 on acceptance 2 and stops on sufficient", () => {
    let calls = 0;
    const harness = makeHarness({
      generateTitle: () =>
        Effect.succeed(
          ++calls === 1
            ? { title: "Continue conversation", assessment: "insufficient" }
            : { title: "Installation and update choices", assessment: "sufficient" },
        ),
    });
    return Effect.gen(function* () {
      const threadId = yield* createThread({ command: "create", thread: "thread" });
      yield* dispatchUserMessage({
        command: "one",
        threadId,
        text: "continuing another conversation",
      });
      yield* execute(threadId);
      assert.equal((yield* stateOf(threadId))?.covered.length, 1);
      yield* dispatchUserMessage({
        command: "two",
        threadId,
        text: "Choose installation, updates, storage and backup options",
      });
      yield* execute(threadId);
      assert.equal((yield* snapshot(threadId)).thread.title, "Installation and update choices");
      assert.equal((yield* stateOf(threadId))?.status, "sufficient");
      yield* dispatchUserMessage({ command: "three", threadId, text: "Use a full backup" });
      assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
      assert.equal(calls, 2);
      for (const [input] of harness.generateThreadTitle.mock.calls) {
        assert.isTrue(input?.titleAssessment);
        assert.equal(input?.linkedContext, "");
        assert.isUndefined(input?.forkSource);
      }
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect.each([false, true])(
    "coalesces messages 2 and 3 during initial failure=%s",
    (failure) => {
      const harness = makeHarness({
        generateTitle: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            if (failure) return yield* Effect.die("synthetic provider failure");
            return { title: "Still vague", assessment: "insufficient" as const };
          }),
      });
      let started: Deferred.Deferred<void>;
      let release: Deferred.Deferred<void>;
      return Effect.gen(function* () {
        started = yield* Deferred.make<void>();
        release = yield* Deferred.make<void>();
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        yield* dispatchUserMessage({ command: "one", threadId, text: "continue" });
        const worker = yield* execute(threadId).pipe(Effect.forkChild);
        yield* Deferred.await(started).pipe(Effect.raceFirst(Fiber.join(worker)));
        yield* dispatchUserMessage({
          command: "two",
          threadId,
          text: "Goal: installation and update choices",
        });
        yield* dispatchUserMessage({
          command: "three",
          threadId,
          text: "Storage, full backup versus clone",
        });
        assert.equal(harness.generateThreadTitle.mock.calls.length, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(worker);
        assert.equal((yield* stateOf(threadId))?.covered.length, 1);
        assert.equal((yield* stateOf(threadId))?.lastRequestId, "one");
        assert.equal((yield* stateOf(threadId))?.pending?.evidence.length, 3);
        yield* execute(threadId);
        assert.equal(harness.generateThreadTitle.mock.calls.length, 2);
        const context = harness.generateThreadTitle.mock.calls[1]![0];
        assert.include(context!.message, "Goal: installation");
        assert.include(context!.message, "full backup");
        assert.equal((yield* stateOf(threadId))?.status, "capped");
        yield* dispatchUserMessage({ command: "four", threadId, text: "Another detail" });
        assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("consumes failed refinement 2 and immediately arms already accepted evidence 3", () => {
    let calls = 0;
    let started: Deferred.Deferred<void>;
    let release: Deferred.Deferred<void>;
    const harness = makeHarness({
      generateTitle: () =>
        Effect.gen(function* () {
          if (++calls === 2) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return yield* Effect.die("synthetic failure");
          }
          return { title: "Still vague", assessment: "insufficient" as const };
        }),
    });
    return Effect.gen(function* () {
      started = yield* Deferred.make<void>();
      release = yield* Deferred.make<void>();
      const threadId = yield* createThread({ command: "create", thread: "thread" });
      yield* dispatchUserMessage({ command: "one", threadId, text: "continue" });
      yield* execute(threadId);
      assert.isNull((yield* stateOf(threadId))?.pending);
      yield* dispatchUserMessage({ command: "two", threadId, text: "clarification" });
      const worker = yield* execute(threadId).pipe(Effect.forkChild);
      yield* Deferred.await(started).pipe(Effect.raceFirst(Fiber.join(worker)));
      yield* dispatchUserMessage({ command: "three", threadId, text: "real goal" });
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(worker);
      assert.equal((yield* stateOf(threadId))?.covered.length, 2);
      assert.equal((yield* stateOf(threadId))?.pending?.evidence.length, 3);
      yield* execute(threadId);
      assert.equal((yield* stateOf(threadId))?.status, "capped");
      assert.equal(calls, 3);
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect(
    "consumes initial failure without retrying until a newer owner message is accepted",
    () => {
      let calls = 0;
      const harness = makeHarness({
        generateTitle: () =>
          ++calls === 1
            ? Effect.die("synthetic initial failure")
            : Effect.succeed({ title: "Installation choices", assessment: "sufficient" }),
      });
      return Effect.gen(function* () {
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        yield* dispatchUserMessage({ command: "one", threadId, text: "continue" });
        yield* execute(threadId);
        const failed = yield* stateOf(threadId);
        assert.equal(calls, 1);
        assert.deepEqual(failed?.covered, [MessageId.make("one:message")]);
        assert.equal(failed?.lastRequestId, "one");
        assert.isNull(failed?.pending);
        assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
        yield* dispatchUserMessage({
          command: "two",
          threadId,
          text: "Goal: installation choices",
        });
        assert.equal((yield* stateOf(threadId))?.pending?.evidence.length, 2);
        yield* execute(threadId);
        assert.equal(calls, 2);
        assert.equal((yield* stateOf(threadId))?.status, "sufficient");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "rolls progress, title and coalesced next effect back together, then replays completion safely",
    () => {
      const harness = makeHarness({
        generateTitle: () => Effect.succeed({ title: "Generated", assessment: "insufficient" }),
      });
      return Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        yield* dispatchUserMessage({ command: "one", threadId, text: "first" });
        yield* dispatchUserMessage({ command: "two", threadId, text: "clarification" });
        const before = yield* stateOf(threadId);
        yield* sql`CREATE TEMP TRIGGER reject_title BEFORE UPDATE ON orchestration_v2_projection_threads
        BEGIN SELECT RAISE(ABORT, 'synthetic transaction fault'); END`;
        const failed = yield* execute(threadId).pipe(Effect.exit);
        assert.equal(failed._tag, "Failure");
        assert.deepEqual(yield* stateOf(threadId), before);
        assert.equal((yield* snapshot(threadId)).thread.title, "New thread");
        yield* sql`DROP TRIGGER reject_title`;
        yield* execute(threadId);
        const completed = yield* stateOf(threadId);
        assert.equal(completed?.covered.length, 1);
        assert.equal(completed?.pending?.evidence.length, 2);
        yield* threads.dispatch({
          type: "thread.title.regeneration.complete",
          threadId,
          commandId: CommandId.make("one:title-complete"),
          requestId: CommandId.make("one"),
          title: "Replay must not win",
        });
        assert.deepEqual(yield* stateOf(threadId), completed);
        assert.equal((yield* snapshot(threadId)).thread.title, "Generated");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "interruption after generating a verdict leaves the pending opportunity recoverable",
    () => {
      let ready: Deferred.Deferred<void>;
      const harness = makeHarness({
        generateTitle: () =>
          Effect.gen(function* () {
            const verdict = { title: "Generated", assessment: "sufficient" as const };
            yield* Deferred.succeed(ready, undefined);
            return yield* Effect.never.pipe(Effect.as(verdict));
          }),
      });
      return Effect.gen(function* () {
        ready = yield* Deferred.make<void>();
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        yield* dispatchUserMessage({ command: "one", threadId, text: "first" });
        const before = yield* stateOf(threadId);
        const fiber = yield* execute(threadId).pipe(Effect.forkChild);
        yield* Deferred.await(ready).pipe(Effect.raceFirst(Fiber.join(fiber)));
        yield* Fiber.interrupt(fiber);
        assert.deepEqual(yield* stateOf(threadId), before);
        assert.equal((yield* snapshot(threadId)).thread.title, "New thread");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  const manualCases = [
    { label: "rename", edit: { title: "Manual" } },
    { label: "same-text rename", edit: { title: "New thread" } },
    { label: "regenerate", edit: { regenerateTitle: true } },
    { label: "cancel", edit: { regenerateTitle: false } },
  ].flatMap(({ label, edit }) =>
    [false, true].map((beforeFirst) => ({ label, edit, beforeFirst })),
  );
  it.effect.each(manualCases)(
    "permanently stops for $label beforeFirst=$beforeFirst",
    ({ edit, beforeFirst }) => {
      let started: Deferred.Deferred<void>;
      let release: Deferred.Deferred<void>;
      const harness = makeHarness({
        generateTitle: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return { title: "Automatic must not revive", assessment: "insufficient" as const };
          }),
      });
      return Effect.gen(function* () {
        started = yield* Deferred.make<void>();
        release = yield* Deferred.make<void>();
        const threads = yield* ThreadManagement.ThreadManagementService;
        const threadId = yield* createThread({ command: "create", thread: "thread" });
        const editTitle = threads.dispatch({
          type: "thread.metadata.update",
          threadId,
          commandId: CommandId.make("manual"),
          ...edit,
        });
        if (beforeFirst) yield* editTitle;
        yield* dispatchUserMessage({ command: "one", threadId, text: "first" });
        if (!beforeFirst) {
          const fiber = yield* execute(threadId).pipe(Effect.forkChild);
          yield* Deferred.await(started).pipe(Effect.raceFirst(Fiber.join(fiber)));
          yield* editTitle;
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(fiber);
        }
        yield* threads.dispatch({
          type: "thread.metadata.update",
          threadId,
          commandId: CommandId.make("rename-back"),
          title: "New thread",
        });
        yield* dispatchUserMessage({ command: "two", threadId, text: "clarification" });
        assert.equal((yield* stateOf(threadId))?.status, "stopped");
        assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
        assert.equal((yield* snapshot(threadId)).thread.title, "New thread");
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect("does not adopt an existing custom title or a seedless first message", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const threadId = yield* createThread({
        command: "create",
        thread: "thread",
        title: "Custom title",
      });
      yield* dispatchUserMessage({ command: "one", threadId, text: "continue" });
      assert.isNull(yield* stateOf(threadId));
      assert.isNotOk((yield* snapshot(threadId)).thread.titleRegeneration);
    }).pipe(Effect.provide(harness.layer));
  });
});
