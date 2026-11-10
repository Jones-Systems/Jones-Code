import { assert, it } from "@effect/vitest";
import {
  ProviderThreadId,
  type OrchestrationV2ProviderThread,
  type ProviderRuntimeBinding,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderDriverKind,
  RunId,
  RunAttemptId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import {
  ExecutionSessionRevalidationError,
  prepareExecutionSession,
} from "./prepareExecutionSession.ts";

it.effect(
  "retirement persists the released identity before returning for restart and handoff; retries keep event IDs",
  () =>
    Effect.gen(function* () {
      for (const transition of ["restart_and_resume", "create_with_handoff"] as const) {
        const fixture = makeFixture(transition);
        yield* fixture.start;
        const firstIds = fixture.writes[0]!.events.map((event) => event.id);
        yield* fixture.start;
        assert.deepEqual(fixture.order, ["detach", "persist", "open", "detach", "persist", "open"]);
        assert.deepEqual(
          fixture.writes[1]!.events.map((event) => event.id),
          firstIds,
        );
        const write = fixture.writes[0]!;
        assert.equal(write.activeAttemptId, fixture.projection.runs[0]!.activeAttemptId);
        assert.equal(write.expectedStatus, "starting");
        const detached = write.events[0]!;
        assert.equal(detached.type, "provider-session.detached");
        assert.equal(
          detached.providerInstanceId,
          fixture.projection.providerSessions[0]!.providerInstanceId,
        );
        if (detached.type === "provider-session.detached")
          assert.equal(
            detached.payload.providerSessionId,
            fixture.projection.providerSessions[0]!.id,
          );
        assert.deepEqual(
          write.events.map((event) => event.type),
          transition === "restart_and_resume"
            ? ["provider-session.detached", "provider-thread.updated"]
            : ["provider-session.detached"],
        );
      }
    }),
);

it.effect(
  "retirement failure, persistence failure and stale attempt each prevent replacement opening",
  () =>
    Effect.gen(function* () {
      for (const fault of ["detach", "persist", "stale"] as const) {
        const fixture = makeFixture("create_with_handoff", fault);
        const result = yield* Effect.exit(fixture.start);
        assert.equal(result._tag, "Failure");
        assert.deepEqual(fixture.order, fault === "detach" ? ["detach"] : ["detach", "persist"]);
      }
    }),
);

function makeFixture(
  transition: "restart_and_resume" | "create_with_handoff" | "switch_model_in_session" | "reuse",
  fault?: "detach" | "persist" | "stale" | "selection",
  continuation = false,
  retirement?: {
    readonly marked: boolean;
    readonly wait: Effect.Effect<void>;
    readonly stoppedBinding?: ProviderRuntimeBinding;
    readonly stoppedProvider?: OrchestrationV2ProviderThread;
    readonly stopped?: boolean;
  },
) {
  const projection = {
    thread: { id: ThreadId.make("retirement-thread") },
    runs: [
      {
        id: RunId.make("retirement-run"),
        threadId: ThreadId.make("retirement-thread"),
        activeAttemptId: RunAttemptId.make("retirement-attempt"),
        providerInstanceId: ProviderInstanceId.make("target"),
        modelSelection: { instanceId: ProviderInstanceId.make("target"), model: "synthetic" },
        ...(continuation ? { restartContinuationOfRunId: RunId.make("crashed-source") } : {}),
      },
    ],
    providerThreads: [
      retirement?.stoppedProvider ?? {
        providerSessionId: ProviderSessionId.make("target-session"),
      },
    ],
    providerSessions: [
      {
        id: ProviderSessionId.make("retired-session"),
        providerInstanceId: ProviderInstanceId.make("source"),
        driver: ProviderDriverKind.make("codex"),
      },
    ],
  } as unknown as OrchestrationV2ThreadProjection;
  const order: string[] = [];
  const writes: Array<Parameters<Sink.EventSinkV2Shape["writeIfRunCurrent"]>[0]> = [];
  const start = prepareExecutionSession(
    projection.runs[0]!,
    projection.providerThreads[0]!,
    projection,
    fault === "selection"
      ? Effect.fail(
          new ExecutionSessionRevalidationError({
            runId: projection.runs[0]!.id,
            cause: "Selection changed while retirement was pending.",
          }),
        )
      : Effect.void,
  ).pipe(
    Effect.tap(
      Effect.sync(() => {
        order.push("open");
      }),
    ),
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(Switch.ProviderSwitchServiceV2)({
          plan: () =>
            Effect.succeed({
              instanceChanged: true,
              modelChanged: true,
              targetProviderThreadId: null,
              releaseProviderSessionIds:
                transition === "switch_model_in_session" || transition === "reuse"
                  ? []
                  : [projection.providerSessions[0]!.id],
              transition: { type: transition },
            }),
        }),
        Layer.mock(Sessions.ProviderSessionManagerV2)({
          awaitThreadRetirement: () => retirement?.wait ?? Effect.void,
          // No live runtime reports background work, so released sessions detach.
          get: () => Effect.succeed(Option.none()),
          hasRetiredWriter: () => Effect.succeed(retirement?.marked ?? false),
          detach: Effect.fn("PrepareExecutionSessionTest.detach")(function* ({
            providerSessionId,
            threadId,
            requireWriterRetirement,
          }) {
            assert.isTrue(requireWriterRetirement);
            assert.equal(threadId, projection.thread.id);
            assert.equal(providerSessionId, projection.providerSessions[0]!.id);
            order.push("detach");
            if (fault === "detach")
              return yield* new Sessions.ProviderSessionCloseError({ providerSessionId });
          }),
        }),
        Layer.mock(Sink.EventSinkV2)({
          readStoppedRuntime: () => Effect.succeed(retirement?.stoppedBinding ?? null),
          hasStoppedRuntime: () => Effect.succeed(retirement?.stopped ?? false),
          writeIfRunCurrent: Effect.fn("PrepareExecutionSessionTest.writeIfRunCurrent")(
            function* (input) {
              order.push("persist");
              writes.push(input);
              if (fault === "persist")
                return yield* new Sink.EventSinkWriteError({ eventCount: input.events.length });
              return { committed: fault !== "stale", storedEvents: [] };
            },
          ),
        }),
      ),
    ),
  );
  return { projection, start, order, writes };
}

it.effect.each(["switch_model_in_session", "create_with_handoff"] as const)(
  "a %s owner continuation reserves a fresh logical session",
  (transition) =>
    Effect.gen(function* () {
      const fixture = makeFixture(transition, undefined, true);
      yield* fixture.start;
      const update = fixture.writes[0]!.events.find(
        (event) => event.type === "provider-thread.updated",
      );
      assert.isDefined(update);
      if (update?.type === "provider-thread.updated") {
        assert.notEqual(
          update.payload.providerSessionId,
          fixture.projection.providerThreads[0]!.providerSessionId,
        );
        assert.equal(
          update.payload.providerSessionId,
          `self-configure:${fixture.projection.runs[0]!.id}:${fixture.projection.runs[0]!.activeAttemptId}`,
        );
      }
      assert.equal(fixture.writes[0]!.expectedStatus, "starting");
    }),
);

it.effect(
  "a selection changed during retirement prevents replacement persistence and opening",
  () =>
    Effect.gen(function* () {
      const fixture = makeFixture("restart_and_resume", "selection");
      const result = yield* Effect.exit(fixture.start);
      assert.equal(result._tag, "Failure");
      assert.deepEqual(fixture.order, ["detach"]);
      assert.deepEqual(fixture.writes, []);
    }),
);

it.effect(
  "a cancelled transfer's source stays non-reusable; preparation waits before revalidation and persists the fresh key",
  () =>
    Effect.gen(function* () {
      for (const stale of [false, true]) {
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const fixture = makeFixture("reuse", stale ? "selection" : undefined, false, {
          marked: true,
          wait: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(released))),
        });
        const preparing = yield* fixture.start.pipe(Effect.exit, Effect.forkChild);
        yield* Deferred.await(entered);
        assert.deepEqual(fixture.order, []);
        assert.deepEqual(fixture.writes, []);
        yield* Deferred.succeed(released, undefined);
        const result = yield* Fiber.join(preparing);
        assert.equal(result._tag, stale ? "Failure" : "Success");
        if (stale) assert.deepEqual(fixture.writes, []);
        else {
          assert.equal(fixture.writes[0]!.expectedStatus, "starting");
          const update = fixture.writes[0]!.events.find(
            (event) => event.type === "provider-thread.updated",
          );
          assert.equal(update?.type, "provider-thread.updated");
          if (update?.type === "provider-thread.updated")
            assert.equal(
              update.payload.providerSessionId,
              `self-configure:${fixture.projection.runs[0]!.id}:${fixture.projection.runs[0]!.activeAttemptId}`,
            );
        }
      }
    }),
);

it.effect("an unmarked reusable runtime keeps its logical session and writes no replacement", () =>
  Effect.gen(function* () {
    const fixture = makeFixture("reuse");
    const result = yield* fixture.start;
    assert.strictEqual(result, fixture.projection.providerThreads[0]);
    assert.deepEqual(fixture.writes, []);
    assert.deepEqual(fixture.order, ["open"]);
  }),
);

it.effect.each(["exact", "wrong-session", "wrong-native", "unknown", "new-generation"] as const)(
  "ordinary send after Stop requires exact completed proof: %s",
  (mode) =>
    Effect.gen(function* () {
      const binding: ProviderRuntimeBinding = {
        threadId: ThreadId.make("retirement-thread"),
        providerThreadId: ProviderThreadId.make("stopped-provider"),
        providerSessionId: ProviderSessionId.make("stopped-session"),
        providerInstanceId: ProviderInstanceId.make("target"),
        driver: ProviderDriverKind.make("codex"),
        nativeThreadId: "stopped-native",
        runtimeGeneration: "stopped-generation",
      };
      const provider = {
        id: binding.providerThreadId,
        appThreadId: binding.threadId,
        providerSessionId: binding.providerSessionId,
        providerInstanceId: binding.providerInstanceId,
        driver: binding.driver,
        nativeThreadRef: {
          driver: binding.driver,
          nativeId: binding.nativeThreadId,
          strength: "strong",
        },
        ...(mode === "new-generation"
          ? { runtimeIdentity: { runtimeGeneration: "current-generation" } }
          : {}),
      } as unknown as OrchestrationV2ProviderThread;
      const fixture = makeFixture("reuse", undefined, false, {
        marked: false,
        wait: Effect.void,
        stoppedProvider: provider,
        stopped: mode !== "unknown" && mode !== "new-generation",
        stoppedBinding: {
          ...binding,
          ...(mode === "wrong-session"
            ? { providerSessionId: ProviderSessionId.make("foreign-session") }
            : {}),
          ...(mode === "wrong-native" ? { nativeThreadId: "foreign-native" } : {}),
        },
      });
      const result = yield* fixture.start;
      if (mode === "exact") {
        assert.equal(result.providerSessionId, "self-configure:retirement-run:retirement-attempt");
        assert.deepEqual(result.nativeThreadRef, provider.nativeThreadRef);
        assert.deepEqual(fixture.writes[0]!.runtimeIdentityBoundary, {
          expectedGeneration: null,
          expectedActiveProviderThreadId: provider.id,
          expectedProviderThreadIdentity: {
            id: provider.id,
            appThreadId: provider.appThreadId,
            driver: provider.driver,
            providerInstanceId: provider.providerInstanceId,
            providerSessionId: provider.providerSessionId,
            nativeThreadRef: provider.nativeThreadRef,
          },
        });
      } else {
        assert.strictEqual(result, provider);
        assert.deepEqual(fixture.writes, []);
      }
    }),
);
