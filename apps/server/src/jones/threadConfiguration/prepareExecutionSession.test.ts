import { assert, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ProviderDriverKind,
  RunId,
  RunAttemptId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import { prepareExecutionSession } from "./prepareExecutionSession.ts";

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
  transition: "restart_and_resume" | "create_with_handoff" | "switch_model_in_session",
  fault?: "detach" | "persist" | "stale" | "selection",
  continuation = false,
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
    providerThreads: [{ providerSessionId: ProviderSessionId.make("target-session") }],
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
      ? Effect.fail("Selection changed while retirement was pending.")
      : Effect.void,
  ).pipe(
    Effect.andThen(
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
                transition === "switch_model_in_session"
                  ? []
                  : [projection.providerSessions[0]!.id],
              transition: { type: transition },
            }),
        }),
        Layer.mock(Sessions.ProviderSessionManagerV2)({
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
