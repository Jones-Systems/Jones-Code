import { expect, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as GoalStart from "./GoalStartDispatch.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("The durable-dispatch fixture never starts a provider"),
} as ProviderAdapterV2Shape;
const database = Sqlite.layerMemory;
const runtime = Replay.layerWithRegistry(
  { name: "goal-start-dispatch" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
);
const threadId = ThreadId.make("goal-thread");
function layer(lostAck = false) {
  const threads = Layer.effect(
    Threads.ThreadManagementService,
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      return Threads.ThreadManagementService.of({
        getThreadRecords: orchestrator.getThreadRecords,
        dispatch: (command) =>
          orchestrator.dispatch(command).pipe(
            Effect.flatMap((accepted) =>
              lostAck
                ? Effect.fail(
                    new Orchestrator.OrchestratorDispatchError({
                      commandId: command.commandId,
                      commandType: command.type,
                      cause: "synthetic lost acknowledgement",
                    }),
                  )
                : Effect.succeed(accepted),
            ),
          ),
      } as Threads.ThreadManagementService["Service"]);
    }),
  ).pipe(Layer.provide(runtime));
  return GoalStart.layer.pipe(
    Layer.provide(threads),
    Layer.provideMerge(runtime),
    Layer.provide(database),
  );
}
const create = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project"),
    title: "Goal",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  return (yield* orchestrator.getThreadProjection(threadId)).thread;
});
it.effect.each([false, true])("idle resume uses one durable run with lostAck=%s", (lostAck) =>
  Effect.gen(function* () {
    const thread = yield* create;
    const service = yield* GoalStart.GoalStartDispatch;
    const guard = yield* service.capture(threadId);
    expect(guard).not.toBeNull();
    const input = { thread, guard: guard!, requestId: "stable-key", text: "/goal resume" };
    const first = yield* service.start(input);
    expect(first.outcome).toBe("accepted");
    expect(yield* service.start(input)).toEqual(first);
    const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(threadId);
    expect(projection.runs).toHaveLength(1);
    expect(projection.messages).toHaveLength(1);
    expect(projection.messages[0]?.text).toBe("/goal resume");
  }).pipe(Effect.scoped, Effect.provide(layer(lostAck))),
);

it.effect("an intervening run rejects resume instead of queuing it", () =>
  Effect.gen(function* () {
    const thread = yield* create;
    const service = yield* GoalStart.GoalStartDispatch;
    const guard = yield* service.capture(threadId);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("other"),
      messageId: MessageId.make("other"),
      threadId,
      text: "Owner work",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
    expect(
      yield* service.start({ thread, guard: guard!, requestId: "resume", text: "/goal resume" }),
    ).toMatchObject({ reason: "dispatch_rejected" });
    expect((yield* orchestrator.getThreadProjection(threadId)).runs).toHaveLength(1);
  }).pipe(Effect.scoped, Effect.provide(layer())),
);
