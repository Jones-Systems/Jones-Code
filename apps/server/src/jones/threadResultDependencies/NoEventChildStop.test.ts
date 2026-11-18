import { assert, it } from "@effect/vitest";
import {
  CommandId,
  NodeId,
  OrchestrationV2Subagent,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ThreadTitleStateError } from "../threadTitle/store.ts";
import * as Fixture from "./testkit.ts";

const decodeSubagent = Schema.decodeUnknownSync(OrchestrationV2Subagent);

it.effect(
  "a no-event child Stop releases its duties in the receipt transaction and never on replay",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const subagentId = NodeId.make("completed-child");
      yield* f.sink.write({
        events: [
          f.event({
            type: "subagent.updated",
            threadId: f.receiver.id,
            payload: decodeSubagent({
              id: subagentId,
              threadId: f.receiver.id,
              runId: f.receiverRun.id,
              parentNodeId: NodeId.make("parent-node"),
              origin: "app_owned",
              createdBy: "agent",
              driver: "codex",
              providerInstanceId: f.waiter.providerInstanceId,
              providerThreadId: null,
              childThreadId: f.waiter.id,
              nativeTaskRef: null,
              prompt: "Synthetic child",
              title: null,
              model: null,
              status: "completed",
              result: "done",
              startedAt: Fixture.time,
              completedAt: Fixture.time,
              updatedAt: Fixture.time,
            }),
          }),
        ],
      });
      yield* f.send("before-stop");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
      const command = {
        type: "subagent.stop" as const,
        commandId: CommandId.make("no-event-child-stop"),
        threadId: f.receiver.id,
        runId: f.receiverRun.id,
        subagentId,
      } satisfies OrchestrationV2ServerCommand;
      const input = {
        commandId: command.commandId,
        commandType: command.type,
        threadId: command.threadId,
        ordinaryCheckoutCommand: command,
        acceptedAt: Fixture.time,
        events: [],
        effects: [],
      };
      const failed = yield* f.sink
        .commitCommand({
          ...input,
          threadTitle: {
            accept: Effect.fail(new ThreadTitleStateError({ cause: "after release" })),
          },
        })
        .pipe(Effect.result);
      assert.equal(failed._tag, "Failure");
      assert.equal((yield* f.store.get("dependency:before-stop"))?.state, "awaiting");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
      assert.equal(yield* f.store.waiterGeneration(f.waiter.id), 0);
      const accepted = yield* f.sink.commitCommand(input);
      assert.isTrue(accepted.committed);
      assert.isEmpty(accepted.storedEvents);
      assert.equal((yield* f.store.get("dependency:before-stop"))?.state, "released");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 0);
      const generation = yield* f.store.waiterGeneration(f.waiter.id);
      assert.equal(generation, 1);
      const next = f.requestInput("after-stop");
      yield* f.send("after-stop", f.receiverRun, {
        ...next,
        localDependency: { ...next.localDependency!, waiterGeneration: generation },
      });
      const replay = yield* f.sink.commitCommand(input);
      assert.isFalse(replay.committed);
      assert.deepEqual(replay.receipt, accepted.receipt);
      assert.isEmpty(replay.storedEvents);
      assert.equal((yield* f.store.get("dependency:after-stop"))?.state, "awaiting");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
      assert.equal(yield* f.store.waiterGeneration(f.waiter.id), generation);
    }).pipe(Effect.provide(Fixture.layer)),
);
