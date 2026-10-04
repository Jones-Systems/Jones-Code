import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { decideOrchestrationCommand } from "../../orchestration/decider.ts";
import { createEmptyReadModel, projectEvent } from "../../orchestration/projector.ts";
import { OrchestrationThreadSettleBlockedError } from "../../orchestration/Errors.ts";
import { makeWorkstreamsNativeProvider } from "./service.ts";
import { makeProviderFixture, binding, request, requestBytesSha256, now } from "./testFixtures.ts";

const crypto = Crypto.make({
  randomBytes: (size) => new Uint8Array(size).fill(1),
  digest: (_algorithm, bytes) => Effect.succeed(bytes),
});

it.effect(
  "native policy denial retains pins, snooze and history and produces only a rejected receipt",
  () =>
    Effect.gen(function* () {
      let model = createEmptyReadModel(now);
      const projectId = ProjectId.make("project-synthetic");
      const threadId = ThreadId.make(request.identity.native_id);
      const commands: OrchestrationCommand[] = [
        {
          type: "project.create",
          commandId: CommandId.make("project-create-synthetic"),
          projectId,
          title: "Synthetic project",
          workspaceRoot: "/synthetic/project",
          createdAt: now,
        },
        {
          type: "thread.create",
          commandId: CommandId.make("thread-create-synthetic"),
          threadId,
          projectId,
          title: "Synthetic thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
          interactionMode: "default",
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt: now,
        },
      ];
      for (const command of commands) {
        const decided = yield* decideOrchestrationCommand({ command, readModel: model });
        for (const event of Array.isArray(decided) ? decided : [decided])
          model = yield* projectEvent(model, { ...event, sequence: model.snapshotSequence + 1 });
      }
      model = {
        ...model,
        threads: model.threads.map((thread) => ({
          ...thread,
          pinnedAt: now,
          pinOrderKey: "synthetic-order",
          snoozedAt: now,
          snoozedUntil: "2030-01-01T00:00:00.000Z",
          session: {
            threadId,
            status: "running" as const,
            providerName: "codex" as const,
            runtimeMode: "full-access" as const,
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        })),
      };
      const before = structuredClone(model);
      const fixture = makeProviderFixture();
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        engine: {
          dispatch: (command) =>
            Effect.gen(function* () {
              const outcome = yield* decideOrchestrationCommand({ command, readModel: model }).pipe(
                Effect.result,
              );
              assert.strictEqual(outcome._tag, "Failure");
              if (outcome._tag !== "Failure") throw new Error("Expected native rejection.");
              assert.strictEqual(outcome.failure._tag, "OrchestrationThreadSettleBlockedError");
              fixture.receipts.set(command.commandId, {
                commandId: command.commandId,
                aggregateKind: "thread",
                aggregateId: threadId,
                acceptedAt: now,
                resultSequence: model.snapshotSequence,
                status: "rejected",
                error: "synthetic policy denial",
              });
              return yield* new OrchestrationThreadSettleBlockedError({ threadId });
            }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
        },
      });
      const result = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(result.state, "terminal");
      if (result.state === "terminal") assert.strictEqual(result.result.native_outcome, "denied");
      assert.deepEqual(model, before);
      assert.strictEqual(fixture.events.size, 0);
      assert.strictEqual(fixture.receipts.size, 1);
    }).pipe(Effect.provideService(Crypto.Crypto, crypto)),
);
