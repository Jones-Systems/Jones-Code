import * as EffectOutbox from "./EffectOutbox.ts";
import {
  createDeterministicAttachmentId,
  createPendingAttachmentId,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  ProjectId,
  PlanId,
  ProviderInstanceId,
  ThreadId,
  MessageId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Config from "../config.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Clones from "../project/ProjectCloneTracker.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Launch from "./ThreadLaunchService.ts";
import * as Sink from "./EventSink.ts";
import * as Bridge from "./QueueCompatibility.ts";
import {
  OrchestratorCommandPreviouslyRejectedError,
  OrchestratorDispatchError,
} from "./Orchestrator.ts";

const command = {
  type: "thread.turn.start",
  commandId: "queue-test",
  threadId: "queue-thread",
  createdAt: "2000-01-01T00:00:00.000Z",
  runtimeMode: "full-access",
  interactionMode: "default",
  message: { messageId: "queue-message", role: "user", text: "Resume", attachments: [] },
};
const configLayer = Config.layerTest(process.cwd(), { prefix: "queue-compatibility-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);
const fixture = (
  failure?: "rejected" | "unknown",
  cloning = false,
  launch?: Launch.ThreadLaunchService["Service"]["launch"],
) =>
  Effect.gen(function* () {
    const received: OrchestrationV2ServerCommand[] = [];
    const services = Layer.mergeAll(
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(null),
        dispatch: (input) => {
          received.push(input);
          if (failure === "rejected")
            return Effect.fail(
              new OrchestratorCommandPreviouslyRejectedError({
                commandId: input.commandId,
                commandType: input.type,
                detail: "fixture rejection",
              }),
            );
          if (failure === "unknown")
            return Effect.fail(
              new OrchestratorDispatchError({
                commandId: input.commandId,
                commandType: input.type,
                cause: "unknown commit outcome",
              }),
            );
          return Effect.succeed({ sequence: 42, storedEvents: [] });
        },
      }),
      Layer.mock(Launch.ThreadLaunchService)({ ...(launch === undefined ? {} : { launch }) }),
      Layer.mock(Projects.ProjectService)({}),
      Layer.mock(Clones.ProjectCloneTracker)({
        get: () => Effect.succeed(cloning ? ({ phase: "running" } as never) : null),
      }),
      Layer.mock(EffectOutbox.EffectOutboxV2)({
        listByThreadId: () => Effect.succeed([]),
        awaitCompletion: () => Effect.void,
      }),
      Layer.mock(Sink.EventSinkV2)({ latestSequence: () => Effect.succeed(42) }),
    );
    const bridge = yield* Bridge.QueueCompatibility.pipe(
      Effect.provide(Bridge.layer.pipe(Layer.provide(services))),
    );
    return { bridge, received };
  });
it.layer(configLayer)("V2 compatibility command intake", (it) => {
  it.effect(
    "maps legacy turn identity to an immediate V2 message without trusting client time",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        assert.deepEqual(yield* f.bridge.dispatch(command), { sequence: 42 });
        assert.lengthOf(f.received, 1);
        assert.include(f.received[0], {
          type: "message.dispatch",
          commandId: CommandId.make("queue-test"),
          threadId: ThreadId.make("queue-thread"),
          messageId: MessageId.make("queue-message"),
          createdBy: "user",
          creationSource: "web",
        });
        assert.notProperty(f.received[0], "createdAt");
      }),
  );
  it.effect("rejects unknown legacy fields before dispatch", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const error = yield* f.bridge.dispatch({ ...command, unsupported: true }).pipe(Effect.flip);
      assert.equal(error.reason, "invalid_command");
      assert.lengthOf(f.received, 0);
    }),
  );
  it.effect("rejects guarded bootstrap before attachment writes or launch", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const error = yield* f.bridge
        .dispatch({
          ...command,
          bootstrap: { runSetupScript: true },
          dispatchGuard: {
            observedSnapshotSequence: 0,
            expectedModelSelection: { instanceId: "codex", model: "gpt-6" },
            expectedSessionStatus: null,
            expectedActiveTurnId: null,
            expectedLatestTurnId: null,
            requireIdle: true,
          },
        })
        .pipe(Effect.flip);
      assert.equal(error.reason, "dispatch_guard_bootstrap_unsupported");
      assert.lengthOf(f.received, 0);
    }),
  );
  it.effect(
    "forwards the complete WebSocket bootstrap guard and original C to the receiving launcher",
    () =>
      Effect.gen(function* () {
        const forwarded: Launch.ThreadLaunchInput[] = [];
        const f = yield* fixture(undefined, false, (input) => {
          forwarded.push(input);
          return Effect.fail(
            new Launch.ThreadLaunchError({
              operation: "release-run",
              commandId: input.commandId,
              projectId: input.projectId,
              threadId: input.threadId,
              cause: new OrchestratorCommandPreviouslyRejectedError({
                commandId: input.preparationReleaseCommandId!,
                commandType: "prepared-run.release",
                detail: "Fixture final guard rejection",
              }),
            }),
          );
        });
        const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6" };
        const dispatchGuard = {
          observedSnapshotSequence: 3,
          expectedModelSelection: modelSelection,
          expectedSessionStatus: null,
          expectedActiveTurnId: null,
          expectedLatestTurnId: null,
          requireIdle: true as const,
        };
        const input = {
          ...command,
          commandId: CommandId.make(command.commandId),
          threadId: ThreadId.make(command.threadId),
          message: { ...command.message, messageId: MessageId.make(command.message.messageId) },
          modelSelection,
          titleSeed: "Forwarded title",
          sourceProposedPlan: {
            threadId: ThreadId.make("source-thread"),
            planId: PlanId.make("source-plan"),
          },
          dispatchGuard,
          bootstrap: {
            createThread: {
              projectId: "project",
              title: "Queue",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt: command.createdAt,
            },
          },
        };
        const rejected = yield* f.bridge.dispatch(input, "legacy_websocket").pipe(Effect.flip);
        assert.equal(rejected.reason, "dispatch_guard_rejected");
        assert.lengthOf(forwarded, 1);
        const launch = forwarded[0]!;
        assert.equal(launch.preparationReleaseCommandId, input.commandId);
        assert.notEqual(launch.commandId, input.commandId);
        assert.notEqual(launch.legacyBootstrap!.birthCommandId, input.commandId);
        assert.notEqual(launch.commandId, launch.legacyBootstrap!.birthCommandId);
        assert.deepEqual(launch.legacyBootstrap!.dispatchGuard, dispatchGuard);
        assert.include(launch, {
          threadId: input.threadId,
          projectId: ProjectId.make("project"),
          title: "Queue",
          runSetupScript: false,
        });
        assert.deepEqual(launch.modelSelection, modelSelection);
        assert.deepEqual(launch.initialMessage, {
          messageId: input.message.messageId,
          text: input.message.text,
          attachments: [],
          context: undefined,
          titleSeed: input.titleSeed,
          sourcePlanRef: input.sourceProposedPlan,
        });
        assert.lengthOf(f.received, 0);
      }),
  );
  it.effect("holds bootstrap while its project clone is running", () =>
    Effect.gen(function* () {
      const f = yield* fixture(undefined, true);
      const error = yield* f.bridge
        .dispatch({
          ...command,
          bootstrap: {
            createThread: {
              projectId: "project",
              title: "Queue",
              modelSelection: { instanceId: "codex", model: "gpt-6" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt: command.createdAt,
            },
          },
        })
        .pipe(Effect.flip);
      assert.equal(error.reason, "orchestration_dispatch_failed");
      assert.lengthOf(f.received, 0);
    }),
  );
  it.effect("cleans only the newly written upload when later preparation fails", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Config.ServerConfig;
      const messageId = MessageId.make("queue-partial-upload");
      const first = {
        type: "image" as const,
        name: "first.png",
        mimeType: "image/png",
        sizeBytes: 6,
        dataUrl: "data:image/png;base64,cGl4ZWxz",
      };
      const error = yield* f.bridge
        .dispatch({
          ...command,
          message: {
            ...command.message,
            messageId,
            attachments: [
              first,
              { ...first, name: "invalid.png", dataUrl: "data:image/jpeg;base64,cGl4ZWxz" },
            ],
          },
        })
        .pipe(Effect.flip);
      assert.equal(error.reason, "invalid_command");
      assert.lengthOf(f.received, 0);
      const file = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: {
          type: "image",
          id: createDeterministicAttachmentId(command.threadId, `${messageId}:0`)! as never,
          name: first.name,
          mimeType: first.mimeType,
          sizeBytes: 6,
        },
      })!;
      assert.isFalse(yield* fs.exists(file));
    }),
  );
  it.effect("rejects a changed retry upload without replacing accepted-or-unknown bytes", () =>
    Effect.gen(function* () {
      const f = yield* fixture("unknown");
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Config.ServerConfig;
      const messageId = MessageId.make("queue-changed-retry");
      const upload = {
        type: "image" as const,
        name: "retry.png",
        mimeType: "image/png",
        sizeBytes: 6,
        dataUrl: "data:image/png;base64,cGl4ZWxz",
      };
      yield* f.bridge
        .dispatch({ ...command, message: { ...command.message, messageId, attachments: [upload] } })
        .pipe(Effect.flip);
      const error = yield* f.bridge
        .dispatch({
          ...command,
          message: {
            ...command.message,
            messageId,
            attachments: [{ ...upload, dataUrl: "data:image/png;base64,b3RoZXI=" }],
          },
        })
        .pipe(Effect.flip);
      assert.equal(error.reason, "invalid_command");
      assert.lengthOf(f.received, 1);
      const accepted =
        f.received[0]!.type === "message.dispatch" ? f.received[0]!.attachments[0]! : undefined;
      assert.isDefined(accepted);
      const file = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: accepted!,
      })!;
      assert.equal(yield* fs.readFileString(file), "pixels");
      yield* fs.remove(file);
    }),
  );
  it.effect("copies a pending upload into its target scope and preserves the pending source", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Config.ServerConfig;
      const id = createPendingAttachmentId();
      const attachment = {
        type: "image" as const,
        id: id as never,
        name: "pending.png",
        mimeType: "image/png",
        sizeBytes: 6,
      };
      const pending = resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment })!;
      yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fs.writeFileString(pending, "pixels");
      yield* f.bridge.dispatch({
        ...command,
        message: {
          ...command.message,
          messageId: MessageId.make("queue-pending-upload"),
          attachments: [attachment],
        },
      });
      const accepted =
        f.received[0]!.type === "message.dispatch" ? f.received[0]!.attachments[0]! : undefined;
      assert.isDefined(accepted);
      assert.notEqual(accepted!.id, id);
      const claimed = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: accepted!,
      })!;
      assert.equal(yield* fs.readFileString(claimed), "pixels");
      assert.isTrue(yield* fs.exists(pending));
      assert.notProperty(f.received[0], "queuedToolBoundaryEligible");
      yield* fs.remove(claimed);
      yield* fs.remove(pending);
    }),
  );
  it.effect("rejects foreign attachment scope before accepting a message", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const error = yield* f.bridge
        .dispatch({
          ...command,
          message: {
            ...command.message,
            attachments: [
              {
                type: "image",
                id: createDeterministicAttachmentId("foreign-thread", "upload"),
                name: "foreign.png",
                mimeType: "image/png",
                sizeBytes: 6,
              },
            ],
          },
        })
        .pipe(Effect.flip);
      assert.equal(error.reason, "invalid_command");
      assert.lengthOf(f.received, 0);
    }),
  );
  it.effect.each(["rejected", "unknown"] as const)(
    "retains uploads only for %s dispatch outcome",
    (failure) =>
      Effect.gen(function* () {
        const f = yield* fixture(failure);
        const fs = yield* FileSystem.FileSystem;
        const config = yield* Config.ServerConfig;
        const error = yield* f.bridge
          .dispatch({
            ...command,
            message: {
              ...command.message,
              attachments: [
                {
                  type: "image",
                  name: "pixels.png",
                  mimeType: "image/png",
                  sizeBytes: 1,
                  dataUrl: "data:image/png;base64,cGl4ZWxz",
                },
              ],
            },
          })
          .pipe(Effect.flip);
        assert.equal(error.reason, "orchestration_dispatch_failed");
        assert.lengthOf(f.received, 1);
        const attachments =
          f.received[0]!.type === "message.dispatch" ? f.received[0]!.attachments : [];
        assert.equal(attachments[0]!.sizeBytes, 6);
        const files = yield* fs.readDirectory(config.attachmentsDir);
        assert.equal(files.length, failure === "unknown" ? 1 : 0);
      }),
  );
});
