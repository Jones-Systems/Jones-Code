import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OrchestrationV2DispatchCommandError,
  OrchestrationV2ThreadLaunchError,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Database from "../../persistence/Sqlite.ts";
import { dispatchDeliveryEvidence, launchDeliveryEvidence } from "./deliveryEvidence.ts";

const layer = Receipts.layer.pipe(Layer.provide(Database.layerMemory));
const threadId = ThreadId.make("thread:delivery-evidence");
const projectId = ProjectId.make("project:delivery-evidence");
const command = (key: string) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(key),
  threadId,
  messageId: MessageId.make(`${key}:message`),
  text: "Keep my command identity",
  attachments: [],
  dispatchMode: { type: "start_immediately" as const },
  createdBy: "user" as const,
  creationSource: "web" as const,
});

it.effect("only an exact typed durable dispatch receipt classifies acceptance or rejection", () =>
  Effect.gen(function* () {
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    for (const row of [
      {
        key: "accepted",
        status: "accepted",
        type: "message.dispatch",
        target: threadId,
        expected: "accepted",
      },
      {
        key: "rejected",
        status: "rejected",
        type: "message.dispatch",
        target: threadId,
        expected: "rejected",
      },
      {
        key: "wrong-type",
        status: "accepted",
        type: "thread.create",
        target: threadId,
        expected: "unknown",
      },
      {
        key: "wrong-aggregate",
        status: "rejected",
        type: "message.dispatch",
        target: ThreadId.make("other"),
        expected: "unknown",
      },
    ] as const) {
      const input = command(row.key);
      yield* receipts.insertIfAbsent({
        commandId: input.commandId,
        commandType: row.type,
        threadId: row.target,
        status: row.status,
        acceptedAt: yield* DateTime.now,
        resultSequence: 0,
        error: null,
      });
      const deliveryEvidence = yield* dispatchDeliveryEvidence(receipts, input);
      assert.deepEqual(deliveryEvidence, {
        kind: "dispatch",
        commandId: input.commandId,
        commandType: input.type,
        threadId,
        status: row.expected,
      });
      const original = new Error("The original post-commit failure");
      const error = new OrchestrationV2DispatchCommandError({
        commandId: input.commandId,
        commandType: input.type,
        message: "Dispatch failed",
        cause: original,
        deliveryEvidence,
      });
      assert.strictEqual(error.cause, original);
      const codec = Schema.toCodecJson(OrchestrationV2DispatchCommandError);
      const decoded = yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(error));
      assert.deepEqual(decoded.deliveryEvidence, deliveryEvidence);
    }
    assert.equal((yield* dispatchDeliveryEvidence(receipts, command("absent"))).status, "unknown");
    const another = yield* receipts.getByCommandId(CommandId.make("accepted"));
    assert.equal(
      (yield* dispatchDeliveryEvidence(
        { getByCommandId: () => Effect.succeed(another) },
        command("different-command"),
      )).status,
      "unknown",
    );
    for (const read of [
      Effect.fail(
        new Receipts.CommandReceiptStoreReadError({ commandId: CommandId.make("unreadable") }),
      ),
      Effect.die(new Error("Synthetic decoding/read defect")),
    ]) {
      assert.equal(
        (yield* dispatchDeliveryEvidence({ getByCommandId: () => read }, command("unreadable")))
          .status,
        "unknown",
      );
    }
  }).pipe(Effect.provide(layer)),
);
const decodeOrchestrationV2ThreadLaunchError = Schema.decodeEffect(
  Schema.toCodecJson(OrchestrationV2ThreadLaunchError),
);

it.effect("launch absence and failed reads leave a requested initial message unknown", () =>
  Effect.gen(function* () {
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const input = {
      commandId: CommandId.make("missing-launch"),
      projectId,
      threadId,
      initialMessage: { messageId: MessageId.make("first"), text: "First", attachments: [] },
    };
    const events = { readByCommandId: () => Stream.die("Absent receipts must not read events") };
    for (const reader of [
      receipts,
      {
        getByCommandId: () =>
          Effect.fail(new Receipts.CommandReceiptStoreReadError({ commandId: input.commandId })),
      },
    ]) {
      const evidence = yield* launchDeliveryEvidence(reader, events, input);
      assert.deepEqual(evidence.creation, { status: "unknown" });
      assert.deepEqual(evidence.initialMessage, {
        status: "unknown",
        commandId: CommandId.make("missing-launch:initial-message"),
        messageId: input.initialMessage.messageId,
      });
    }
    const omitted = yield* launchDeliveryEvidence(receipts, events, {
      ...input,
      initialMessage: undefined,
    });
    assert.deepEqual(omitted.initialMessage, { status: "not-requested" });
    for (const row of [
      {
        key: "launch-rejected",
        type: "thread.create",
        target: threadId,
        status: "rejected",
        expected: "rejected",
      },
      {
        key: "launch-wrong-type",
        type: "thread.metadata.update",
        target: threadId,
        status: "rejected",
        expected: "unknown",
      },
      {
        key: "launch-wrong-target",
        type: "thread.create",
        target: ThreadId.make("other"),
        status: "rejected",
        expected: "unknown",
      },
      {
        key: "launch-unproven-claim",
        type: "thread.create",
        target: threadId,
        status: "accepted",
        expected: "unknown",
      },
    ] as const) {
      const commandId = CommandId.make(row.key);
      yield* receipts.insertIfAbsent({
        commandId,
        commandType: row.type,
        threadId: row.target,
        status: row.status,
        acceptedAt: yield* DateTime.now,
        resultSequence: 0,
        error: null,
      });
      const evidence = yield* launchDeliveryEvidence(
        receipts,
        { readByCommandId: () => Stream.empty },
        { ...input, commandId },
      );
      assert.equal(evidence.creation.status, row.expected);
      assert.equal(evidence.initialMessage.status, "unknown");
    }
    const old = yield* decodeOrchestrationV2ThreadLaunchError({
      _tag: "OrchestrationV2ThreadLaunchError",
      commandId: input.commandId,
      projectId,
      message: "Old server error",
    });
    assert.isUndefined(old.deliveryEvidence);
  }).pipe(Effect.provide(layer)),
);
