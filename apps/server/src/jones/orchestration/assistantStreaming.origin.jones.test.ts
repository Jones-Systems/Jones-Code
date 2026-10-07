import { assert, it } from "@effect/vitest";
import {
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import type { ProviderAdapterV2Event } from "../../orchestration-v2/ProviderAdapter.ts";
import { makeAssistantStreamingFilter } from "../../orchestration-v2/assistantStreaming.ts";
import * as ProviderEventOrigin from "./ProviderEventOrigin.ts";

const driver = ProviderDriverKind.make("codex");
const threadId = ThreadId.make("thread:origin-streaming");
const messageId = MessageId.make("message:origin-streaming");
const now = DateTime.makeUnsafe("2026-10-07T00:00:00Z");
const text = "Ready paragraph\n\nStill arriving";

const samples: ReadonlyArray<ProviderAdapterV2Event> = [
  {
    type: "message.updated",
    driver,
    message: {
      id: messageId,
      threadId,
      runId: null,
      nodeId: null,
      createdBy: "agent",
      creationSource: "provider",
      role: "assistant",
      text,
      attachments: [],
      createdAt: now,
      updatedAt: now,
      streaming: true,
    },
  },
  {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: TurnItemId.make("item:origin-streaming"),
      type: "assistant_message",
      messageId,
      threadId,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 0,
      status: "running",
      title: null,
      startedAt: null,
      completedAt: null,
      updatedAt: now,
      text,
      streaming: true,
    },
  },
];

it.effect("preserves the original producer through both actual streaming clone branches", () =>
  Effect.gen(function* () {
    const runtime = {
      driver,
      instanceId: ProviderInstanceId.make("instance:origin-streaming"),
      providerSessionId: ProviderSessionId.make("session:origin-streaming"),
      eventOriginMode: "captured" as const,
    };
    const producer = ProviderEventOrigin.makeProviderEventProducer(runtime);
    for (const sample of samples) {
      const original = ProviderEventOrigin.stampProviderEvent(Object.freeze(sample), {
        producer: producer.origin,
      });
      const wire = JSON.stringify(original);
      const filtered = makeAssistantStreamingFilter("paragraph")(original, 0);
      assert.isNotNull(filtered);
      assert.notStrictEqual(filtered, original);
      const captured = ProviderEventOrigin.readProviderEventOrigin(filtered!);
      assert.strictEqual(captured?.producer.token, producer.origin.token);
      assert.strictEqual(captured?.producer.revalidateCurrent, producer.origin.revalidateCurrent);
      assert.equal(JSON.stringify(original), wire);
      yield* ProviderEventOrigin.revalidateProviderEventOrigin(filtered!, runtime);
    }
    producer.retire();
    for (const sample of samples) {
      const filtered = makeAssistantStreamingFilter("paragraph")(sample, 0)!;
      assert.equal(
        (yield* ProviderEventOrigin.revalidateProviderEventOrigin(filtered, runtime).pipe(Effect.flip))._tag,
        "ProviderEventOriginStaleError",
      );
    }
  }),
);
