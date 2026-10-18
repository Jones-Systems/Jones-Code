import {
  EventId,
  MessageId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { describe, test } from "vite-plus/test";

import { applyOrchestrationV2ProjectionEvent } from "../state/orchestrationV2Projection.ts";
import { v2Projection, v2Now } from "../state/orchestrationV2TestFixtures.ts";

const thread: OrchestrationV2ThreadProjection = {
  ...v2Projection,
  messages: Array.from({ length: 100 }, (_, index) => ({
    id: MessageId.make(`message-${index}`),
    threadId: v2Projection.thread.id,
    runId: null,
    nodeId: null,
    role: "assistant",
    text: "Message text. ".repeat(40),
    attachments: [],
    streaming: false,
    createdBy: "agent",
    creationSource: "provider",
    createdAt: v2Now,
    updatedAt: v2Now,
  })),
};

const delta: Extract<OrchestrationV2DomainEvent, { type: "message.updated" }> = {
  id: EventId.make("delta"),
  type: "message.updated",
  threadId: thread.thread.id,
  occurredAt: v2Now,
  payload: { ...thread.messages[99]!, text: " next", streaming: true },
};

// V2 replaces the first matching entity; V1 delta/all-duplicate measurements
// from #63/#66 are not comparable. The legacy #66 experiment reported cold-array
// and small-history regressions; memory cost remained unmeasured. No cache is
// adopted here. These cases include immutable array copies.
describe("remote V2 message replay characterization", () => {
  const options = { warmupTime: 100, time: 400 };
  for (const count of [10, 100, 1_000, 10_000]) {
    const loaded: OrchestrationV2ThreadProjection = {
      ...thread,
      messages: Array.from({ length: count }, (_, index) => ({
        ...thread.messages[0]!,
        id: MessageId.make(`message-${index}`),
      })),
    };
    for (const [position, indexes] of [
      ["tail", [count - 1]],
      ["middle", [Math.floor(count / 2)]],
      ["alternating first/tail", [0, count - 1]],
    ] as const) {
      const name = `200 ${position} replacements / ${count} messages`;
      test(name, async ({ bench }) => {
        await bench(name, () => {
          let current = loaded;
          for (let index = 0; index < 200; index += 1) {
            current = applyOrchestrationV2ProjectionEvent(current, {
              ...delta,
              payload: {
                ...loaded.messages[indexes[index % indexes.length]!]!,
                text: `replacement ${index}`,
              },
            })!;
          }
        }).run(options);
      });
    }
    for (const mode of ["fresh array tail", "missing ID append"] as const) {
      const name = `${mode} / ${count} messages (includes input copy)`;
      test(name, async ({ bench }) => {
        await bench(name, () => {
          applyOrchestrationV2ProjectionEvent(
            { ...loaded, messages: [...loaded.messages] },
            {
              ...delta,
              payload: {
                ...loaded.messages.at(-1)!,
                id:
                  mode === "missing ID append"
                    ? MessageId.make("missing")
                    : loaded.messages.at(-1)!.id,
                text: "replacement",
              },
            },
          );
        }).run(options);
      });
    }
  }
  const duplicateLoaded: OrchestrationV2ThreadProjection = {
    ...thread,
    messages: Array.from({ length: 1_000 }, (_, index) => ({
      ...thread.messages[0]!,
      id: MessageId.make(index === 999 ? "message-0" : `message-${index}`),
    })),
  };
  test("200 first-match replacements with duplicate IDs / 1000 messages", async ({ bench }) => {
    await bench("duplicate IDs", () => {
      let current = duplicateLoaded;
      for (let index = 0; index < 200; index += 1) {
        current = applyOrchestrationV2ProjectionEvent(current, {
          ...delta,
          payload: { ...duplicateLoaded.messages[0]!, text: `replacement ${index}` },
        })!;
      }
    }).run(options);
  });
});
