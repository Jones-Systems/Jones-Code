import {
  MessageId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ProjectedTurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, test } from "vite-plus/test";

import { buildThreadFeed, deriveThreadFeedPresentation } from "../../lib/threadActivity";

// #63/#68 measured the removed V1 sort/merge feed, not canonical V2 rows.
// Legacy warm-cache gains do not qualify V2, Hermes, frame time, or memory cost.
// Changed-input cases include preparation in the clock so fresh identities never
// silently become warm cache hits. Compare each case only with the same case.
const options = { warmupTime: 100, time: 400 };
const threadId = ThreadId.make("benchmark-thread");
const now = DateTime.makeUnsafe("2026-09-01T00:00:00.000Z");
function makeRows(count: number): OrchestrationV2ProjectedTurnItem[] {
  return Array.from({ length: count }, (_, index) => {
    const common = {
      id: TurnItemId.make(`item-${index}`),
      threadId,
      runId: RunId.make(`run-${Math.floor(index / 3)}`),
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: index,
      status: "completed" as const,
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
    };
    const item =
      index % 3 === 1
        ? {
            ...common,
            type: "command_execution" as const,
            input: "synthetic-check",
            output: "ok",
            exitCode: 0,
          }
        : {
            ...common,
            type: "assistant_message" as const,
            messageId: MessageId.make(`message-${index}`),
            text: "Synthetic history. ".repeat(8),
            streaming: false,
          };
    return {
      position: index,
      visibility: "local",
      sourceThreadId: threadId,
      sourceItemId: item.id,
      item,
    };
  });
}

for (const count of [30, 300, 3_000]) {
  describe(`mobile V2 feed / ${count} canonical rows`, () => {
    const rows = makeRows(count);
    const feed = buildThreadFeed(rows);
    const firstItem = rows[0]!.item;
    if (firstItem.type !== "assistant_message") throw new Error("Expected assistant fixture");
    const expanded = new Set(rows.map(({ item }) => item.runId!));
    const anchor = {
      id: MessageId.make("local-anchor"),
      role: "user" as const,
      text: "Local feedback",
      turnId: null,
      streaming: false,
      createdAt: "2026-09-01T00:00:01.000Z",
      updatedAt: "2026-09-01T00:00:01.000Z",
    };
    const cases: Record<string, () => ReturnType<typeof buildThreadFeed>> = {
      "unchanged rows (warm)": () => buildThreadFeed(rows),
      "new array, same rows": () => buildThreadFeed([...rows]),
      "all fresh row/item identities": () => buildThreadFeed(makeRows(count)),
      "streaming tail replacement": () =>
        buildThreadFeed(
          rows.map((row, index) =>
            index === count - 1 && row.item.type === "assistant_message"
              ? { ...row, item: { ...row.item, text: "Streaming replacement", streaming: true } }
              : row,
          ),
        ),
      "middle replacement": () =>
        buildThreadFeed(
          rows.map((row, index) =>
            index === Math.floor(count / 2) && row.item.type === "assistant_message"
              ? { ...row, item: { ...row.item, text: "Middle replacement" } }
              : row,
          ),
        ),
      "prepend older page": () =>
        buildThreadFeed([
          {
            ...rows[0]!,
            sourceItemId: TurnItemId.make("older"),
            item: {
              ...firstItem,
              id: TurnItemId.make("older"),
              ordinal: -1,
              messageId: MessageId.make("older-message"),
            },
          },
          ...rows,
        ]),
      "append new row": () =>
        buildThreadFeed([
          ...rows,
          {
            ...rows[0]!,
            position: count,
            sourceItemId: TurnItemId.make("appended"),
            item: {
              ...firstItem,
              id: TurnItemId.make("appended"),
              ordinal: count,
              messageId: MessageId.make("appended-message"),
            },
          },
        ]),
      "local anchored feedback": () => buildThreadFeed(rows, { anchoredMessages: [{ ...anchor }] }),
      "collapsed presentation": () => deriveThreadFeedPresentation(feed, null, new Set()),
      "expanded presentation": () => deriveThreadFeedPresentation(feed, null, expanded),
    };
    for (const [name, run] of Object.entries(cases)) {
      test(name, async ({ bench }) => {
        await bench(name, run).run(options);
      });
    }
  });
}
