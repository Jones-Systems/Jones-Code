import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { QueuedSteerBatchCapability, QueuedSteerBatchCommand } from "./queuedSteerBatch.ts";
import { ExecutionEnvironmentCapabilities } from "../environment.ts";
it("keeps the batch capability optional for old environments", () => {
  const legacy = Schema.decodeUnknownSync(
    Schema.Struct({ queuedSteerBatch: ExecutionEnvironmentCapabilities.fields.queuedSteerBatch }),
  )({});
  expect(legacy.queuedSteerBatch).toBeUndefined();
  expect(
    Schema.is(QueuedSteerBatchCapability)({
      maxInputChars: 120000,
      maxAttachments: 100,
      maxItems: 100,
      maxImageBytes: 83886080,
    }),
  ).toBe(true);
});
it("requires a nonempty bounded member list with current message identities", () => {
  const command = {
    type: "queued-message.promote-batch-to-steer",
    commandId: "batch",
    threadId: "thread",
    targetRunId: "target",
    items: [
      { queuedRunId: "queue", messageId: "message", messageUpdatedAt: "2026-10-20T00:00:00.000Z" },
    ],
  };
  const decode = Schema.decodeUnknownSync(QueuedSteerBatchCommand);
  expect(decode(command).items).toHaveLength(1);
  expect(() => decode({ ...command, items: [] })).toThrow();
  expect(() =>
    decode({ ...command, items: Array.from({ length: 101 }, () => command.items[0]) }),
  ).toThrow();
});
