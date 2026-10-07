import { expect, it } from "vite-plus/test";
import { CommandId, ThreadId, MessageId, ImportedHistoryStart } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import { importedHistoryIdentity } from "./commands.ts";
it("binds schema-encoded message command and delivery to fixed SHA256 golden bytes", async () => {
  const command: ImportedHistoryStart = {
    type: "thread.imported-history.start", commandId: CommandId.make("command:test"), threadId: ThreadId.make("thread:test"), reviewedBasis: "c".repeat(64),
    delivery: { type: "message", command: { type: "message.dispatch", commandId: CommandId.make("command:test"), threadId: ThreadId.make("thread:test"), messageId: MessageId.make("message:test"), createdBy: "user", creationSource: "web", text: "Synthetic π 😀", attachments: [], dispatchMode: { type: "start_immediately" } } },
  };
  const encoded = Schema.encodeSync(ImportedHistoryStart)(command);
  expect(importedHistoryCanonicalJson(encoded)).toBe("{\"commandId\":\"command:test\",\"delivery\":{\"command\":{\"attachments\":[],\"commandId\":\"command:test\",\"createdBy\":\"user\",\"creationSource\":\"web\",\"dispatchMode\":{\"type\":\"start_immediately\"},\"messageId\":\"message:test\",\"text\":\"Synthetic π 😀\",\"threadId\":\"thread:test\",\"type\":\"message.dispatch\"},\"type\":\"message\"},\"reviewedBasis\":\"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"threadId\":\"thread:test\",\"type\":\"thread.imported-history.start\"}");
  expect(await Effect.runPromise(importedHistoryIdentity(command))).toEqual({
    commandDigest: "43caac27b6a576e935b121058af2de57177eb9229a65379a8d26435fb2bec2bc",
    deliveryDigest: "6b71dbfd5045c244e5c7808bfe0cf79e82a6fcc600275a2960adda59be1f50f8",
  });
});
