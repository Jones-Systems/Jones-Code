import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { OrchestrationV2ThreadHistoryPage } from "../orchestrationV2.ts";

const codec = Schema.toCodecJson(OrchestrationV2ThreadHistoryPage);
const decode = Schema.decodeUnknownSync(codec);
const encode = Schema.encodeSync(codec);
const remoteChild = {
  environmentId: "environment:remote",
  threadId: "thread:remote",
  label: "Linked environment",
  runId: "run:remote",
  stoppedThere: false,
  stopCount: 2,
};
const item = {
  id: "item:subagent",
  type: "subagent",
  threadId: "thread:parent",
  runId: "run:parent",
  nodeId: "node:parent",
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "completed",
  title: null,
  startedAt: null,
  completedAt: null,
  updatedAt: "2026-10-25T00:00:00.000Z",
  subagentId: "node:child",
  origin: "app_owned",
  driver: "codex",
  providerInstanceId: "codex",
  childThreadId: null,
  remoteChild,
  prompt: "",
  promptOmitted: true,
  result: null,
  resultOmitted: true,
};
const page = {
  snapshotSequence: 10,
  items: [
    {
      position: 0,
      visibility: "local",
      sourceThreadId: "thread:parent",
      sourceItemId: item.id,
      item,
    },
  ],
  nextCursor: null,
  hasMoreHistory: false,
};

describe("Jones fields on imported upstream wire codecs", () => {
  it("round trips remote task evidence and dedup markers through compact ID history pages", () => {
    const plain = decode(page);
    const wire = JSON.parse(JSON.stringify(encode({ ...plain, idTable: true })));
    expect(wire.ids).toContain("thread:parent");
    expect(typeof wire.items[0].item.threadId).toBe("number");
    expect(wire.items[0].item.remoteChild).toEqual(remoteChild);
    expect(decode(wire)).toEqual(plain);
    expect(encode(plain)).toEqual(page);
    expect(plain.items[0]?.item).toMatchObject({
      remoteChild,
      promptOmitted: true,
      resultOmitted: true,
    });
  });

  it("keeps earlier local task history decodable without remote evidence or body markers", () => {
    const {
      remoteChild: _remote,
      promptOmitted: _prompt,
      resultOmitted: _result,
      ...legacy
    } = item;
    const decoded = decode({
      ...page,
      items: [
        {
          ...page.items[0],
          item: { ...legacy, childThreadId: "thread:local", prompt: "Task", result: "Done" },
        },
      ],
    });
    expect(decoded.items[0]?.item).toMatchObject({
      childThreadId: "thread:local",
      prompt: "Task",
      result: "Done",
    });
    expect(decoded.items[0]?.item).not.toHaveProperty("remoteChild");
  });
});
