import { ClientSettingsSchema } from "../settings.ts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  OrchestrationV2ThreadHistoryPage,
  OrchestrationV2TurnItem,
  OrchestrationV2Checkpoint,
} from "../orchestrationV2.ts";

const turnItemCodec = Schema.toCodecJson(OrchestrationV2TurnItem);
const encodeTurnItem = Schema.encodeSync(turnItemCodec);
const decodeTurnItem = Schema.decodeUnknownSync(turnItemCodec);
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

describe("foundation upgrade compatibility", () => {
  it("decodes old design settings while preserving Jones client defaults", () => {
    const decode = Schema.decodeUnknownSync(ClientSettingsSchema);
    // Upstream removed both old design toggles; stored values decode to current defaults.
    for (const redesignEnabled of [true, false]) {
      for (const legacySidebarEnabled of [true, false]) {
        expect(decode({ redesignEnabled, legacySidebarEnabled })).toEqual(decode({}));
        expect(decode({ redesignEnabled, legacySidebarEnabled }).promptsDefaultStage).toBe(
          "pending",
        );
      }
    }
  });

  it("keeps reason and linkStatus required for new upstream link records", () => {
    const decode = decodeTurnItem;
    const command = {
      id: "item:link",
      type: "link_request",
      threadId: item.threadId,
      runId: item.runId,
      nodeId: item.nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "completed",
      title: null,
      startedAt: null,
      completedAt: null,
      updatedAt: item.updatedAt,
      reason: "Synthetic linked workspace",
      linkStatus: "pending",
    };
    expect(encodeTurnItem(decode(command))).toEqual(command);
    const { reason: _reason, ...withoutReason } = command;
    const { linkStatus: _status, ...withoutStatus } = command;
    expect(() => decode(withoutReason)).toThrow();
    expect(() => decode(withoutStatus)).toThrow();
  });

  it("round trips a capped checkpoint prefix with totals for the full file set", () => {
    const codec = Schema.toCodecJson(OrchestrationV2Checkpoint);
    const files = Array.from({ length: 200 }, (_, index) => ({
      path: `file-${index}.ts`,
      kind: "modified",
      additions: 1,
      deletions: 0,
    }));
    const wire = {
      id: "checkpoint:1",
      threadId: "thread:parent",
      scopeId: "scope:1",
      runId: "run:parent",
      nodeId: "node:parent",
      parentCheckpointId: null,
      ordinalWithinScope: 1,
      appRunOrdinal: 1,
      ref: "refs/t3/checkpoint-1",
      status: "ready",
      files,
      filesTotal: { count: 231, additions: 300, deletions: 40 },
      capturedAt: "2026-10-25T00:00:00.000Z",
    };
    const value = Schema.decodeSync(codec)(wire);
    expect(value.files).toHaveLength(200);
    expect(value.filesTotal?.count).toBe(231);
    expect(Schema.encodeSync(codec)(value)).toEqual(wire);
    const { filesTotal: _totals, ...oldWire } = wire;
    expect(Schema.encodeSync(codec)(Schema.decodeSync(codec)(oldWire))).toEqual(oldWire);
  });
});
