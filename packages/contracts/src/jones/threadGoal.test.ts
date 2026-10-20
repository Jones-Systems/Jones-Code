import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { ThreadGoalInput } from "./threadGoal.ts";
import { OrchestratorMcpThreadDetail, OrchestratorMcpThreadListItem } from "../orchestratorMcp.ts";

const decodeList = Schema.decodeUnknownSync(OrchestratorMcpThreadListItem);
const decodeDetail = Schema.decodeUnknownSync(OrchestratorMcpThreadDetail);
const decodeGoalInput = Schema.decodeUnknownSync(ThreadGoalInput);

const oldListItem = {
  threadId: "thread",
  title: "Title",
  createdBy: "user",
  creationSource: "web",
  status: "idle",
  latestRunId: null,
  providerInstanceId: "codex",
  model: "test",
  runtimeMode: "full-access",
  interactionMode: "default",
  linkedPullRequest: null,
  settled: false,
  settledAt: null,
  snoozed: false,
  snoozedUntil: null,
  parentThreadId: null,
  relationshipToParent: null,
  itemCount: 0,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
};
const oldDetail = {
  ...oldListItem,
  projectId: "project",
  activeRunId: null,
  titleRegeneration: null,
  branch: null,
  worktreePath: null,
  runCount: 0,
  pendingRequestCount: 0,
  archived: false,
};

describe("additive MCP goal contracts", () => {
  it("decodes older detail and list payloads with no goal fields", () => {
    expect(decodeList(oldListItem).goal).toBeUndefined();
    expect(decodeDetail(oldDetail).goal).toBeUndefined();
  });
  it("retains projected goal state and marks its source", () => {
    expect(
      decodeDetail({
        ...oldDetail,
        goal: { objective: "Finish all work", status: "paused", tokensUsed: 42 },
        goalSource: "provider_projection",
      }),
    ).toMatchObject({
      goal: { status: "paused", tokensUsed: 42 },
      goalSource: "provider_projection",
    });
  });
  it("rejects unknown actions and empty replacement objectives", () => {
    expect(() => decodeGoalInput({ action: "complete" })).toThrow();
    expect(() => decodeGoalInput({ action: "replace", objective: " " })).toThrow();
  });
});
