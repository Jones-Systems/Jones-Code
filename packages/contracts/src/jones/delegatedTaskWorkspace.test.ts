import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { OrchestrationV2Command } from "../orchestrationV2.ts";
import { OrchestratorMcpDelegateTaskInput } from "../orchestratorMcp.ts";

const decode = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);

describe("delegate_task workspaceStrategy", () => {
  it("decodes the new and existing worktree shapes used by top-level launches", () => {
    expect(
      decode({
        task: "Implement the lane",
        workspaceStrategy: {
          type: "worktree",
          baseRef: "work/hq-foundation",
          branch: "work/hq-t1",
          startFromOrigin: false,
        },
      }).workspaceStrategy,
    ).toEqual({
      type: "worktree",
      baseRef: "work/hq-foundation",
      branch: "work/hq-t1",
      startFromOrigin: false,
    });
    expect(
      decode({
        task: "Continue the lane",
        workspaceStrategy: { type: "existing_worktree", worktreePath: "/repo-worktrees/lane" },
      }).workspaceStrategy,
    ).toEqual({ type: "existing_worktree", worktreePath: "/repo-worktrees/lane" });
  });

  it("keeps the field optional so existing delegations decode unchanged", () => {
    const legacy = decode({ task: "Review changes", isolation: "worktree" });
    expect(legacy.workspaceStrategy).toBeUndefined();
    expect(legacy.isolation).toBe("worktree");
  });

  it("accepts an explicit child project beside a workspace strategy", () => {
    const request = decode({
      task: "Implement HQ T1",
      projectId: "project:hq",
      workspaceStrategy: { type: "existing_worktree", worktreePath: "/hq-worktrees/t1" },
    });
    expect(request.projectId).toBe("project:hq");
    expect(decode({ task: "Review changes" }).projectId).toBeUndefined();
  });

  it("decodes delegated_task.request commands written before and after projectId", () => {
    const decodeCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
    const earlier = {
      type: "delegated_task.request",
      createdBy: "agent",
      creationSource: "mcp",
      commandId: "command:delegate",
      parentThreadId: "thread:parent",
      parentRunId: "run:parent",
      parentNodeId: "node:parent",
      task: "Inspect the lane",
      modelSelection: { instanceId: "claudeAgent", model: "claude-opus-5-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
    };
    const legacy = decodeCommand(earlier);
    expect(legacy.type === "delegated_task.request" ? legacy.projectId : "wrong").toBeUndefined();
    const crossProject = decodeCommand({
      ...earlier,
      projectId: "project:hq",
      workspaceStrategy: { type: "existing_worktree", worktreePath: "/hq-worktrees/t1" },
    });
    expect(crossProject.type === "delegated_task.request" ? crossProject.projectId : null).toBe(
      "project:hq",
    );
  });

  it("rejects root and incomplete strategies", () => {
    expect(() => decode({ task: "Run", workspaceStrategy: { type: "root" } })).toThrow();
    expect(() =>
      decode({ task: "Run", workspaceStrategy: { type: "existing_worktree" } }),
    ).toThrow();
    expect(() =>
      decode({ task: "Run", workspaceStrategy: { type: "existing_worktree", worktreePath: " " } }),
    ).toThrow();
  });
});
