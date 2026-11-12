import { describe, expect, it } from "vite-plus/test";

import {
  T3_MCP_TOOL_NAMES,
  resolveT3McpSummaryAction,
  resolveT3McpToolDefinition,
  resolveT3McpToolPresentation,
} from "./t3McpToolPresentation.ts";

describe("resolveT3McpToolPresentation", () => {
  it("presents linked thread observations, mutations and handoffs", () => {
    for (const [name, displayName] of [
      ["t3_thread_links", "List linked threads"],
      ["t3_thread_link", "Update thread links"],
      ["t3_thread_handoff", "Hand off thread work"],
    ]) {
      expect(resolveT3McpToolPresentation(`mcp__t3_code__${name}`)).toEqual({
        displayName,
        logo: "t3-code",
      });
    }
  });
  it("presents durable owner response requests across provider tool names", () => {
    for (const name of ["t3_request_user_response", "mcp__t3_code__t3_request_user_response"]) {
      expect(resolveT3McpToolPresentation(name)).toEqual({
        displayName: "Request owner response",
        logo: "t3-code",
      });
    }
  });
  it("presents deferred self-settlement as a request across provider name forms", () => {
    expect(T3_MCP_TOOL_NAMES.has("t3_thread_settle")).toBe(true);
    for (const prefix of ["", "mcp__t3-code__", "T3-code."]) {
      const name = `${prefix}t3_thread_settle`;
      expect(resolveT3McpToolDefinition(name)).toEqual({
        displayName: "Request settlement of this thread",
        labels: [
          "Request settlement of",
          "Requesting settlement of",
          "Requested settlement of",
          "this thread",
        ],
        icon: "t3-code",
        summaryAction: "thread-settle",
      });
      expect(resolveT3McpToolPresentation(name)).toEqual({
        displayName: "Request settlement of this thread",
        logo: "t3-code",
      });
      expect(resolveT3McpToolDefinition(name)?.summaryAction ?? null).toBe("thread-settle");
    }
    expect(resolveT3McpToolDefinition("mcp__another-server__t3_thread_settle")).toBeNull();
  });

  it("recognizes every T3 tool across provider prefixes and completion suffixes", () => {
    for (const tool of T3_MCP_TOOL_NAMES) {
      const presentation = resolveT3McpToolPresentation(tool);
      for (const prefix of [
        "mcp__t3-code__",
        "mcp__t3_code__",
        "mcp__t3code__",
        "T3-code.",
        "t3code.",
        "t3-code-direct.",
        "mcp__t3_code_direct__",
        "t3_code/",
        "t3code:",
        "mcp_t3-code_",
        "T3 Code ",
        "t3-code · ",
      ]) {
        expect(resolveT3McpToolPresentation(`${prefix}${tool} completed`), tool).toEqual(
          presentation,
        );
      }
      expect(resolveT3McpToolPresentation(`mcp__another-server__${tool}`), tool).toBeNull();
    }
  });

  it("matches OpenCode 2's per-thread server names, whose thread ids hold underscores", () => {
    expect(
      resolveT3McpToolPresentation("t3-code-thread_opencode2-adapter_delegate_task")?.displayName,
    ).toBe("Delegate a child task");
    expect(resolveT3McpToolPresentation("t3-code-thread_opencode2-adapter_not_a_tool")).toBeNull();
  });
});

describe("resolveT3McpToolDefinition", () => {
  it("labels an action tool call as the action it ran", () => {
    expect(
      resolveT3McpToolDefinition("mcp__t3-code__preview_act", { action: "type" })?.labels,
    ).toEqual(resolveT3McpToolDefinition("preview_type")?.labels);
    expect(resolveT3McpToolDefinition("t3_thread_transfer", { action: "fork" })?.displayName).toBe(
      resolveT3McpToolDefinition("t3_thread_fork")?.displayName,
    );
    expect(resolveT3McpToolDefinition("preview_act", { action: "unknown" })?.labels[2]).toBe(
      "Used",
    );
  });
});

describe("resolveT3McpSummaryAction", () => {
  it("summarizes an action tool by the action each call ran", () => {
    expect(resolveT3McpSummaryAction("mcp__t3-code__t3_queue", { action: "list" })).toBe(
      "queue-list",
    );
    expect(resolveT3McpSummaryAction("t3_queue", { action: "edit" })).toBe("queue-edit");
    expect(resolveT3McpSummaryAction("t3_thread_transfer", { action: "status" })).toBe(
      "thread-transfers",
    );
    expect(resolveT3McpSummaryAction("t3_thread_configure", {})).toBe("thread-configuration");
    expect(resolveT3McpSummaryAction("schedule_task", { scheduledTaskId: "task-1" })).toBe(
      "schedule-update",
    );
    expect(resolveT3McpSummaryAction("delegate_task", { task: "x" })).toBe("delegate");
  });
});
