import { describe, expect, it } from "vite-plus/test";

import {
  T3_MCP_TOOL_NAMES,
  resolveT3McpToolDefinition,
  resolveT3McpToolPresentation,
  resolveT3McpToolSummaryAction,
} from "./t3McpToolPresentation.ts";

describe("resolveT3McpToolPresentation", () => {
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
      expect(resolveT3McpToolSummaryAction(name)).toBe("thread-settle");
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

it("resolves keyed launch lookup and fence across provider names", () => {
  for (const [name, labels, summaryAction] of [
    [
      "t3_thread_launch_lookup",
      ["Look up", "Looking up", "Looked up", "a keyed thread launch"],
      "thread-read",
    ],
    [
      "t3_thread_launch_fence",
      ["Resolve", "Resolving", "Resolved", "an unknown thread launch"],
      "thread-launch-resolve",
    ],
  ] as const) {
    for (const prefix of ["", "mcp__t3-code__", "T3-code."]) {
      expect(resolveT3McpToolDefinition(`${prefix}${name}`)).toEqual({
        displayName: `${labels[0]} ${labels[3]}`,
        labels,
        icon: "t3-code",
        summaryAction,
      });
    }
    expect(resolveT3McpToolDefinition(`mcp__another-server__${name}`)).toBeNull();
  }
});
