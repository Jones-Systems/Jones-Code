import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import { Tool } from "effect/ai";
import {
  CLIENT_GUARDED_RPC_SCOPES,
  ORCHESTRATION_V2_WS_METHODS,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
} from "@t3tools/contracts";
import { RPC_REQUIRED_SCOPES } from "../../auth/RpcAuthorization.ts";
import { CLAUDE_READ_ONLY_T3_MCP_ALLOWED_TOOLS } from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { OrchestratorToolkit } from "../../mcp/toolkits/orchestrator/tools.ts";
import { screensRemoteOperation } from "../remoteAdmissionIntegration/operations.ts";

it("registers read-only links separately from controlled mutations and screens both new writes", () => {
  const read = OrchestratorToolkit.tools.t3_thread_links;
  expect(Context.get(read.annotations, Tool.Readonly)).toBe(true);
  expect(CLAUDE_READ_ONLY_T3_MCP_ALLOWED_TOOLS).toContain("mcp__t3-code__t3_thread_links");
  for (const name of ["t3_thread_link", "t3_thread_handoff"] as const) {
    const tool = OrchestratorToolkit.tools[name];
    expect(Context.get(tool.annotations, Tool.Readonly)).toBe(false);
    expect(screensRemoteOperation(name, {}, false)).toBe(true);
    expect(CLAUDE_READ_ONLY_T3_MCP_ALLOWED_TOOLS).not.toContain(`mcp__t3-code__${name}`);
  }
  expect(RPC_REQUIRED_SCOPES[ORCHESTRATION_V2_WS_METHODS.listLinkedThreads]).toBe(
    AuthOrchestrationReadScope,
  );
  expect(ORCHESTRATION_V2_WS_METHODS.listLinkedThreads in CLIENT_GUARDED_RPC_SCOPES).toBe(false);
  for (const name of ["linkThreads", "unlinkThreads", "acceptThreadHandoff"] as const)
    expect(CLIENT_GUARDED_RPC_SCOPES[ORCHESTRATION_V2_WS_METHODS[name]]).toBe(
      AuthOrchestrationOperateScope,
    );
});
