import { assert, describe, it } from "@effect/vitest";
import { Tool } from "effect/ai";

import { OrchestratorToolkit } from "../../../../../mcp/toolkits/orchestrator/tools.ts";
import { ProjectToolkit } from "../../../../../mcp/toolkits/project/tools.ts";

const CreateThreadsTool = OrchestratorToolkit.tools.create_threads;
const DelegateTaskTool = OrchestratorToolkit.tools.delegate_task;
const ThreadUpdateTool = OrchestratorToolkit.tools.t3_thread_update;

describe("orchestrator MCP tool guidance", () => {
  it("discovers self settlement with a required replay key and no target selector", () => {
    const tool = OrchestratorToolkit.tools["t3_thread_settle"];
    assert.isDefined(tool);
    const schema = Tool.getJsonSchema(tool) as {
      readonly properties?: Readonly<Record<string, unknown>>;
      readonly required?: ReadonlyArray<string>;
    };
    assert.deepEqual(Object.keys(schema.properties ?? {}), ["projectRemaining", "clientRequestId"]);
    assert.include(schema.required ?? [], "clientRequestId");
    assert.include(tool.description ?? "", "checkpoint");
  });

  it("routes separate-session delegation to delegate_task and keeps launches standalone", () => {
    const delegateSchema = JSON.stringify(Tool.getJsonSchema(DelegateTaskTool));
    assert.include(delegateSchema, '"workspaceStrategy"');
    assert.include(delegateSchema, '"existing_worktree"');
    assert.include(delegateSchema, '"worktreePath"');
    assert.notInclude(delegateSchema, '"root"');
    const delegate = DelegateTaskTool.description ?? "";
    assert.include(delegate, "its own provider session");
    assert.include(delegate, "Lineage");
    assert.include(delegate, "Do not use t3_thread_launch or create_threads for delegated work");
    assert.include(delegateSchema, '"projectId"');
    assert.include(delegate, "also set projectId; that requires workspaceStrategy");
    assert.include(
      delegate,
      "including a Claude agent asked to make subagents, always use this with a Claude target rather than a native Claude subagent tool, even when that tool supports the chosen model",
    );
    assert.include(delegate, "For other providers, prefer native subagent tools");
    assert.include(
      CreateThreadsTool.description ?? "",
      "Claude child work always uses delegate_task, even when a native Claude subagent tool supports the chosen model",
    );
    assert.include(
      CreateThreadsTool.description ?? "",
      "For other providers, prefer native subagents",
    );
    assert.include(CreateThreadsTool.description ?? "", "no parent link");
    assert.include(
      OrchestratorToolkit.tools.orchestrator_capabilities.description ?? "",
      "delegate_task with workspaceStrategy",
    );
    const launch = ProjectToolkit.tools.t3_thread_launch.description ?? "";
    assert.include(launch, "only when the user requests a standalone top-level thread");
    assert.include(launch, "use delegate_task");
    assert.notInclude(launch, "requests independent work");
  });

  it("keeps compact catalog search alongside environment selection and workspace guidance", () => {
    const tool = OrchestratorToolkit.tools.orchestrator_capabilities;
    const schema = Tool.getJsonSchema(tool) as {
      readonly properties?: Readonly<Record<string, unknown>>;
    };
    assert.hasAllKeys(schema.properties ?? {}, ["environmentId", "search"]);
    assert.include(tool.description ?? "", "omittedModels");
    assert.include(tool.description ?? "", "pass search");
    assert.include(tool.description ?? "", "delegate_task with workspaceStrategy");
  });

  it("publishes thread metadata actions from an object-root schema", () => {
    const schema = Tool.getJsonSchema(ThreadUpdateTool) as {
      readonly type?: unknown;
      readonly properties?: Readonly<Record<string, unknown>>;
    };

    assert.equal(schema.type, "object");
    assert.hasAllKeys(schema.properties ?? {}, [
      "environmentId",
      "threadId",
      "action",
      "title",
      "pullRequest",
      "clientRequestId",
    ]);
    assert.include(ThreadUpdateTool.description ?? "", "Workspace and branch changes");
  });
});
