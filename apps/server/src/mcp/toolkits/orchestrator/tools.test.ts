import { assert, describe, it } from "@effect/vitest";
import { Tool } from "effect/ai";

import {
  CreateThreadsTool,
  DelegateTaskTool,
  OrchestratorToolkit,
  ScheduleTaskTool,
  ThreadUpdateTool,
} from "./tools.ts";
import { ProjectToolkit } from "../project/tools.ts";

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

  it("directs subagent requests to delegation instead of ordinary threads", () => {
    assert.include(DelegateTaskTool.description ?? "", "child agent/subagent");
    assert.include(DelegateTaskTool.description ?? "", "cross-provider");
    assert.include(CreateThreadsTool.description ?? "", "not delegation");
    assert.include(CreateThreadsTool.description ?? "", "call delegate_task");
    assert.include(DelegateTaskTool.description ?? "", "waitTimedOut");
    assert.include(DelegateTaskTool.description ?? "", "does not cancel the child");
    assert.include(DelegateTaskTool.description ?? "", "keep that taskId");
    assert.include(DelegateTaskTool.description ?? "", "call delegate_task again");
    assert.include(DelegateTaskTool.description ?? "", "childThreadId is backing storage");
    assert.include(
      OrchestratorToolkit.tools.t3_thread_send.description ?? "",
      "a requested follow-up creates a fresh task",
    );
    assert.include(
      OrchestratorToolkit.tools.task_cancel.description ?? "",
      "This includes later child-thread runs, even after the task is terminal",
    );
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

  it("documents wait timeout as a parent budget, not a child failure", () => {
    const schema = Tool.getJsonSchema(DelegateTaskTool) as {
      readonly properties?: Readonly<
        Record<
          string,
          {
            readonly description?: unknown;
            readonly anyOf?: ReadonlyArray<{ readonly description?: unknown }>;
          }
        >
      >;
    };
    const mode = schema.properties?.mode;
    const timeoutMs = schema.properties?.timeoutMs;
    const modeText = [mode?.description, ...(mode?.anyOf ?? []).map((entry) => entry.description)]
      .filter((value) => typeof value === "string")
      .join(" ");
    const timeoutText = [
      timeoutMs?.description,
      ...(timeoutMs?.anyOf ?? []).map((entry) => entry.description),
    ]
      .filter((value) => typeof value === "string")
      .join(" ");
    assert.include(modeText, "Defaults to async");
    assert.include(timeoutText, "does not cancel the child");
  });

  it("publishes an actionable schedule schema and compatibility string branch", () => {
    const schema = Tool.getJsonSchema(ScheduleTaskTool) as {
      readonly type?: unknown;
      readonly properties?: Readonly<
        Record<string, { readonly description?: unknown; readonly anyOf?: ReadonlyArray<unknown> }>
      >;
    };

    assert.equal(schema.type, "object");
    assert.isString(schema.properties?.schedule?.description);
    assert.isAtLeast(schema.properties?.schedule?.anyOf?.length ?? 0, 2);
    assert.include(ScheduleTaskTool.description ?? "", "STRUCTURED OBJECT");
    assert.include(ScheduleTaskTool.description ?? "", "nextRunAt");
  });

  it("publishes thread metadata actions from an object-root schema", () => {
    const schema = Tool.getJsonSchema(ThreadUpdateTool) as {
      readonly type?: unknown;
      readonly properties?: Readonly<Record<string, unknown>>;
    };

    assert.equal(schema.type, "object");
    assert.hasAllKeys(schema.properties ?? {}, [
      "threadId",
      "action",
      "title",
      "pullRequest",
      "clientRequestId",
    ]);
    assert.include(ThreadUpdateTool.description ?? "", "Workspace and branch changes");
  });
});
