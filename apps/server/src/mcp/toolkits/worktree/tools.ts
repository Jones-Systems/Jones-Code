import * as ConversationPolicy from "../../../jones/conversationWorkspace/ConversationPolicy.ts";
import {
  WorktreeMcpFailure,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  VcsListRefsInput,
  VcsListRefsResult,
  WorktreeMcpHandoffInput,
  WorktreeMcpHandoffResult,
  WorktreeMcpStatusResult,
  WorktreeProvenanceEntry,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import { Tool, Toolkit } from "effect/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as WorktreeMcpService from "../../WorktreeMcpService.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  WorktreeMcpService.WorktreeMcpService,
];

const WorktreeToolFailure = Schema.Union([WorktreeMcpFailure, OrchestratorMcpFailure]);

const WorktreeHandoffTool = Tool.make("t3_worktree_handoff", {
  description:
    "Move THIS thread into a new git worktree on a new branch. Runs the setup script by default. A separate agent in a worktree: `t3_thread_launch` with `workspaceStrategy`. Call last in your turn. The move ends the live session. `continuationPrompt` resumes work there next turn. Without it, the thread waits. The worktree stays after the thread is deleted. Fails if this thread has a worktree.",
  parameters: WorktreeMcpHandoffInput,
  success: WorktreeMcpHandoffResult,
  failure: WorktreeToolFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Hand off thread to a git worktree")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

const WorktreeStatusTool = Tool.make("t3_worktree_status", {
  description:
    "Report this thread's worktree: attached or not, path, branch, project root, and the `startFromOrigin` default. Check before `t3_worktree_handoff`.",
  // No `parameters`: Tool.make defaults to Tool.EmptyParams, which serializes
  // to a top-level `type: "object"` JSON Schema. An explicit empty
  // Schema.Struct({}) serializes to `anyOf: [object, array]`, which is not a
  // valid MCP tool input schema and makes clients reject the whole server.
  success: WorktreeMcpStatusResult,
  failure: WorktreeToolFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Get thread worktree status")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const WorktreeListTool = Tool.make("t3_worktree_list", {
  description:
    "List branch refs and checkout paths for a thread's workspace. Omit `threadId` for this thread. Detached worktrees with no branch are left out of refs; provenance includes known detached and historical incarnations. Use provenancePath to look up a path within this project.",
  parameters: Schema.Struct({
    environmentId: OrchestratorMcpEnvironmentTarget,
    threadId: Schema.optional(ThreadId),
    provenancePath: Schema.optional(Schema.String),
    query: VcsListRefsInput.fields.query,
    cursor: VcsListRefsInput.fields.cursor,
    limit: VcsListRefsInput.fields.limit,
    refKind: VcsListRefsInput.fields.refKind,
    includeMatchingRemoteRefs: VcsListRefsInput.fields.includeMatchingRemoteRefs,
  }),
  success: Schema.Struct({
    ...VcsListRefsResult.fields,
    provenance: Schema.optional(Schema.Array(WorktreeProvenanceEntry)),
  }),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    ProjectService.ProjectService,
    GitWorkflowService.GitWorkflowService,
  ],
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
const sourceTool = <Name extends string, Fields extends Schema.Struct.Fields>(
  name: Name,
  parameters: Schema.Struct<Fields>,
  description: string,
) =>
  Tool.make(name, {
    description,
    parameters,
    success: Schema.Struct({ commit: Schema.String, content: Schema.String }),
    failure: WorktreeToolFailure,
    failureMode: "return",
    dependencies: [
      McpInvocationContext.McpInvocationContext,
      ConversationPolicy.ConversationPolicy,
    ],
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true);
const SourceRead = sourceTool(
  "t3_source_read",
  Schema.Struct({ path: Schema.String }),
  "Read a file blob at this conversation turn's recorded source commit. Symlinks return their blob, never a filesystem target. Output/time are bounded. Read root instructions here before working.",
);
const SourceList = sourceTool(
  "t3_source_list",
  Schema.Struct({ path: Schema.optional(Schema.String) }),
  "List a tree at the recorded source commit, without a worktree or filesystem reads. Empty path lists the root.",
);
const SourceSearch = sourceTool(
  "t3_source_search",
  Schema.Struct({ query: Schema.String, path: Schema.optional(Schema.String) }),
  "Search committed source for literal text at the recorded source commit. No worktree, text conversion, external grep or fetch; bounded output/time.",
);
const SourceHistory = sourceTool(
  "t3_source_history",
  Schema.Struct({
    operation: Schema.Literals(["log", "show", "diff"]),
    from: Schema.optional(Schema.String),
  }),
  "Read bounded log/show/diff from Git objects at the recorded source commit. Diff requires an exact from commit. Never fetches, runs external diff or text conversion, or writes objects/refs/index.",
);
export const WorktreeToolkit = Toolkit.make(
  SourceRead,
  SourceList,
  SourceSearch,
  SourceHistory,
  WorktreeHandoffTool,
  WorktreeStatusTool,
  WorktreeListTool,
);
