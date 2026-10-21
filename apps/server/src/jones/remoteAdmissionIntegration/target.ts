import * as Schema from "effect/Schema";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ScheduledTasks from "../../scheduledTasks/ScheduledTaskService.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";

class RemoteAdmissionTargetUnavailable extends Schema.TaggedError<RemoteAdmissionTargetUnavailable>()(
  "RemoteAdmissionTargetUnavailable",
  {},
) {}

export const resolveTarget = Effect.fn("RemoteAdmissionTarget.resolveTarget")(function*(params: Record<string, unknown>, invocation: McpInvocationScope) {
    const projectService = yield* Effect.serviceOption(ProjectService.ProjectService);
    const projectionService = yield* Effect.serviceOption(ProjectionStore.ProjectionStoreV2);
    const scheduledId = params.scheduledTaskId ?? params.taskId;
    let scheduled: unknown = null;
    let scheduledProjectId: string | null = null;
    let scheduledThreadId: string | null = null;
    if (typeof scheduledId === "string") {
      const scheduler = yield* Effect.serviceOption(ScheduledTasks.ScheduledTaskService);
      if (Option.isNone(scheduler))
        return yield* Effect.fail(new RemoteAdmissionTargetUnavailable({}));
      const tasks = yield* scheduler.value.list();
      const task = tasks.tasks.find((candidate) => candidate.id === scheduledId);
      scheduledProjectId = task?.projectId ?? null;
      scheduledThreadId = task?.threadId ?? null;
      scheduled = task
        ? {
            id: task.id,
            prompt: task.prompt,
            enabled: task.enabled,
            projectId: task.projectId,
            threadId: task.threadId,
            workspaceStrategy: task.workspaceStrategy,
            modelSelection: task.modelSelection,
            runtimeMode: task.runtimeMode,
            interactionMode: task.interactionMode,
          }
        : null;
    }
    const threadIds = [
      ...new Set(
        [
          params.threadId,
          params.sourceThreadId,
          params.targetThreadId,
          invocation.thread?.threadId,
          scheduledThreadId,
        ].filter((value): value is string => typeof value === "string"),
      ),
    ];
    const projectIds = new Set(
      [params.projectId, scheduledProjectId].filter(
        (value): value is string => typeof value === "string",
      ),
    );
    const threads = yield* Effect.forEach(threadIds, (id) =>
      Effect.gen(function* () {
        if (Option.isNone(projectionService))
          return yield* new RemoteAdmissionTargetUnavailable({});
        const projections = projectionService.value;
        const thread = yield* projections.getThreadShell(ThreadId.make(id));
        if (!thread) return { id, missing: true };
        projectIds.add(thread.projectId);
        const queued =
          typeof params.queuedRunId === "string" || Array.isArray(params.queuedRunIds)
            ? yield* projections.getThreadRecords(thread.id, ["runs", "messages"])
            : null;
        const queuedIds = new Set([
          params.queuedRunId,
          ...(Array.isArray(params.queuedRunIds) ? params.queuedRunIds : []),
        ]);
        const queuedRuns = queued?.runs.filter((run) => queuedIds.has(run.id));
        return {
          id: thread.id,
          projectId: thread.projectId,
          modelSelection: thread.modelSelection,
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          branch: thread.branch,
          worktreePath: thread.worktreePath,
          threadMessagesBlocked: thread.threadMessagesBlocked ?? false,
          deleted: thread.deletedAt !== null,
          queue: queuedRuns?.map((run) => ({
            id: run.id,
            status: run.status,
            message: queued?.messages
              .filter((message) => message.id === run.userMessageId)
              .map((message) => ({ text: message.text, attachments: message.attachments })),
          })),
        };
      }),
    );
    const projectStates = yield* Effect.forEach([...projectIds].sort(), (id) =>
      Effect.gen(function* () {
        if (Option.isNone(projectService)) return yield* new RemoteAdmissionTargetUnavailable({});
        const project = yield* projectService.value.getById(ProjectId.make(id));
        if (Option.isNone(project)) return { id, missing: true };
        const value = project.value;
        return {
          id,
          workspaceRoot: value.workspaceRoot,
          defaultModelSelection: value.defaultModelSelection,
          defaultThreadEnvMode: value.defaultThreadEnvMode,
          scripts: value.scripts,
          deletedAt: value.deletedAt,
        };
      }),
    );
    return { threads, projects: projectStates, scheduled };
  });
