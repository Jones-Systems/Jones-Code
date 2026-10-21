import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  type Project,
  type OrchestrationV2ThreadShell,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ScheduledTasks from "../../scheduledTasks/ScheduledTaskService.ts";
import { resolveTarget } from "./target.ts";

it.effect(
  "resolved project and thread configuration changes are observable without dispatch",
  () => {
    let model = "reviewed-model";
    let root = "/synthetic/reviewed";
    const projects = Layer.succeed(ProjectService.ProjectService, {
      getById: () =>
        Effect.succeed(
          Option.some({
            id: "project",
            workspaceRoot: root,
            defaultModelSelection: { instanceId: "claude", model },
            scripts: [],
            deletedAt: null,
          } as unknown as Project),
        ),
    } as unknown as ProjectService.ProjectService["Service"]);
    const projections = Layer.succeed(ProjectionStore.ProjectionStoreV2, {
      getThreadShell: () =>
        Effect.succeed({
          id: "thread",
          projectId: "project",
          modelSelection: { instanceId: "claude", model },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "task",
          worktreePath: root,
          deletedAt: null,
        } as unknown as OrchestrationV2ThreadShell),
    } as unknown as ProjectionStore.ProjectionStoreV2["Service"]);
    const scope = {
      environmentId: EnvironmentId.make("receiver"),
      capabilities: new Set<"orchestration">(["orchestration"]),
      issuedAt: 1,
      requestNamespace: "client",
      thread: undefined,
      client: undefined,
    };
    return Effect.gen(function* () {
      const before = yield* resolveTarget({ threadId: "thread" }, scope);
      root = "/synthetic/changed";
      model = "changed-model";
      const after = yield* resolveTarget({ threadId: "thread" }, scope);
      expect(before).not.toEqual(after);
      expect(before.projects[0]).toMatchObject({
        workspaceRoot: "/synthetic/reviewed",
        defaultModelSelection: { model: "reviewed-model" },
      });
      expect(after.threads[0]).toMatchObject({
        worktreePath: "/synthetic/changed",
        modelSelection: { model: "changed-model" },
      });
    }).pipe(Effect.provide(Layer.merge(projects, projections)));
  },
);

it.effect(
  "schedule execution binds its implicit project and destination thread before review",
  () => {
    let workspaceRoot = "/synthetic/scheduled";
    const projects = Layer.succeed(ProjectService.ProjectService, {
      getById: () =>
        Effect.succeed(
          Option.some({
            id: "scheduled-project",
            workspaceRoot,
            defaultModelSelection: { instanceId: "claude", model: "default" },
            scripts: [],
            deletedAt: null,
          } as unknown as Project),
        ),
    } as unknown as ProjectService.ProjectService["Service"]);
    const projections = Layer.succeed(ProjectionStore.ProjectionStoreV2, {
      getThreadShell: () =>
        Effect.succeed({
          id: "scheduled-thread",
          projectId: "scheduled-project",
          modelSelection: { instanceId: "claude", model: "scheduled-model" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "task",
          worktreePath: workspaceRoot,
          deletedAt: null,
        } as unknown as OrchestrationV2ThreadShell),
    } as unknown as ProjectionStore.ProjectionStoreV2["Service"]);
    const scheduler = Layer.succeed(ScheduledTasks.ScheduledTaskService, {
      list: () =>
        Effect.succeed({
          tasks: [
            {
              id: "schedule",
              prompt: "Scheduled work",
              projectId: "scheduled-project",
              threadId: "scheduled-thread",
              enabled: true,
              workspaceStrategy: { type: "root" },
              modelSelection: { instanceId: "claude", model: "scheduled-model" },
              runtimeMode: "full-access",
              interactionMode: "default",
            } as unknown as ScheduledTask,
          ],
        }),
    } as unknown as ScheduledTasks.ScheduledTaskService["Service"]);
    const scope = {
      environmentId: EnvironmentId.make("receiver"),
      capabilities: new Set<"orchestration">(["orchestration"]),
      issuedAt: 1,
      requestNamespace: "client",
      thread: undefined,
      client: undefined,
    };
    return Effect.gen(function* () {
      const reviewed = yield* resolveTarget({ taskId: "schedule" }, scope);
      expect(reviewed.projects[0]).toMatchObject({
        id: "scheduled-project",
        workspaceRoot: "/synthetic/scheduled",
      });
      expect(reviewed.threads[0]).toMatchObject({
        id: "scheduled-thread",
        worktreePath: "/synthetic/scheduled",
      });
      workspaceRoot = "/synthetic/changed";
      expect(yield* resolveTarget({ taskId: "schedule" }, scope)).not.toEqual(reviewed);
    }).pipe(Effect.provide(Layer.mergeAll(projects, projections, scheduler)));
  },
);
