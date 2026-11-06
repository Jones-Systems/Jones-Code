import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type Project,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import { LinkedRequestSentAtOverride } from "./LinkedRequestContext.ts";
import * as RequestContextSender from "./RequestContextSender.ts";

const source = liveThreadShell(ThreadId.make("source"));
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("sender"),
  requestNamespace: "sender-session",
  issuedAt: 1,
  capabilities: new Set(["orchestration"]),
  thread: undefined,
  client: undefined,
};
const project: Project = {
  id: source.projectId,
  title: " Sender\nproject ",
  workspaceRoot: "/never-sent",
  faviconPath: "/never-fetched.png",
  projectIcon: { kind: "emoji", emoji: "🚀" },
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-10-26T00:00:00.000Z",
  updatedAt: "2026-10-26T00:00:00.000Z",
  deletedAt: null,
};
const projects = Layer.mock(ProjectService.ProjectService)({
  getById: () => Effect.succeedSome(project),
});
const send = (
  caller: OrchestrationV2ThreadShell | undefined = source,
  toolName = "t3_thread_launch",
  args: Record<string, unknown> = {},
  readOnly = false,
  getThreadShell: ThreadManagement.ThreadManagementService["Service"]["getThreadShell"] = (id) =>
    Effect.succeed({ ...liveThreadShell(id), title: " Local\nparent " }),
) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    return yield* RequestContextSender.contextFor({
      scope,
      caller: {
        scope,
        threads,
        caller,
        limits: { runtimeMode: "auto", interactionMode: "default" },
      },
      toolName,
      args,
      readOnly,
    });
  }).pipe(Effect.provide(Layer.mock(ThreadManagement.ThreadManagementService)({ getThreadShell })));

it.effect("uses the caller's thread, project and local subagent parent before delegatedFrom", () =>
  Effect.gen(function* () {
    const context = yield* send({
      ...source,
      title: " Draft\u202e\n title ",
      lineage: {
        ...source.lineage,
        relationshipToParent: "subagent",
        parentThreadId: ThreadId.make("local-parent"),
      },
      delegatedFrom: {
        environmentId: EnvironmentId.make("remote"),
        threadId: ThreadId.make("remote-parent"),
        title: "Remote",
      },
    });
    expect(context).toMatchObject({
      thread: { threadId: source.id, title: "Draft title" },
      parent: { threadId: "local-parent", title: "Local parent" },
      project: { projectId: project.id, title: "Sender project", icon: project.projectIcon },
    });
    expect(context?.parent).not.toHaveProperty("environmentId");
    expect(JSON.stringify(context)).not.toContain("never-");
  }).pipe(Effect.provide(projects)),
);

it.effect("reports a delegated parent without resolving it in the source environment", () =>
  Effect.gen(function* () {
    const context = yield* send(
      {
        ...source,
        delegatedFrom: {
          environmentId: EnvironmentId.make("remote"),
          threadId: ThreadId.make("same-id"),
          title: " Remote\nparent ",
        },
      },
      "t3_thread_launch",
      {},
      false,
      () => Effect.die("must not resolve a remote parent"),
    );
    expect(context?.parent).toEqual({
      environmentId: "remote",
      threadId: "same-id",
      title: "Remote parent",
    });
  }).pipe(Effect.provide(projects)),
);

it.effect.each(["missing", "failed", "defect", "throw"] as const)(
  "omits a %s project without losing the thread",
  (kind) =>
    Effect.gen(function* () {
      const context = yield* send();
      expect(context?.thread?.threadId).toBe(source.id);
      expect(context?.project).toBeUndefined();
    }).pipe(
      Effect.provide(
        Layer.mock(ProjectService.ProjectService)({
          getById: () => {
            if (kind === "throw") throw new Error("lookup crashed synchronously");
            return kind === "failed"
              ? Effect.fail(
                  new ProjectService.ProjectOperationError({
                    operation: "read-project",
                    cause: "synthetic",
                  }),
                )
              : kind === "defect"
                ? Effect.die("lookup crashed")
                : Effect.succeedNone;
          },
        }),
      ),
    ),
);

it.effect("propagates interruption from project enrichment", () =>
  Effect.gen(function* () {
    const exit = yield* send().pipe(Effect.exit);
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
  }).pipe(
    Effect.provide(Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.interrupt })),
  ),
);

it.effect.each(["missing", "failed", "defect", "throw"] as const)(
  "omits a %s local parent independently",
  (kind) =>
    Effect.gen(function* () {
      const context = yield* send(
        {
          ...source,
          lineage: {
            ...source.lineage,
            relationshipToParent: "subagent",
            parentThreadId: ThreadId.make("parent"),
          },
        },
        "t3_thread_launch",
        {},
        false,
        () => {
          if (kind === "throw") throw new Error("parent lookup crashed");
          return kind === "failed"
            ? Effect.fail(
                new Orchestrator.OrchestratorProjectionError({ threadId: ThreadId.make("parent") }),
              )
            : kind === "defect"
              ? Effect.die("parent lookup crashed")
              : Effect.succeed(null);
        },
      );
      expect(context?.parent).toBeUndefined();
      expect(context?.project?.projectId).toBe(project.id);
    }).pipe(Effect.provide(projects)),
);

it.effect("drops invalid IDs per part and invalid icons without dropping the project", () =>
  Effect.gen(function* () {
    const context = yield* send({
      ...source,
      id: ThreadId.make("t".repeat(300)),
      delegatedFrom: {
        environmentId: EnvironmentId.make("bad\u202eid"),
        threadId: ThreadId.make("parent"),
        title: "Parent",
      },
    });
    expect(context?.thread).toBeUndefined();
    expect(context?.parent).toBeUndefined();
    expect(context?.project).toEqual({ projectId: project.id, title: "Sender project" });
    const invalidProject = yield* send().pipe(
      Effect.provide(
        Layer.mock(ProjectService.ProjectService)({
          getById: () => Effect.succeedSome({ ...project, id: ProjectId.make("p".repeat(300)) }),
        }),
      ),
    );
    expect(invalidProject?.thread?.threadId).toBe(source.id);
    expect(invalidProject?.project).toBeUndefined();
  }).pipe(
    Effect.provide(
      Layer.mock(ProjectService.ProjectService)({
        getById: () =>
          Effect.succeedSome({ ...project, projectIcon: { kind: "emoji", emoji: "" } }),
      }),
    ),
  ),
);

it.effect.each([null, undefined])("keeps untitled parts and omits a %s icon", (icon) =>
  Effect.gen(function* () {
    const context = yield* send({
      ...source,
      title: "\u202e\n",
      delegatedFrom: {
        environmentId: EnvironmentId.make("remote"),
        threadId: ThreadId.make("parent"),
        title: "\u202e\n",
      },
    });
    expect(context?.thread).toEqual({ threadId: source.id });
    expect(context?.parent).toEqual({ threadId: "parent", environmentId: "remote" });
    expect(context?.project).toEqual({ projectId: project.id });
  }).pipe(
    Effect.provide(
      Layer.mock(ProjectService.ProjectService)({
        getById: () => Effect.succeedSome({ ...project, title: "\u202e\n", projectIcon: icon }),
      }),
    ),
  ),
);

it.effect("propagates interruption from local parent enrichment", () =>
  Effect.gen(function* () {
    const exit = yield* send(
      {
        ...source,
        lineage: {
          ...source.lineage,
          relationshipToParent: "subagent",
          parentThreadId: ThreadId.make("parent"),
        },
      },
      "t3_thread_launch",
      {},
      false,
      () => Effect.interrupt,
    ).pipe(Effect.exit);
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
  }).pipe(Effect.provide(projects)),
);

it.effect("a caller without a thread sends only base fields and never looks up a project", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(0);
    const threads = yield* ThreadManagement.ThreadManagementService;
    const context = yield* RequestContextSender.contextFor({
      scope,
      caller: {
        scope,
        threads,
        caller: undefined,
        limits: { runtimeMode: "auto", interactionMode: "default" },
      },
      toolName: "schedule_task",
      args: {},
      readOnly: false,
    });
    expect(context).toEqual({
      schemaVersion: "t3.linked-request-context/v1",
      sentAt: "1970-01-01T00:00:00.000Z",
    });
  }).pipe(
    Effect.provide(
      Layer.merge(
        Layer.mock(ThreadManagement.ThreadManagementService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
      ),
    ),
  ),
);

it.effect.each([
  "t3_thread_send",
  "t3_thread_launch",
  "t3_thread_link",
  "schedule_task",
  "update_scheduled_task",
  "run_scheduled_task_now",
])("screens %s", (tool) =>
  send(source, tool).pipe(
    Effect.map((context) => expect(context?.thread?.threadId).toBe(source.id)),
    Effect.provide(projects),
  ),
);

it.effect.each([
  ["t3_thread_read", {}, false],
  ["t3_thread_wait", {}, false],
  ["t3_thread_interrupt", {}, false],
  ["task_cancel", {}, false],
  ["t3_thread_goal", { action: "cancel" }, false],
  ["t3_thread_goal", { action: "pause" }, false],
  ["t3_thread_goal", { action: "stop" }, false],
  ["custom_read", {}, true],
] as const)("does not attach context to %s (%j)", ([tool, args, readOnly]) =>
  send(source, tool, args, readOnly).pipe(
    Effect.map((context) => expect(context).toBeUndefined()),
    Effect.provide(projects),
  ),
);

it.effect.each([undefined, "bad", "2026-02-30T00:00:00Z", "2026-10-20T12:34:56.789Z"])(
  "uses only a calendar-valid sent-time override: %s",
  (override) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(1000);
      const context = yield* send().pipe(
        Effect.provideService(LinkedRequestSentAtOverride, override),
      );
      expect(context?.sentAt).toBe(
        override === "2026-10-20T12:34:56.789Z" ? override : "1970-01-01T00:00:01.000Z",
      );
    }).pipe(Effect.provide(projects)),
);
