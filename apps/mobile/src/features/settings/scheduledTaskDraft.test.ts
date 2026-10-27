import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  DEFAULT_SERVER_SETTINGS,
  type ServerConfig,
  ProviderDriverKind,
  ProviderInstanceId,
  ProjectId,
  ScheduledTaskId,
  ScheduledTask,
  ScheduledTaskUpsertInput,
} from "@t3tools/contracts";
import {
  scheduledTaskDefaultModel,
  createDraft,
  editDraft,
  hasScheduledTaskDraftChanges,
  DEFAULT_SCHEDULE,
  scheduleDraftForTask,
  scheduleFromDraft,
  scheduledTaskUpsertInputFromDraft,
} from "./scheduledTaskDraft";

describe("scheduleDraftForTask", () => {
  it("opens legacy sub-minute schedules at the writable minimum", () => {
    const draft = scheduleDraftForTask({ schedule: { type: "interval", everyMs: 30_000 } });
    expect(draft.intervalMinutes).toBe("1");
    expect(scheduleFromDraft(draft)).toEqual({ type: "interval", everyMs: 60_000 });
  });

  it("round-trips dedupe settings and rejects invalid windows", () => {
    const dedupe = {
      replayKey: "{{headers.delivery-id}}",
      coalesceKey: "{{body.id}}",
      coalesceWindowSeconds: 3600,
    };
    const draft = scheduleDraftForTask({ schedule: { type: "webhook", signature: null, dedupe } });
    expect(scheduleFromDraft(draft)).toMatchObject({ dedupe });
    expect(
      scheduleFromDraft({
        ...draft,
        replayKey: " \n ",
        coalesceKey: " \t ",
        coalesceWindowSeconds: "",
      }),
    ).toMatchObject({ dedupe: {} });
    expect(
      scheduleFromDraft({
        ...draft,
        replayKey: `  ${dedupe.replayKey}  `,
        coalesceKey: `\t${dedupe.coalesceKey}\n`,
      }),
    ).toMatchObject({ dedupe });
    expect(
      scheduleFromDraft({ ...draft, replayKey: "", coalesceKey: "", coalesceWindowSeconds: "" }),
    ).toMatchObject({ dedupe: {} });
    for (const coalesceWindowSeconds of ["9", "3601", "1.5", "abc"]) {
      expect(scheduleFromDraft({ ...draft, coalesceWindowSeconds })).toBeNull();
    }
  });

  it("keeps a webhook signature on save without sending a secret", () => {
    const signature = {
      header: "x-hub-signature-256",
      encoding: "hex" as const,
      prefix: "sha256=",
    };
    const saved = scheduleFromDraft(
      scheduleDraftForTask({ schedule: { type: "webhook", signature } }),
    );
    expect(saved).toEqual({ type: "webhook", signature, maxDeliveryAgeMinutes: null, dedupe: {} });
    expect(saved?.type === "webhook" && saved.signature && "secret" in saved.signature).toBe(false);
  });

  it("round-trips webhook filters and refuses an invalid one", () => {
    const filters = [
      { path: "headers.x-github-event", op: "equals", value: "pull_request" },
      { path: "body.label.name", op: "in", value: ["needs review", "agent:fix"] },
    ] as const;
    const draft = scheduleDraftForTask({ schedule: { type: "webhook", signature: null, filters } });
    // Untouched rows are left out, so the server keeps whatever is stored now.
    expect(scheduleFromDraft(draft)).not.toHaveProperty("filters");
    expect(scheduleFromDraft({ ...draft, filters: [] })).toMatchObject({ filters: [] });
    expect(scheduleFromDraft({ ...draft, filters: draft.filters.slice(1) })).toMatchObject({
      filters: filters.slice(1),
    });
    expect(
      scheduleFromDraft({ ...draft, filters: [{ id: "f1", path: "", op: "exists", value: "" }] }),
    ).toBeNull();
  });
});

describe("scheduleFromDraft", () => {
  it("rejects an empty day selection rather than silently scheduling every day", () => {
    expect(scheduleFromDraft({ ...DEFAULT_SCHEDULE, weekdays: [] })).toBeNull();
  });

  it.each([
    ["1.5", 90_000],
    [String(60_001 / 60_000), 60_001],
  ])("preserves a valid %s minute interval when saving", (intervalMinutes, everyMs) => {
    expect(scheduleFromDraft({ ...DEFAULT_SCHEDULE, mode: "interval", intervalMinutes })).toEqual({
      type: "interval",
      everyMs,
    });
  });

  it.each(["NaN", "Infinity", "9007199254740991", "0.5"])(
    "rejects intervals that cannot be written as safe whole milliseconds: %s",
    (intervalMinutes) => {
      expect(
        scheduleFromDraft({ ...DEFAULT_SCHEDULE, mode: "interval", intervalMinutes }),
      ).toBeNull();
    },
  );
});

const legacyTask: ScheduledTask = {
  id: ScheduledTaskId.make("legacy-task"),
  title: "Review issues",
  prompt: "Review open issues",
  enabled: true,
  schedule: { type: "interval", everyMs: 60_000 },
  projectId: ProjectId.make("project"),
  threadId: null,
  workspaceStrategy: { type: "worktree", baseRef: "release" },
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  runtimeMode: "full-access",
  interactionMode: "default",
  createdBy: "user",
  creationSource: "web",
  createdAt: "2026-09-17T00:00:00.000Z",
  updatedAt: "2026-09-17T00:00:00.000Z",
  nextRunAt: null,
  lastRunAt: null,
  lastRunStatus: "never",
  lastRunError: null,
  runCount: 0,
};

describe("editing scheduled task branch settings", () => {
  it("opens an automatic worktree base as an empty editable field", () => {
    const task = {
      ...legacyTask,
      workspaceStrategy: { type: "worktree" as const, startFromOrigin: true },
    };
    const draft = editDraft(task);

    expect(draft.baseRef).toBe("");
    expect(draft.startFromOrigin).toBe(true);
    expect(draft.task?.workspaceStrategy).not.toHaveProperty("baseRef");
    expect(hasScheduledTaskDraftChanges(draft, editDraft(task))).toBe(false);
  });

  it("keeps an omitted origin flag on the local base branch", () => {
    const draft = editDraft(legacyTask);
    expect(draft.baseRef).toBe("release");
    expect(draft.startFromOrigin).toBe(false);
  });

  it.each([true, false])("preserves an explicit origin flag of %s", (startFromOrigin) => {
    const draft = editDraft({
      ...legacyTask,
      workspaceStrategy: { type: "worktree", baseRef: "release", startFromOrigin },
    });
    expect(draft.startFromOrigin).toBe(startFromOrigin);
  });
});

it("continues to default newly created tasks to origin", () => {
  expect(createDraft(null, null).startFromOrigin).toBe(true);
});

describe("scheduled task model defaults", () => {
  const instanceId = ProviderInstanceId.make("codex");
  const projectId = ProjectId.make("project");
  const environmentSelection = {
    instanceId,
    model: "environment-model",
    options: [{ id: "reasoning", value: "high" }],
  };
  const projectSelection = { instanceId, model: "project-model" };
  const config = {
    settings: { ...DEFAULT_SERVER_SETTINGS, defaultModelSelection: environmentSelection },
    providers: [
      {
        instanceId,
        driver: "codex",
        displayName: "Codex",
        enabled: true,
        installed: true,
        status: "ready",
        auth: { status: "authenticated" },
        models: [
          { slug: "first-model", name: "First", isCustom: false, capabilities: null },
          {
            slug: "catalog-default",
            name: "Default",
            isDefault: true,
            isCustom: false,
            capabilities: null,
          },
          { slug: "environment-model", name: "Environment", isCustom: false, capabilities: null },
          { slug: "project-model", name: "Project", isCustom: false, capabilities: null },
        ],
      },
    ],
  } as unknown as ServerConfig;
  const resolve = scheduledTaskDefaultModel;
  it("prefers the project's configured model", () => {
    expect(resolve(config, { id: projectId, defaultModelSelection: projectSelection })).toEqual(
      projectSelection,
    );
    expect(
      resolve(
        {
          ...config,
          settings: {
            ...config.settings,
            projectSettingsOverrides: {
              [projectId]: { defaultModelSelection: projectSelection },
            },
          },
        },
        { id: projectId },
      ),
    ).toEqual(projectSelection);
  });
  it("uses the advertised default instead of catalog order when no default is configured", () => {
    expect(
      resolve({ ...config, settings: { ...config.settings, defaultModelSelection: null } }, null),
    ).toEqual({ instanceId, model: "catalog-default" });
  });
  it("falls back to the environment default when the project provider is unavailable", () => {
    expect(
      resolve(config, {
        id: projectId,
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("unavailable"),
          model: "missing",
        },
      }),
    ).toEqual(environmentSelection);
  });
  it("does not choose an implicit model on a disabled provider", () => {
    expect(
      resolve(
        {
          ...config,
          settings: {
            ...config.settings,
            providerInstances: {
              [instanceId]: { driver: ProviderDriverKind.make("codex"), enabled: false },
            },
          },
          providers: config.providers.map((provider) => ({ ...provider, enabled: false })),
        },
        null,
      ),
    ).toBeNull();
  });
});

it("rejects dedupe keys without valid request placeholders", () => {
  expect(
    scheduleFromDraft({ ...DEFAULT_SCHEDULE, mode: "webhook", replayKey: "literal" }),
  ).toBeNull();
  expect(
    scheduleFromDraft({ ...DEFAULT_SCHEDULE, mode: "webhook", coalesceKey: "{{body.bad" }),
  ).toBeNull();
});

describe("scheduled-task form save and reopen", () => {
  it.each([true, false])(
    "saves the current webhook signature while keeping automatic base and origin %s",
    (startFromOrigin) => {
      const staleSignature = {
        header: "x-old-signature",
        encoding: "hex" as const,
        prefix: "old=",
      };
      const currentSignature = {
        header: "x-current-signature",
        encoding: "hex" as const,
        prefix: "sha256=",
      };
      const task: ScheduledTask = {
        ...legacyTask,
        workspaceStrategy: { type: "worktree", startFromOrigin },
        schedule: { type: "webhook", signature: staleSignature, maxDeliveryAgeMinutes: 45 },
      };
      const draft = editDraft(task);
      const currentSchedule = scheduleFromDraft({
        ...draft.schedule,
        signature: currentSignature,
      });
      const saved = Schema.decodeUnknownSync(ScheduledTaskUpsertInput)(
        scheduledTaskUpsertInputFromDraft(draft, currentSchedule),
      );

      expect(saved.schedule).toEqual({
        type: "webhook",
        signature: currentSignature,
        maxDeliveryAgeMinutes: 45,
        dedupe: {},
      });
      expect(saved.workspaceStrategy).toEqual({ type: "worktree", startFromOrigin });
      expect(saved.workspaceStrategy).not.toHaveProperty("baseRef");
      expect(draft.schedule.signature).toEqual(staleSignature);
      const reopened = editDraft(Schema.decodeUnknownSync(ScheduledTask)({ ...task, ...saved }));
      expect(reopened.schedule.signature).toEqual(currentSignature);
      expect(reopened.baseRef).toBe("");
      expect(reopened.startFromOrigin).toBe(startFromOrigin);
    },
  );

  it.each([true, false])("keeps automatic base omitted with origin %s", (startFromOrigin) => {
    const task: ScheduledTask = {
      ...legacyTask,
      workspaceStrategy: { type: "worktree", startFromOrigin },
    };
    const draft = editDraft(task);
    const saved = Schema.decodeUnknownSync(ScheduledTaskUpsertInput)(
      scheduledTaskUpsertInputFromDraft(draft),
    );
    expect(saved.workspaceStrategy).toEqual({ type: "worktree", startFromOrigin });
    expect(saved.workspaceStrategy).not.toHaveProperty("baseRef");
    expect(saved).toMatchObject({
      id: task.id,
      requireExisting: true,
      title: task.title,
      prompt: task.prompt,
      projectId: task.projectId,
      modelSelection: task.modelSelection,
      schedule: task.schedule,
      enabled: task.enabled,
      threadId: task.threadId,
      runtimeMode: task.runtimeMode,
      interactionMode: task.interactionMode,
      creationSource: task.creationSource,
    });
    const reopened = editDraft(Schema.decodeUnknownSync(ScheduledTask)({ ...task, ...saved }));
    expect(reopened.baseRef).toBe("");
    expect(reopened.startFromOrigin).toBe(startFromOrigin);
    expect(hasScheduledTaskDraftChanges(draft, reopened)).toBe(false);
  });

  it.each([true, false])("trims an explicit base without changing origin %s", (startFromOrigin) => {
    const draft = { ...editDraft(legacyTask), baseRef: " release/stable ", startFromOrigin };
    const saved = Schema.decodeUnknownSync(ScheduledTaskUpsertInput)(
      scheduledTaskUpsertInputFromDraft(draft),
    );
    expect(saved.workspaceStrategy).toEqual({
      type: "worktree",
      baseRef: "release/stable",
      startFromOrigin,
    });
    const reopened = editDraft(
      Schema.decodeUnknownSync(ScheduledTask)({ ...legacyTask, ...saved }),
    );
    expect(reopened.baseRef).toBe("release/stable");
    expect(reopened.startFromOrigin).toBe(startFromOrigin);
    expect(reopened.prompt).toBe(legacyTask.prompt);
    expect(reopened.modelSelection).toEqual(legacyTask.modelSelection);
  });
});
