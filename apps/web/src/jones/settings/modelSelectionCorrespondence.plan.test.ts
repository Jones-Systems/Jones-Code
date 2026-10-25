import {
  DEFAULT_SERVER_SETTINGS,
  DEFAULT_UNIFIED_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ModelSelection,
  type ServerSettings,
  type ServerSettingsPatch,
  type ServerProvider,
} from "@t3tools/contracts";
import { resolveModelPreferences } from "@t3tools/shared/serverSettings";
import { describe, expect, it } from "vite-plus/test";

import {
  planScopedSettingsPatch,
  resolveScopedSettingsTargets,
} from "../../components/settings/scopedSettings";
import { resolveSettingsScope } from "../../components/settings/settingsScope";
import type { SidebarProjectSnapshot } from "../../sidebarProjectGrouping";
import {
  createModelSelectionResolver,
  MODEL_SELECTION_KEYS,
  modelSelectionsAreMixed,
} from "./modelSelectionCorrespondence";

function provider(instanceId: string): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(instanceId),
    driver: ProviderDriverKind.make("codex"),
    displayName: "Jace",
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-01-01T00:00:00.000Z",
    models: ["gpt-5", "gpt-5-new"].map((slug) => ({
      slug,
      name: slug,
      isCustom: false,
      capabilities: {},
    })),
    slashCommands: [],
    skills: [],
  };
}

const laptop = {
  environmentId: EnvironmentId.make("laptop"),
  label: "Laptop",
  connection: { phase: "connected" as const },
  serverConfig: {
    settings: {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        codex_jace_codex: { driver: ProviderDriverKind.make("codex"), enabled: true, config: {} },
      },
    },
    environment: { capabilities: { projectSettingsOverrides: true } },
  },
};
const remote = {
  ...laptop,
  environmentId: EnvironmentId.make("test"),
  label: "TEST",
  serverConfig: {
    ...laptop.serverConfig,
    settings: {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        codex_jace: { driver: ProviderDriverKind.make("codex"), enabled: true, config: {} },
        codex_other: { driver: ProviderDriverKind.make("codex"), enabled: true, config: {} },
      },
    },
  },
};
const environments = [laptop, remote];
const all = resolveSettingsScope({}, [], environments);
const representative = {
  environmentId: laptop.environmentId,
  label: laptop.label,
  settings: laptop.serverConfig.settings,
  projectId: null,
};
const context = {
  representative,
  providersByEnvironment: new Map([
    [laptop.environmentId, [provider("codex_jace_codex")]],
    [
      remote.environmentId,
      [provider("codex_jace"), { ...provider("codex_other"), displayName: "Other" }],
    ],
  ]),
};
const settingsForDestination = (destination: { settings: ServerSettings }) => ({
  ...DEFAULT_UNIFIED_SETTINGS,
  ...destination.settings,
  ...resolveModelPreferences(destination.settings, DEFAULT_UNIFIED_SETTINGS),
});
const resolver = createModelSelectionResolver({ ...context, settingsForDestination });
const sourceSelection = { instanceId: ProviderInstanceId.make("codex_jace_codex"), model: "gpt-5" };
const remoteSelection = { instanceId: ProviderInstanceId.make("codex_jace"), model: "gpt-5" };
const options = [{ id: "reasoningEffort", value: "high" }];

function modelPatch(
  key: (typeof MODEL_SELECTION_KEYS)[number],
  value: Partial<ModelSelection> | null,
): ServerSettingsPatch {
  return { [key]: value };
}

function withSelection(key: (typeof MODEL_SELECTION_KEYS)[number]) {
  return environments.map((environment) => ({
    ...environment,
    serverConfig: {
      ...environment.serverConfig,
      settings: {
        ...environment.serverConfig.settings,
        [key]:
          environment.environmentId === laptop.environmentId ? sourceSelection : remoteSelection,
      },
    },
  }));
}

function projectGroup(includeRemote = true): SidebarProjectSnapshot {
  const members = [laptop, ...(includeRemote ? [remote] : [laptop])].map((environment, index) => ({
    id: ProjectId.make(`project-${index}`),
    environmentId: environment.environmentId,
    title: "Project",
    workspaceRoot: `/repo-${index}`,
    physicalProjectKey: `${environment.environmentId}:/repo-${index}`,
    environmentLabel: environment.label,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
  }));
  return {
    ...members[0]!,
    projectKey: "group",
    displayName: "Project",
    memberProjects: members,
    memberProjectRefs: members.map((member) => ({
      environmentId: member.environmentId,
      projectId: member.id,
    })),
    groupedProjectCount: 2,
    environmentPresence: "remote-only",
    allRemoteMembersAreDesktopLocal: false,
    allRemoteMembersAreWsl: false,
    remoteEnvironmentLabels: [remote.label],
  };
}

describe("cross-environment model selection planning", () => {
  it("writes remote codex_jace instead of representative codex_jace_codex", () => {
    // Reflect.apply also exercises the pre-resolver planner, which ignored the fourth argument.
    const plan = Reflect.apply(planScopedSettingsPatch, undefined, [
      all,
      environments,
      {
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("codex_jace_codex"),
          model: "gpt-5",
        },
      },
      resolver,
    ]) as ReturnType<typeof planScopedSettingsPatch>;
    expect(plan.unavailableReason).toBeNull();
    expect(
      plan.serverWrites.find((write) => write.environmentId === remote.environmentId)?.patch,
    ).toEqual({
      textGenerationModelSelection: {
        instanceId: ProviderInstanceId.make("codex_jace"),
        model: "gpt-5",
      },
    });
  });

  it.each(MODEL_SELECTION_KEYS)("translates explicit IDs for all scope: %s", (key) => {
    const plan = planScopedSettingsPatch(all, environments, { [key]: sourceSelection }, resolver);
    expect(plan.unavailableReason).toBeNull();
    expect(plan.serverWrites.map((write) => write.patch[key])).toEqual([
      sourceSelection,
      remoteSelection,
    ]);
  });

  it.each(MODEL_SELECTION_KEYS)(
    "replaces explicit selections without inheriting old options: %s",
    (key) => {
      const targets = withSelection(key).map((environment) => ({
        ...environment,
        serverConfig: {
          ...environment.serverConfig,
          settings: {
            ...environment.serverConfig.settings,
            [key]: { ...remoteSelection, options },
          },
        },
      }));
      const plan = planScopedSettingsPatch(all, targets, { [key]: sourceSelection }, resolver);
      expect(plan.serverWrites[1]?.patch[key]).toEqual(remoteSelection);
    },
  );

  it.each(MODEL_SELECTION_KEYS)(
    "preserves destination identity for model-only and options-only patches: %s",
    (key) => {
      const targets = withSelection(key);
      const modelPlan = planScopedSettingsPatch(
        all,
        targets,
        { [key]: { model: "gpt-5-new" } },
        resolver,
      );
      expect(modelPlan.serverWrites[1]?.patch[key]).toEqual({
        ...remoteSelection,
        model: "gpt-5-new",
      });
      const optionsPlan = planScopedSettingsPatch(all, targets, { [key]: { options } }, resolver);
      expect(optionsPlan.serverWrites[1]?.patch[key]).toEqual({ ...remoteSelection, options });
    },
  );

  it("keeps a destination's different selected provider for partial writes", () => {
    const targets = withSelection("defaultModelSelection");
    const destination = targets[1]!;
    const different = {
      ...destination,
      serverConfig: {
        ...destination.serverConfig,
        settings: {
          ...destination.serverConfig.settings,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex_other"),
            model: "gpt-5",
          },
        },
      },
    };
    const plan = planScopedSettingsPatch(
      all,
      [targets[0]!, different],
      modelPatch("defaultModelSelection", { options }),
      resolver,
    );
    expect(plan.serverWrites[1]?.patch.defaultModelSelection).toEqual({
      instanceId: "codex_other",
      model: "gpt-5",
      options,
    });
  });

  it.each([{ model: "gpt-5" }, { options }])(
    "refuses partial writes without a destination base: %j",
    (value) => {
      const plan = planScopedSettingsPatch(
        all,
        environments,
        modelPatch("defaultModelSelection", value),
        resolver,
      );
      expect(plan.serverWrites).toEqual([]);
      expect(plan.unavailableReason).toContain("TEST");
    },
  );

  it("completes explicit instance-only patches from the destination model without old options", () => {
    const targets = withSelection("defaultModelSelection");
    const plan = planScopedSettingsPatch(
      all,
      targets,
      modelPatch("defaultModelSelection", { instanceId: sourceSelection.instanceId }),
      resolver,
    );
    expect(plan.serverWrites[1]?.patch.defaultModelSelection).toEqual(remoteSelection);
  });

  it("resolves each project member before completing its override", () => {
    const group = projectGroup();
    const scope = resolveSettingsScope({ project: group.projectKey }, [group], environments);
    const plan = planScopedSettingsPatch(
      scope,
      environments,
      { defaultModelSelection: sourceSelection },
      resolver,
    );
    expect(plan.unavailableReason).toBeNull();
    expect(
      plan.serverWrites[0]?.patch.projectSettingsOverrides?.[group.memberProjects[0]!.id]
        ?.defaultModelSelection,
    ).toEqual(sourceSelection);
    expect(
      plan.serverWrites[1]?.patch.projectSettingsOverrides?.[group.memberProjects[1]!.id]
        ?.defaultModelSelection,
    ).toEqual(remoteSelection);
  });

  it("uses effective project overrides for partial model writes", () => {
    const group = projectGroup();
    const targets = environments.map((environment, index) => ({
      ...environment,
      serverConfig: {
        ...environment.serverConfig,
        settings: {
          ...environment.serverConfig.settings,
          projectSettingsOverrides: {
            [group.memberProjects[index]!.id]: {
              defaultModelSelection:
                index === 0
                  ? sourceSelection
                  : { ...remoteSelection, instanceId: ProviderInstanceId.make("codex_other") },
            },
          },
        },
      },
    }));
    const scope = resolveSettingsScope({ project: group.projectKey }, [group], targets);
    const plan = planScopedSettingsPatch(
      scope,
      targets,
      modelPatch("defaultModelSelection", { options }),
      resolver,
    );
    expect(
      plan.serverWrites[1]?.patch.projectSettingsOverrides?.[group.memberProjects[1]!.id]
        ?.defaultModelSelection,
    ).toEqual({ ...remoteSelection, instanceId: "codex_other", options });
  });

  it("uses the loaded file tier's effective project model for partial writes", () => {
    const group = projectGroup();
    const scope = resolveSettingsScope({ project: group.projectKey }, [group], environments);
    const targets = resolveScopedSettingsTargets(scope, environments).map((target, index) => ({
      ...target,
      settings: {
        ...target.settings,
        defaultModelSelection: index === 0 ? sourceSelection : remoteSelection,
      },
    }));
    const plan = planScopedSettingsPatch(
      scope,
      environments,
      modelPatch("defaultModelSelection", { options }),
      createModelSelectionResolver({ ...context, targets, settingsForDestination }),
    );
    expect(
      plan.serverWrites[1]?.patch.projectSettingsOverrides?.[group.memberProjects[1]!.id]
        ?.defaultModelSelection,
    ).toEqual({ ...remoteSelection, options });
  });

  it("leaves two checkouts on the representative's environment unchanged", () => {
    const group = projectGroup(false);
    const scope = resolveSettingsScope({ project: group.projectKey }, [group], environments);
    const patch = { defaultModelSelection: sourceSelection };
    expect(planScopedSettingsPatch(scope, environments, patch, resolver)).toEqual(
      planScopedSettingsPatch(scope, environments, patch),
    );
  });

  it("preserves environment and checkout plans on the representative's environment", () => {
    const group = projectGroup();
    const scopes = [
      resolveSettingsScope({ machine: laptop.environmentId }, [group], environments),
      resolveSettingsScope(
        {
          project: group.projectKey,
          machine: laptop.environmentId,
          checkout: group.memberProjects[0]!.physicalProjectKey,
        },
        [group],
        environments,
      ),
    ];
    for (const scope of scopes) {
      const patch = { defaultModelSelection: sourceSelection };
      expect(planScopedSettingsPatch(scope, environments, patch, resolver)).toEqual(
        planScopedSettingsPatch(scope, environments, patch),
      );
    }
  });

  it("refuses the whole save when any destination cannot resolve, even with client settings", () => {
    const missing = {
      ...context,
      providersByEnvironment: new Map([[laptop.environmentId, [provider("codex_jace_codex")]]]),
    };
    const plan = planScopedSettingsPatch(
      all,
      environments,
      { defaultModelSelection: sourceSelection, fontSizeCode: 15 },
      createModelSelectionResolver({ ...missing, settingsForDestination }),
    );
    expect(plan.serverWrites).toEqual([]);
    expect(plan.unavailableReason).toContain("still loading on TEST");
    expect(plan.hasClientWrite).toBe(true);
  });

  it("refuses project writes atomically when another member cannot resolve", () => {
    const group = projectGroup();
    const scope = resolveSettingsScope({ project: group.projectKey }, [group], environments);
    const plan = planScopedSettingsPatch(
      scope,
      environments,
      { defaultModelSelection: sourceSelection },
      createModelSelectionResolver({
        ...context,
        providersByEnvironment: new Map(),
        settingsForDestination,
      }),
    );
    expect(plan.serverWrites).toEqual([]);
    expect(plan.unavailableReason).toContain("still loading on TEST");
  });

  it("refuses explicit model writes without a representative but preserves unrelated writes", () => {
    const noRepresentative = createModelSelectionResolver({
      ...context,
      representative: null,
      settingsForDestination,
    });
    expect(
      planScopedSettingsPatch(
        all,
        environments,
        { defaultModelSelection: sourceSelection },
        noRepresentative,
      ).serverWrites,
    ).toEqual([]);
    expect(
      planScopedSettingsPatch(all, environments, { defaultAutoPull: true }, noRepresentative),
    ).toEqual(planScopedSettingsPatch(all, environments, { defaultAutoPull: true }));
  });

  it.each(MODEL_SELECTION_KEYS)("preserves null reset and inherit semantics: %s", (key) => {
    const patch = { [key]: null } as ServerSettingsPatch;
    expect(planScopedSettingsPatch(all, environments, patch, resolver)).toEqual(
      planScopedSettingsPatch(all, environments, patch),
    );
    const group = projectGroup();
    const scope = resolveSettingsScope({ project: group.projectKey }, [group], environments);
    expect(planScopedSettingsPatch(scope, environments, patch, resolver)).toEqual(
      planScopedSettingsPatch(scope, environments, patch),
    );
  });

  it("keeps concrete reset defaults literal while validating destination readiness", () => {
    const defaults = {
      ...context,
      providersByEnvironment: new Map([
        [laptop.environmentId, [provider("codex")]],
        [remote.environmentId, [{ ...provider("codex"), displayName: "Other" }]],
      ]),
    };
    const patch = {
      textGenerationModelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5",
      },
    };
    expect(
      planScopedSettingsPatch(
        all,
        environments,
        patch,
        createModelSelectionResolver({ ...defaults, settingsForDestination }),
      ).serverWrites,
    ).toEqual([]);
    const literal = createModelSelectionResolver(
      { ...defaults, settingsForDestination },
      { literalModelSelections: true },
    );
    const plan = planScopedSettingsPatch(all, environments, patch, literal);
    expect(plan.unavailableReason).toBeNull();
    expect(plan.serverWrites.map((write) => write.patch)).toEqual([patch, patch]);
    const unavailable = createModelSelectionResolver(
      { ...defaults, providersByEnvironment: new Map(), settingsForDestination },
      { literalModelSelections: true },
    );
    expect(planScopedSettingsPatch(all, environments, patch, unavailable).serverWrites).toEqual([]);
  });

  it.each(MODEL_SELECTION_KEYS.filter((key) => key !== "defaultModelSelection"))(
    "requires destination text-generation support for %s before dispatch",
    (key) => {
      const unsupported = createModelSelectionResolver({
        ...context,
        settingsForDestination,
        providersByEnvironment: new Map([
          [laptop.environmentId, [provider("codex_jace_codex")]],
          [remote.environmentId, [{ ...provider("codex_jace"), supportsTextGeneration: false }]],
        ]),
      });
      expect(
        planScopedSettingsPatch(all, environments, { [key]: sourceSelection }, unsupported)
          .serverWrites,
      ).toEqual([]);
      expect(
        planScopedSettingsPatch(
          all,
          environments,
          { defaultModelSelection: sourceSelection },
          unsupported,
        ).unavailableReason,
      ).toBeNull();
    },
  );

  it("rejects a model absent from the destination catalog before dispatch", () => {
    expect(
      planScopedSettingsPatch(
        all,
        environments,
        { defaultModelSelection: { ...sourceSelection, model: "missing" } },
        resolver,
      ).serverWrites,
    ).toEqual([]);
  });
});

describe("semantic scoped model comparison", () => {
  it.each(MODEL_SELECTION_KEYS)("treats corresponding destination IDs as uniform: %s", (key) => {
    const targets = resolveScopedSettingsTargets(all, withSelection(key));
    expect(
      modelSelectionsAreMixed({ ...context, representative: targets[0]! }, targets, [key]),
    ).toBe(false);
  });

  it("marks persisted foreign IDs mixed and ignores readiness when correspondence succeeds", () => {
    const targets = resolveScopedSettingsTargets(all, withSelection("defaultModelSelection"));
    const mixed = [
      targets[0]!,
      {
        ...targets[1]!,
        settings: { ...targets[1]!.settings, defaultModelSelection: sourceSelection },
      },
    ];
    expect(
      modelSelectionsAreMixed({ ...context, representative: targets[0]! }, mixed, [
        "defaultModelSelection",
      ]),
    ).toBe(true);
    const unavailable = {
      ...context,
      representative: targets[0]!,
      providersByEnvironment: new Map([
        [laptop.environmentId, [provider("codex_jace_codex")]],
        [
          remote.environmentId,
          [{ ...provider("codex_jace"), availability: "unavailable" as const }],
        ],
      ]),
    };
    expect(modelSelectionsAreMixed(unavailable, targets, ["defaultModelSelection"])).toBe(false);
  });

  it("marks unloaded, unresolvable, and different options mixed", () => {
    const targets = resolveScopedSettingsTargets(all, withSelection("defaultModelSelection"));
    expect(
      modelSelectionsAreMixed(
        { ...context, representative: targets[0]!, providersByEnvironment: new Map() },
        targets,
        ["defaultModelSelection"],
      ),
    ).toBe(true);
    const different = [
      targets[0]!,
      {
        ...targets[1]!,
        settings: {
          ...targets[1]!.settings,
          defaultModelSelection: { ...remoteSelection, options },
        },
      },
    ];
    expect(
      modelSelectionsAreMixed({ ...context, representative: targets[0]! }, different, [
        "defaultModelSelection",
      ]),
    ).toBe(true);
  });

  it("preserves null and non-model comparison", () => {
    const targets = resolveScopedSettingsTargets(all, environments);
    expect(modelSelectionsAreMixed(context, targets, ["defaultModelSelection"])).toBe(false);
    const different = [
      targets[0]!,
      { ...targets[1]!, settings: { ...targets[1]!.settings, defaultAutoPull: true } },
    ];
    expect(modelSelectionsAreMixed(context, different, ["defaultAutoPull"])).toBe(true);
  });
});
