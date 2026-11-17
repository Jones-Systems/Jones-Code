import {
  DEFAULT_CLIENT_SETTINGS,
  DEFAULT_SERVER_SETTINGS,
  DEFAULT_UNIFIED_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { describe, expect, it } from "vite-plus/test";

import { deriveProviderInstanceEntries } from "../../providerInstances";
import {
  defaultModelSelectionsAreMixed,
  modelSelectionFailureMessage,
  modelSelectionReadiness,
  resolveProviderCorrespondence,
  scopedModelDisabledReason,
} from "./modelSelectionCorrespondence";

function provider(instanceId: string, overrides: Partial<ServerProvider> = {}): ServerProvider {
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
    models: [{ slug: "gpt-5", name: "GPT-5", isCustom: false, capabilities: {} }],
    slashCommands: [],
    skills: [],
    ...overrides,
  };
}

function match(source: readonly ServerProvider[], destination: readonly ServerProvider[]) {
  return resolveProviderCorrespondence(
    deriveProviderInstanceEntries(source),
    deriveProviderInstanceEntries(destination),
    source[0]!.instanceId,
  );
}

const source = provider("codex_jace_codex");
const remote = provider("codex_jace");

describe("provider correspondence", () => {
  it.each(["jace", "malcolm", "work", "personal", "secondary"])(
    "maps the configured Codex label for %s without claiming account identity",
    (name) => {
      const a = provider(`codex_${name}_codex`, { displayName: name });
      const b = provider(`codex_${name}`, { displayName: name });
      expect(match([a], [b])).toMatchObject({
        entry: { instanceId: b.instanceId },
        basis: "explicit-name",
        account: "not-comparable",
      });
    },
  );

  it("maps the Jace Claude pair by its explicit configured label", () => {
    expect(
      match(
        [provider("claude_jace_local", { driver: ProviderDriverKind.make("claudeAgent") })],
        [provider("claude_jace", { driver: ProviderDriverKind.make("claudeAgent") })],
      ),
    ).toMatchObject({ entry: { instanceId: "claude_jace" }, basis: "explicit-name" });
  });

  it("maps default Codex by exact ID without an explicit account label", () => {
    expect(match([provider("codex", { displayName: "Codex" })], [provider("codex")])).toMatchObject(
      { basis: "exact-id", entry: { instanceId: "codex" } },
    );
  });

  it.each([
    { driver: ProviderDriverKind.make("claudeAgent") },
    { displayName: "Different" },
    { auth: { status: "authenticated" as const, email: "other@example.invalid" } },
    { auth: { status: "authenticated" as const, workspaceId: "other-workspace" } },
  ])("refuses an exact-ID contradiction without falling back: %j", (contradiction) => {
    const a = provider("collision", {
      auth: {
        status: "authenticated",
        email: "jace@example.invalid",
        workspaceId: "workspace",
      },
    });
    expect(match([a], [provider("collision", contradiction), remote])).toEqual({
      failure: "id-conflict",
    });
  });

  it("refuses an exact ID when another same-driver instance carries the source name", () => {
    expect(match([source], [provider(source.instanceId), remote])).toEqual({
      failure: "id-conflict",
    });
  });

  it("rejects a mirror ID collision during round trip", () => {
    expect(
      match([source, provider(remote.instanceId, { displayName: "Other" })], [remote]),
    ).toEqual({ failure: "id-conflict" });
  });

  it("counts disabled duplicate labels in the destination's full catalog", () => {
    expect(match([source], [remote, provider("duplicate", { enabled: false })])).toEqual({
      failure: "ambiguous",
    });
  });

  it("counts duplicate labels in the source's full catalog", () => {
    expect(match([source, provider("duplicate", { enabled: false })], [remote])).toEqual({
      failure: "ambiguous",
    });
  });

  it.each([
    [source, provider("remote", { driver: ProviderDriverKind.make("claudeAgent") })],
    [provider("local", { displayName: "Codex" }), provider("remote", { displayName: "Codex" })],
    [
      provider("same_humanized_id", { displayName: undefined }),
      provider("SameHumanizedId", { displayName: undefined }),
    ],
    [source, provider("remote", { displayName: "jace" })],
    [source, provider("remote", { displayName: "" })],
  ])("does not infer correspondence from a driver, humanized ID, or folded name", (a, b) => {
    expect(match([a], [b])).toEqual({ failure: "no-match" });
  });

  it("trims explicit names", () => {
    expect(match([provider("local", { displayName: " Jace " })], [remote])).toMatchObject({
      basis: "explicit-name",
    });
  });

  it("compares authenticated emails after trimming and case folding, ignoring profileId", () => {
    expect(
      match(
        [
          provider("local", {
            auth: { status: "authenticated", email: " JACE@example.invalid ", profileId: "local" },
          }),
        ],
        [
          provider("remote", {
            auth: { status: "authenticated", email: "jace@EXAMPLE.invalid", profileId: "remote" },
          }),
        ],
      ),
    ).toMatchObject({ basis: "explicit-name", account: "matched" });
  });

  it.each([{ email: "other@example.invalid" }, { workspaceId: "other" }])(
    "rejects comparable authenticated fields on a name match: %j",
    (fields) => {
      expect(
        match(
          [
            provider("local", {
              auth: {
                status: "authenticated",
                email: "jace@example.invalid",
                workspaceId: "workspace",
              },
            }),
          ],
          [provider("remote", { auth: { status: "authenticated", ...fields } })],
        ),
      ).toEqual({ failure: "account-mismatch" });
    },
  );

  it.each([
    { status: "authenticated" as const },
    { status: "unauthenticated" as const, email: "other@example.invalid" },
    { status: "authenticated" as const, type: "api-key", email: "other@example.invalid" },
  ])("keeps absent or incomparable authentication unknown: %j", (auth) => {
    expect(
      match(
        [
          provider("local", {
            auth: { status: "authenticated", type: "oauth", email: "jace@example.invalid" },
          }),
        ],
        [provider("remote", { auth })],
      ),
    ).toMatchObject({ account: "not-comparable" });
  });

  it("compares workspace IDs exactly and can match without email", () => {
    const a = provider("local", { auth: { status: "authenticated", workspaceId: "Workspace" } });
    expect(match([a], [provider("remote", { auth: a.auth })])).toMatchObject({
      account: "matched",
    });
    expect(
      match(
        [a],
        [provider("remote", { auth: { status: "authenticated", workspaceId: "workspace" } })],
      ),
    ).toEqual({ failure: "account-mismatch" });
  });

  it("allows ACP exact IDs only and refuses known differing registry agents", () => {
    const a = provider("acp-local", { driver: ProviderDriverKind.make("acpRegistry") });
    expect(match([a], [a])).toMatchObject({ basis: "exact-id" });
    expect(
      match([a], [provider("acp-remote", { driver: ProviderDriverKind.make("acpRegistry") })]),
    ).toEqual({ failure: "no-match" });
    const entries = deriveProviderInstanceEntries([a]);
    expect(
      resolveProviderCorrespondence(
        entries.map((entry) => ({ ...entry, acpRegistryAgentId: "agent-a" })),
        entries.map((entry) => ({ ...entry, acpRegistryAgentId: "agent-b" })),
        a.instanceId,
      ),
    ).toEqual({ failure: "id-conflict" });
  });

  it("distinguishes unloaded catalogs and an unknown source", () => {
    expect(resolveProviderCorrespondence(undefined, [], source.instanceId)).toEqual({
      failure: "not-loaded",
    });
    expect(resolveProviderCorrespondence([], undefined, source.instanceId)).toEqual({
      failure: "not-loaded",
    });
    expect(resolveProviderCorrespondence([], [], source.instanceId)).toEqual({
      failure: "unknown-source",
    });
  });

  it("keeps refusal messages free of authentication fields", () => {
    const representative = {
      environmentId: EnvironmentId.make("laptop"),
      label: "Laptop",
      settings: DEFAULT_UNIFIED_SETTINGS,
      projectId: null,
    };
    const destination = {
      ...representative,
      environmentId: EnvironmentId.make("test"),
      label: "TEST",
    };
    const message = scopedModelDisabledReason(
      {
        representative,
        providersByEnvironment: new Map([
          [
            representative.environmentId,
            [
              provider("local", {
                auth: {
                  status: "authenticated",
                  email: "jace@example.invalid",
                  workspaceId: "private-source-workspace",
                },
              }),
            ],
          ],
          [
            destination.environmentId,
            [
              provider("remote", {
                auth: {
                  status: "authenticated",
                  email: "other@example.invalid",
                  workspaceId: "private-destination-workspace",
                },
              }),
            ],
          ],
        ]),
      },
      destination,
      { instanceId: ProviderInstanceId.make("local"), model: "gpt-5" },
      DEFAULT_UNIFIED_SETTINGS,
    );
    expect(message).toContain("TEST");
    expect(message).toContain("account-mismatch");
    expect(message).not.toContain("jace@example.invalid");
    expect(message).not.toContain("other@example.invalid");
    expect(message).not.toContain("private-source-workspace");
    expect(message).not.toContain("private-destination-workspace");
    expect(modelSelectionFailureMessage("not-loaded", destination)).toContain("still loading");
  });
});

describe("destination model readiness", () => {
  const projectId = ProjectId.make("project");
  const readySettings = {
    ...DEFAULT_UNIFIED_SETTINGS,
    providerInstances: {
      [remote.instanceId]: { driver: remote.driver, enabled: true, config: {} },
    },
  };
  const destination = {
    environmentId: EnvironmentId.make("test"),
    label: "TEST",
    projectId: null,
    settings: readySettings,
  };
  const selection = { instanceId: remote.instanceId, model: "gpt-5" };

  it("accepts an enabled and available destination model", () => {
    expect(modelSelectionReadiness(selection, destination, [remote], readySettings)).toBeNull();
  });

  it.each([{ enabled: false }, { availability: "unavailable" as const }, { models: [] }])(
    "rejects unavailable destination readiness: %j",
    (overrides) => {
      const providers = [{ ...remote, ...overrides }];
      const settings =
        overrides.enabled === false
          ? {
              ...DEFAULT_UNIFIED_SETTINGS,
              providerInstances: {
                [remote.instanceId]: {
                  driver: ProviderDriverKind.make("codex"),
                  enabled: false,
                  config: {},
                },
              },
            }
          : readySettings;
      expect(
        modelSelectionReadiness(selection, { ...destination, settings }, providers, settings),
      ).toBe("unavailable");
    },
  );

  it("honors project disablement and enablement in both directions", () => {
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        [remote.instanceId]: {
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          config: {},
        },
      },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [remote.instanceId]: { enabled: false } } },
      },
    };
    expect(
      modelSelectionReadiness(
        selection,
        { ...destination, projectId, settings },
        [remote],
        settings,
      ),
    ).toBe("unavailable");
    const enabled = {
      ...settings,
      providerInstances: {
        [remote.instanceId]: {
          driver: ProviderDriverKind.make("codex"),
          enabled: false,
          config: {},
        },
      },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [remote.instanceId]: { enabled: true } } },
      },
    };
    expect(
      modelSelectionReadiness(
        selection,
        { ...destination, projectId, settings: enabled },
        [{ ...remote, enabled: false }],
        enabled,
      ),
    ).toBeNull();
  });

  it("accepts destination custom models", () => {
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        [remote.instanceId]: {
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          config: { customModels: ["custom-model"] },
        },
      },
    };
    expect(
      modelSelectionReadiness(
        { ...selection, model: "custom-model" },
        { ...destination, settings },
        [remote],
        settings,
      ),
    ).toBeNull();
  });

  it("requires text generation support only for text generation", () => {
    const providers = [{ ...remote, supportsTextGeneration: false }];
    expect(modelSelectionReadiness(selection, destination, providers, readySettings, true)).toBe(
      "unavailable",
    );
    expect(modelSelectionReadiness(selection, destination, providers, readySettings)).toBeNull();
  });

  it("refuses an unloaded destination catalog", () => {
    expect(modelSelectionReadiness(selection, destination, undefined, readySettings)).toBe(
      "not-loaded",
    );
  });
});

function defaultProvider({
  instanceId,
  provider: driver = ProviderDriverKind.make("codex"),
  models,
}: {
  instanceId: string;
  provider?: ProviderDriverKind;
  models: readonly string[];
}): ServerProvider {
  return provider(instanceId, {
    driver,
    displayName: undefined,
    models: models.map((slug) => ({ slug, name: slug, isCustom: false, capabilities: {} })),
  });
}

describe("scoped default model selections", () => {
  // regression: #27997 equal automatic defaults can resolve to different providers
  it("compares automatic defaults using each environment's models", () => {
    const codex = {
      projectId: null,
      settings: DEFAULT_UNIFIED_SETTINGS,
      providers: [defaultProvider({ instanceId: "codex", models: ["gpt-6-astra"] })],
    };
    const claude = {
      projectId: null,
      settings: DEFAULT_UNIFIED_SETTINGS,
      providers: [
        defaultProvider({
          instanceId: "claudeAgent",
          provider: ProviderDriverKind.make("claudeAgent"),
          models: ["claude-sonnet-4-6"],
        }),
      ],
    };

    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [codex, claude])).toBe(true);
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [claude, codex])).toBe(true);
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [codex, codex])).toBe(false);
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [claude])).toBe(false);
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [])).toBe(false);
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [codex, { ...codex, providers: [] }]),
    ).toBe(true);
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [
        codex,
        { ...codex, providers: [defaultProvider({ instanceId: "codex", models: ["gpt-6-sol"] })] },
      ]),
    ).toBe(true);

    const projectId = ProjectId.make("claude-only");
    const project = {
      projectId,
      settings: {
        ...DEFAULT_SERVER_SETTINGS,
        projectSettingsOverrides: {
          [projectId]: {
            providerInstances: { [ProviderInstanceId.make("codex")]: { enabled: false } },
          },
        },
      },
      providers: [...codex.providers, ...claude.providers],
    };
    const remoteProject = { ...claude, projectId: ProjectId.make("remote-project") };
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [project, remoteProject])).toBe(
      false,
    );
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [remoteProject, project])).toBe(
      false,
    );
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [
        project,
        { ...project, projectId: ProjectId.make("other-project") },
      ]),
    ).toBe(true);

    const configured = {
      projectId: null,
      settings: {
        ...DEFAULT_SERVER_SETTINGS,
        providerModelPreferences: {
          [ProviderInstanceId.make("codex")]: {
            hiddenModels: ["gpt-6-astra"],
            modelOrder: [],
          },
        },
      },
      providers: [defaultProvider({ instanceId: "codex", models: ["gpt-6-astra", "gpt-6-sol"] })],
    };
    const inherited = { ...configured, settings: DEFAULT_SERVER_SETTINGS };
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [configured, inherited])).toBe(
      true,
    );
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [inherited, configured])).toBe(
      true,
    );
    const clientSettings = {
      ...DEFAULT_CLIENT_SETTINGS,
      providerModelPreferences: configured.settings.providerModelPreferences,
    };
    expect(defaultModelSelectionsAreMixed(clientSettings, [configured, inherited])).toBe(false);
    expect(defaultModelSelectionsAreMixed(clientSettings, [inherited, configured])).toBe(false);
    expect(
      defaultModelSelectionsAreMixed(clientSettings, [
        { ...inherited, settings: { ...DEFAULT_SERVER_SETTINGS, providerModelPreferences: {} } },
        inherited,
      ]),
    ).toBe(true);
  });

  it("recognizes matching Malcolm Codex defaults with different local provider IDs", () => {
    const model = "gpt-6.2-sol";
    const target = (id: string, effort = "high", auth?: ServerProvider["auth"]) => {
      const instance = defaultProvider({ instanceId: id, models: [model] });
      return {
        projectId: null,
        settings: {
          ...DEFAULT_SERVER_SETTINGS,
          defaultModelSelection: createModelSelection(instance.instanceId, model, [
            { id: "reasoningEffort", value: effort },
          ]),
        },
        providers: [{ ...instance, displayName: "Malcolm Codex", auth: auth ?? instance.auth }],
      };
    };
    const laptop = target("codex_malcolm_codex");
    const testServer = target("codex_malcolm");

    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [laptop, testServer])).toBe(
      false,
    );
    expect(defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [testServer, laptop])).toBe(
      false,
    );
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [
        laptop,
        target("codex_malcolm", "xhigh"),
      ]),
    ).toBe(true);
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [
        laptop,
        target("codex_malcolm", "high", { status: "authenticated", email: "one@example.invalid" }),
        target("third_registration", "high", {
          status: "authenticated",
          email: "two@example.invalid",
        }),
      ]),
    ).toBe(true);
  });

  it("keeps automatic, explicit, and different saved options distinct", () => {
    const providers = [defaultProvider({ instanceId: "codex", models: ["gpt-6-astra"] })];
    const selected = {
      projectId: null,
      providers,
      settings: {
        ...DEFAULT_UNIFIED_SETTINGS,
        defaultModelSelection: createModelSelection(
          ProviderInstanceId.make("codex"),
          "gpt-6-astra",
        ),
      },
    };
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [selected, { ...selected }]),
    ).toBe(false);
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [
        selected,
        { projectId: null, providers, settings: DEFAULT_UNIFIED_SETTINGS },
      ]),
    ).toBe(true);
    expect(
      defaultModelSelectionsAreMixed(DEFAULT_CLIENT_SETTINGS, [
        selected,
        {
          ...selected,
          settings: {
            ...selected.settings,
            defaultModelSelection: {
              ...selected.settings.defaultModelSelection,
              options: [{ id: "reasoningEffort", value: "xhigh" }],
            },
          },
        },
      ]),
    ).toBe(true);
  });
});
