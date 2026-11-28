import { resolveSettings } from "@anthropic-ai/claude-agent-sdk";
import type { ClaudeSettings, ServerProviderModel } from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  checkClaudeProviderStatus,
  parseClaudeEffortCapSources,
  probeClaudeCapabilities,
  type ClaudeCapabilitiesProbe,
} from "../../provider/ClaudeProvider.ts";
import {
  capClaudeCatalogEffort,
  normalizeClaudeCatalogEffort,
  resolveClaudeReportedModelSlug,
  type ClaudeModelCatalog,
} from "../../provider/ClaudeModelCatalog.ts";
import { windowLimitsModel } from "../../provider/claudeUsageLimits.ts";
import { claudeAccountConfigPath } from "../../provider/claudeResetCredits.ts";
import { claudeLocalSettingsDirectory } from "../../provider/Drivers/ClaudeHome.ts";
import { classifyClaudeEvaluatorLaunchArgs } from "../../textGeneration/jones/claudeEvaluatorLaunch.ts";
import {
  claudeAllowance,
  currentObservation,
  validPercent,
  type AllowanceWindow,
} from "./policy.ts";

const SUBSCRIPTION_TYPES = new Set([
  "pro",
  "max",
  "maxplan",
  "max5",
  "max20",
  "team",
  "enterprise",
  "free",
  "claudeprosubscription",
  "claudemaxsubscription",
  "claudemax5xsubscription",
  "claudemax20xsubscription",
  "claudeteamsubscription",
  "claudeenterprisesubscription",
  "claudefreesubscription",
]);

type ClaudeSubscriptionAccount = Partial<
  Pick<
    ClaudeCapabilitiesProbe,
    "email" | "organization" | "apiProvider" | "tokenSource" | "apiKeySource" | "subscriptionType"
  >
>;

export function hasClaudeSubscription(probe: ClaudeSubscriptionAccount): boolean {
  const token = probe.tokenSource?.toLowerCase().replace(/[.\s_-]+/g, "");
  return (
    !!probe.email?.trim() &&
    !!probe.organization?.trim() &&
    (probe.apiProvider === "firstParty" || probe.apiProvider === undefined) &&
    (token === "oauth" || token === "claudeai") &&
    (!probe.apiKeySource || probe.apiKeySource === "none") &&
    SUBSCRIPTION_TYPES.has(probe.subscriptionType?.toLowerCase().replace(/[\s_-]+/g, "") ?? "")
  );
}

export function claudeAutoAccountIdentity(
  workspaceId: string | null | undefined,
  email: string | null | undefined,
  organizationName: string | null | undefined,
): string | undefined {
  const workspace = workspaceId?.trim().toLowerCase();
  const address = email?.trim().toLowerCase();
  const organization = organizationName?.trim();
  return workspace && address && organization
    ? JSON.stringify([workspace, address, organization])
    : undefined;
}

const ALTERNATE_CREDENTIALS = [
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_AUTH_TOKEN",
  "ANTHROPIC_CUSTOM_HEADERS",
  "CLAUDE_CODE_OAUTH_TOKEN",
  "CLAUDE_CODE_OAUTH_TOKEN_FILE",
  "CLAUDE_CODE_OAUTH_TOKEN_FD",
  "CLAUDE_CONFIG_DIR",
  "ANTHROPIC_MODEL",
  "ANTHROPIC_DEFAULT_HAIKU_MODEL",
  "ANTHROPIC_DEFAULT_SONNET_MODEL",
  "ANTHROPIC_DEFAULT_OPUS_MODEL",
];
const ALTERNATE_PROVIDERS = [
  "CLAUDE_CODE_USE_BEDROCK",
  "CLAUDE_CODE_USE_VERTEX",
  "CLAUDE_CODE_USE_FOUNDRY",
];

function hasAlternateRoute(
  environment: Readonly<Record<string, unknown>>,
  configDirAllowed = false,
): boolean {
  if (
    ALTERNATE_CREDENTIALS.some(
      (key) => !(configDirAllowed && key === "CLAUDE_CONFIG_DIR") && Boolean(environment[key]),
    )
  )
    return true;
  if (
    ALTERNATE_PROVIDERS.some((key) => {
      const value = String(environment[key] ?? "")
        .trim()
        .toLowerCase();
      return value !== "" && value !== "0" && value !== "false";
    })
  )
    return true;
  const base = environment.ANTHROPIC_BASE_URL;
  if (base == null || base === "") return false;
  const url = typeof base === "string" ? URL.parse(base) : null;
  return url?.href.replace(/\/$/, "") !== "https://api.anthropic.com";
}

const SettingsObject = Schema.Record(Schema.String, Schema.Unknown);
const decodeSettings = Schema.decodeUnknownEffect(fromLenientJson(SettingsObject));
const asSettings = Schema.decodeUnknownOption(SettingsObject);

function hasAlternateSettings(settings: Readonly<Record<string, unknown>>): boolean {
  if (
    settings.apiKeyHelper ||
    settings.awsAuthRefresh ||
    settings.awsCredentialExport ||
    settings.policyHelper
  )
    return true;
  if (settings.modelOverrides != null) {
    const overrides = asSettings(settings.modelOverrides);
    if (overrides._tag !== "Some" || Object.keys(overrides.value).length > 0) return true;
  }
  if (settings.env === undefined) return false;
  const env = asSettings(settings.env);
  return env._tag !== "Some" || hasAlternateRoute(env.value);
}

/** Inspect the same workspace-local and account settings that can supersede the spawn environment. */
const readNativeClaudeRoute = Effect.fn("AccountRouting.readNativeClaudeRoute")(function* (input: {
  readonly cwd: string;
  readonly configDir: string;
  readonly customHome: boolean;
  readonly environment: NodeJS.ProcessEnv;
}) {
  if (hasAlternateRoute(input.environment, true)) return undefined;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const localRoot = yield* claudeLocalSettingsDirectory(input.cwd);
  const paths = [
    path.join(input.configDir, "settings.json"),
    path.join(input.configDir, "remote-settings.json"),
    path.join(input.cwd, ".claude", "settings.json"),
    path.join(input.cwd, ".claude", "settings.local.json"),
    ...(localRoot === input.cwd ? [] : [path.join(localRoot, ".claude", "settings.local.json")]),
  ];
  const localSettings = yield* Effect.forEach(paths, (file) =>
    fs.readFileString(file).pipe(
      Effect.flatMap(decodeSettings),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(null)),
      Effect.orElseSucceed(() => undefined),
    ),
  );
  if (localSettings.some((settings) => settings === undefined)) return undefined;
  return yield* Effect.tryPromise({
    try: () =>
      resolveSettings({
        cwd: input.cwd,
        settingSources: input.customHome ? ["project", "local"] : ["user", "project", "local"],
      }),
    catch: () => undefined,
  }).pipe(
    Effect.map(({ effective, sources }) => {
      const records = [
        ...localSettings.filter((settings) => settings != null),
        ...sources.map(({ settings }) => settings),
      ];
      return [effective, ...records].some(hasAlternateSettings) ? undefined : records;
    }),
    Effect.orElseSucceed(() => undefined),
  );
});

export const hasNativeClaudeRoute = Effect.fn("AccountRouting.hasNativeClaudeRoute")(function* (
  input: Parameters<typeof readNativeClaudeRoute>[0],
) {
  return (yield* readNativeClaudeRoute(input)) !== undefined;
});

const decodeClaudeAccountMetadata = Schema.decodeUnknownEffect(
  fromLenientJson(
    Schema.Struct({
      oauthAccount: Schema.Struct({
        organizationUuid: Schema.String,
        organizationName: Schema.String,
        emailAddress: Schema.String,
      }),
    }),
  ),
);

export const readClaudeAutoIdentity = Effect.fn("AccountRouting.readClaudeAutoIdentity")(function* (
  configPath: string,
  account?: Pick<ClaudeSubscriptionAccount, "email" | "organization">,
) {
  const fs = yield* FileSystem.FileSystem;
  return yield* fs.readFileString(configPath).pipe(
    Effect.flatMap(decodeClaudeAccountMetadata),
    Effect.map(({ oauthAccount }) => {
      const identity = claudeAutoAccountIdentity(
        oauthAccount.organizationUuid,
        oauthAccount.emailAddress,
        oauthAccount.organizationName,
      );
      // SDK AccountInfo.organization is the native organization name. The CLI
      // does not report its UUID here, so same-email, same-name orgs cannot be
      // distinguished by native evidence alone. Bind the local UUID as well.
      return account === undefined ||
        identity ===
          claudeAutoAccountIdentity(
            oauthAccount.organizationUuid,
            account.email,
            account.organization,
          )
        ? identity
        : undefined;
    }),
    Effect.orElseSucceed(() => undefined),
  );
});

export function claudeAutoModels(
  catalog: ClaudeModelCatalog,
  models: ReadonlyArray<ServerProviderModel>,
  capabilities: ClaudeCapabilitiesProbe,
): ReadonlyArray<ServerProviderModel> {
  // The display helper needs a level at or below the cap in order to relabel an option.
  // Supply a low option only to the cap calculation for models without a low effort.
  const capInputs = models.map((model) => {
    if (!model.capabilities?.optionDescriptors) return model;
    return {
      ...model,
      capabilities: {
        ...model.capabilities,
        optionDescriptors: model.capabilities.optionDescriptors.map((descriptor) =>
          descriptor.id === "effort" &&
          descriptor.type === "select" &&
          !descriptor.options.some(({ id }) => id === "low")
            ? { ...descriptor, options: [...descriptor.options, { id: "low", label: "Low" }] }
            : descriptor,
        ),
      },
    };
  });
  const capped = new Map(
    capClaudeCatalogEffort(catalog, capInputs, capabilities.effortCapSources ?? []).map((model) => [
      model.slug,
      model,
    ]),
  );
  return models.flatMap((model) => {
    if (model.isCustom) return [];
    const native = (capabilities.models ?? []).filter(
      ({ value, resolvedModel }) =>
        value !== "default" &&
        resolveClaudeReportedModelSlug(catalog, resolvedModel ?? value) === model.slug,
    );
    if (native.length === 0) return [];
    const descriptors = model.capabilities?.optionDescriptors;
    if (!descriptors || !model.capabilities) return [model];
    const effectiveEffort = capped
      .get(model.slug)
      ?.capabilities?.optionDescriptors?.find((descriptor) => descriptor.id === "effort");
    return [
      {
        ...model,
        capabilities: {
          ...model.capabilities,
          optionDescriptors: descriptors.map((descriptor) => {
            if (descriptor.id !== "effort" || descriptor.type !== "select") return descriptor;
            const options = descriptor.options.filter((option) => {
              if (option.id === "default") return true;
              const nativeEffort = normalizeClaudeCatalogEffort(catalog, option.id, model.slug);
              return (
                effectiveEffort?.type === "select" &&
                effectiveEffort.options.some(
                  ({ id, label }) => id === option.id && label === option.label,
                ) &&
                native.every(
                  ({ supportsEffort, supportedEffortLevels }) =>
                    supportsEffort !== false &&
                    supportedEffortLevels?.some((effort) => effort === nativeEffort),
                )
              );
            });
            return { ...descriptor, options };
          }),
        },
      },
    ];
  });
}

export const observeClaudeNativeAccount = Effect.fn("AccountRouting.observeClaudeNativeAccount")(
  function* (input: {
    readonly cwd: string;
    readonly configDir: string;
    readonly customHome: boolean;
    readonly settings: ClaudeSettings;
    readonly environment: NodeJS.ProcessEnv;
    readonly catalog: ClaudeModelCatalog;
  }) {
    if (!input.settings.enabled) return undefined;
    const args = classifyClaudeEvaluatorLaunchArgs(input.settings.launchArgs);
    // Auto's promptless probe owns its setting sources and model selection.
    const modelOverridden = tokenizeCliArgs(input.settings.launchArgs).some((arg) =>
      /^--(?:fallback-)?model(?:=|$)/.test(arg),
    );
    if (!args.qualified || args.args.length > 0 || modelOverridden) return undefined;
    const routeSettings = yield* readNativeClaudeRoute(input);
    if (!routeSettings) return undefined;
    const accountConfig = yield* claudeAccountConfigPath(
      input.settings.homePath.trim() || input.environment.CLAUDE_CONFIG_DIR?.trim()
        ? input.configDir
        : undefined,
    );
    const priorIdentity = yield* readClaudeAutoIdentity(accountConfig);
    if (!priorIdentity) return undefined;
    const observedAt = yield* Clock.currentTimeMillis;
    const verified = yield* probeClaudeCapabilities(
      input.settings,
      input.environment,
      input.cwd,
      true,
      true,
    ).pipe(Effect.orElseSucceed(() => undefined));
    if (!verified || !hasClaudeSubscription(verified)) return undefined;
    const accountIdentity = yield* readClaudeAutoIdentity(accountConfig, verified);
    if (accountIdentity !== priorIdentity) return undefined;
    const { effortCapSources: managedCaps, ...uncapped } = verified;
    const draft = yield* checkClaudeProviderStatus(
      input.settings,
      () => Effect.succeed(uncapped),
      input.environment,
      input.cwd,
      input.catalog,
      undefined,
      undefined,
      true,
    );
    if (draft.auth.status !== "authenticated" || draft.auth.endpoint) return undefined;
    const launchCaps = parseClaudeEffortCapSources({
      sources: routeSettings.map((settings) => ({ settings })),
    });
    return {
      accountIdentity,
      models: claudeAutoModels(input.catalog, draft.models, {
        ...verified,
        effortCapSources: [...(launchCaps ?? []), ...(managedCaps ?? [])],
      }),
      allowance: (model: string, now: number) =>
        claudeProbeAllowance(verified, model, observedAt, now),
    };
  },
);

export function claudeProbeAllowance(
  capabilities: ClaudeCapabilitiesProbe,
  model: string,
  observedAt: number,
  now: number,
) {
  if (!currentObservation(observedAt, now) || !hasClaudeSubscription(capabilities)) {
    return undefined;
  }
  if (!capabilities.usage) return undefined;
  const checkedAt = capabilities.usageCheckedAt ?? "";
  if (!currentObservation(Date.parse(checkedAt), now)) return undefined;
  const limits = capabilities.usage.rate_limits;
  if (!limits) return undefined;
  const modelNames = [model];
  for (const reported of capabilities.models ?? []) {
    if (reported.value === model && reported.resolvedModel) modelNames.push(reported.resolvedModel);
  }
  const hour = 3_600_000;
  const week = 7 * 24 * hour;
  const session = readWindow(limits.five_hour, 5 * hour);
  const overallWeek = readWindow(limits.seven_day, week);
  const scoped = limits as {
    readonly model_scoped?: unknown;
    readonly seven_day_sonnet?: unknown;
    readonly spend?: unknown;
  };
  const applicable = [
    { id: "seven_day_opus", value: limits.seven_day_opus },
    { id: "seven_day_sonnet", value: scoped.seven_day_sonnet },
    ...readScopedWindows(scoped.model_scoped),
  ].filter(
    ({ id, value }) =>
      id === undefined ||
      (value != null && modelNames.some((name) => windowLimitsModel({ id }, name))),
  );
  const scopedWindows = applicable.map(({ id, value }) =>
    id === undefined ? undefined : readWindow(value, week),
  );
  const weekly = [overallWeek, ...scopedWindows].filter((window) => window !== undefined);
  const allowance = claudeAllowance({
    session,
    weekly,
    hasOverallWeek: capabilities.usage.rate_limits_available === true && !!overallWeek,
    hasUnreadableWindows: scopedWindows.some((window) => window === undefined),
    now,
  });
  const paid = asSettings(scoped.spend);
  const extraUsageDisabled = limits.extra_usage?.is_enabled === false;
  const spendDisabled = paid._tag === "Some" && paid.value.enabled === false;
  const paidFallbackDisabled =
    (extraUsageDisabled || spendDisabled) &&
    (limits.extra_usage == null || extraUsageDisabled) &&
    (scoped.spend == null || spendDisabled);
  return allowance.status === "unknown" && !paidFallbackDisabled ? undefined : allowance;
}

function readWindow(raw: unknown, durationMs: number): AllowanceWindow | undefined {
  const result = asSettings(raw);
  if (result._tag !== "Some" || !validPercent(result.value.utilization)) return undefined;
  const reset = Date.parse(
    typeof result.value.resets_at === "string" ? result.value.resets_at : "",
  );
  return {
    usedPercent: result.value.utilization,
    resetsAt: Number.isFinite(reset) ? reset : null,
    durationMs,
  };
}

function readScopedWindows(
  raw: unknown,
): ReadonlyArray<{ readonly id?: string; readonly value: unknown }> {
  if (raw == null) return [];
  if (!Array.isArray(raw)) return [{ value: raw }];
  return raw.map((entry: unknown) => {
    const value = asSettings(entry);
    if (value._tag !== "Some" || typeof value.value.display_name !== "string")
      return { value: entry };
    const name = value.value.display_name
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_");
    return /[a-z0-9]/.test(name)
      ? { id: `seven_day_${name}`, value: value.value }
      : { value: entry };
  });
}
