import { resolveSettings } from "@anthropic-ai/claude-agent-sdk";
import type { ClaudeSettings } from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import {
  checkClaudeProviderStatus,
  probeClaudeCapabilities,
  type ClaudeCapabilitiesProbe,
} from "../../provider/ClaudeProvider.ts";
import {
  resolveClaudeReportedModelSlug,
  type ClaudeModelCatalog,
} from "../../provider/ClaudeModelCatalog.ts";
import { windowLimitsModel } from "../../provider/claudeUsageLimits.ts";
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

function hasClaudeSubscription(probe: ClaudeCapabilitiesProbe): boolean {
  const token = probe.tokenSource?.toLowerCase().replace(/[.\s_-]+/g, "");
  return (
    !!probe.email?.trim() &&
    (probe.apiProvider === "firstParty" || probe.apiProvider === undefined) &&
    (token === "oauth" || token === "claudeai") &&
    (!probe.apiKeySource || probe.apiKeySource === "none") &&
    SUBSCRIPTION_TYPES.has(probe.subscriptionType?.toLowerCase().replace(/[\s_-]+/g, "") ?? "")
  );
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
  return url?.origin !== "https://api.anthropic.com" || !!url.username || !!url.password;
}

const SettingsObject = Schema.Record(Schema.String, Schema.Unknown);
const decodeSettings = Schema.decodeUnknownEffect(fromLenientJson(SettingsObject));
const asSettings = Schema.decodeUnknownOption(SettingsObject);

function hasAlternateSettings(settings: Readonly<Record<string, unknown>>): boolean {
  if (settings.apiKeyHelper || settings.awsAuthRefresh || settings.awsCredentialExport) return true;
  if (settings.env === undefined) return false;
  const env = asSettings(settings.env);
  return env._tag !== "Some" || hasAlternateRoute(env.value);
}

/** Inspect the same workspace-local and account settings that can supersede the spawn environment. */
const hasNativeClaudeRoute = Effect.fn("AccountRouting.hasNativeClaudeRoute")(function* (input: {
  readonly cwd: string;
  readonly configDir: string;
  readonly customHome: boolean;
  readonly environment: NodeJS.ProcessEnv;
}) {
  if (hasAlternateRoute(input.environment, true)) return false;
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
  const localNative = yield* Effect.forEach(paths, (file) =>
    fs.readFileString(file).pipe(
      Effect.flatMap(decodeSettings),
      Effect.map((settings) => !hasAlternateSettings(settings)),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(true)),
      Effect.orElseSucceed(() => false),
    ),
  );
  if (!localNative.every(Boolean)) return false;
  return yield* Effect.tryPromise({
    try: () =>
      resolveSettings({
        cwd: input.cwd,
        settingSources: input.customHome ? ["project", "local"] : ["user", "project", "local"],
      }),
    catch: () => false,
  }).pipe(
    Effect.map(({ effective, sources }) =>
      [effective, ...sources.map(({ settings }) => settings)].every(
        (settings) => !hasAlternateSettings(settings),
      ),
    ),
    Effect.orElseSucceed(() => false),
  );
});

export const observeClaudeNativeAccount = Effect.fn("AccountRouting.observeClaudeNativeAccount")(
  function* (input: {
    readonly cwd: string;
    readonly configDir: string;
    readonly customHome: boolean;
    readonly settings: ClaudeSettings;
    readonly environment: NodeJS.ProcessEnv;
    readonly catalog: ClaudeModelCatalog;
  }) {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const args = classifyClaudeEvaluatorLaunchArgs(input.settings.launchArgs);
    // The stock promptless probe owns its setting sources and model selection.
    const modelOverridden = tokenizeCliArgs(input.settings.launchArgs).some((arg) =>
      /^--(?:fallback-)?model(?:=|$)/.test(arg),
    );
    if (!args.qualified || args.args.length > 0 || modelOverridden) return undefined;
    if (!(yield* hasNativeClaudeRoute(input))) return undefined;
    const observedAt = yield* Clock.currentTimeMillis;
    const capabilities = yield* Ref.make<ClaudeCapabilitiesProbe | undefined>(undefined);
    const draft = yield* checkClaudeProviderStatus(
      input.settings,
      () =>
        probeClaudeCapabilities(input.settings, input.environment, input.cwd).pipe(
          Effect.tap((probe) => Ref.set(capabilities, probe)),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
        ),
      input.environment,
      input.cwd,
      input.catalog,
    );
    const verified = yield* Ref.get(capabilities);
    if (!verified || !hasClaudeSubscription(verified)) return undefined;
    if (draft.auth.status !== "authenticated" || draft.auth.endpoint) return undefined;
    const advertised = new Set(
      (verified.models ?? []).flatMap(({ value, resolvedModel }) =>
        [value, resolvedModel].flatMap((id) =>
          id === undefined ? [] : [resolveClaudeReportedModelSlug(input.catalog, id)],
        ),
      ),
    );
    return {
      models: draft.models.filter((model) => advertised.has(model.slug)),
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
  if (!capabilities.usage) return { status: "unknown" as const };
  const checkedAt = capabilities.usageCheckedAt ?? "";
  if (!currentObservation(Date.parse(checkedAt), now)) return undefined;
  const limits = capabilities.usage.rate_limits;
  if (!capabilities.usage.rate_limits_available || !limits)
    return limits?.extra_usage?.is_enabled ? undefined : { status: "unknown" as const };
  const modelNames = [model];
  for (const reported of capabilities.models ?? []) {
    if (reported.value === model && reported.resolvedModel) modelNames.push(reported.resolvedModel);
  }
  const hour = 3_600_000;
  const week = 7 * 24 * hour;
  const session = readWindow(limits.five_hour, 5 * hour);
  const overallWeek = readWindow(limits.seven_day, week);
  const scoped = limits as { readonly model_scoped?: unknown; readonly seven_day_sonnet?: unknown };
  const applicable = [
    { id: "seven_day_opus", value: limits.seven_day_opus },
    { id: "seven_day_sonnet", value: scoped.seven_day_sonnet },
    ...readScopedWindows(scoped.model_scoped),
  ].filter((window) => modelNames.some((name) => windowLimitsModel(window, name)));
  const weekly = [overallWeek, ...applicable.map(({ value }) => readWindow(value, week))].filter(
    (window) => window !== undefined,
  );
  const allowance = claudeAllowance({ session, weekly, hasOverallWeek: !!overallWeek, now });
  return allowance.status === "unknown" && limits.extra_usage?.is_enabled ? undefined : allowance;
}

const readRawWindow = Schema.decodeUnknownOption(
  Schema.Struct({
    utilization: Schema.Number,
    resets_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
  }),
);

function readWindow(raw: unknown, durationMs: number): AllowanceWindow | undefined {
  const result = readRawWindow(raw);
  if (result._tag !== "Some" || !validPercent(result.value.utilization)) return undefined;
  const reset = Date.parse(result.value.resets_at ?? "");
  return {
    usedPercent: result.value.utilization,
    resetsAt: Number.isFinite(reset) ? reset : null,
    durationMs,
  };
}

function readScopedWindows(
  raw: unknown,
): ReadonlyArray<{ readonly id: string; readonly value: unknown }> {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((entry: unknown) => {
    const value = asSettings(entry);
    if (value._tag !== "Some" || typeof value.value.display_name !== "string") return [];
    const id = value.value.display_name.toLowerCase().replace(/[^a-z0-9]+/g, "_");
    return id ? [{ id: `seven_day_${id}`, value: value.value }] : [];
  });
}
