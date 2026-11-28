import {
  ProviderInstanceId,
  type QualifiedQuotaMap,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { isNativeCodexAutoRoute } from "../../provider/CodexProvider.ts";
import type { ClaudeCapabilitiesProbe } from "../../provider/ClaudeProvider.ts";
import type { ClaudeModelCatalog } from "../../provider/ClaudeModelCatalog.ts";
import {
  beginQuotaProbe,
  makeAccountRoutingQuota,
  makeQualifiedQuota,
  readPrivateQuotaProof,
} from "../../provider/qualifiedQuota.ts";
import {
  claudeAutoAccountIdentity,
  claudeAutoModels,
  claudeProbeAllowance,
  hasClaudeSubscription,
} from "./claudeEvidence.ts";
import {
  CodexRoutedAccountUsageLimit,
  codexAllowance,
  makeCodexRoutingQuota,
  routedCodexUsageFailure,
} from "./codexEvidence.ts";
import { chooseAccount, claudeAllowance, type AllowanceWindow } from "./policy.ts";

const checkedAt = "2026-11-18T16:00:00.000Z";
const now = Date.parse(checkedAt);
const hour = 3_600_000;
const day = 24 * hour;
const a = ProviderInstanceId.make("a");
const b = ProviderInstanceId.make("b");
const c = ProviderInstanceId.make("c");

const window = (usedPercent: number, resetIn: number, durationMs: number): AllowanceWindow => ({
  usedPercent,
  resetsAt: now + resetIn,
  durationMs,
});
const session = (used: number, resetIn = 3 * hour) => window(used, resetIn, 5 * hour);
const weekly = (used: number, resetIn = 2 * day) => window(used, resetIn, 7 * day);
const claude = (fiveHour: AllowanceWindow | undefined, ...weeks: ReadonlyArray<AllowanceWindow>) =>
  claudeAllowance({ session: fiveHour, weekly: weeks, hasOverallWeek: weeks.length > 0, now });

describe("account routing policy", () => {
  it("recognizes an attributed Codex allowance refusal through startup error wrappers", () => {
    const refusal = new CodexRoutedAccountUsageLimit();
    const wrapped = new Error("Failed to start", {
      cause: new Error("Native startup", { cause: refusal }),
    });
    expect(routedCodexUsageFailure(wrapped)?.message).toBe(refusal.message);
    expect(routedCodexUsageFailure(new Error(refusal.message))).toBeUndefined();
  });

  it("ranks useful weekly surplus per day and caps it with applicable model windows", () => {
    const slow = claude(session(30), weekly(20, 5 * day));
    const fast = claude(session(30), weekly(40, 2 * day));
    const scoped = claude(session(30), weekly(40, 2 * day), weekly(80, 2 * day));
    expect(
      chooseAccount(
        "claudeAgent",
        [
          { instanceId: a, allowance: slow },
          { instanceId: b, allowance: fast },
        ],
        now,
      ),
    ).toEqual({ instanceId: b });
    expect(
      chooseAccount(
        "claudeAgent",
        [
          { instanceId: a, allowance: slow },
          { instanceId: b, allowance: scoped },
        ],
        now,
      ),
    ).toEqual({ instanceId: a });
  });

  it("admits 30% session and strictly more than 10% weekly, including with another missing window", () => {
    expect(claude(session(70), weekly(89.999)).status).toBe("eligible");
    expect(claude(session(70.01), weekly(20)).status).toBe("blocked");
    expect(claude(undefined, weekly(90)).status).toBe("blocked");
    expect(claude(session(70), weekly(90), weekly(95, 4 * day))).toEqual({
      status: "blocked",
      resetsAt: now + 4 * day,
    });
    expect(claude(undefined, weekly(40)).status).toBe("unknown");
  });

  it("takes the five-hour detour only while all weekly and five-hour guards hold", () => {
    const normal = { instanceId: a, allowance: claude(session(50), weekly(20)) };
    const alternate = (used: number, week: AllowanceWindow, resetIn = hour) => ({
      instanceId: b,
      allowance: claude(session(used, resetIn), week),
    });
    expect(chooseAccount("claudeAgent", [normal, alternate(60, weekly(55))], now)).toEqual({
      instanceId: b,
    });
    expect(chooseAccount("claudeAgent", [normal, alternate(60.1, weekly(55))], now)).toEqual({
      instanceId: a,
    });
    expect(chooseAccount("claudeAgent", [normal, alternate(50, weekly(89, 5 * day))], now)).toEqual(
      { instanceId: a },
    );
    expect(chooseAccount("claudeAgent", [normal, alternate(50, weekly(55), 0)], now)).toEqual({
      instanceId: a,
    });
    expect(
      chooseAccount(
        "claudeAgent",
        [
          { ...normal, allowance: claude(session(50), weekly(20, 11 * hour)) },
          alternate(50, weekly(55)),
        ],
        now,
      ),
    ).toEqual({ instanceId: a });
  });

  it("falls back only to verified unknown, deterministically, and otherwise reports reset", () => {
    const unknown = { status: "unknown" as const };
    const blocked = claude(session(80), weekly(97));
    expect(
      chooseAccount(
        "claudeAgent",
        [
          { instanceId: a, allowance: blocked },
          { instanceId: c, allowance: unknown },
          { instanceId: b, allowance: unknown },
        ],
        now,
      ),
    ).toEqual({ instanceId: b });
    expect(chooseAccount("claudeAgent", [{ instanceId: a, allowance: blocked }], now)).toEqual({
      nextReset: now + 2 * day,
    });
    expect(chooseAccount("claudeAgent", [], now)).toEqual({ nextReset: null });
  });
});

describe("Codex contextual evidence", () => {
  const input = {
    instanceId: a,
    probeId: "dd09a06e-b3a2-47ce-8177-2a84d7fb06ec",
    attemptedAt: checkedAt,
    probeCompletedAt: checkedAt,
    quotaReceivedAt: checkedAt,
    accountId: "synthetic-account",
    ordinaryUsageAllowed: true,
  };
  const map = {
    codex: {
      secondary: { usedPercent: 55, resetsAt: (now + day) / 1_000, windowDurationMins: 10_080 },
    },
    tool_model: {
      normalModelSlug: "gpt-scoped",
      primary: { usedPercent: 100, resetsAt: (now + hour) / 1_000, windowDurationMins: 300 },
      credits: { balance: "1000", hasCredits: true, unlimited: true },
    },
  } satisfies QualifiedQuotaMap;

  it("uses the backend model mapping and excludes credits as allowance", () => {
    const quota = makeAccountRoutingQuota({ ...input, rateLimitsByLimitId: map });
    expect(codexAllowance(quota, "gpt-general", now)?.status).toBe("eligible");
    expect(codexAllowance(quota, "gpt-scoped", now)).toEqual({
      status: "blocked",
      resetsAt: now + hour,
    });
    expect(codexAllowance(quota, "gpt-general", now + 120_001)).toBeUndefined();
  });

  it("distinguishes ordinary denial, verified unknown, and objects without private proof", () => {
    const denied = makeAccountRoutingQuota({ ...input, ordinaryUsageAllowed: false });
    const unknown = makeAccountRoutingQuota(input);
    expect(codexAllowance(denied, "gpt-general", now)).toEqual({
      status: "blocked",
      resetsAt: null,
    });
    expect(codexAllowance(unknown, "gpt-general", now)).toEqual({ status: "unknown" });
    expect(codexAllowance({ ...unknown }, "gpt-general", now)).toBeUndefined();
    expect(
      codexAllowance(makeAccountRoutingQuota({ ...input, accountId: "" }), "gpt-general", now),
    ).toBeUndefined();
  });

  it("keeps another model's block out of the selected model's weekly pace", () => {
    const quota = makeAccountRoutingQuota({
      ...input,
      rateLimitsByLimitId: {
        ...map,
        tool_model: { ...map.tool_model, rateLimitReachedType: "rate_limit_reached" },
      },
    });
    expect(quota.status).toBe("unknown");
    expect(codexAllowance(quota, "gpt-general", now)?.status).toBe("eligible");
    expect(codexAllowance(quota, "gpt-scoped", now)?.status).toBe("blocked");
  });

  it("preserves legacy and model-scoped blockers when both response forms are present", () => {
    const quota = makeCodexRoutingQuota({
      ...input,
      rateLimitsByLimitId: map,
      legacyRateLimit: { limitId: "codex", spendControlReached: true },
    });
    expect(codexAllowance(quota, "gpt-general", now)).toEqual({
      status: "blocked",
      resetsAt: null,
    });
    const legacyScoped = makeCodexRoutingQuota({
      ...input,
      rateLimitsByLimitId: { codex: map.codex },
      legacyRateLimit: { ...map.tool_model, limitId: "tool_model" },
    });
    expect(codexAllowance(legacyScoped, "gpt-scoped", now)).toEqual({
      status: "blocked",
      resetsAt: now + hour,
    });
    expect(codexAllowance(legacyScoped, "gpt-general", now)?.status).toBe("eligible");
  });

  it("excludes contradictory quota evidence from verified-unknown fallback", () => {
    const inconsistent = makeCodexRoutingQuota({
      ...input,
      rateLimitsByLimitId: map,
      legacyRateLimit: {
        limitId: "codex",
        secondary: { ...map.codex.secondary, usedPercent: 10 },
      },
    });
    expect(codexAllowance(inconsistent, "gpt-general", now)).toBeUndefined();
    expect(codexAllowance(inconsistent, "gpt-scoped", now)?.status).toBe("blocked");
    const conflictingScope = makeAccountRoutingQuota({
      ...input,
      rateLimitsByLimitId: { codex: { ...map.codex, limitId: "other_bucket" } },
    });
    expect(codexAllowance(conflictingScope, "gpt-general", now)).toBeUndefined();
  });

  it("keeps contextual and failover proof lifetimes independent", () => {
    const probeBinding = beginQuotaProbe(input.instanceId, input.probeId);
    const failover = makeQualifiedQuota({ ...input, rateLimitsByLimitId: map, probeBinding });
    const contextual = makeAccountRoutingQuota({ ...input, rateLimitsByLimitId: map });
    expect(readPrivateQuotaProof(failover)?.ordinaryUsageAllowed).toBe(true);
    expect(readPrivateQuotaProof(contextual)).toBeUndefined();
    expect(codexAllowance(failover, "gpt-general", now)).toBeUndefined();
  });

  it("rejects custom providers and project or environment endpoints", () => {
    expect(isNativeCodexAutoRoute({ model_provider: "openai" }, {})).toBe(true);
    expect(isNativeCodexAutoRoute({ model_provider: "gateway" }, {})).toBe(false);
    expect(isNativeCodexAutoRoute({ chatgpt_base_url: "https://gateway.invalid" }, {})).toBe(false);
    expect(isNativeCodexAutoRoute({}, { OPENAI_BASE_URL: "https://api.openai.com:3550" })).toBe(
      false,
    );
  });
});

describe("Claude contextual evidence", () => {
  const resetAt = "2026-11-19T16:00:00.000Z";
  const probe = {
    email: "synthetic@example.invalid",
    organization: "Synthetic Organization",
    subscriptionType: "pro",
    tokenSource: "oauth",
    apiKeySource: "none",
    apiProvider: "firstParty",
    slashCommands: [],
    usageCheckedAt: checkedAt,
    usage: {
      rate_limits_available: true,
      rate_limits: {
        five_hour: { utilization: 33, resets_at: "2026-11-18T17:00:00.000Z" },
        seven_day: { utilization: 20, resets_at: resetAt },
        seven_day_opus: { utilization: 90, resets_at: resetAt },
        ...({
          model_scoped: [{ display_name: "Fable", utilization: 95, resets_at: resetAt }],
        } as object),
      },
    },
  } satisfies ClaudeCapabilitiesProbe;

  it("matches both explicit model-scoped and legacy applicable weekly windows", () => {
    expect(claudeProbeAllowance(probe, "claude-sonnet-4-6", now, now)?.status).toBe("eligible");
    expect(claudeProbeAllowance(probe, "claude-opus-4-6", now, now)?.status).toBe("blocked");
    expect(claudeProbeAllowance(probe, "claude-fable-5", now, now)?.status).toBe("blocked");
  });

  it("keeps unreadable applicable windows and excludes paid-enabled incomplete observations", () => {
    const usageWith = (fields: Record<string, unknown>): ClaudeCapabilitiesProbe => ({
      ...probe,
      usage: { ...probe.usage, rate_limits: { ...probe.usage.rate_limits, ...fields } },
    });
    const unreadableSonnet = { seven_day_sonnet: { utilization: null, resets_at: resetAt } };
    expect(
      claudeProbeAllowance(usageWith(unreadableSonnet), "claude-sonnet-4-6", now, now),
    ).toBeUndefined();
    expect(
      claudeProbeAllowance(
        usageWith({ ...unreadableSonnet, spend: { enabled: false } }),
        "claude-sonnet-4-6",
        now,
        now,
      ),
    ).toEqual({
      status: "unknown",
    });
    for (const paid of [{ extra_usage: { is_enabled: true } }, { spend: { enabled: true } }]) {
      const incomplete = usageWith({ ...unreadableSonnet, ...paid });
      expect(claudeProbeAllowance(incomplete, "claude-sonnet-4-6", now, now)).toBeUndefined();
      expect(claudeProbeAllowance(incomplete, "claude-opus-4-6", now, now)?.status).toBe("blocked");
    }
    const malformedScope = usageWith({
      model_scoped: [{ utilization: 1 }],
      extra_usage: { is_enabled: false },
    });
    expect(claudeProbeAllowance(malformedScope, "claude-sonnet-4-6", now, now)?.status).toBe(
      "unknown",
    );
    const unreadableReset = usageWith({ seven_day_sonnet: { utilization: 95, resets_at: {} } });
    expect(claudeProbeAllowance(unreadableReset, "claude-sonnet-4-6", now, now)).toEqual({
      status: "blocked",
      resetsAt: null,
    });
  });

  it("admits only reported native efforts within the effective model cap", () => {
    const model: ServerProviderModel = {
      slug: "claude-test-opus",
      name: "Synthetic Opus",
      isCustom: false,
      capabilities: {
        optionDescriptors: [
          {
            id: "effort",
            label: "Reasoning",
            type: "select",
            options: ["low", "medium", "high", "xhigh", "max"].map((id) => ({ id, label: id })),
          },
        ],
      },
    };
    const catalog: ClaudeModelCatalog = { models: [{ model, runtime: {}, compatibility: {} }] };
    const models = claudeAutoModels(catalog, [model], {
      ...probe,
      models: [
        {
          value: "claude-test-opus",
          displayName: model.name,
          description: "",
          supportsEffort: true,
          supportedEffortLevels: ["medium", "high", "xhigh", "max"],
        },
      ],
      effortCapSources: [{ modelMaxEffortLevels: new Map([[model.slug, "high"]]) }],
    });
    const effort = models[0]?.capabilities?.optionDescriptors?.[0];
    expect(effort?.type === "select" && effort.options.map(({ id }) => id)).toEqual([
      "medium",
      "high",
    ]);
  });

  it("binds the private Claude UUID and email to the observed organization name", () => {
    const identity = claudeAutoAccountIdentity("org-test", "person@example.invalid", "Test Org");
    expect(claudeAutoAccountIdentity(" ORG-TEST ", " Person@EXAMPLE.invalid ", " Test Org ")).toBe(
      identity,
    );
    expect(claudeAutoAccountIdentity("org-other", "person@example.invalid", "Test Org")).not.toBe(
      identity,
    );
    expect(
      claudeAutoAccountIdentity("org-test", "person@example.invalid", "Different Org"),
    ).not.toBe(identity);
    expect(claudeAutoAccountIdentity("", "person@example.invalid", "Test Org")).toBeUndefined();
    expect(claudeAutoAccountIdentity("org-test", undefined, "Test Org")).toBeUndefined();
    expect(
      claudeAutoAccountIdentity("org-test", "person@example.invalid", undefined),
    ).toBeUndefined();
  });

  it("requires affirmative paid fallback evidence for unreadable usage and actual subscription proof", () => {
    const { usage: _usage, ...withoutUsage } = probe;
    expect(claudeProbeAllowance(withoutUsage, "claude-opus-4-6", now, now)).toBeUndefined();
    expect(hasClaudeSubscription(probe)).toBe(true);
    for (const patch of [
      { email: undefined },
      { organization: undefined },
      { apiProvider: "bedrock" },
      { apiKeySource: "environment" },
      { tokenSource: undefined },
      { subscriptionType: undefined },
    ]) {
      expect(hasClaudeSubscription({ ...probe, ...patch })).toBe(false);
      expect(
        claudeProbeAllowance({ ...probe, ...patch }, "claude-opus-4-6", now, now),
      ).toBeUndefined();
    }
    expect(claudeProbeAllowance(probe, "claude-sonnet-4-6", now, now + 120_001)).toBeUndefined();
  });
});
