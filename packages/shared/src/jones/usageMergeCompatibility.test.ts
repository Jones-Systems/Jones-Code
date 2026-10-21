import {
  USAGE_CONTRACT_VERSION,
  type EnvironmentId,
  type UsageBucket,
  type UsageDay,
  type UsageProviderKind,
  UsageSummary,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { isModelCostUnknown, mergeUsage, type EnvironmentUsage } from "../usageMerge.ts";

function bucket(overrides: Partial<UsageBucket> = {}): UsageBucket {
  return {
    day: "2026-08-07" as UsageDay,
    provider: "claude",
    model: "claude-fable-5",
    totals: {
      uncachedInputTokens: 100,
      cachedInputTokens: 1000,
      cacheCreationTokens: 10,
      outputTokens: 50,
      reasoningTokens: 0,
    },
    costUsd: 10,
    cacheSavingsUsd: 2,
    costSource: "modelPriced",
    records: 5,
    unpricedRecords: 0,
    sessions: 1,
    ...overrides,
  };
}

function summary(
  buckets: readonly UsageBucket[],
  sources: readonly {
    provider: UsageProviderKind;
    hostId: string;
    homePath: string;
    volumeId?: string;
    distinctSessions?: number;
  }[],
  contractVersion: number = USAGE_CONTRACT_VERSION,
): UsageSummary {
  return {
    contractVersion,
    readAt: "2026-08-07T00:00:00.000Z",
    timeZone: "UTC",
    sinceDay: "2026-08-01" as UsageDay,
    untilDay: "2026-08-31" as UsageDay,
    buckets,
    sources: sources.map((source) => ({
      fingerprint: {
        hostId: source.hostId,
        provider: source.provider,
        resolvedHomePath: source.homePath,
        volumeId: source.volumeId ?? `vol-${source.hostId}`,
      },
      status: "ok" as const,
      scannedFiles: 1,
      skippedFiles: 0,
      malformedRecords: 0,
      distinctSessions: source.distinctSessions ?? 1,
      message: null,
    })),
    pricing: { status: "fresh", source: "litellm", fetchedAt: null, knownModels: 10 },
    scanDurationMs: 1,
  };
}

function environment(id: string, usageSummary: UsageSummary): EnvironmentUsage {
  return { environmentId: id as EnvironmentId, label: id, summary: usageSummary };
}

describe("mergeUsage", () => {
  it("reconciles provider and model detail vectors after copied and overlapping sources", () => {
    const claudeSource = {
      provider: "claude" as const,
      hostId: "mac",
      homePath: "/claude",
      distinctSessions: 2,
    };
    const accepted = [
      bucket({
        sourcePath: "/claude",
        model: "primary",
        costUsd: 6,
        costSource: "providerReported",
        records: 2,
        totals: {
          uncachedInputTokens: 100,
          cachedInputTokens: 50,
          cacheCreationTokens: 10,
          outputTokens: 30,
          reasoningTokens: 5,
        },
      }),
      bucket({
        sourcePath: "/claude",
        model: "primary",
        day: "2026-08-06" as UsageDay,
        costUsd: 2,
        records: 5,
        unpricedRecords: 2,
        totals: {
          uncachedInputTokens: 200,
          cachedInputTokens: 100,
          cacheCreationTokens: 20,
          outputTokens: 40,
          reasoningTokens: 10,
        },
      }),
    ];
    const complete = summary(accepted, [claudeSource]);
    const partial = summary(
      [
        ...accepted.map((entry) => ({
          ...entry,
          costUsd: 900,
          totals: { ...entry.totals, outputTokens: 9000 },
        })),
        bucket({
          sourcePath: "/claude",
          day: "2026-08-08" as UsageDay,
          model: "secondary",
          costUsd: 0,
          costSource: "unpriced",
          records: 3,
          unpricedRecords: 3,
          totals: {
            uncachedInputTokens: 30,
            cachedInputTokens: 0,
            cacheCreationTokens: 0,
            outputTokens: 10,
            reasoningTokens: 4,
          },
        }),
      ],
      [claudeSource],
    );
    const merged = mergeUsage(
      [
        environment("env-a", complete),
        environment("env-copy", complete),
        environment("env-partial", {
          ...partial,
          readAt: "2026-08-09T00:00:00.000Z",
          sources: partial.sources.map((source) => ({
            ...source,
            status: "partial",
            distinctSessions: 3,
          })),
        }),
        environment(
          "env-codex",
          summary(
            [
              bucket({
                provider: "codex",
                model: "codex-model",
                costUsd: 4,
                costSource: "providerReported",
                records: 1,
                totals: {
                  uncachedInputTokens: 10,
                  cachedInputTokens: 20,
                  cacheCreationTokens: 0,
                  outputTokens: 5,
                  reasoningTokens: 2,
                },
              }),
            ],
            [{ provider: "codex", hostId: "linux", homePath: "/codex" }],
          ),
        ),
      ],
      USAGE_CONTRACT_VERSION,
    );

    expect(merged.providers.find((entry) => entry.provider === "claude")).toMatchObject({
      costUsd: 8,
      totalTokens: 590,
      records: 10,
      sessions: 3,
      totals: {
        uncachedInputTokens: 330,
        cachedInputTokens: 150,
        cacheCreationTokens: 30,
        outputTokens: 80,
        reasoningTokens: 19,
      },
      providerReportedRecords: 2,
      modelPricedRecords: 3,
      unpricedRecords: 5,
    });
    expect(merged.models.find((entry) => entry.model === "primary")).toMatchObject({
      totalTokens: 550,
      totals: {
        uncachedInputTokens: 300,
        cachedInputTokens: 150,
        cacheCreationTokens: 30,
        outputTokens: 70,
        reasoningTokens: 15,
      },
      providerReportedRecords: 2,
      modelPricedRecords: 3,
      unpricedRecords: 2,
    });
    expect(merged.models.find((entry) => entry.model === "secondary")).toMatchObject({
      totalTokens: 40,
      providerReportedRecords: 0,
      modelPricedRecords: 0,
      unpricedRecords: 3,
    });
    expect(merged.totalTokens).toBe(625);
    expect(merged.records).toBe(11);
    expect(merged.sessions).toBe(4);
    expect(merged.duplicateSources).toHaveLength(2);
    for (const field of [
      "uncachedInputTokens",
      "cachedInputTokens",
      "cacheCreationTokens",
      "outputTokens",
      "reasoningTokens",
    ] as const) {
      expect(merged.providers.reduce((sum, entry) => sum + entry.totals[field], 0)).toBe(
        merged[field],
      );
      expect(merged.models.reduce((sum, entry) => sum + entry.totals[field], 0)).toBe(
        merged[field],
      );
    }
    expect(merged.costQuality.providerReportedShare).toBe(3 / 11);
    expect(merged.costQuality.modelPricedShare).toBe(3 / 11);
    expect(merged.costQuality.unpricedShare).toBe(5 / 11);
  });

  it("retains zero-priced coverage and does not count reasoning twice", () => {
    const merged = mergeUsage(
      [
        environment(
          "env-a",
          summary(
            [
              bucket({
                records: 1,
                costUsd: 0,
                totals: {
                  uncachedInputTokens: 0,
                  cachedInputTokens: 0,
                  cacheCreationTokens: 0,
                  outputTokens: 20,
                  reasoningTokens: 20,
                },
              }),
            ],
            [{ provider: "claude", hostId: "mac", homePath: "/claude" }],
          ),
        ),
      ],
      USAGE_CONTRACT_VERSION,
    );

    expect(merged.providers[0]).toMatchObject({
      totalTokens: 20,
      records: 1,
      providerReportedRecords: 0,
      modelPricedRecords: 1,
      unpricedRecords: 0,
    });
    expect(merged.models[0]).toMatchObject({
      totalTokens: 20,
      modelPricedRecords: 1,
      unpricedRecords: 0,
    });
    expect(isModelCostUnknown(merged.models[0]!)).toBe(false);
  });
});
