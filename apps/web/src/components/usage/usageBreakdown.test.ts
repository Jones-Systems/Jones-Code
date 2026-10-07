import { USAGE_CONTRACT_VERSION } from "@t3tools/contracts";
import { mergeUsage, type MergedUsage, type ModelTotals } from "@t3tools/shared/usageMerge";
import { describe, expect, it } from "vite-plus/test";

import {
  cacheHitRate,
  costPerMillionTokens,
  modelShare,
  sortModelsByTokens,
  selectUsageBreakdown,
} from "./usageBreakdown";

const model = (
  name: string,
  totalTokens: number,
  costUsd: number,
  overrides: Partial<ModelTotals> = {},
): ModelTotals => ({
  model: name,
  provider: "codex",
  costUsd,
  totalTokens,
  tokens: {
    uncachedInputTokens: totalTokens,
    cachedInputTokens: 0,
    cacheCreationTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
  },
  records: 1,
  totals: {
    uncachedInputTokens: totalTokens,
    cachedInputTokens: 0,
    cacheCreationTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
  },
  providerReportedRecords: 0,
  modelPricedRecords: 1,
  unpricedRecords: 0,
  unpricedTokens: 0,
  costShare: 0,
  tokenShare: 0,
  ...overrides,
});

describe("sortModelsByTokens", () => {
  it("sorts by tokens, breaks ties by cost, and leaves the input alone", () => {
    const models = [
      model("lower-cost", 100, 1),
      model("more-tokens", 200, 2),
      model("higher-cost", 100, 3),
    ];

    expect(sortModelsByTokens(models).map((item) => item.model)).toEqual([
      "more-tokens",
      "higher-cost",
      "lower-cost",
    ]);
    expect(models.map((item) => item.model)).toEqual(["lower-cost", "more-tokens", "higher-cost"]);
  });
});

describe("modelShare", () => {
  it("follows the selected metric", () => {
    const priced = model("priced", 100, 9, { costShare: 0.9, tokenShare: 0.25 });

    expect(modelShare(priced, "cost")).toBe(0.9);
    expect(modelShare(priced, "tokens")).toBe(0.25);
  });

  it("has no cost share for an unknown cost but keeps its token share", () => {
    const unpriced = model("unpriced", 300, 0, {
      unpricedRecords: 1,
      unpricedTokens: 300,
      tokenShare: 0.75,
    });

    expect(modelShare(unpriced, "cost")).toBeNull();
    expect(modelShare(unpriced, "tokens")).toBe(0.75);
  });
});

describe("model rates", () => {
  it("counts cache writes as misses and leaves unpriced tokens out of $/1M", () => {
    const mixed = model("mixed", 4_000_000, 6, {
      tokens: {
        uncachedInputTokens: 1_000_000,
        cachedInputTokens: 1_000_000,
        cacheCreationTokens: 2_000_000,
        outputTokens: 0,
        reasoningTokens: 0,
      },
      records: 4,
      unpricedRecords: 1,
      unpricedTokens: 1_000_000,
    });

    expect(cacheHitRate(mixed)).toBe(0.25);
    expect(costPerMillionTokens(mixed)).toBe(2);
    expect(costPerMillionTokens({ ...mixed, unpricedRecords: 4 })).toBeNull();
  });
});

describe("selectUsageBreakdown", () => {
  const models = [
    model("codex-a", 100, 2),
    model("codex-b", 200, 3),
    { ...model("claude-a", 300, 5), provider: "claude" as const },
  ];
  const merged: MergedUsage = {
    ...mergeUsage([], USAGE_CONTRACT_VERSION),
    costUsd: 10,
    totalTokens: 600,
    models,
    providers: [
      {
        ...models[0]!,
        provider: "codex",
        totalTokens: 300,
        costUsd: 5,
        records: 2,
        sessions: 1,
        tokenShare: 0.5,
      },
      { ...models[2]!, provider: "claude", sessions: 1, tokenShare: 0.5 },
    ],
    daily: [
      {
        day: "2026-08-11",
        costUsd: 10,
        totalTokens: 600,
        byProvider: new Map([
          ["codex", { costUsd: 5, totalTokens: 300 }],
          ["claude", { costUsd: 5, totalTokens: 300 }],
        ]),
      },
      {
        day: "2026-08-10",
        costUsd: 4,
        totalTokens: 40,
        byProvider: new Map([["claude", { costUsd: 4, totalTokens: 40 }]]),
      },
    ],
    hourly: [
      {
        day: "2026-08-11",
        hourStart: "2026-08-11T10:00:00.000Z",
        costUsd: 10,
        totalTokens: 600,
        byProvider: new Map([
          ["codex", { costUsd: 5, totalTokens: 300 }],
          ["claude", { costUsd: 5, totalTokens: 300 }],
        ]),
      },
    ],
  };

  it("scopes models and time cells while preserving the accepted aggregate", () => {
    const detail = selectUsageBreakdown(merged, "codex");
    expect(detail.providerTotals).toBe(merged.providers[0]);
    expect(detail.models.map((entry) => [entry.model, entry.costShare])).toEqual([
      ["codex-a", 0.4],
      ["codex-b", 0.6],
    ]);
    expect(detail.models.map((entry) => entry.tokenShare)).toEqual([1 / 3, 2 / 3]);
    expect(detail.daily).toHaveLength(1);
    expect(detail.daily[0]).toMatchObject({ day: "2026-08-11", costUsd: 5, totalTokens: 300 });
    expect([...detail.daily[0]!.byProvider]).toEqual([["codex", { costUsd: 5, totalTokens: 300 }]]);
    expect(detail.hourly[0]).toMatchObject({
      hourStart: "2026-08-11T10:00:00.000Z",
      costUsd: 5,
      totalTokens: 300,
    });
    expect(merged.models[0]?.costShare).toBe(0);
    expect(merged.daily[0]?.costUsd).toBe(10);
    expect(merged.daily[0]?.byProvider.size).toBe(2);
  });

  it("restores original model and time arrays for All providers or an absent provider", () => {
    for (const provider of [null, "cursor"] as const) {
      const detail = selectUsageBreakdown(merged, provider);
      expect(detail.providerTotals).toBeNull();
      expect(detail.models).toBe(merged.models);
      expect(detail.daily).toBe(merged.daily);
      expect(detail.hourly).toBe(merged.hourly);
    }
  });

  it("keeps zero-dollar model shares finite", () => {
    const zeroCost = {
      ...merged,
      providers: merged.providers.map((entry) => ({ ...entry, costUsd: 0 })),
      models: merged.models.map((entry) => ({ ...entry, costUsd: 0 })),
    };
    expect(selectUsageBreakdown(zeroCost, "codex").models.map((entry) => entry.costShare)).toEqual([
      0, 0,
    ]);
  });
});
