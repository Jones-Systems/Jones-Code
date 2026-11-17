import type { ModelTotals } from "@t3tools/shared/usageMerge";
import { describe, expect, it } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { UsageModelDialog } from "./UsageModelDialog";

const counters = {
  uncachedInputTokens: 100_000,
  cachedInputTokens: 900_000,
  cacheCreationTokens: 0,
  outputTokens: 0,
  reasoningTokens: 0,
};
const modelFixture: ModelTotals = {
  model: "claude-sonnet-4-5",
  provider: "claude",
  costUsd: 2,
  totalTokens: 1_000_000,
  tokens: counters,
  totals: counters,
  records: 1,
  providerReportedRecords: 1,
  modelPricedRecords: 0,
  unpricedRecords: 0,
  unpricedTokens: 0,
  costShare: 1,
  tokenShare: 1,
};

describe("UsageModelDialog", () => {
  it("exposes each stat as its own label and value to screen readers", async () => {
    await using _screen = await render(
      <UsageModelDialog
        model={modelFixture}
        environments={[]}
        metric="cost"
        chartWindow={{
          days: [],
          hours: [],
          resolution: "day",
          timeZone: "UTC",
          referenceTime: undefined,
          hour12: false,
        }}
        onSetPrice={() => {}}
        onClose={() => {}}
      />,
    );

    const dialog = page.getByRole("dialog");
    for (const [label, value] of [
      ["Cost", "$2.00"],
      ["Tokens", "1M"],
      ["Per 1M tokens", "$2.00"],
      ["Cache hit", "90.0%"],
      ["Cache %", "90.0%"],
      ["Cache read", "900K"],
      ["Cache write", "0 reported"],
    ] as const) {
      await expect
        .element(dialog.getByRole("term").getByText(label, { exact: true }))
        .toBeVisible();
      await expect
        .element(dialog.getByRole("definition").getByText(value, { exact: true }).first())
        .toBeVisible();
    }
  });

  it("keeps unpriced model costs unknown while exposing recorded cache counters", async () => {
    await using _screen = await render(
      <UsageModelDialog
        model={{
          ...modelFixture,
          costUsd: 0,
          totalTokens: 1_040_000,
          unpricedRecords: 1,
          unpricedTokens: 1_040_000,
          providerReportedRecords: 0,
          tokens: { ...counters, cacheCreationTokens: 40_000 },
          totals: { ...counters, cacheCreationTokens: 40_000 },
        }}
        environments={[]}
        metric="cost"
        chartWindow={{
          days: [],
          hours: [],
          resolution: "day",
          timeZone: "UTC",
          referenceTime: undefined,
          hour12: false,
        }}
        onSetPrice={() => {}}
        onClose={() => {}}
      />,
    );

    const dialog = page.getByRole("dialog");
    await expect
      .element(dialog.getByRole("definition").getByText("Unpriced", { exact: true }))
      .toBeVisible();
    await expect
      .element(dialog.getByRole("definition").getByText("40K", { exact: true }))
      .toBeVisible();
    await expect
      .element(
        dialog.getByText(
          "Recorded counters may omit token breakdowns. Zero does not establish complete coverage.",
          { exact: true },
        ),
      )
      .toBeVisible();
    await expect.element(dialog.getByText("Cost by type", { exact: true })).not.toBeInTheDocument();
    await expect
      .element(dialog.getByRole("button", { name: "Set price", exact: true }))
      .toBeVisible();
  });
});
