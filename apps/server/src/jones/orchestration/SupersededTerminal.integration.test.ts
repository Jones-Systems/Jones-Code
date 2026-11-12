import { it } from "@effect/vitest";
import { historyScenario } from "./HistoricalTerminalHistory.testkit.ts";

it.effect(
  "retains the exact interrupted snapshot after a committed restart and exhausted pending-input admission",
  () => historyScenario({ mode: "superseded-same-session", normalizationFailure: true }),
);

it.effect(
  "consumes snapshot then logical terminal after restart without settling the successor or repeating control",
  () =>
    historyScenario({
      mode: "superseded-same-session",
      normalizationFailure: true,
      consumer: true,
    }),
);

it.effect.each([
  "advanced-successor",
  "wrong-generation",
  "unrelated-successor",
  "wrong-native-turn",
] as const)("preserves the original normalization error when historical proof refuses %s", (mode) =>
  historyScenario({ mode, normalizationFailure: true }),
);

it.effect(
  "refuses terminal history without runtime evidence after ordinary normalization fails",
  () =>
    historyScenario({
      mode: "ordinary-current-guard-race",
      normalizationFailure: true,
      missingEvidence: true,
    }),
);

it.effect(
  "does not reconstruct a historical snapshot from a logical terminal after ordinary normalization fails",
  () =>
    historyScenario({
      mode: "ordinary-current-guard-race",
      normalizationFailure: true,
      logicalOnly: true,
    }),
);

it.effect.each(["found", "missing", "unavailable", "mismatched"] as const)(
  "reconciles an uncertain historical commit with exact retained readback: %s",
  (readback) =>
    historyScenario({ mode: "ordinary-same-session-race", uncertainPublication: true, readback }),
);
