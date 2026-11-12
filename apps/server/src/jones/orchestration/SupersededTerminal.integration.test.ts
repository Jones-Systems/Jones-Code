import { it } from "@effect/vitest";
import { historyScenario } from "./HistoricalTerminalHistory.testkit.ts";

it.effect.each([false, true])(
  "retains the old terminal when restart selects another account, reused account: %s",
  (reusedAccount) =>
    historyScenario({ mode: "superseded-rebound-session", accountSwitch: true, reusedAccount }),
);

it.effect.each([false, true])(
  "retains an account-switch terminal for a failed successor, reused account: %s",
  (reusedAccount) =>
    historyScenario({ mode: "failed-replacement", accountSwitch: true, reusedAccount }),
);

it.effect.each([
  "session",
  "instance",
  "conversation",
  "generation",
  "revision",
  "old-session",
  "old-instance",
] as const)("refuses account-switch history after %s drift", (replacementChange) =>
  historyScenario({
    mode: "changed-replacement",
    accountSwitch: true,
    reusedAccount: true,
    replacementChange,
  }),
);

it.effect.each(["changed-replacement", "wrong-generation", "advanced-successor"] as const)(
  "refuses account-switch terminal history when its captured proof changes: %s",
  (mode) => historyScenario({ mode, accountSwitch: true }),
);

it.effect("preserves the raw rejection reason after its logical terminal companion", () =>
  historyScenario({ mode: "changed-replacement", logicalCompanion: true }),
);

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
