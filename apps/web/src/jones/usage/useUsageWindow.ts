import type { UsageSummaryInput } from "@t3tools/contracts";
import { makeWindow } from "@t3tools/shared/usageFormat";
import { useState } from "react";

import {
  readUsagePagePreferences,
  saveUsagePagePreferences,
  type UsagePagePreferences,
} from "../../components/usage/usagePagePreferences";
import { WINDOW_OPTIONS, type UsageMetric } from "../../components/usage/usageShortcuts";
import {
  makeRollingUsageWindow,
  toLocalDateTimeValue,
  validateCustomUsageWindow,
} from "./usageDateRange";

export type UsageWindowSelection =
  | {
      readonly kind: "day";
      readonly days: UsagePagePreferences["windowDays"];
      readonly window: UsageSummaryInput;
    }
  | { readonly kind: "hours"; readonly hours: number; readonly window: UsageSummaryInput }
  | { readonly kind: "custom"; readonly window: UsageSummaryInput };

export const QUICK_USAGE_HOUR_OPTIONS = [1, 3, 6, 12] as const;

function isUsageWindowDays(value: number): value is UsagePagePreferences["windowDays"] {
  return WINDOW_OPTIONS.some((option) => option.days === value);
}

export function useUsageWindow() {
  const [preferences, setPreferences] = useState(readUsagePagePreferences);
  const [windowSelection, setWindowSelection] = useState<UsageWindowSelection>(() => ({
    kind: "day",
    days: preferences.windowDays,
    window: makeWindow(
      preferences.windowDays,
      undefined,
      preferences.windowDays === 1 ? "hour" : "day",
    ),
  }));
  const metric = preferences.metric;
  const windowDays = windowSelection.kind === "day" ? windowSelection.days : preferences.windowDays;
  const { window } = windowSelection;
  const [customSinceValue, setCustomSinceValue] = useState("");
  const [customUntilValue, setCustomUntilValue] = useState("");
  const [customOriginalWindow, setCustomOriginalWindow] = useState<UsageSummaryInput>();
  const customWindowValidation = validateCustomUsageWindow(
    customSinceValue,
    customUntilValue,
    undefined,
    customOriginalWindow,
  );
  const selectWindow = (days: number) => {
    if (!isUsageWindowDays(days)) return;
    const nextPreferences = { metric, windowDays: days };
    setPreferences(nextPreferences);
    saveUsagePagePreferences(nextPreferences);
    setCustomSinceValue("");
    setCustomUntilValue("");
    setCustomOriginalWindow(undefined);
    setWindowSelection({
      kind: "day",
      days,
      window: makeWindow(days, undefined, days === 1 ? "hour" : "day"),
    });
  };
  const selectHourWindow = (hours: (typeof QUICK_USAGE_HOUR_OPTIONS)[number]) => {
    const nextWindow = makeRollingUsageWindow(hours);
    setWindowSelection({ kind: "hours", hours, window: nextWindow });
    if (nextWindow.sinceTime !== undefined && nextWindow.untilTime !== undefined) {
      setCustomOriginalWindow(nextWindow);
      setCustomSinceValue(toLocalDateTimeValue(new Date(nextWindow.sinceTime)));
      setCustomUntilValue(toLocalDateTimeValue(new Date(nextWindow.untilTime)));
    }
  };
  const applyCustomWindow = () => {
    const validation = validateCustomUsageWindow(
      customSinceValue,
      customUntilValue,
      undefined,
      customOriginalWindow,
    );
    if (!validation.ok) return;
    setCustomOriginalWindow(validation.window);
    setWindowSelection({ kind: "custom", window: validation.window });
  };
  const clearCustomWindow = () => {
    setCustomSinceValue("");
    setCustomUntilValue("");
    selectWindow(preferences.windowDays);
  };
  const selectMetric = (nextMetric: UsageMetric) => {
    const nextPreferences = { metric: nextMetric, windowDays };
    setPreferences(nextPreferences);
    saveUsagePagePreferences(nextPreferences);
  };
  const refreshUsageWindow = () => {
    const nextWindow =
      windowSelection.kind === "day"
        ? makeWindow(windowDays, undefined, windowDays === 1 ? "hour" : "day")
        : windowSelection.kind === "hours"
          ? makeRollingUsageWindow(windowSelection.hours)
          : windowSelection.window;
    const windowChanged =
      nextWindow.sinceDay !== window.sinceDay ||
      nextWindow.untilDay !== window.untilDay ||
      nextWindow.sinceTime !== window.sinceTime ||
      nextWindow.untilTime !== window.untilTime;
    if (windowChanged && windowSelection.kind !== "custom") {
      setWindowSelection({ ...windowSelection, window: nextWindow });
    }
    return nextWindow;
  };

  return {
    metric,
    windowDays,
    window,
    windowSelection,
    customSinceValue,
    customUntilValue,
    customWindowValidation,
    setCustomSinceValue,
    setCustomUntilValue,
    selectWindow,
    selectHourWindow,
    applyCustomWindow,
    clearCustomWindow,
    selectMetric,
    refreshUsageWindow,
  };
}
