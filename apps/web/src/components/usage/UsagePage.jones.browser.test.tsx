import {
  EnvironmentId,
  UsageDay,
  USAGE_CONTRACT_VERSION,
  type UsageBucket,
  type UsageSummary,
  type UsageSummaryInput,
} from "@t3tools/contracts";
import { mergeUsage } from "@t3tools/shared/usageMerge";
import { act, render } from "../../../test/render";
import { cdp } from "vite-plus/test/browser";
import type { ReactElement, ReactNode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

import { saveUsagePagePreferences } from "./usagePagePreferences";
import type { UsageView } from "../../state/usage";

const testState = vi.hoisted(() => ({
  useUsage: vi.fn(),
  navigate: vi.fn(),
  canGoBack: true,
}));

vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("@tanstack/react-router", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-router")>()),
  useNavigate: () => testState.navigate,
  useCanGoBack: () => testState.canGoBack,
}));
vi.mock("../../state/usage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/usage")>()),
  useUsage: testState.useUsage,
}));
vi.mock("../ui/scroll-area", () => ({ ScrollArea: "div" }));
vi.mock("../ui/select", () => ({
  Select: "div",
  SelectItem: "div",
  SelectPopup: "div",
  SelectTrigger: "div",
  SelectValue: "div",
}));
vi.mock("../ui/sidebar", () => ({ SidebarInset: "div" }));
vi.mock("../ui/toggle-group", async () => {
  const React = await import("react");
  return {
    Toggle: "button",
    ToggleGroup: ({
      children,
      onValueChange,
      ...props
    }: {
      readonly children?: ReactNode;
      readonly onValueChange?: (value: readonly string[]) => void;
      readonly [key: string]: unknown;
    }) =>
      React.createElement(
        "div",
        props,
        React.Children.map(children, (child) => {
          if (!React.isValidElement<{ value: string }>(child)) return child;
          const toggle = child as ReactElement<{ value: string; onClick?: () => void }>;
          return React.cloneElement(toggle, {
            onClick: () => onValueChange?.([toggle.props.value]),
          });
        }),
      ),
  };
});
vi.mock("../WorkspaceBreadcrumb", () => ({
  WorkspaceBreadcrumb: "div",
  WorkspaceBreadcrumbItem: "div",
  WorkspaceBreadcrumbSeparator: "span",
  WorkspaceBreadcrumbText: "span",
}));
vi.mock("../WorkspacePageContainer", () => ({ WorkspacePageContainer: "main" }));
vi.mock("../WorkspacePageHeader", () => ({ WorkspacePageHeader: "header" }));
vi.mock("./UsageProviderChart", async () => {
  const React = await import("react");
  return {
    UsageProviderChart: ({
      daily,
    }: {
      readonly daily: readonly { costUsd: number; totalTokens: number }[];
    }) =>
      React.createElement("div", {
        "data-testid": "usage-chart",
        "data-cost": daily.reduce((sum, day) => sum + day.costUsd, 0),
        "data-tokens": daily.reduce((sum, day) => sum + day.totalTokens, 0),
      }),
  };
});
vi.mock("./UsagePriceOverrides", () => ({ UsagePriceOverrides: () => null }));
vi.mock("../../jones/usage/SavedTokenAccounting", () => ({ SavedTokenAccounting: () => null }));
vi.mock("./usageProviders", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./usageProviders")>();
  return {
    ...actual,
    PROVIDER_ORDER: ["codex", "claude"],
    PROVIDER_PRESENTATION: {
      codex: { ...actual.PROVIDER_PRESENTATION.codex, color: "white" },
      claude: { ...actual.PROVIDER_PRESENTATION.claude, color: "orange" },
    },
  };
});

import { UsagePage } from "./UsagePage";
const environments = [
  {
    environmentId: EnvironmentId.make("test-environment"),
    label: "Test environment",
    isPending: false,
    canReadDiagnostics: true,
    isConnected: true,
    needsCursorKeychainAccess: false,
    error: null,
    summary: {
      contractVersion: USAGE_CONTRACT_VERSION,
      readAt: "2026-08-11T12:37:00.000Z",
      sinceDay: UsageDay.make("2026-08-10"),
      untilDay: UsageDay.make("2026-08-11"),
      timeZone: "UTC",
      buckets: [],
      sources: [],
      pricing: { status: "fresh", source: "test", fetchedAt: null, knownModels: 1 },
      scanDurationMs: 1,
    } satisfies UsageSummary,
  },
];

function mockAnsweredUsage(view: Omit<UsageView, "shown">) {
  testState.useUsage.mockImplementation((window: UsageSummaryInput) => ({
    ...view,
    shown: { window, merged: view.merged },
  }));
}

function setupUsage(
  view: Omit<UsageView, "shown"> = {
    merged: mergeUsage([], USAGE_CONTRACT_VERSION),
    environments,
    selectedEnvironments: environments,
    isPending: false,
    isPartial: false,
    refresh: vi.fn(),
  },
): Disposable {
  const key = "t3code:usage-page-preferences:v1";
  const previous = localStorage.getItem(key);
  saveUsagePagePreferences({ metric: "tokens", windowDays: 30 });
  testState.navigate.mockClear();
  testState.canGoBack = true;
  mockAnsweredUsage(view);
  return {
    [Symbol.dispose]() {
      if (previous === null) localStorage.removeItem(key);
      else localStorage.setItem(key, previous);
      testState.useUsage.mockReset();
      testState.navigate.mockClear();
    },
  };
}

async function browserTimeZone(timezoneId: "UTC" | "America/New_York"): Promise<AsyncDisposable> {
  const previous = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const session = cdp();
  await session.send("Emulation.setTimezoneOverride", { timezoneId });
  return {
    async [Symbol.asyncDispose]() {
      await session.send("Emulation.setTimezoneOverride", { timezoneId: previous });
    },
  };
}

function fixedTime(now: Date): Disposable {
  vi.setSystemTime(now);
  return { [Symbol.dispose]: () => void vi.useRealTimers() };
}

describe("UsagePage Jones exact ranges", () => {
  it("selects an exact three-hour range from the overflow panel", async () => {
    using _state = setupUsage();
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    const trigger = container.querySelector<HTMLButtonElement>(
      '[aria-label="Additional usage ranges"]',
    );
    expect(trigger).not.toBeNull();

    await act(() => trigger?.click());
    const threeHours = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "3h",
    );
    expect(threeHours).toBeDefined();
    await act(() => threeHours?.click());

    const input = testState.useUsage.mock.calls.at(-1)?.[0];
    expect(input).toMatchObject({ resolution: "hour", timeZone: expect.any(String) });
    expect(Date.parse(input.untilTime) - Date.parse(input.sinceTime)).toBe(3 * 60 * 60 * 1000);
  });

  it("applies and clears an exact multi-day custom range", async () => {
    using _state = setupUsage();
    await using _timezone = await browserTimeZone("UTC");
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    const trigger = container.querySelector<HTMLButtonElement>(
      '[aria-label="Additional usage ranges"]',
    );
    expect(trigger).not.toBeNull();
    await act(() => trigger?.click());

    const setInputValue = (label: string, value: string) => {
      const input = document.querySelector<HTMLInputElement>(`[aria-label="${label}"]`);
      expect(input).not.toBeNull();
      const valueSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
      if (!input || !valueSetter) throw new Error("The custom range input is unavailable.");
      valueSetter.call(input, value);
      input.dispatchEvent(new Event("input", { bubbles: true }));
    };
    await act(() => setInputValue("Custom range start", "2026-09-15T10:30"));
    await act(() => setInputValue("Custom range end", "2026-09-18T12:37"));
    const apply = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Apply range",
    );
    expect(apply?.disabled).toBe(false);
    await act(() => apply?.click());

    const customInput = testState.useUsage.mock.calls.at(-1)?.[0];
    expect(customInput).toMatchObject({
      sinceDay: "2026-09-15",
      untilDay: "2026-09-18",
      timeZone: "UTC",
      resolution: "exactDay",
      sinceTime: "2026-09-15T10:30:00.000Z",
      untilTime: "2026-09-18T12:37:00.000Z",
    });

    await act(() => trigger?.click());
    const clear = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Clear custom selection",
    );
    expect(clear?.disabled).toBe(false);
    await act(() => clear?.click());
    expect(testState.useUsage.mock.calls.at(-1)?.[0]).not.toHaveProperty("sinceTime");
  });

  it("preserves repeated-hour bounds when applying an unchanged rolling prefill", async () => {
    using _state = setupUsage();
    await using _timezone = await browserTimeZone("America/New_York");
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    using _clock = fixedTime(new Date("2026-11-01T06:30:00.000Z"));
    const trigger = container.querySelector<HTMLButtonElement>(
      '[aria-label="Additional usage ranges"]',
    )!;
    await act(() => trigger.click());
    const oneHour = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "1h",
    )!;
    await act(() => oneHour.click());
    const rollingInput = testState.useUsage.mock.calls.at(-1)?.[0];
    expect(rollingInput).toMatchObject({
      sinceTime: "2026-11-01T05:30:00.000Z",
      untilTime: "2026-11-01T06:30:00.000Z",
    });

    await act(() => trigger.click());
    for (const label of ["Custom range start", "Custom range end"]) {
      expect(document.querySelector<HTMLInputElement>(`[aria-label="${label}"]`)?.value).toBe(
        "2026-11-01T01:30",
      );
    }
    const apply = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Apply range",
    )!;
    expect(apply.disabled).toBe(false);
    await act(() => apply.click());
    expect(testState.useUsage.mock.calls.at(-1)?.[0]).toEqual(rollingInput);
  });

  it("explains an edited repeated local time instead of applying an arbitrary offset", async () => {
    using _state = setupUsage();
    await using _timezone = await browserTimeZone("America/New_York");
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    using _clock = fixedTime(new Date("2026-11-01T08:00:00.000Z"));
    const trigger = container.querySelector<HTMLButtonElement>(
      '[aria-label="Additional usage ranges"]',
    )!;
    await act(() => trigger.click());
    const inputs = [
      ["Custom range start", "2026-11-01T01:30"],
      ["Custom range end", "2026-11-01T02:30"],
    ];
    const valueSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!;
    for (const [label, value] of inputs) {
      const input = document.querySelector<HTMLInputElement>(`[aria-label="${label}"]`)!;
      await act(() => {
        valueSetter.call(input, value);
        input.dispatchEvent(new Event("input", { bubbles: true }));
      });
    }
    const apply = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Apply range",
    )!;
    expect(apply.disabled).toBe(true);
    expect(document.querySelector('[role="alert"]')?.textContent).toBe(
      "This local time occurs twice when clocks move back. Choose a time outside the repeated hour or keep the original range time unchanged.",
    );
    expect(testState.useUsage.mock.calls.at(-1)?.[0]).not.toHaveProperty("sinceTime");
  });
});

function usageFixture(includeCodex = true, environmentId = "test-environment") {
  const buckets: UsageBucket[] = [
    ...(includeCodex
      ? [
          {
            provider: "codex" as const,
            model: "codex-known",
            day: UsageDay.make("2026-08-11"),
            hourStart: "2026-08-11T10:00:00.000Z",
            totals: {
              uncachedInputTokens: 100,
              cachedInputTokens: 200,
              cacheCreationTokens: 30,
              outputTokens: 40,
              reasoningTokens: 10,
            },
            records: 2,
            sessions: 1,
            costUsd: 5,
            cacheSavingsUsd: 1,
            costSource: "modelPriced" as const,
            unpricedRecords: 0,
          },
          {
            provider: "codex" as const,
            model: "codex-unpriced",
            day: UsageDay.make("2026-08-11"),
            hourStart: "2026-08-11T10:00:00.000Z",
            totals: {
              uncachedInputTokens: 10,
              cachedInputTokens: 0,
              cacheCreationTokens: 0,
              outputTokens: 20,
              reasoningTokens: 5,
            },
            records: 1,
            sessions: 1,
            costUsd: 0,
            cacheSavingsUsd: 0,
            costSource: "unpriced" as const,
            unpricedRecords: 1,
          },
        ]
      : []),
    {
      provider: "claude",
      model: "claude-known",
      day: UsageDay.make("2026-08-10"),
      hourStart: "2026-08-10T09:00:00.000Z",
      totals: {
        uncachedInputTokens: 300,
        cachedInputTokens: 400,
        cacheCreationTokens: 50,
        outputTokens: 60,
        reasoningTokens: 0,
      },
      records: 3,
      sessions: 2,
      costUsd: 10,
      cacheSavingsUsd: 2,
      costSource: "providerReported",
      unpricedRecords: 0,
    },
  ];
  const summary: UsageSummary = {
    ...environments[0]!.summary,
    buckets,
    sources: [...new Set(buckets.map((bucket) => bucket.provider))].map((provider) => ({
      fingerprint: {
        provider,
        hostId: environmentId,
        resolvedHomePath: `/${provider}`,
        volumeId: environmentId,
      },
      status: "ok",
      scannedFiles: 1,
      skippedFiles: 0,
      malformedRecords: 0,
      distinctSessions: provider === "codex" ? 1 : 2,
      message: null,
    })),
  };
  const environment = {
    ...environments[0]!,
    environmentId: EnvironmentId.make(environmentId),
    summary,
  };
  return {
    merged: mergeUsage([environment], USAGE_CONTRACT_VERSION),
    environments: [environment],
    selectedEnvironments: [environment],
    isPending: false,
    isPartial: false,
    refresh: vi.fn(async () => undefined),
  };
}

describe("UsagePage provider and model details", () => {
  async function clickLabel(container: HTMLElement, label: string) {
    const button = container.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
    expect(button).not.toBeNull();
    await act(() => button!.click());
  }

  async function clickText(container: HTMLElement, text: string, within: ParentNode = container) {
    const button = [...within.querySelectorAll("button")].find(
      (item) => item.textContent?.trim() === text,
    );
    expect(button).toBeDefined();
    await act(() => button!.click());
  }

  function breakdownSection(container: HTMLElement) {
    const section = container.querySelector('[aria-label="Usage breakdown"]')?.closest("section");
    expect(section).toBeDefined();
    expect(section).not.toBeNull();
    return section!;
  }

  function details(container: HTMLElement) {
    const section = container.querySelector<HTMLElement>("#usage-provider-details");
    expect(section).not.toBeNull();
    return section!;
  }

  function metric(section: HTMLElement, label: string) {
    return [...section.querySelectorAll("dt")].find((element) => element.textContent === label)
      ?.nextElementSibling?.textContent;
  }

  function modelNames(container: HTMLElement) {
    return [...container.querySelectorAll('tbody button[aria-label$="token details"]')].map(
      (button) => button.previousElementSibling?.textContent?.trim(),
    );
  }

  it("shows a compact all-token cache percentage and reported cache writes", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    const totals = [...container.querySelectorAll("section")].find(
      (section) => section.querySelector("h2")?.textContent === "Totals",
    )!;
    expect(metric(totals, "Cache %")).toBe("49.6%");
    expect(metric(totals, "Cache write")).toBe("80");
    expect(totals.textContent).not.toContain("600 / ");

    await clickLabel(container, "Codex usage details");
    expect(metric(details(container), "Cache %")).toBe("50.0%");
    expect(metric(details(container), "Cache write")).toBe("30");
    await clickLabel(container, "codex-unpriced token details");
    const modelDetails = container.querySelector<HTMLElement>(
      '[role="region"][aria-label="codex-unpriced token details"]',
    )!;
    expect(metric(modelDetails, "Cache write")).toBe("0 reported");
  });

  it("keeps cost and token shares separate across metric changes and provider focus", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    const percentages = () =>
      [...container.querySelectorAll("thead th[aria-label]")].map((cell) =>
        cell.getAttribute("aria-label"),
      );
    expect(percentages()).toEqual(["Cost percentage", "Token percentage"]);
    const claude = container.querySelector("tbody tr")!;
    expect(claude.querySelector("td:nth-child(4)")?.textContent).toBe("66.7%");
    expect(claude.querySelector("td:nth-child(6)")?.textContent).toBe("66.9%");
    await clickText(container, "Cost");
    expect(percentages()).toEqual(["Cost percentage", "Token percentage"]);
    await clickLabel(container, "Codex usage details");
    const known = container.querySelector("tbody tr")!;
    expect(known.querySelector("td:nth-child(4)")?.textContent).toBe("100.0%");
    expect(known.querySelector("td:nth-child(6)")?.textContent).toBe("92.5%");
    const unknown = container.querySelectorAll("tbody tr")[1]!;
    expect(unknown.querySelector("td:nth-child(4)")?.textContent).toBe("—");
    expect(unknown.querySelector("td:nth-child(6)")?.textContent).toBe("7.5%");
  });

  it("uses an empty cache percentage when no tokens were reported", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    mockAnsweredUsage({ ...usageFixture(), merged: mergeUsage([], USAGE_CONTRACT_VERSION) });
    await screen.rerender(<UsagePage />);
    const totals = [...container.querySelectorAll("section")].find(
      (section) => section.querySelector("h2")?.textContent === "Totals",
    )!;
    expect(metric(totals, "Cache %")).toBe("—");
    expect(metric(totals, "Cache write")).toBe("0 reported");
  });

  it("opens one provider, scopes its details and shares, and keeps summary/chart aggregate", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    const summary = container.querySelector(".text-4xl")?.textContent;
    const chart = container.querySelector('[data-testid="usage-chart"]')!.outerHTML;
    expect(modelNames(container)).toEqual(["claude-known", "codex-known", "codex-unpriced"]);
    await clickLabel(container, "Codex usage details");
    expect(
      container.querySelector('[aria-label="Codex usage details"]')?.getAttribute("aria-expanded"),
    ).toBe("true");
    expect(modelNames(container)).toEqual(["codex-known", "codex-unpriced"]);
    expect(metric(details(container), "Uncached input")).toBe("110");
    expect(metric(details(container), "Cached input")).toBe("200");
    expect(metric(details(container), "Cache write")).toBe("30");
    expect(metric(details(container), "Output")).toBe("60");
    expect(metric(details(container), "Reasoning · included in output")).toBe("15");
    expect(metric(details(container), "Processed tokens")).toBe("400");
    expect(metric(details(container), "Recorded responses")).toBe("3");
    expect(metric(details(container), "Model priced")).toBe("2");
    expect(metric(details(container), "Unpriced")).toBe("1");
    expect(container.querySelector("tbody tr")?.textContent).toContain("92.5%");
    await clickText(container, "Cost");
    expect(container.querySelector("tbody tr")?.textContent).toContain("100.0%");
    await clickText(container, "Tokens");
    expect(container.textContent).toContain("Shares are within Codex's API estimate");
    expect(container.querySelector(".text-4xl")?.textContent).toBe(summary);
    expect(container.querySelector('[data-testid="usage-chart"]')!.outerHTML).toBe(chart);

    await clickLabel(container, "Claude Code usage details");
    expect(
      container.querySelector('[aria-label="Codex usage details"]')?.getAttribute("aria-expanded"),
    ).toBe("false");
    expect(details(container).textContent).toContain("Claude Code details");
    expect(metric(details(container), "Provider reported")).toBe("3");
    expect(modelNames(container)).toEqual(["claude-known"]);
    expect(container.querySelectorAll("#usage-provider-details")).toHaveLength(1);

    await clickText(container, "All providers", breakdownSection(container));
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    expect(modelNames(container)).toHaveLength(3);
  });

  it("closes from the disclosure and close button, restoring aggregate rows and focus", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    await clickLabel(container, "Codex usage details");
    await clickLabel(container, "Codex usage details");
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    await clickLabel(container, "Codex usage details");
    await clickLabel(container, "Close Codex details");
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    expect(modelNames(container)).toHaveLength(3);
    expect(document.activeElement).toBe(
      container.querySelector('[aria-label="Codex usage details"]'),
    );
  });

  it("filters daily and hourly totals and restores the complete time breakdown", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    await clickLabel(container, "Codex usage details");
    await clickText(container, "Day");
    expect(container.querySelectorAll("tbody tr")).toHaveLength(1);
    expect(container.querySelector("tbody")?.textContent).toContain("400");
    expect([...container.querySelectorAll("thead th")].map((cell) => cell.textContent)).toEqual([
      "Day",
      "Codex",
      "Total",
      "Tokens",
    ]);
    await clickText(container, "All providers", breakdownSection(container));
    expect(container.querySelectorAll("tbody tr")).toHaveLength(2);

    const range = container.querySelector<HTMLButtonElement>(
      '[aria-label="Additional usage ranges"]',
    )!;
    await act(() => range.click());
    const oneHour = [...document.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "1h",
    )!;
    await act(() => oneHour.click());
    await clickLabel(container, "Codex usage details");
    expect([...container.querySelectorAll("thead th")].map((cell) => cell.textContent)).toEqual([
      "Hour",
      "Codex",
      "Total",
      "Tokens",
    ]);
    expect(container.querySelectorAll("tbody tr")).toHaveLength(1);
    expect(container.querySelector("tbody")?.textContent).toContain("400");
  });

  it("expands model counters and retains unpriced versus known zero-dollar costs", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    await clickLabel(container, "Codex usage details");
    await clickLabel(container, "codex-unpriced token details");
    const modelDetails = container.querySelector<HTMLElement>(
      '[role="region"][aria-label="codex-unpriced token details"]',
    )!;
    expect(metric(modelDetails, "Recorded responses")).toBe("1");
    expect(metric(modelDetails, "API estimate")).toBe("Unpriced");
    expect(metric(modelDetails, "Output")).toBe("20");
    expect(metric(modelDetails, "Reasoning · included in output")).toBe("5");
    await clickLabel(container, "codex-known token details");
    expect(
      container.querySelector('[role="region"][aria-label="codex-unpriced token details"]'),
    ).toBeNull();

    const fixture = usageFixture();
    mockAnsweredUsage({
      ...fixture,
      merged: {
        ...fixture.merged,
        models: fixture.merged.models.map((model) =>
          model.model === "codex-known" ? { ...model, costUsd: 0 } : model,
        ),
      },
    });
    await screen.rerender(<UsagePage />);
    const known = container.querySelector<HTMLElement>(
      '[role="region"][aria-label="codex-known token details"]',
    )!;
    expect(metric(known, "API estimate")).toBe("$0.00");
  });

  it("recomputes from new environment data and clears a provider that disappears", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    await clickLabel(container, "Codex usage details");
    const next = usageFixture(true, "other-environment");
    mockAnsweredUsage({
      ...next,
      merged: {
        ...next.merged,
        providers: next.merged.providers.map((provider) =>
          provider.provider === "codex" ? { ...provider, records: 7 } : provider,
        ),
      },
    });
    await screen.rerender(<UsagePage />);
    expect(metric(details(container), "Recorded responses")).toBe("7");

    mockAnsweredUsage(usageFixture(false, "claude-environment"));
    await screen.rerender(<UsagePage />);
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    expect(modelNames(container)).toEqual(["claude-known"]);
    mockAnsweredUsage(usageFixture());
    await screen.rerender(<UsagePage />);
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    expect(modelNames(container)).toHaveLength(3);
  });

  it("resets provider and model focus when the environment selection changes", async () => {
    using _state = setupUsage(usageFixture());
    await using screen = await render(<UsagePage />, { router: false });
    const { container } = screen;
    await clickLabel(container, "Codex usage details");
    await clickLabel(container, "codex-known token details");
    const trigger = container.querySelector<HTMLButtonElement>('[data-slot="menu-trigger"]')!;
    await act(() => trigger.click());
    const allEnvironments = [
      ...document.querySelectorAll<HTMLElement>('[role="menuitemcheckbox"]'),
    ].find((item) => item.textContent?.trim() === "All environments")!;
    expect(allEnvironments).toBeDefined();
    await act(() => allEnvironments.click());
    expect(testState.useUsage.mock.calls.at(-1)?.[1]).toEqual(new Set());
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    expect(
      container.querySelector('[role="region"][aria-label="codex-known token details"]'),
    ).toBeNull();
    expect(modelNames(container)).toHaveLength(3);
    if (!document.querySelector('[role="menuitemcheckbox"]')) {
      await act(() => trigger.click());
    }
    const restoredSelection = [
      ...document.querySelectorAll<HTMLElement>('[role="menuitemcheckbox"]'),
    ].find((item) => item.textContent?.trim() === "All environments")!;
    await act(() => restoredSelection.click());
    expect(testState.useUsage.mock.calls.at(-1)?.[1]).toBeNull();
    expect(container.querySelector("#usage-provider-details")).toBeNull();
    expect(
      container.querySelector('[role="region"][aria-label="codex-known token details"]'),
    ).toBeNull();
    expect(modelNames(container)).toHaveLength(3);
  });
});
