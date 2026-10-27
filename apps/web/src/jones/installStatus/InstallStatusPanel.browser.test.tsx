import { applyThemePalette } from "../../themePalette";
import { describe, expect } from "vite-plus/test";
import { it } from "vite-plus/test";
import type { DesktopUpdateState } from "@t3tools/contracts";
import { act, render, type Screen } from "../../../test/render";
import "../../index.css";
import { InstallStatusPanel } from "./InstallStatusPanel";
import { startInstallPlanController } from "./InstallPlanController";
import { handOffJonesTargets, routeInstallStatusPill } from "./entrypoints";
import { createScriptedInstallPlan, FIXTURE_HOSTS } from "../updates/fixtures/install-preview-plan";

const source = "a".repeat(40);
function initialDesktop(): DesktopUpdateState {
  return {
    enabled: true,
    status: "available",
    channel: "latest",
    currentVersion: "0.1.0-fixture.1",
    availableVersion: "0.1.0-fixture.2",
    downloadedVersion: null,
    releaseNotes: [],
    omittedReleaseCount: 0,
    downloadPercent: null,
    checkedAt: null,
    message: null,
    errorContext: null,
    canRetry: false,
    hostArch: "arm64",
    appArch: "arm64",
    runningUnderArm64Translation: false,
    jones: {
      source: "jones-actions",
      channel: "jones-main",
      phase: "available",
      currentVersion: "0.1.0-fixture.1",
      capability: { check: true, download: true, install: true, operations: 1 },
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: "b".repeat(40),
        tree: "c".repeat(40),
      },
      provenance: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: source,
        sourceTree: "d".repeat(40),
        version: "0.1.0-fixture.2",
        workflow: "fixture-only",
        runId: 1,
        runAttempt: 1,
        artifactId: 9001,
        artifactDigest: "e".repeat(64),
        platform: "darwin",
        architecture: "arm64",
      },
    },
  };
}
async function setup() {
  let desktop = initialDesktop();
  const updates = new Set<(value: DesktopUpdateState) => void>();
  const calls: { action: string; input?: unknown }[] = [];
  const fixture = createScriptedInstallPlan({
    readDesktop: () => desktop,
    publishDesktop: (state) => {
      desktop = state;
      fixture.laptopChanged();
      for (const listener of updates) listener(state);
    },
    record: (action, input) => calls.push({ action, input }),
  });
  const bridge = {
    installPlan: fixture.request,
    onInstallPlan: fixture.onInstallPlan,
    getUpdateState: async () => desktop,
    onUpdateState: (listener: (value: DesktopUpdateState) => void) => {
      updates.add(listener);
      return () => {
        updates.delete(listener);
      };
    },
  };
  const controller = startInstallPlanController(fixture.services(bridge), fixture.store);
  fixture.store.open();
  const screen = await render(
    <div className="flex h-[640px] w-80 flex-col bg-sidebar text-sidebar-foreground" data-shell>
      <div className="min-h-24 flex-1 overflow-y-auto" data-threads tabIndex={0}>
        {Array.from({ length: 30 }, (_, i) => (
          <p key={i} className="p-2">
            Fixture thread {i + 1}
          </p>
        ))}
      </div>
      <footer className="flex shrink-0 flex-col gap-2 p-2">
        <InstallStatusPanel store={fixture.store} />
        <div data-devices>VPS · Test · Mini · Laptop</div>
      </footer>
    </div>,
    { router: false },
  );
  await expect.poll(() => fixture.store.get().native).not.toBeNull();
  const publish = async (state: DesktopUpdateState) => {
    await act(() => {
      desktop = state;
      fixture.laptopChanged();
      for (const listener of updates) listener(state);
      controller.wake();
    });
  };
  return {
    fixture,
    bridge,
    screen,
    calls,
    controller,
    desktop: () => desktop,
    publish,
    async stageLaptop() {
      const current = desktop;
      await publish({
        ...current,
        status: "downloaded",
        downloadedVersion: current.availableVersion,
        jones: { ...current.jones!, phase: "staged", stagedHandle: "fixture-laptop-stage" },
      });
    },
    async commitLaptop() {
      const current = desktop;
      await publish({
        ...current,
        status: "up-to-date",
        currentVersion: current.availableVersion!,
        jones: {
          ...current.jones!,
          phase: "committed",
          installedSource: {
            repository: "Jones-Systems/Jones-Code",
            sha: source,
            tree: "d".repeat(40),
          },
        },
      });
    },
    async [Symbol.asyncDispose]() {
      controller.close();
      await screen[Symbol.asyncDispose]();
    },
  };
}
const button = (screen: Screen, name: string) => screen.getByRole("button", { name, exact: true });
const visibleDone = (screen: Screen) =>
  [...screen.container.querySelectorAll<HTMLButtonElement>("button")].filter(
    (node) => node.textContent === "Done" && !node.closest("[inert]"),
  );
const click = async (screen: Screen, name: string) => {
  await button(screen, name).click();
};

// These use real browser interactions and the real pure model/runner against scripted receipts.
// They cannot qualify native installs, host worker exit, Electron IPC or publication.
describe("Install status receipt fixture", () => {
  it("uses one editing Done, applies its draft once, and discards a collapsed draft", async () => {
    await using world = await setup();
    const { screen, fixture, calls } = world;
    await click(screen, "Edit");
    expect(visibleDone(screen)).toHaveLength(1);
    expect(screen.container.textContent).not.toMatch(/Save|Cancel/);
    await click(screen, "All");
    expect(fixture.store.get().previewSelection).toHaveLength(0);
    await click(screen, "None");
    await screen.getByRole("checkbox", { name: "Include VPS" }).click();
    expect(
      calls.filter(
        (c) => c.action === "fixture-installPlan" && (c.input as { type: string }).type === "edit",
      ),
    ).toHaveLength(0);
    await click(screen, "Done");
    expect(fixture.store.get().previewSelection.map((d) => d.label)).toEqual(["VPS"]);
    await click(screen, "Edit");
    await click(screen, "All");
    await click(screen, "Collapse Install status");
    await click(screen, "Expand Install status");
    expect(fixture.store.get().previewSelection.map((d) => d.label)).toEqual(["VPS"]);
    expect(visibleDone(screen)).toHaveLength(0);
    await click(screen, "Download on Laptop");
    expect(fixture.snapshot().current?.devices.map((d) => d.label)).toEqual(["VPS"]);
    expect(calls.filter((c) => c.action === "fixture-host-operationStart")).toHaveLength(0);
    await click(screen, "VPS update details");
    await click(screen, "Remove from queue");
    expect(fixture.snapshot().current?.devices[0]?.membership).toBe("removed");
    expect(calls.filter((c) => c.action === "fixture-host-operationStop")).toHaveLength(0);
  });

  it("finishes Laptop-only, reserves the header slot, dismisses, reopens read-only and previews a newer build", async () => {
    await using world = await setup();
    const { fixture, screen, calls } = world;
    const threads = screen.container.querySelector<HTMLElement>("[data-threads]")!;
    expect(threads.scrollHeight).toBeGreaterThan(threads.clientHeight);
    const panel = screen.container.querySelector("[data-install-status]")!;
    expect(
      panel.compareDocumentPosition(screen.container.querySelector("[data-devices]")!) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    await click(screen, "Download on Laptop");
    await world.stageLaptop();
    expect(visibleDone(screen)).toHaveLength(0);
    await click(screen, "Install on Laptop");
    expect(calls.filter((c) => c.action === "fixture-review")).toHaveLength(1);
    await click(screen, "Collapse Install status");
    const rects = () =>
      ["[data-install-title]", "[data-install-chevron]"].map((selector) => {
        const rect = screen.container.querySelector(selector)!.getBoundingClientRect();
        return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
      });
    const before = rects();
    const slot = screen.container.querySelector<HTMLElement>("[data-install-done-slot]")!;
    expect(slot.hasAttribute("inert")).toBe(true);
    expect(slot.getAttribute("aria-hidden")).toBe("true");
    expect(getComputedStyle(slot).visibility).toBe("hidden");
    await world.commitLaptop();
    expect(visibleDone(screen)).toHaveLength(1);
    expect(rects()).toEqual(before);
    expect(slot.hasAttribute("inert")).toBe(false);
    const effectsBeforeDone = calls.filter(
      (c) => c.action.startsWith("fixture-host-") || c.action.startsWith("fixture-laptop-"),
    ).length;
    await click(screen, "Done with update plan");
    expect(fixture.snapshot().current).toBeNull();
    expect(
      calls.filter(
        (c) => c.action.startsWith("fixture-host-") || c.action.startsWith("fixture-laptop-"),
      ).length,
    ).toBe(effectsBeforeDone);
    expect(await routeInstallStatusPill(world.bridge, world.desktop(), false)).toBe(
      "last-and-check",
    );
    await click(screen, "Expand Install status");
    expect(screen.container.textContent).toContain("Last update");
    expect(screen.container.textContent).not.toContain("Download on Laptop");
    expect(visibleDone(screen)).toHaveLength(0);
    const desktop = world.desktop();
    await world.publish({
      ...desktop,
      status: "available",
      availableVersion: "0.1.0-fixture.3",
      jones: {
        ...desktop.jones!,
        phase: "available",
        provenance: {
          ...desktop.jones!.provenance!,
          sourceSha: "f".repeat(40),
          version: "0.1.0-fixture.3",
        },
      },
    });
    expect(await routeInstallStatusPill(world.bridge, world.desktop(), false)).toBe("opened");
    expect(screen.container.querySelector("[data-plan-preview]")).not.toBeNull();
    expect(fixture.store.get().previewSelection).toHaveLength(0);
  });

  it("recomputes Done for added VPS and Mini and never automatically installs their downloads", async () => {
    await using world = await setup();
    const { screen, fixture, controller, calls } = world;
    await click(screen, "Download on Laptop");
    await world.stageLaptop();
    await click(screen, "Install on Laptop");
    await world.commitLaptop();
    expect(visibleDone(screen)).toHaveLength(1);
    await click(screen, "Edit");
    await screen.getByRole("checkbox", { name: "Include VPS" }).click();
    await screen.getByRole("checkbox", { name: "Include Mini" }).click();
    await click(screen, "Done");
    controller.wake();
    await expect
      .poll(
        () =>
          fixture.snapshot().current?.devices.length === 2 &&
          fixture
            .snapshot()
            .current?.devices.every((d) => d.operations.at(-1)?.receipt?.phase === "running"),
      )
      .toBe(true);
    expect(visibleDone(screen)).toHaveLength(0);
    await act(() => fixture.settleHost("VPS", "staged"));
    await act(() => fixture.settleHost("Mini", "staged"));
    expect(visibleDone(screen)).toHaveLength(0);
    expect(
      calls.filter(
        (c) =>
          c.action === "fixture-host-operationStart" &&
          (c.input as { input: { action: { kind: string } } }).input.action.kind === "install",
      ),
    ).toHaveLength(0);
    await click(screen, "VPS update details");
    await click(screen, "Install on VPS");
    await act(() => fixture.settleHost("VPS", "committed"));
    expect(visibleDone(screen)).toHaveLength(0);
    await click(screen, "Mini update details");
    await click(screen, "Install on Mini");
    await act(() => fixture.settleHost("Mini", "committed"));
    expect(visibleDone(screen)).toHaveLength(1);
    expect(calls.filter((c) => c.action === "fixture-review")).toHaveLength(3);
  });

  it("shows Stop requested until the exact stopped or held receipt, with no Retry or Done while held", async () => {
    await using world = await setup();
    const { screen, fixture, controller, calls } = world;
    await click(screen, "Edit");
    await screen.getByRole("checkbox", { name: "Include VPS" }).click();
    await click(screen, "Done");
    await click(screen, "Download on Laptop");
    await world.stageLaptop();
    await click(screen, "Install on Laptop");
    await world.commitLaptop();
    controller.wake();
    await expect
      .poll(() => fixture.snapshot().current?.devices[0]?.operations.at(-1)?.receipt?.phase)
      .toBe("running");
    await click(screen, "VPS update details");
    await click(screen, "Stop download on VPS");
    expect(screen.container.textContent).toContain("Stop requested");
    expect(visibleDone(screen)).toHaveLength(0);
    expect(calls.filter((c) => c.action === "fixture-host-operationStop")).toHaveLength(1);
    await act(() => fixture.settleHost("VPS", "held"));
    expect(screen.container.textContent).toContain("Stop not confirmed");
    expect(screen.container.textContent).toContain("unreaped");
    expect(screen.container.querySelector('input[aria-label="Operation ID"]')).not.toBeNull();
    expect(screen.container.textContent).not.toContain("Retry");
    expect(visibleDone(screen)).toHaveLength(0);
    const sent = calls.filter((c) => c.action === "fixture-host-operationStart").length;
    await click(screen, "Check again");
    expect(calls.filter((c) => c.action === "fixture-host-operationStart")).toHaveLength(sent);
    await act(() => fixture.settleHost("VPS", "stopped"));
    expect(screen.container.textContent).toContain("Stopped · nothing installed");
    expect(visibleDone(screen)).toHaveLength(1);
  });

  it("keeps unsupported devices out and uses a neutral dashed separator before and after Laptop verification", async () => {
    const oldClass = document.documentElement.className;
    const oldStyle = document.documentElement.style.cssText;
    const oldTheme = document.documentElement.getAttribute("data-theme-id");
    document.documentElement.classList.add("dark");
    applyThemePalette("t3-chat", "dark");
    try {
      await using world = await setup();
      const { fixture, screen } = world;
      await act(() => fixture.setSupported("Test", false));
      await click(screen, "Edit");
      expect(
        screen.container
          .querySelector('[role="checkbox"][aria-label="Include Test"]')
          ?.getAttribute("aria-disabled"),
      ).toBe("true");
      await click(screen, "All");
      await click(screen, "Done");
      expect(fixture.store.get().previewSelection.map((d) => d.label)).toEqual(["VPS", "Mini"]);
      const gate = screen.container.querySelector<HTMLElement>("[data-install-gate]")!;
      const before = getComputedStyle(gate);
      expect(before.borderTopStyle).toBe("dashed");
      // Match the app's contrast-adjusted border token rather than the raw palette value.
      const probe = document.createElement("div");
      probe.style.borderTop = "1px dashed var(--color-sidebar-border)";
      gate.append(probe);
      const neutralBorder = getComputedStyle(probe).borderTopColor;
      expect(before.borderTopColor).toBe(neutralBorder);
      probe.remove();
      const neutral = before.color;
      await click(screen, "Edit");
      await click(screen, "None");
      await click(screen, "Done");
      await click(screen, "Download on Laptop");
      await world.stageLaptop();
      await click(screen, "Install on Laptop");
      await world.commitLaptop();
      expect(screen.container.textContent).toContain("Laptop verified · Others go");
      expect(getComputedStyle(gate).color).toBe(neutral);
      expect(getComputedStyle(gate).borderTopStyle).toBe("dashed");
      expect(getComputedStyle(gate).borderTopColor).toBe(neutralBorder);
      expect(screen.container.querySelector('[role="progressbar"]')).toBeNull();
    } finally {
      document.documentElement.className = oldClass;
      document.documentElement.style.cssText = oldStyle;
      if (oldTheme === null) document.documentElement.removeAttribute("data-theme-id");
      else document.documentElement.setAttribute("data-theme-id", oldTheme);
    }
  });

  it("hands Update Everything hosts to membership visibly before any host dispatch", async () => {
    await using world = await setup();
    const results = await act(() => handOffJonesTargets(FIXTURE_HOSTS));
    expect(results.map((r) => [r.label, r.message])).toEqual(
      FIXTURE_HOSTS.map((h) => [h.label, "Handed to Install status"]),
    );
    expect(world.fixture.snapshot().current?.origin).toBe("update-everything");
    expect(world.fixture.snapshot().current?.devices.map((d) => d.label)).toEqual([
      "VPS",
      "Mini",
      "Test",
    ]);
    expect(world.calls.filter((c) => c.action === "fixture-host-operationStart")).toHaveLength(0);
    expect(world.calls.filter((c) => c.action === "fixture-laptop-download")).toHaveLength(1);
  });
});
