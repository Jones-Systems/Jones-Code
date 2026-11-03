import { act, render } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { JonesBuildStatus } from "./JonesBuildStatus";
import type { ExactSourceHost, ExactSourceTarget } from "./ExactSourceUpdateAction";
import { page } from "vite-plus/test/browser";
import { AsyncResult } from "effect/reactivity";

const alternativeHost = vi.hoisted(() => ({ value: undefined as ExactSourceHost | undefined }));
vi.mock("./ProductionExactSourceUpdateAction", async () => {
  const { ExactSourceUpdateAction } = await import("./ExactSourceUpdateAction");
  return {
    ProductionExactSourceUpdateAction: (target: ExactSourceTarget) => {
      if (alternativeHost.value === undefined)
        throw new Error("Missing synthetic alternative host");
      return <ExactSourceUpdateAction {...target} host={alternativeHost.value} />;
    },
  };
});

const environmentId = EnvironmentId.make("synthetic-host");
const installedSha = "d994dd4".padEnd(40, "1");
const offeredSha = "0368cbb".padEnd(40, "2");
const available = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "available",
  environmentId,
  message: "A newer main build is available.",
  currentVersion: "0.0.45-preview.20261013.38343490676.1",
  installedSource: {
    repository: "Jones-Systems/Jones-Code",
    sha: installedSha,
    tree: "c".repeat(40),
  },
  provenance: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: offeredSha,
    artifactId: 1,
    version: "0.0.45-preview.20261016.38974945620.1",
  },
  outcome: {
    status: "committed",
    fromVersion: "0.0.45-preview.20261011.38178738344.1",
    targetVersion: "0.0.45-preview.20261013.38343490676.1",
  },
  capability: { check: true, download: true, install: true },
} as unknown as JonesUpdateState;

function statusUi(
  props: Partial<Parameters<typeof JonesBuildStatus>[0]> = {},
  onRequest = vi.fn(),
) {
  return (
    <JonesBuildStatus
      environmentId={environmentId}
      state={available}
      freshness="fresh"
      comparison="different"
      onRequest={onRequest}
      {...props}
    />
  );
}
async function renderStatus(props: Partial<Parameters<typeof JonesBuildStatus>[0]> = {}) {
  const onRequest = vi.fn();
  const screen = await render(statusUi(props, onRequest));
  return { ...screen, onRequest };
}
const buttonNamed = (container: HTMLElement, name: string) =>
  [...container.querySelectorAll("button")].find((node) => node.textContent === name)!;
const badges = (container: HTMLElement) => [
  ...container.querySelectorAll<HTMLElement>('span[data-slot="tooltip-trigger"]:has(> .sr-only)'),
];
const precedes = (first: Node, second: Node) =>
  (first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;

describe("Jones build status", () => {
  it("shows retained and alternative builds at narrow width and replaces only after exact confirmation", async () => {
    const retained = {
      ...available,
      environmentId: EnvironmentId.make("generic-connections-test"),
      phase: "staged" as const,
      stagedHandle: "retained",
      capability: { ...available.capability, exactSource: true, replaceStaged: true },
      migrationPlan: { pendingUpstream: [42], pendingJones: [7] },
      alternativeCandidate: {
        checkedAt: "2026-10-29T00:00:00Z",
        expiresAt: "2099-01-01T00:00:00Z",
        provenance: { ...available.provenance!, sourceSha: "f".repeat(40), artifactId: 3 },
      },
    };
    const confirm = vi.fn(async () => true);
    const run = vi.fn(async () =>
      AsyncResult.success({ ...retained, provenance: retained.alternativeCandidate.provenance }),
    );
    const snapshot = {
      observation: { state: retained, freshness: "fresh" as const },
      canOperate: true,
    };
    alternativeHost.value = { snapshot, readLatest: () => snapshot, confirm, run };
    await page.viewport(360, 800);
    try {
      await using screen = await renderStatus({
        state: retained,
        environmentId: retained.environmentId,
      });
      expect(screen.container.textContent).toContain("Downloaded0368cbb");
      expect(screen.container.textContent).toContain("Alternativefffffff");
      expect(screen.container.scrollWidth).toBeLessThanOrEqual(360);
      await act(async () => buttonNamed(screen.container, "Details").click());
      expect(screen.container.textContent).toContain(
        "Downloaded migrations1 upstream, 1 Jones pending",
      );
      expect(screen.container.textContent).toContain(
        "Alternative migrationsChecked after download",
      );
      await act(async () => buttonNamed(screen.container, "Replace download").click());
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining("0368cbb"));
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining("fffffff"));
      await expect.poll(() => run.mock.calls.length).toBe(1);
      await expect.poll(() => screen.container.textContent).toContain("replacement requested");
      expect(screen.container.scrollWidth).toBeLessThanOrEqual(360);
      expect(run.mock.calls[0]).toEqual([
        {
          action: "downloadExact",
          input: {
            targetSource: "f".repeat(40),
            replace: expect.objectContaining({
              stagedHandle: "retained",
              sourceSha: offeredSha,
              alternative: retained.alternativeCandidate,
            }),
          },
        },
      ]);
      expect(screen.onRequest).not.toHaveBeenCalled();
    } finally {
      alternativeHost.value = undefined;
      await page.viewport(1280, 800);
    }
  });

  const failed: JonesUpdateState = {
    ...available,
    phase: "error",
    message: "Previous attempt needs reconciliation",
    updateId: "11111111-1111-4111-8111-111111111111",
    settlement: "none",
    launcherSettlement: "supported",
    launcherRefresh: "none",
    outcome: {
      status: "blocked",
      fromVersion: available.currentVersion!,
      targetVersion: "0.1.0-preview.20261023.1.1",
    },
  };

  it("reconciles one supported attempt with a new correlation ID and never offers retirement", async () => {
    await using screen = await renderStatus({ state: failed });
    const { container, onRequest } = screen;
    expect(container.textContent).toContain("Previous attempt needs reconciliation");
    expect(container.querySelector('[aria-label*="attempt ID"]')).not.toBeNull();
    await act(async () => buttonNamed(container, "Reconcile").click());
    expect(onRequest).toHaveBeenCalledTimes(1);
    expect(onRequest).toHaveBeenCalledWith({
      action: "reconcile",
      input: { operationId: failed.updateId, requestId: expect.stringMatching(/^[a-f0-9-]{36}$/) },
    });
    expect(container.textContent).not.toContain("Record retired");
  });

  it("hides reconciliation for old launchers and older servers, and disables stale requests", async () => {
    await using screen = await renderStatus({
      state: { ...failed, launcherSettlement: "unsupported", launcherRefresh: "recommended" },
    });
    const { container, onRequest } = screen;
    expect(buttonNamed(container, "Reconcile")).toBeUndefined();
    expect(container.textContent).toContain("Launcher refresh recommended");
    await screen.rerender(statusUi({ state: available }, onRequest));
    expect(buttonNamed(container, "Reconcile")).toBeUndefined();
    expect(container.textContent).not.toContain("Launcher refresh recommended");
    await screen.rerender(statusUi({ state: failed, freshness: "stale" }, onRequest));
    expect(buttonNamed(container, "Reconcile").disabled).toBe(true);
    await act(async () => buttonNamed(container, "Reconcile").click());
    expect(onRequest).not.toHaveBeenCalled();
  });

  it("shows proved no-effect failure and typed refusal without implying installation", async () => {
    await using screen = await renderStatus({
      state: { ...failed, settlement: "launcher-recorded", cause: "backup refused" },
    });
    const { container } = screen;
    expect(container.textContent).toContain(
      "Update did not start: backup refused. Nothing changed.",
    );
    expect(buttonNamed(container, "Reconcile")).toBeUndefined();
    expect(container.textContent).toContain("No changes:");
    await screen.rerender(statusUi({ state: { ...failed, settlementRefusal: "restore-pending" } }));
    expect(container.querySelector('[role="alert"]')?.textContent).toBe(
      "Recovery is still pending.",
    );
  });

  it("shows both builds compactly and keeps exact identities one disclosure away", async () => {
    await using screen = await renderStatus();
    const { container, onRequest } = screen;
    const text = container.textContent!;
    expect(text).toContain("Installedd994dd4");
    expect(text).toContain("Available0368cbb");
    expect(text).toContain("Different source");
    expect(text).not.toContain("0.0.45-preview");
    expect(text).not.toContain(offeredSha);
    expect(
      container.querySelector(
        '[aria-label^="Copy available version 0.0.45-preview.20261016.38974945620.1;"]',
      ),
    ).not.toBeNull();

    const details = buttonNamed(container, "Details");
    expect(details.getAttribute("aria-expanded")).toBe("false");
    await act(async () => details.click());
    expect(details.getAttribute("aria-expanded")).toBe("true");
    const opened = container.textContent!;
    expect(opened).toContain("0.0.45-preview.20261016.38974945620.1");
    expect(opened).toContain(offeredSha);
    expect(opened).toContain(installedSha);
    expect(opened).toContain(
      "Installed: 0.0.45-preview.20261011.38178738344.1 → 0.0.45-preview.20261013.38343490676.1",
    );

    await act(async () => buttonNamed(container, "Download").click());
    expect(onRequest).toHaveBeenCalledWith({
      action: "download",
      input: { artifactId: 1, sourceSha: offeredSha },
    });
  });

  it("shows colliding build times with seconds and copies the full canonical version", async () => {
    using writeText = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
    await using screen = await renderStatus({
      state: {
        ...available,
        currentBuildStartedAt: "2026-10-20T21:13:43Z",
        provenance: { ...available.provenance!, buildStartedAt: "2026-10-20T21:13:47Z" },
      },
    });
    const { container } = screen;
    const installed = buttonNamed(container, "1020·1713:43");
    expect(installed).toBeDefined();
    expect(buttonNamed(container, "1020·1713:47")).toBeDefined();
    expect(installed.getAttribute("aria-label")).toContain("Built October 20, 17:13 Eastern");
    await act(async () => installed.click());
    expect(writeText).toHaveBeenCalledWith(available.currentVersion);
    await act(async () => buttonNamed(container, "Details").click());
    expect(container.textContent).toContain("2026, 17:13:43 EDT");
    expect(container.textContent).toContain("2026, 17:13:47 EDT");
  });

  it("places the installed comparison before the arrow, apart from the candidate badge", async () => {
    await using screen = await renderStatus({
      installedComparison: "older",
      comparison: "matches",
    });
    const { container, onRequest } = screen;
    const [installed, offered] = badges(container);
    expect(badges(container)).toHaveLength(2);
    expect(installed!.textContent).toBe("Installed compared with this laptop: Older");
    expect(offered!.textContent).toBe("Available compared with this laptop: Matches laptop");
    const installedVersion = container.querySelector(
      '[aria-label^="Copy installed version 0.0.45-preview.20261013.38343490676.1;"]',
    )!;
    const arrow = container.querySelector("svg.lucide-arrow-right")!;
    const offeredVersion = container.querySelector(
      '[aria-label^="Copy available version 0.0.45-preview.20261016.38974945620.1;"]',
    )!;
    expect(precedes(installedVersion, installed!)).toBe(true);
    expect(precedes(installed!, arrow)).toBe(true);
    expect(precedes(arrow, offeredVersion)).toBe(true);
    expect(precedes(offeredVersion, offered!)).toBe(true);

    await act(async () => buttonNamed(container, "Details").click());
    const opened = container.textContent!;
    expect(opened).toContain(
      "Installed vs this laptopInstalled from an older source commit than this laptop.",
    );
    expect(opened).toContain(
      "Available vs this laptopBuilt from the source commit installed on this laptop.",
    );

    await act(async () => buttonNamed(container, "Download").click());
    expect(onRequest).toHaveBeenCalledWith({
      action: "download",
      input: { artifactId: 1, sourceSha: offeredSha },
    });
  });

  it("labels each installed comparison with its own tone", async () => {
    const expected = [
      ["matches", "Matches", "success"],
      ["older", "Older", "warning"],
      ["newer", "Newer", "info"],
      ["different", "Different source", "warning"],
      ["diverged", "Diverged", "warning"],
    ] as const;
    for (const [installedComparison, label, tone] of expected) {
      await using screen = await renderStatus({ installedComparison, comparison: null });
      const { container } = screen;
      expect(badges(container)).toHaveLength(1);
      expect(badges(container)[0]!.textContent).toBe(
        `Installed compared with this laptop: ${label}`,
      );
      expect(badges(container)[0]!.className).toContain(`text-${tone}-foreground`);
    }
  });

  it("explains an uncompared installed source in Details without a badge", async () => {
    await using screen = await renderStatus({
      installedComparison: "unavailable",
      comparison: "different",
    });
    const { container } = screen;
    expect(badges(container).map((badge) => badge.textContent)).toEqual([
      "Available compared with this laptop: Different source",
    ]);
    await act(async () => buttonNamed(container, "Details").click());
    expect(container.textContent).toContain(
      "Installed vs this laptopNot compared. Comparing needs the full installed source commit of both this host and this laptop.",
    );

    await screen.rerender(statusUi({ comparison: null }));
    expect(badges(container)).toHaveLength(0);
    expect(container.textContent).not.toContain("vs this laptop");
  });

  it("keeps Install routing and names the downloaded build in its comparison", async () => {
    const staged = {
      ...available,
      phase: "staged",
      stagedHandle: "staged-synthetic",
    } as unknown as JonesUpdateState;
    await using screen = await renderStatus({
      state: staged,
      installedComparison: "newer",
      comparison: "different",
    });
    const { container, onRequest } = screen;
    expect(buttonNamed(container, "Download")).toBeUndefined();
    expect(badges(container).map((badge) => badge.textContent)).toEqual([
      "Installed compared with this laptop: Newer",
      "Downloaded compared with this laptop: Different source",
    ]);
    await act(async () => buttonNamed(container, "Details").click());
    expect(container.textContent).toContain("Downloaded vs this laptopBuilt from a different");
    await act(async () => buttonNamed(container, "Install").click());
    expect(onRequest).toHaveBeenCalledWith({
      action: "install",
      input: {
        stagedHandle: "staged-synthetic",
        environmentId,
        currentVersion: available.currentVersion,
      },
    });

    await screen.rerender(
      statusUi({
        state: staged,
        freshness: "stale",
        freshnessMessage: "Update status is temporarily unavailable; retrying.",
        installedComparison: "unavailable",
        comparison: "unavailable",
      }),
    );
    expect(badges(container)).toHaveLength(0);
    expect(container.textContent).toContain("Update status is temporarily unavailable; retrying.");
    expect(buttonNamed(container, "Install").disabled).toBe(true);
  });

  it("keeps a failed install outcome and stale status visible without opening details", async () => {
    await using screen = await renderStatus({
      state: {
        ...available,
        outcome: { ...available.outcome!, status: "rolled-back", reason: "health check failed" },
      },
      freshness: "stale",
      freshnessMessage: "Update status is temporarily unavailable; retrying.",
    });
    const { container } = screen;
    const text = container.textContent!;
    expect(text).toContain(
      "Rolled back: 0.0.45-preview.20261011.38178738344.1 → 0.0.45-preview.20261013.38343490676.1 · health check failed",
    );
    expect(text).toContain("Update status is temporarily unavailable; retrying.");
    expect(buttonNamed(container, "Download").getAttribute("aria-disabled")).toBe("true");
    expect(buttonNamed(container, "Check").disabled).toBe(true);
  });

  const noProvenance = {
    ...available,
    provenance: undefined,
  } as unknown as JonesUpdateState;

  it("keeps Download visible while waiting for the laptop, explains on hover and keyboard focus, and blocks activation", async () => {
    const reason =
      "This host follows its laptop and waits for the laptop to report its installed build.";
    await using screen = await renderStatus({
      state: {
        ...noProvenance,
        phase: "no-new",
        policy: { role: "follower", leaderSource: null },
      },
    });
    const { container, onRequest } = screen;
    const download = buttonNamed(container, "Download");
    expect(download.getAttribute("aria-disabled")).toBe("true");
    expect(download.disabled).toBe(false);
    const tooltip = () => document.querySelector('[role="tooltip"]');
    await userEvent.hover(download);
    await vi.waitFor(() => expect(tooltip()?.textContent).toBe(reason));
    await userEvent.unhover(download);
    await vi.waitFor(() => expect(tooltip()).toBeNull());
    await act(async () => buttonNamed(container, "Check").focus());
    await userEvent.keyboard("{Tab}");
    expect(document.activeElement).toBe(download);
    await vi.waitFor(() => expect(tooltip()?.textContent).toBe(reason));
    expect(download.getAttribute("aria-describedby")).toBe(tooltip()?.id);
    await userEvent.keyboard("{Enter} ");
    await act(async () => download.click());
    expect(onRequest).not.toHaveBeenCalled();
  });

  const blockedCases: readonly {
    name: string;
    props: Partial<Parameters<typeof JonesBuildStatus>[0]>;
    reason: string;
  }[] = [
    {
      name: "loading without state",
      props: { state: null, freshness: "loading" },
      reason: "Waiting for this host's update status.",
    },
    {
      name: "unsupported without state",
      props: { state: null, freshness: "unsupported" },
      reason: "This host does not support Jones build updates.",
    },
    {
      name: "unreachable without state",
      props: { state: null, freshness: "stale", freshnessMessage: "Waiting for a connection." },
      reason: "Waiting for a connection.",
    },
    {
      name: "fresh without state",
      props: { state: null },
      reason: "No update status is available for this host.",
    },
    {
      name: "stale with a previously offered build",
      props: { freshness: "stale", freshnessMessage: "Refreshing this host's update status." },
      reason: "Refreshing this host's update status.",
    },
    {
      name: "pending request before follower or capability reasons",
      props: {
        pending: true,
        state: { ...available, policy: { role: "follower", leaderSource: null } },
      },
      reason: "An update request is in progress. Wait for it to finish.",
    },
    ...(["checking", "downloading", "verifying", "preparing", "installing"] as const).map(
      (phase) => ({
        name: phase,
        props: { state: { ...available, phase, message: `${phase} this build` } },
        reason:
          phase === "installing"
            ? "Installing. Server restarting. Waiting for the launcher outcome."
            : `${phase} this build`,
      }),
    ),
    {
      name: "current build without provenance",
      props: { state: { ...noProvenance, phase: "no-new", message: "Already up to date." } },
      reason: "Already up to date.",
    },
    {
      name: "building without provenance",
      props: { state: { ...noProvenance, phase: "building", message: "Build is still running." } },
      reason: "Build is still running.",
    },
    {
      name: "missing qualified provenance",
      props: { state: noProvenance },
      reason: "No qualified build is available to download.",
    },
    {
      name: "unavailable capability with its reason",
      props: {
        state: {
          ...available,
          capability: { ...available.capability, download: false, reason: "unsupported-platform" },
        },
      },
      reason: "Build downloads are not supported on this host's platform.",
    },
    {
      name: "unavailable capability without a reason",
      props: { state: { ...available, capability: { ...available.capability, download: false } } },
      reason: "Build downloads are not available on this host.",
    },
    {
      name: "different follower source before capability or phase",
      props: {
        state: {
          ...available,
          phase: "blocked",
          capability: { ...available.capability, download: false },
          policy: { role: "follower", leaderSource: installedSha },
        },
      },
      reason:
        "This host follows its laptop and installs only the laptop's installed source d994dd4.",
    },
    {
      name: "failed update",
      props: { state: failed, actionError: "The request failed." },
      reason: "Previous attempt needs reconciliation",
    },
  ];

  it.each(blockedCases)(
    "keeps Download disabled with the current reason: $name",
    async ({ props, reason }) => {
      await using screen = await renderStatus(props);
      const { container, onRequest } = screen;
      const download = buttonNamed(container, "Download");
      expect(download).toBeDefined();
      expect(download.getAttribute("aria-disabled")).toBe("true");
      await act(async () => download.focus());
      await vi.waitFor(() =>
        expect(document.querySelector('[role="tooltip"]')?.textContent).toBe(reason),
      );
      await userEvent.keyboard("{Enter} ");
      await act(async () => download.click());
      expect(onRequest).not.toHaveBeenCalled();
    },
  );

  it("enables the exact offered download after freshness returns even if installation has a capability reason or previous action error", async () => {
    const state = {
      ...available,
      capability: { ...available.capability, install: false, reason: "bootstrap-required" },
    } as JonesUpdateState;
    await using screen = await renderStatus({ state, freshness: "stale" });
    const { container, onRequest } = screen;
    const blocked = buttonNamed(container, "Download");
    expect(blocked.getAttribute("aria-disabled")).toBe("true");
    await act(async () => blocked.focus());
    await vi.waitFor(() =>
      expect(document.querySelector('[role="tooltip"]')?.id).toBe(
        blocked.getAttribute("aria-describedby"),
      ),
    );
    await screen.rerender(statusUi({ state, actionError: "Previous request failed." }, onRequest));
    const download = buttonNamed(container, "Download");
    expect(download).toBe(blocked);
    expect(document.activeElement).toBe(download);
    await vi.waitFor(() => expect(download.getAttribute("aria-describedby")).toBeNull());
    expect(download.getAttribute("aria-disabled")).toBeNull();
    expect(download.disabled).toBe(false);
    await act(async () => download.click());
    expect(onRequest).toHaveBeenCalledExactlyOnceWith({
      action: "download",
      input: { artifactId: 1, sourceSha: offeredSha },
    });
  });
});

describe("a host that follows its laptop", () => {
  const staged = (sourceSha: string, leaderSource: string | null) =>
    ({
      ...available,
      phase: "staged",
      stagedHandle: "handle-1",
      provenance: { ...available.provenance!, sourceSha },
      policy: { role: "follower", leaderSource },
    }) as JonesUpdateState;

  it("offers Install only for the laptop's installed build", async () => {
    await using screen = await renderStatus({ state: staged(offeredSha, offeredSha) });
    const { container } = screen;
    expect(buttonNamed(container, "Install").disabled).toBe(false);
    expect(container.textContent).toContain("Follows its laptop · takes only source 0368cbb");

    await screen.rerender(statusUi({ state: staged(offeredSha, installedSha) }));
    expect(buttonNamed(container, "Install").disabled).toBe(true);
    expect(container.textContent).toContain(
      "Downloaded build is not the laptop's, so it will not install",
    );

    await screen.rerender(statusUi({ state: staged(offeredSha, null) }));
    expect(buttonNamed(container, "Install").disabled).toBe(true);
    expect(container.textContent).toContain("waiting for the laptop's installed build");
  });
});
