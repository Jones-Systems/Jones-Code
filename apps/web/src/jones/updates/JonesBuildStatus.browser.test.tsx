import { act, render } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { JonesBuildStatus } from "./JonesBuildStatus";

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
      expect(badges(container)[0]!.textContent).toBe(`Installed compared with this laptop: ${label}`);
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
    expect(buttonNamed(container, "Download").disabled).toBe(true);
    expect(buttonNamed(container, "Check").disabled).toBe(true);
  });

  it("renders nothing for an unsupported host and only the status while a host is unreachable", async () => {
    await using screen = await renderStatus({ state: null, freshness: "unsupported" });
    const { container } = screen;
    expect(container.textContent).toBe("");
    await screen.rerender(
      statusUi({
        state: null,
        freshness: "stale",
        freshnessMessage: "Update status is waiting for a connection.",
      }),
    );
    expect(container.textContent).toBe("Update status is waiting for a connection.");
  });
});
