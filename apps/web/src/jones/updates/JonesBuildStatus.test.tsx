// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
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

let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function render(props: Partial<Parameters<typeof JonesBuildStatus>[0]> = {}) {
  const onRequest = vi.fn();
  await act(async () => {
    root.render(
      <JonesBuildStatus
        environmentId={environmentId}
        state={available}
        freshness="fresh"
        comparison="different"
        onRequest={onRequest}
        {...props}
      />,
    );
  });
  return onRequest;
}
const buttonNamed = (name: string) =>
  [...container.querySelectorAll("button")].find((node) => node.textContent === name)!;
const badges = () => [
  ...container.querySelectorAll<HTMLElement>('span[data-slot="tooltip-trigger"]:has(> .sr-only)'),
];
const precedes = (first: Node, second: Node) =>
  (first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;

describe("Jones build status", () => {
  it("shows both builds compactly and keeps exact identities one disclosure away", async () => {
    const onRequest = await render();
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

    const details = buttonNamed("Details");
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

    await act(async () => buttonNamed("Download").click());
    expect(onRequest).toHaveBeenCalledWith({
      action: "download",
      input: { artifactId: 1, sourceSha: offeredSha },
    });
  });

  it("shows colliding build times with seconds and copies the full canonical version", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    await render({
      state: {
        ...available,
        currentBuildStartedAt: "2026-10-20T21:13:43Z",
        provenance: { ...available.provenance!, buildStartedAt: "2026-10-20T21:13:47Z" },
      },
    });
    const installed = buttonNamed("1020·1713:43");
    expect(installed).toBeDefined();
    expect(buttonNamed("1020·1713:47")).toBeDefined();
    expect(installed.getAttribute("aria-label")).toContain("Built October 20, 17:13 Eastern");
    await act(async () => installed.click());
    expect(writeText).toHaveBeenCalledWith(available.currentVersion);
    await act(async () => buttonNamed("Details").click());
    expect(container.textContent).toContain("2026, 17:13:43 EDT");
    expect(container.textContent).toContain("2026, 17:13:47 EDT");
  });

  it("places the installed comparison before the arrow, apart from the candidate badge", async () => {
    const onRequest = await render({ installedComparison: "older", comparison: "matches" });
    const [installed, offered] = badges();
    expect(badges()).toHaveLength(2);
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

    await act(async () => buttonNamed("Details").click());
    const opened = container.textContent!;
    expect(opened).toContain(
      "Installed vs this laptopInstalled from an older source commit than this laptop.",
    );
    expect(opened).toContain(
      "Available vs this laptopBuilt from the source commit installed on this laptop.",
    );

    await act(async () => buttonNamed("Download").click());
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
      await render({ installedComparison, comparison: null });
      expect(badges()).toHaveLength(1);
      expect(badges()[0]!.textContent).toBe(`Installed compared with this laptop: ${label}`);
      expect(badges()[0]!.className).toContain(`text-${tone}-foreground`);
    }
  });

  it("explains an uncompared installed source in Details without a badge", async () => {
    await render({ installedComparison: "unavailable", comparison: "different" });
    expect(badges().map((badge) => badge.textContent)).toEqual([
      "Available compared with this laptop: Different source",
    ]);
    await act(async () => buttonNamed("Details").click());
    expect(container.textContent).toContain(
      "Installed vs this laptopNot compared. Comparing needs the full installed source commit of both this host and this laptop.",
    );

    await render({ comparison: null });
    expect(badges()).toHaveLength(0);
    expect(container.textContent).not.toContain("vs this laptop");
  });

  it("keeps Install routing and names the downloaded build in its comparison", async () => {
    const staged = {
      ...available,
      phase: "staged",
      stagedHandle: "staged-synthetic",
    } as unknown as JonesUpdateState;
    const onRequest = await render({
      state: staged,
      installedComparison: "newer",
      comparison: "different",
    });
    expect(badges().map((badge) => badge.textContent)).toEqual([
      "Installed compared with this laptop: Newer",
      "Downloaded compared with this laptop: Different source",
    ]);
    await act(async () => buttonNamed("Details").click());
    expect(container.textContent).toContain("Downloaded vs this laptopBuilt from a different");
    await act(async () => buttonNamed("Install").click());
    expect(onRequest).toHaveBeenCalledWith({
      action: "install",
      input: {
        stagedHandle: "staged-synthetic",
        environmentId,
        currentVersion: available.currentVersion,
      },
    });

    await render({
      state: staged,
      freshness: "stale",
      freshnessMessage: "Update status is temporarily unavailable; retrying.",
      installedComparison: "unavailable",
      comparison: "unavailable",
    });
    expect(badges()).toHaveLength(0);
    expect(container.textContent).toContain("Update status is temporarily unavailable; retrying.");
    expect(buttonNamed("Install").disabled).toBe(true);
  });

  it("keeps a failed install outcome and stale status visible without opening details", async () => {
    await render({
      state: {
        ...available,
        outcome: { ...available.outcome!, status: "rolled-back", reason: "health check failed" },
      },
      freshness: "stale",
      freshnessMessage: "Update status is temporarily unavailable; retrying.",
    });
    const text = container.textContent!;
    expect(text).toContain(
      "Rolled back: 0.0.45-preview.20261011.38178738344.1 → 0.0.45-preview.20261013.38343490676.1 · health check failed",
    );
    expect(text).toContain("Update status is temporarily unavailable; retrying.");
    expect(buttonNamed("Download").disabled).toBe(true);
    expect(buttonNamed("Check").disabled).toBe(true);
  });

  it("renders nothing for an unsupported host and only the status while a host is unreachable", async () => {
    await render({ state: null, freshness: "unsupported" });
    expect(container.textContent).toBe("");
    await render({
      state: null,
      freshness: "stale",
      freshnessMessage: "Update status is waiting for a connection.",
    });
    expect(container.textContent).toBe("Update status is waiting for a connection.");
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
    await render({ state: staged(offeredSha, offeredSha) });
    expect(buttonNamed("Install").disabled).toBe(false);
    expect(container.textContent).toContain("Follows its laptop · takes only source 0368cbb");

    await render({ state: staged(offeredSha, installedSha) });
    expect(buttonNamed("Install").disabled).toBe(true);
    expect(container.textContent).toContain(
      "Downloaded build is not the laptop's, so it will not install",
    );

    await render({ state: staged(offeredSha, null) });
    expect(buttonNamed("Install").disabled).toBe(true);
    expect(container.textContent).toContain("waiting for the laptop's installed build");
  });
});
