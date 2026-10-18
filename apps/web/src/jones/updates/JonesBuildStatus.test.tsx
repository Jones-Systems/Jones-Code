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

describe("Jones build status", () => {
  it("shows both builds compactly and keeps exact identities one disclosure away", async () => {
    const onRequest = await render();
    const text = container.textContent!;
    expect(text).toContain("Installed0.45 · 0676");
    expect(text).toContain("Available0.45 · 5620");
    expect(text).toContain("Different source");
    expect(text).not.toContain("0.0.45-preview");
    expect(text).not.toContain(offeredSha);
    expect(
      container.querySelector(
        '[aria-label="Copy available version 0.0.45-preview.20261016.38974945620.1"]',
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
    expect(text).toContain("Rolled back: 0.45 · 8344 → 0.45 · 0676 · health check failed");
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
