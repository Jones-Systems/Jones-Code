// @vitest-environment jsdom
import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { SidebarEnvironmentFilters } from "./SidebarEnvironmentFilters";
import { SIDEBAR_ENVIRONMENT_FILTERS } from "./model";

vi.mock("../../branding", () => ({
  APP_VERSION: "0.1.0-preview.20261020.39438472507.1",
  APP_SOURCE_SHA: "abcdef0".padEnd(40, "1"),
  APP_BUILD_STARTED_AT: undefined,
}));
vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("./state", () => ({ sidebarEnvironmentFiltersAtom: {} }));
vi.mock("@effect/atom-react", () => ({
  useAtom: () =>
    useState(Object.fromEntries(SIDEBAR_ENVIRONMENT_FILTERS.map((label) => [label, true]))),
}));
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
describe("sidebar running build", () => {
  it("keeps the source fallback and exact identity available while changing a filter", async () => {
    await act(async () => root.render(<SidebarEnvironmentFilters />));
    const note = container.querySelector('[role="note"]')!;
    expect(note.textContent).toBe("abcdef0");
    expect(note.getAttribute("aria-label")).toContain("Build time not recorded");
    expect(note.getAttribute("aria-label")).toContain("0.1.0-preview.20261020.39438472507.1");
    const filter = container.querySelector("button")!;
    expect(filter.getAttribute("aria-pressed")).toBe("true");
    await act(async () => filter.click());
    expect(filter.getAttribute("aria-pressed")).toBe("false");
    expect(note.textContent).toBe("abcdef0");
  });
});
