import { useState } from "react";
import { act, render } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
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
describe("sidebar running build", () => {
  it("keeps the source fallback and exact identity available while changing a filter", async () => {
    await using screen = await render(<SidebarEnvironmentFilters />);
    const { container } = screen;
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
