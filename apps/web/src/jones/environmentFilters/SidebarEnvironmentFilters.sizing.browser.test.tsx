import "../../index.css";
import { useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { SidebarEnvironmentFilters } from "./SidebarEnvironmentFilters";
import { SIDEBAR_ENVIRONMENT_FILTERS } from "./model";

vi.mock("../../branding", () => ({
  APP_VERSION: "0.1.0-preview.20261025.1.1",
  APP_SOURCE_SHA: "abcdef0".padEnd(40, "1"),
  APP_BUILD_STARTED_AT: "2026-10-25T04:27:00Z",
}));
vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("./state", () => ({ sidebarEnvironmentFiltersAtom: {} }));
vi.mock("@effect/atom-react", () => ({
  useAtom: () =>
    useState(Object.fromEntries(SIDEBAR_ENVIRONMENT_FILTERS.map((label) => [label, true]))),
}));

describe("sidebar version fitting", () => {
  it("uses the available space down to 6px, shortens only then, and restores on expansion", async () => {
    const container = document.createElement("div");
    container.style.width = "420px";
    await using _screen = await render(<SidebarEnvironmentFilters />, { container });
    const note = container.querySelector<HTMLElement>('[role="note"]')!;
    const buttons = container.querySelector<HTMLElement>('[role="group"]')!;
    const fontSize = () => Number.parseFloat(getComputedStyle(note).fontSize);
    const fits = () => {
      expect(
        note.getBoundingClientRect().left - buttons.getBoundingClientRect().right,
      ).toBeGreaterThanOrEqual(1.9);
      expect(note.getBoundingClientRect().right).toBeLessThanOrEqual(
        container.getBoundingClientRect().right - 7.9,
      );
      expect(fontSize()).toBeGreaterThanOrEqual(6);
      expect(getComputedStyle(note).display).not.toBe("none");
    };
    expect(note.textContent).toBe("1025·0027");
    const fullWidth = note.getBoundingClientRect().width;
    const maximum = fontSize();
    const atSix = buttons.getBoundingClientRect().width + 18 + (fullWidth * 6.05) / maximum;
    container.style.width = `${atSix}px`;
    await expect.poll(fontSize).toBeLessThan(6.1);
    expect(note.textContent).toBe("1025·0027");
    fits();
    container.style.width = `${atSix - 2}px`;
    await expect.poll(() => note.textContent).toBe("0027");
    fits();
    expect(note.getAttribute("aria-label")).toContain("00:27:00 EDT");
    const filter = buttons.querySelector("button")!;
    await act(async () => filter.click());
    expect(filter.getAttribute("aria-pressed")).toBe("false");
    container.style.width = "420px";
    await expect.poll(() => note.textContent).toBe("1025·0027");
    await expect.poll(fontSize).toBe(maximum);
    fits();
  });
});
