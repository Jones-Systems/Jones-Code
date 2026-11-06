import { useRef, useState } from "react";
import { render } from "../../../test/render";
import { expect, it } from "vite-plus/test";
import { useHeaderControlsClearance } from "./useHeaderControlsClearance";

it("keeps telemetry clear as fixed controls grow, the header narrows, and controls move to another panel", async () => {
  function Fixture() {
    const headerRef = useRef<HTMLDivElement>(null);
    const [controls, setControls] = useState<HTMLDivElement | null>(null);
    const clearance = useHeaderControlsClearance(headerRef, controls, 0);
    return (
      <div data-workspace style={{ position: "relative", width: 640 }}>
        <div
          ref={headerRef}
          data-header
          style={{ display: "flex", boxSizing: "border-box", paddingRight: clearance, height: 48 }}
        >
          <span style={{ flex: "none", width: 120 }}>Thread title</span>
          <div data-status style={{ flex: 1, minWidth: 0, overflow: "hidden" }}>
            Laptop · 46% · 2.0 mb/s
          </div>
        </div>
        <div
          ref={setControls}
          data-controls
          style={{ position: "absolute", top: 0, right: 0, display: "flex", height: 48, gap: 4 }}
        >
          {["Monitor", "Details", "Terminal", "Add"].map((name) => (
            <button key={name} style={{ width: 32 }}>
              {name}
            </button>
          ))}
        </div>
      </div>
    );
  }
  const container = document.createElement("div");
  await using _screen = await render(<Fixture />, { container, router: false });
  const workspace = container.querySelector<HTMLElement>("[data-workspace]")!;
  const header = container.querySelector<HTMLElement>("[data-header]")!;
  const controls = container.querySelector<HTMLElement>("[data-controls]")!;
  const status = container.querySelector<HTMLElement>("[data-status]")!;
  const gap = () => controls.getBoundingClientRect().left - status.getBoundingClientRect().right;
  await expect.poll(gap).toBe(8);
  const extra = document.createElement("button");
  extra.style.width = "32px";
  extra.textContent = "Maximize";
  controls.append(extra);
  await expect.poll(gap).toBe(8);
  workspace.style.width = "360px";
  await expect.poll(gap).toBe(8);
  expect(status.getBoundingClientRect().width).toBeGreaterThan(0);
  header.style.width = "180px";
  await expect.poll(() => header.style.paddingRight).toBe("0px");
  header.style.width = "100%";
  extra.remove();
  workspace.style.width = "640px";
  await expect.poll(gap).toBe(8);
});
