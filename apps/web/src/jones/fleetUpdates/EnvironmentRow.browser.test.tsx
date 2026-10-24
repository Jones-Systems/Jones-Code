import { render } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { EnvironmentId } from "@t3tools/contracts";
import { EnvironmentRow } from "../../components/settings/EnvironmentRow";
import { JonesBlockedHostUpdates } from "./JonesBlockedHostUpdates";
import "../../index.css";

const { execute } = vi.hoisted(() => ({ execute: vi.fn() }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => execute }));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/runtime", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/state/runtime")>()),
  createRuntimeCommand: () => ({}),
}));
vi.mock("./blockedHostCommand", () => ({ executeBlockedHostUpdate: {} }));

describe("environment row control layout", () => {
  it.each([false, true])(
    "keeps multiline update controls above the subtitle and clickable (historical=%s)",
    async (historical) => {
      await page.viewport(1280, 900);
      execute.mockReset();
      execute.mockResolvedValue({
        _tag: "Success",
        value: {
          currency: "current",
          state: {
            phase: "blocked",
            message: "This host requires an update.",
            capability: { check: true, download: false, install: false },
          },
        },
      });
      const environmentId = EnvironmentId.make("synthetic-layout-host");
      const container = document.createElement("div");
      container.style.width = "720px";
      container.style.padding = "40px";
      await using screen = await render(
        <EnvironmentRow
          kind="server"
          label="Synthetic incompatible Jones host"
          subtitle={<span className="block">Saved connection is switched off</span>}
          below={
            <p className="text-xs">
              Reconnect when the host supports this app. The saved connection can remain switched
              off while its qualified update support is inspected.
            </p>
          }
        >
          <JonesBlockedHostUpdates environmentId={environmentId} historical={historical} />
        </EnvironmentRow>,
        { container, router: false },
      );
      const check = screen.getByRole("button", { name: "Check for builds", exact: true });
      await expect.element(check).toBeEnabled();
      if (historical) {
        await expect
          .element(
            screen.getByText(
              "Previously required an update. The saved connection remains switched off.",
            ),
          )
          .toBeVisible();
      }
      const button = check.element();
      const rect = button.getBoundingClientRect();
      const hit = document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2);
      expect(hit === button || (hit !== null && button.contains(hit))).toBe(true);
      await check.click();
      expect(execute.mock.calls.filter(([input]) => input.request.action === "check")).toEqual([
        [{ environmentId, request: { action: "check" } }],
      ]);
      const controls = button.parentElement!.parentElement!;
      const subtitle = screen
        .getByText("Saved connection is switched off", { exact: true })
        .element();
      expect(subtitle.getBoundingClientRect().top).toBeGreaterThanOrEqual(
        controls.getBoundingClientRect().bottom,
      );
    },
  );

  it("retains the compact name-line minimum for short header controls", async () => {
    await page.viewport(1280, 900);
    await using screen = await render(
      <EnvironmentRow
        kind="server"
        label="Compact host"
        subtitle="Saved connection"
        below={<p>Connection details</p>}
      >
        <span data-testid="compact-control" className="block size-3" />
      </EnvironmentRow>,
      { router: false },
    );
    const controls = screen.getByTestId("compact-control").element().parentElement!;
    const name = screen.getByText("Compact host", { exact: true }).element();
    expect(controls.getBoundingClientRect().height).toBe(name.getBoundingClientRect().height);
    expect(controls.getBoundingClientRect().height).toBe(
      Number.parseFloat(getComputedStyle(document.documentElement).fontSize) * 1.25,
    );
  });
});
