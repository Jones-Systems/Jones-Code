import { EnvironmentId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { Menu, MenuPopup, MenuTrigger } from "../../components/ui/menu";
import { DeviceDisplayNameEditor } from "./DeviceDisplayNameEditor";

const state = vi.hoisted(() => ({
  name: null as string | null,
  phase: "connected",
  supported: true,
  update: vi.fn(),
}));
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironment: () => ({
    label: "Original Mac",
    displayLabel: state.name ?? "Mini",
    connection: { phase: state.phase },
    serverConfig: {
      settings: { deviceDisplayName: state.name },
      environment: {
        platform: { machine: "mac-mini" },
        capabilities: { deviceDisplayName: state.supported },
      },
    },
  }),
}));
vi.mock("../../state/session", async (original) => ({
  ...(await original<typeof import("../../state/session")>()),
  useEnvironmentScope: () => true,
}));
vi.mock("../../hooks/useSettings", async (original) => ({
  ...(await original<typeof import("../../hooks/useSettings")>()),
  useUpdateEnvironmentSettings: () => state.update,
}));

it("saves and resets through Settings, reads back the shared name, and locks offline/older servers", async () => {
  state.name = null;
  state.phase = "connected";
  state.supported = true;
  state.update.mockReset().mockImplementation((patch, callbacks) => {
    state.name = patch.deviceDisplayName;
    callbacks.onSaved();
  });
  const ui = () => (
    <Menu open>
      <MenuTrigger>Device actions</MenuTrigger>
      <MenuPopup>
        <DeviceDisplayNameEditor environmentId={EnvironmentId.make("synthetic-mini")} />
      </MenuPopup>
    </Menu>
  );
  await using screen = await render(ui());
  const trigger = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].find(
    (node) => node.textContent === "Device display name",
  )!;
  await act(() => trigger.focus());
  await userEvent.keyboard("{ArrowRight}");
  await expect.element(page.getByRole("textbox", { name: "Device display name" })).toBeVisible();
  const input = () =>
    document.querySelector<HTMLInputElement>('input[aria-label="Device display name"]')!;
  await userEvent.fill(input(), "Office");
  await userEvent.keyboard("{Enter}");
  expect(state.update.mock.calls[0]?.[0]).toEqual({ deviceDisplayName: "Office" });
  await screen.rerender(ui());
  expect(input().value).toBe("Office");
  const reset = [...document.querySelectorAll<HTMLButtonElement>("button")].find(
    (node) => node.textContent === "Reset to automatic",
  )!;
  await userEvent.click(reset);
  expect(state.update.mock.calls[1]?.[0]).toEqual({ deviceDisplayName: null });
  state.phase = "offline";
  await screen.rerender(ui());
  expect(input().disabled).toBe(true);
  expect(document.body.textContent).toContain("Connect to this environment");
  state.phase = "connected";
  state.supported = false;
  await screen.rerender(ui());
  expect(input().disabled).toBe(true);
  expect(document.body.textContent).toContain("does not support device display names");
});
