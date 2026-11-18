import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import "~/index.css";
import { WorkstreamAutoRouteSettings } from "./WorkstreamAutoRouteSettings";

const state = vi.hoisted(() => ({ enabled: true, update: vi.fn() }));
vi.mock("~/components/settings/useScopedSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/components/settings/useScopedSettings")>()),
  useScopedSettings: (select: (settings: { workstreamAutoRouteEnabled: boolean }) => unknown) =>
    select({ workstreamAutoRouteEnabled: state.enabled }),
  useScopedSettingsMixed: () => false,
  useUpdateScopedSettings: () => state.update,
}));

// A writable primary environment, so the server-scoped row is interactive.
vi.mock("~/state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/state/session")>()),
  useEnvironmentScope: () => true,
}));
vi.mock("~/hooks/useSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/hooks/useSettings")>()),
  usePrimarySettingsAvailable: () => true,
}));

const name = "Assign queued prompts to a Workstream automatically";

it("defaults on and writes the off value", async () => {
  expect(DEFAULT_SERVER_SETTINGS.workstreamAutoRouteEnabled).toBe(true);
  state.enabled = true;
  state.update.mockClear();
  await using screen = await render(<WorkstreamAutoRouteSettings />);
  const toggle = screen.getByRole("switch", { name });
  await expect.element(toggle).toBeChecked();
  await expect
    .element(screen.getByRole("button", { name: "Reset Workstream routing to default" }))
    .not.toBeInTheDocument();
  await toggle.click();
  expect(state.update).toHaveBeenCalledWith({ workstreamAutoRouteEnabled: false });
});

it("offers reset to the default when switched off", async () => {
  state.enabled = false;
  state.update.mockClear();
  await using screen = await render(<WorkstreamAutoRouteSettings />);
  await expect.element(screen.getByRole("switch", { name })).not.toBeChecked();
  await screen.getByRole("button", { name: "Reset Workstream routing to default" }).click();
  expect(state.update).toHaveBeenCalledWith({ workstreamAutoRouteEnabled: true });
});
