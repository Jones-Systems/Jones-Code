import "~/index.css";
import { RegistryProvider } from "@effect/atom-react";
import { describe, expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
const fixture = vi.hoisted(() => ({
  statuses: ["ready", "offline", "update_required", "read_only", "sync_failed"],
}));
vi.mock("../../state/workstreamsPersonal", () => ({
  usePersonalWorkstreams: () => ({
    state: {
      environments: fixture.statuses.map((status, index) => ({
        environmentId: `env-${index}`,
        status,
      })),
    },
  }),
}));
vi.mock("../../state/environments", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/environments")>()),
  useEnvironments: () => ({
    environments: fixture.statuses.map((_, index) => ({
      environmentId: `env-${index}`,
      label: `Synthetic ${index}`,
    })),
  }),
  usePrimaryEnvironmentId: () => null,
  usePrimaryEnvironment: () => null,
}));
vi.mock("../../state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/session")>()),
  useEnvironmentScope: () => false,
  useEnvironmentsWithScope: () => new Set(),
}));
vi.mock("../../hooks/useSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useSettings")>()),
  usePrimarySettingsAvailable: () => true,
}));
vi.mock("../../components/settings/useScopedSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../components/settings/useScopedSettings")>()),
  useScopedSettingsMixed: () => false,
  useClearProjectOverrides: () => () => {},
  useClearScopedSettings: () => () => {},
}));
vi.mock("../../components/settings/SettingsScopeSentence", () => ({
  SettingsScopeSentence: () => null,
}));
import { WorkstreamsSetupStatus } from "./WorkstreamsSetupStatus";
describe("personal Workstreams settings", () => {
  it("uses the normal grouped card and one plain status row per environment", async () => {
    await using screen = await render(
      <RegistryProvider>
        <WorkstreamsSetupStatus />
      </RegistryProvider>,
    );
    const section = screen.container.querySelector("section")!;
    expect(section.textContent).toContain("Workstreams");
    const rows = section.querySelectorAll('[data-slot="settings-row"]');
    expect(rows.length).toBe(5);
    expect([...rows].map((row) => row.textContent)).toEqual([
      "Synthetic 0Ready",
      "Synthetic 1Offline",
      "Synthetic 2Update required",
      "Synthetic 3Read only",
      "Synthetic 4Couldn't sync",
    ]);
    expect(rows[0]?.parentElement?.className).toContain("rounded-xl");
    expect(rows[0]?.parentElement?.className).toContain("border");
    expect(section.querySelectorAll("button,a").length).toBe(0);
  });
});
