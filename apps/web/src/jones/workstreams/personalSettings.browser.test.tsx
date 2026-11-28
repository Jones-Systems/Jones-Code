import "~/index.css";
import { RegistryProvider } from "@effect/atom-react";
import { describe, expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
const fixture = vi.hoisted(() => ({
  statuses: ["ready", "offline", "update_required", "read_only", "sync_failed"],
  fleetEnabled: false,
  cancelOperation: vi.fn(async () => ({ ok: true })),
}));
vi.mock("../../state/workstreamsPersonal", () => ({
  usePersonalWorkstreams: () => ({
    controller: fixture.fleetEnabled
      ? { manageFleet: vi.fn(), cancelOperation: fixture.cancelOperation }
      : undefined,
    state: {
      workstreams: [
        { id: "alpha", name: "Alpha" },
        { id: "beta", name: "Beta" },
      ],
      fleetAuthorityStatus: "ready",
      fleet: fixture.fleetEnabled
        ? {
            role: "authority",
            authorityEnvironmentId: "env-0",
            rosterRevision: 1,
            members: ["env-1"],
            operations: [
              {
                requestId: "close-alpha",
                workstreamId: "alpha",
                kind: "close",
                status: "pending",
                peers: [{ environmentId: "env-1", state: "blocked", reason: "peer_unavailable" }],
              },
              {
                requestId: "close-beta",
                workstreamId: "beta",
                kind: "close",
                status: "applied",
                peers: [],
              },
            ],
          }
        : undefined,
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

  it("names the waiting environment and lets the owner cancel a pending close", async () => {
    fixture.fleetEnabled = true;
    try {
      await using screen = await render(
        <RegistryProvider>
          <WorkstreamsSetupStatus />
        </RegistryProvider>,
      );
      await expect.element(screen.getByText("Alpha · Close requested")).toBeInTheDocument();
      await expect
        .element(screen.getByText("Synthetic 1 · Waiting for the environment to respond"))
        .toBeInTheDocument();
      await expect.element(screen.getByText("Beta · Close requested")).not.toBeInTheDocument();
      await screen.getByRole("button", { name: "Cancel close request" }).click();
      expect(fixture.cancelOperation).toHaveBeenCalledWith("close-alpha");
    } finally {
      fixture.fleetEnabled = false;
    }
  });
});
