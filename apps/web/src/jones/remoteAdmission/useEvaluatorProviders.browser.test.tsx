import { RegistryProvider, useAtomValue } from "@effect/atom-react";
import { EnvironmentId, ProviderInstanceId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { render } from "../../../test/render";
import { TooltipProvider } from "../../components/ui/tooltip";
import { serverEnvironment } from "../../state/server";
import { RemoteAdmissionSettings } from "./RemoteAdmissionSettings";
import { admissionSnapshot } from "./testFixtures";
import "../../index.css";

const request = vi.hoisted(() => vi.fn());
vi.mock("./runtime", () => ({ remoteAdmissionRequest: request }));
vi.mock("../../state/environments", () => ({
  useEnvironment: () => ({ label: "Synthetic receiver", connection: { phase: "connected" } }),
  useEnvironmentIds: () => [],
  useEnvironments: () => ({ environments: [], isReady: true }),
}));
vi.mock("../../hooks/useSettings", async () => {
  const { DEFAULT_UNIFIED_SETTINGS } = await import("@t3tools/contracts/settings");
  return {
    useEnvironmentSettings: () => ({
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        "synthetic-claude": { driver: "claudeAgent", enabled: true },
        "synthetic-codex": { driver: "codex", enabled: true },
      },
    }),
    useEnvironmentModelPreferences: () => ({
      favorites: [{ provider: "synthetic-codex", model: "gpt-6.1-sol" }],
    }),
    useToggleEnvironmentModelFavorite: () => vi.fn(),
  };
});
vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  const Stream = await import("effect/Stream");
  const { ProviderDriverKind, ProviderInstanceId } = await import("@t3tools/contracts");
  const { DEFAULT_RESOLVED_KEYBINDINGS } = await import("@t3tools/shared/keybindings");
  const providers = [
    {
      instanceId: ProviderInstanceId.make("synthetic-claude"),
      driver: ProviderDriverKind.make("claudeAgent"),
      displayName: "Synthetic Claude",
      model: "claude-opus-5-5",
      name: "Opus",
    },
    {
      instanceId: ProviderInstanceId.make("synthetic-codex"),
      driver: ProviderDriverKind.make("codex"),
      displayName: "Synthetic Codex",
      model: "gpt-6.1-sol",
      name: "Sol",
    },
  ].map(({ model, name, ...provider }) => ({
    ...provider,
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-24T12:00:00Z",
    supportsRemoteRequestEvaluation: true,
    models: [{ slug: model, name, isCustom: false, capabilities: {}, isDefault: true }],
    slashCommands: [],
    skills: [],
  }));
  const projection = Atom.make(
    Stream.concat(Stream.make({ source: "live", config: { providers } }), Stream.never),
  );
  return {
    serverEnvironment: { configProjection: () => projection },
    EMPTY_SERVER_PROVIDERS: [],
    primaryServerKeybindingsAtom: Atom.make(DEFAULT_RESOLVED_KEYBINDINGS),
  };
});

const environmentId = EnvironmentId.make("evaluator-open-config-stream");

function SubscriptionProbe() {
  const projection = useAtomValue(serverEnvironment.configProjection({ environmentId, input: {} }));
  return <output>{projection.waiting ? "Configuration subscription open" : ""}</output>;
}

it("selects and saves a supported Codex evaluator while the configuration subscription remains open", async () => {
  const snapshot = {
    ...admissionSnapshot([]),
    policy: {
      enabled: true,
      revision: "synthetic-policy",
      modelSelection: {
        instanceId: ProviderInstanceId.make("synthetic-claude"),
        model: "claude-opus-5-5",
      },
    },
  };
  request.mockReset().mockResolvedValue(snapshot);
  await using screen = await render(
    <RegistryProvider>
      <TooltipProvider>
        <SubscriptionProbe />
        <RemoteAdmissionSettings environmentId={environmentId} />
      </TooltipProvider>
    </RegistryProvider>,
  );
  await expect.element(screen.getByText("Configuration subscription open")).toBeVisible();
  await expect
    .element(screen.getByRole("button", { name: "Evaluator model", exact: true }))
    .toBeEnabled();
  await userEvent.click(screen.getByRole("button", { name: "Evaluator model", exact: true }));
  await userEvent.click(page.getByRole("option", { name: /Sol/ }));
  await userEvent.click(screen.getByRole("button", { name: "Save model", exact: true }));
  await expect
    .poll(() => request.mock.calls)
    .toContainEqual([
      environmentId,
      {
        action: "policy",
        input: {
          enabled: true,
          expectedRevision: "synthetic-policy",
          modelSelection: {
            instanceId: ProviderInstanceId.make("synthetic-codex"),
            model: "gpt-6.1-sol",
          },
        },
      },
    ]);
});
