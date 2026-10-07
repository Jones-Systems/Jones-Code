// @vitest-environment jsdom
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS, type ClientSettings } from "@t3tools/contracts/settings";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { deriveProviderInstanceEntries } from "../../providerInstances";
import { TooltipProvider } from "../ui/tooltip";
import { ProviderModelPicker } from "./ProviderModelPicker";

let favorites: ClientSettings["favorites"] = [];

vi.mock("~/hooks/useSettings", () => ({
  useClientSettings: (select: (settings: ClientSettings) => unknown) =>
    select({ ...DEFAULT_CLIENT_SETTINGS, favorites }),
  useUpdateClientSettings: () => vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => [] }));
vi.mock("../../state/server", () => ({ primaryServerKeybindingsAtom: {} }));
vi.mock("./ChatGptSharingControl", () => ({ ChatGptSharingControl: () => null }));

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
  favorites = [];
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

it.each([
  {
    name: "new chat without favorites",
    saved: false,
    locked: false,
    unavailable: false,
    setup: false,
  },
  {
    name: "existing chat with favorites",
    saved: true,
    locked: false,
    unavailable: false,
    setup: false,
  },
  { name: "locked conversation", saved: true, locked: true, unavailable: false, setup: false },
  {
    name: "unavailable selected model",
    saved: true,
    locked: false,
    unavailable: true,
    setup: false,
  },
  { name: "provider needing setup", saved: true, locked: false, unavailable: false, setup: true },
])("opens Favorites on every opening for $name", async ({ saved, locked, unavailable, setup }) => {
  const instanceId = ProviderInstanceId.make("opencode");
  const provider: ServerProvider = {
    instanceId,
    driver: ProviderDriverKind.make("opencode"),
    enabled: true,
    installed: true,
    version: null,
    status: unavailable || setup ? "error" : "ready",
    auth: { status: setup ? "unauthenticated" : "authenticated" },
    ...(setup ? { setup: { canAuthenticate: true, canInstall: false } } : {}),
    checkedAt: "2026-10-01T00:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  };
  favorites = saved ? [{ provider: instanceId, model: "test-model" }] : [];
  const entries = deriveProviderInstanceEntries([provider]);
  const onInstanceModelChange = vi.fn();
  const render = async (open: boolean) => {
    await act(async () =>
      root.render(
        <TooltipProvider>
          <ProviderModelPicker
            open={open}
            activeInstanceId={instanceId}
            model="test-model"
            lockedProvider={locked ? provider.driver : null}
            instanceEntries={entries}
            modelOptionsByInstance={
              new Map([
                [
                  instanceId,
                  [{ slug: "test-model", name: "Test model", isUnavailable: unavailable }],
                ],
              ])
            }
            onInstanceModelChange={onInstanceModelChange}
            {...(setup ? { onOpenProviderSetup: vi.fn() } : {})}
          />
        </TooltipProvider>,
      ),
    );
  };
  const favoritesButton = () =>
    document.querySelector<HTMLButtonElement>('button[aria-label="Favorites"]');
  const providerButton = () =>
    document.querySelector<HTMLButtonElement>('[data-model-picker-provider="opencode"] button');

  await render(true);
  expect(favoritesButton()?.getAttribute("aria-pressed")).toBe("true");
  await act(async () => providerButton()!.click());
  expect(providerButton()?.getAttribute("aria-pressed")).toBe("true");
  await render(false);
  await render(true);
  expect(favoritesButton()?.getAttribute("aria-pressed")).toBe("true");
  expect(onInstanceModelChange).not.toHaveBeenCalled();
});
