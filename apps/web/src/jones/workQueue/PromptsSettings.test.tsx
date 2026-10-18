// @vitest-environment jsdom
import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { DEFAULT_CLIENT_SETTINGS, type ClientSettings } from "@t3tools/contracts/settings";

const storage = vi.hoisted(() => ({
  saved: null as ClientSettings | null,
  getClientSettings: vi.fn<() => Promise<ClientSettings | null>>(),
  setClientSettings: vi.fn<(settings: ClientSettings) => Promise<void>>(),
}));
vi.mock("~/localApi", () => ({ ensureLocalApi: () => ({ persistence: storage }) }));
vi.mock("../../components/settings/settingsLayout", () => ({
  SETTINGS_PICKER_TRIGGER_CLASSNAME: "",
  SettingsPageContainer: ({ children }: { children: ReactNode }) => children,
  SettingsSection: ({ children, title }: { children: ReactNode; title: string }) => (
    <section>
      <h2>{title}</h2>
      {children}
    </section>
  ),
  SettingsRow: ({ control }: { control: ReactNode }) => control,
}));
vi.mock("../../components/ui/select", () => ({
  Select: ({
    value,
    onValueChange,
    children,
  }: {
    value: string;
    onValueChange: (value: string) => void;
    children: ReactNode;
  }) => (
    <select value={value} onChange={(event) => onValueChange(event.target.value)}>
      {children}
    </select>
  ),
  SelectTrigger: () => null,
  SelectValue: () => null,
  SelectPopup: ({ children }: { children: ReactNode }) => children,
  SelectItem: ({ value, children }: { value: string; children: ReactNode }) => (
    <option value={value}>{children}</option>
  ),
}));

import { PromptsSettings } from "./PromptsSettings";
import {
  __resetClientSettingsPersistenceForTests,
  ensureClientSettingsHydrated,
} from "../../hooks/useSettings";

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  storage.saved = null;
  storage.getClientSettings.mockReset().mockImplementation(async () => storage.saved);
  storage.setClientSettings.mockReset().mockImplementation(async (settings) => {
    storage.saved = settings;
  });
  __resetClientSettingsPersistenceForTests();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  __resetClientSettingsPersistenceForTests();
  vi.unstubAllGlobals();
});

describe("PromptsSettings persistence", () => {
  it.each(["queued", "sent"] as const)(
    "saves %s and restores it after client hydration",
    async (stage) => {
      storage.saved = { ...DEFAULT_CLIENT_SETTINGS, wordWrap: false };
      await act(async () => {
        await ensureClientSettingsHydrated();
        root.render(<PromptsSettings />);
      });
      expect(container.querySelector("select")?.value).toBe("pending");
      expect(container.querySelector("h2")?.textContent).toBe("Jones Code");
      const control = container.querySelector("select")!;
      const saved = new Promise<void>((resolve) => {
        storage.setClientSettings.mockImplementationOnce(async (settings) => {
          storage.saved = settings;
          resolve();
        });
      });
      await act(async () => {
        control.value = stage;
        control.dispatchEvent(new Event("change", { bubbles: true }));
        await saved;
      });
      expect(storage.saved).toMatchObject({ promptsDefaultStage: stage, wordWrap: false });
      await act(() => root.unmount());
      __resetClientSettingsPersistenceForTests();
      root = createRoot(container);
      await act(async () => {
        await ensureClientSettingsHydrated();
        root.render(<PromptsSettings />);
      });
      expect(container.querySelector("select")?.value).toBe(stage);
    },
  );
});
