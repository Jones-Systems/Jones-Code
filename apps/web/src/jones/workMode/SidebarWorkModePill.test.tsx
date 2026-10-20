import { act, type ReactElement, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { SidebarWorkModePill } from "./SidebarWorkModePill";

const state = vi.hoisted(() => ({
  environment: {
    environmentId: "work-mode-test-environment",
    label: "Test VPS",
    entry: { enabled: true },
    connection: { phase: "connected" },
    serverConfig: {
      environment: { capabilities: { workMode: true } },
      settings: { workModeEnabled: false },
    },
  },
  environments: [] as Array<{
    environmentId: string;
    label: string;
    entry: { enabled: boolean };
    connection: { phase: string };
    serverConfig: {
      environment: { capabilities: { workMode: boolean } };
      settings: { workModeEnabled: boolean };
    } | null;
  }>,
  initialConfigs: new Map<string, unknown>(),
  access: vi.fn(),
  update: vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => state.initialConfigs }));
vi.mock("../../state/session", () => ({
  environmentSession: { initialConfigValueAtom: () => "initial-config" },
  readEnvironmentScopeAccess: (...args: unknown[]) => state.access(...args),
}));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ isReady: true, environments: state.environments }),
}));
vi.mock("../../state/server", () => ({ serverEnvironment: { updateSettings: "update-settings" } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => state.update }));
vi.mock("../../components/ui/tooltip", () => ({
  Tooltip: ({ children }: { children: ReactNode }) => children,
  TooltipTrigger: ({ render }: { render: ReactElement }) => render,
  TooltipPopup: ({ children }: { children: ReactNode }) => children,
}));

beforeEach(() => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  state.environment.connection.phase = "connected";
  state.environment.serverConfig.environment.capabilities.workMode = true;
  state.environment.serverConfig.settings.workModeEnabled = false;
  state.environments = [state.environment];
  state.initialConfigs = new Map([
    [state.environment.environmentId, state.environment.serverConfig],
  ]);
  state.access.mockReset().mockReturnValue({ status: "granted" });
  state.update.mockReset();
  state.update.mockImplementation(
    async (request: { environmentId: string; input: { patch: { workModeEnabled: boolean } } }) => {
      const target = state.environments.find(
        (environment) => environment.environmentId === request.environmentId,
      );
      if (target?.serverConfig) target.serverConfig.settings = { ...request.input.patch };
      return { _tag: "Success", value: request.input.patch };
    },
  );
});

async function mount() {
  let renderer!: ReactTestRenderer;
  await act(async () => {
    renderer = create(<SidebarWorkModePill />);
  });
  return renderer;
}
async function dispose(renderer: ReactTestRenderer) {
  await act(async () => {
    renderer.unmount();
  });
}

describe("Work mode sidebar interaction", () => {
  it("changes connected environments and renders their persisted state on remount", async () => {
    const renderer = await mount();
    try {
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(false);
      await act(async () => {
        renderer.root.findByType("button").props.onClick();
      });
      expect(state.update).toHaveBeenCalledWith({
        environmentId: "work-mode-test-environment",
        input: { patch: { workModeEnabled: true } },
      });
      state.environment = {
        ...state.environment,
        serverConfig: { ...state.environment.serverConfig, settings: { workModeEnabled: true } },
      };
      state.environments = [state.environment];
      await act(async () => {
        renderer.update(<SidebarWorkModePill />);
      });
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(true);
      await act(async () => {
        renderer.root.findByType("button").props.onClick();
      });
      expect(state.update).toHaveBeenLastCalledWith({
        environmentId: "work-mode-test-environment",
        input: { patch: { workModeEnabled: false } },
      });
    } finally {
      await dispose(renderer);
    }
    const reloaded = await mount();
    try {
      expect(reloaded.root.findByType("button").props["aria-checked"]).toBe(false);
    } finally {
      await dispose(reloaded);
    }
  });

  it("enables every connected target even when the laptop is already on", async () => {
    state.environment.serverConfig.settings.workModeEnabled = true;
    const vps = structuredClone(state.environment);
    vps.environmentId = "vps";
    vps.label = "VPS";
    vps.serverConfig.settings.workModeEnabled = false;
    const mini = structuredClone(vps);
    mini.environmentId = "mini";
    mini.label = "Mac Mini";
    state.environments.push(vps, mini);
    state.initialConfigs.set("vps", vps.serverConfig);
    state.initialConfigs.set("mini", mini.serverConfig);
    const renderer = await mount();
    try {
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(false);
      await act(async () => renderer.root.findByType("button").props.onClick());
      expect(state.update.mock.calls.map(([request]) => request.environmentId).sort()).toEqual([
        "mini",
        "vps",
      ]);
      expect(
        state.environments.every((target) => target.serverConfig?.settings.workModeEnabled),
      ).toBe(true);
      await act(async () => renderer.update(<SidebarWorkModePill />));
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(true);
      state.update.mockClear();
      await act(async () => renderer.root.findByType("button").props.onClick());
      expect(state.update).toHaveBeenCalledTimes(3);
      expect(
        state.environments.every(
          (target) => target.serverConfig?.settings.workModeEnabled === false,
        ),
      ).toBe(true);
    } finally {
      await dispose(renderer);
    }
  });

  it("finishes a partially failed disable without turning successful targets back on", async () => {
    state.environment.serverConfig.settings.workModeEnabled = true;
    const vps = structuredClone(state.environment);
    vps.environmentId = "vps";
    vps.label = "VPS";
    state.environments.push(vps);
    state.initialConfigs.set("vps", vps.serverConfig);
    const save = state.update.getMockImplementation()!;
    state.update.mockImplementation(async (request) =>
      request.environmentId === "vps" ? { _tag: "Failure" } : save(request),
    );
    const renderer = await mount();
    try {
      await act(async () => renderer.root.findByType("button").props.onClick());
      expect(state.environment.serverConfig.settings.workModeEnabled).toBe(false);
      expect(vps.serverConfig.settings.workModeEnabled).toBe(true);
      expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain("VPS");
      state.update.mockImplementation(save);
      state.update.mockClear();
      await act(async () => renderer.root.findByType("button").props.onClick());
      expect(state.update).toHaveBeenCalledExactlyOnceWith({
        environmentId: "vps",
        input: { patch: { workModeEnabled: false } },
      });
      expect(
        state.environments.every(
          (target) => target.serverConfig?.settings.workModeEnabled === false,
        ),
      ).toBe(true);
    } finally {
      await dispose(renderer);
    }
  });

  it.each(["offline", "disabled", "loading", "unsupported"])(
    "updates ready targets while naming an %s target it cannot update",
    async (kind) => {
      const unavailable = structuredClone(state.environment);
      unavailable.environmentId = "unavailable";
      unavailable.label = "Unavailable server";
      state.environments.push(unavailable);
      if (kind === "offline") unavailable.connection.phase = "disconnected";
      if (kind === "disabled") unavailable.entry.enabled = false;
      if (kind === "unsupported")
        unavailable.serverConfig.environment.capabilities.workMode = false;
      if (kind !== "loading")
        state.initialConfigs.set(unavailable.environmentId, unavailable.serverConfig);
      const renderer = await mount();
      try {
        await act(async () => renderer.root.findByType("button").props.onClick());
        expect(state.update).toHaveBeenCalledExactlyOnceWith({
          environmentId: state.environment.environmentId,
          input: { patch: { workModeEnabled: true } },
        });
        expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain(
          "Unavailable server",
        );
        expect(unavailable.serverConfig.settings.workModeEnabled).toBe(false);
      } finally {
        await dispose(renderer);
      }
    },
  );

  it.each(["scope_missing", "signed_out"])(
    "reports a %s target without writing to it",
    async (status) => {
      state.access.mockReturnValue({ status });
      const renderer = await mount();
      try {
        await act(async () => renderer.root.findByType("button").props.onClick());
        expect(state.update).not.toHaveBeenCalled();
        expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain(
          state.environment.label,
        );
      } finally {
        await dispose(renderer);
      }
    },
  );

  it("rejects a success response that does not contain the requested saved value", async () => {
    state.update.mockResolvedValue({ _tag: "Success", value: { workModeEnabled: false } });
    const renderer = await mount();
    try {
      await act(async () => renderer.root.findByType("button").props.onClick());
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(false);
      expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain(
        "Could not save",
      );
    } finally {
      await dispose(renderer);
    }
  });

  it("retains a failed target when it disconnects before a retry", async () => {
    state.update.mockRejectedValueOnce(new Error("connection lost"));
    const renderer = await mount();
    try {
      await act(async () => renderer.root.findByType("button").props.onClick());
      state.environment.connection.phase = "disconnected";
      const vps = structuredClone(state.environment);
      vps.environmentId = "vps";
      vps.label = "VPS";
      vps.connection.phase = "connected";
      state.environments.push(vps);
      state.initialConfigs.set(vps.environmentId, vps.serverConfig);
      await act(async () => renderer.update(<SidebarWorkModePill />));
      state.update.mockClear();
      await act(async () => renderer.root.findByType("button").props.onClick());
      expect(state.update).not.toHaveBeenCalled();
      expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain(
        state.environment.label,
      );
    } finally {
      await dispose(renderer);
    }
  });

  it.each(["offline", "unsupported"])("does not write for an %s target", async (kind) => {
    if (kind === "offline") state.environment.connection.phase = "disconnected";
    else state.environment.serverConfig.environment.capabilities.workMode = false;
    const renderer = await mount();
    try {
      expect(renderer.root.findByType("button").props.disabled).toBe(true);
      await act(async () => {
        renderer.root.findByType("button").props.onClick();
      });
      expect(state.update).not.toHaveBeenCalled();
    } finally {
      await dispose(renderer);
    }
  });

  it("releases the save latch after a rejected request so the owner can retry", async () => {
    state.update.mockRejectedValueOnce(new Error("disconnected"));
    const renderer = await mount();
    try {
      await act(async () => {
        renderer.root.findByType("button").props.onClick();
      });
      expect(renderer.root.findByType("button").props["aria-busy"]).toBe(false);
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(false);
      expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain(
        "Could not save",
      );
      await act(async () => {
        renderer.root.findByType("button").props.onClick();
      });
      expect(state.update).toHaveBeenCalledTimes(2);
      expect(renderer.root.findByType("button").props["aria-busy"]).toBe(false);
      expect(renderer.root.findByProps({ role: "status" }).children.join("")).not.toContain(
        "Could not save",
      );
    } finally {
      await dispose(renderer);
    }
  });

  it("suppresses duplicate clicks while saving and reports failed saves without changing state", async () => {
    let finish!: (value: unknown) => void;
    state.update.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const renderer = await mount();
    try {
      await act(async () => {
        const button = renderer.root.findByType("button");
        button.props.onClick();
        button.props.onClick();
      });
      expect(state.update).toHaveBeenCalledTimes(1);
      expect(renderer.root.findByType("button").props["aria-busy"]).toBe(true);
      await act(async () => {
        finish({ _tag: "Failure" });
      });
      expect(renderer.root.findByType("button").props["aria-checked"]).toBe(false);
      expect(renderer.root.findByProps({ role: "status" }).children.join("")).toContain(
        "Could not save",
      );
    } finally {
      await dispose(renderer);
    }
  });
});
