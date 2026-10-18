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
  update: vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => state.environment.serverConfig }));
vi.mock("../../state/session", () => ({
  environmentSession: { initialConfigValueAtom: () => "initial-config" },
}));
vi.mock("../../state/presentation", () => ({
  useEnvironmentPresentation: () => ({ isReady: true }),
}));
vi.mock("../../state/environments", () => ({ usePrimaryEnvironment: () => state.environment }));
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
  state.update.mockReset();
  state.update.mockImplementation(
    async (request: { input: { patch: { workModeEnabled: boolean } } }) => ({
      _tag: "Success",
      value: request.input.patch,
    }),
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
  it("changes only the primary environment and renders its persisted state on remount", async () => {
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
      expect(reloaded.root.findByType("button").props["aria-checked"]).toBe(true);
    } finally {
      await dispose(reloaded);
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
