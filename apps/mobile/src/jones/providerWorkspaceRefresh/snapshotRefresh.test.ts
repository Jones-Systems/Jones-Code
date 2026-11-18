import { afterEach, expect, it, vi } from "vite-plus/test";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { EnvironmentId, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";

const harness = vi.hoisted(() => ({ state: null as unknown }));
vi.mock("../../state/server", () => ({
  serverEnvironment: {
    providersValueAtom: () => (harness.state as { atom: unknown }).atom,
    refreshProviders: {
      label: "refresh",
      run: (...args: unknown[]) =>
        (harness.state as { run: (...args: unknown[]) => unknown }).run(...args),
    },
  },
}));
import { mobileProviderWorkspaceSnapshotRefresh } from "./snapshotRefresh";

afterEach(() => vi.useRealTimers());

it("shares models-only pending discovery between composer and picker and accepts omitted reply providers", async () => {
  const checkedAt = "2026-11-16T20:00:00.000Z";
  vi.useFakeTimers({ now: Date.parse(checkedAt) });
  const environmentId = EnvironmentId.make("mobile-refresh-test");
  const instanceId = ProviderInstanceId.make("mobile-refresh-provider");
  const snapshot = {
    cwd: "/project",
    checkedAt,
    modelsPending: true,
    models: [],
    slashCommands: [],
    skills: [],
  };
  const provider = {
    instanceId,
    driver: "codex",
    enabled: true,
    installed: true,
    status: "ready",
    models: [],
    slashCommands: [],
    skills: [],
    workspaceSnapshots: [snapshot],
  } as unknown as ServerProvider;
  const atom = Atom.make<ReadonlyArray<ServerProvider> | null>([provider]);
  const registry = AtomRegistry.make();
  const run = vi.fn(async () => {
    registry.set(atom, [
      { ...provider, workspaceSnapshots: [{ ...snapshot, modelsPending: false }] },
    ]);
    return AsyncResult.success({});
  });
  harness.state = { atom, run };
  const target = { environmentId, instanceId, cwd: "/project" };
  const composer = registry.mount(mobileProviderWorkspaceSnapshotRefresh(target));
  const picker = registry.mount(mobileProviderWorkspaceSnapshotRefresh({ ...target }));
  try {
    await vi.advanceTimersByTimeAsync(0);
    expect(run).toHaveBeenCalledTimes(1);
    expect(run).toHaveBeenCalledWith(registry, {
      environmentId,
      input: { instanceId, cwd: "/project" },
    });
    await vi.advanceTimersByTimeAsync(10_000);
    expect(run).toHaveBeenCalledTimes(1);
  } finally {
    picker();
    composer();
    registry.dispose();
  }
});
