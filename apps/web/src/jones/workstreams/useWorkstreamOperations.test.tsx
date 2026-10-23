// @vitest-environment jsdom
import { RegistryContext } from "@effect/atom-react";
import { Atom, AtomRegistry } from "effect/reactivity";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { beforeEach, afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { EnvironmentCatalogState } from "@t3tools/client-runtime/state/connections";
import type { ServerConfig, EnvironmentId } from "@t3tools/contracts";
import { EnvironmentId as Id } from "@t3tools/contracts";
import { useWorkstreamOperations } from "./useWorkstreamOperations";
import { operationFixtureData, operationFixtureResult, setupFixture } from "./operations.fixtures";
import { WORKSTREAM_OPERATIONS_CONTRACT } from "@t3tools/contracts/jones/workstreamOperations";
const remote = Id.make("env:remote");
const primary = Id.make("env:a");
const catalog = Atom.make<EnvironmentCatalogState>({ isReady: true, entries: new Map() }).pipe(
  Atom.keepAlive,
);
const config = Atom.family((_id: EnvironmentId) =>
  Atom.make<ServerConfig | null>(null).pipe(Atom.keepAlive),
);
const fixture = vi.hoisted(() => ({
  submit: vi.fn(),
  read: vi.fn(),
  setup: vi.fn(),
  selected: [] as string[],
}));
vi.mock("../../connection/catalog", () => ({
  environmentCatalog: {
    get catalogValueAtom() {
      return catalog;
    },
  },
}));
vi.mock("../../state/session", () => ({
  environmentSession: { initialConfigValueAtom: (id: EnvironmentId) => config(id) },
}));
vi.mock("./environmentWorkstreamsRequest", () => ({
  environmentOperationsTransport: (id: string) => ({
    submit: (...args: unknown[]) => {
      fixture.selected.push(id);
      return fixture.submit(...args);
    },
    read: fixture.read,
    setup: fixture.setup,
  }),
}));
let registry: ReturnType<typeof AtomRegistry.make>;
let root: Root;
let container: HTMLDivElement;
let latest: ReturnType<typeof useWorkstreamOperations>;
const refresh = vi.fn();
function Mounted() {
  latest = useWorkstreamOperations({
    data: operationFixtureData,
    primaryEnvironmentId: primary,
    refresh,
  });
  return null;
}
const mount = async () =>
  act(async () =>
    root.render(
      <RegistryContext.Provider value={registry}>
        <Mounted />
      </RegistryContext.Provider>,
    ),
  );
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  registry = AtomRegistry.make();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  window.localStorage.clear();
  refresh.mockReset();
  fixture.selected = [];
  fixture.read.mockReset().mockResolvedValue({ operations: [] });
  fixture.submit.mockReset().mockResolvedValue(operationFixtureResult());
  fixture.setup.mockReset().mockResolvedValue(setupFixture);
  registry.set(catalog, {
    isReady: true,
    entries: new Map([
      [primary, { enabled: true }],
      [remote, { enabled: true }],
    ]),
  } as unknown as EnvironmentCatalogState);
  for (const id of [primary, remote])
    registry.set(config(id), {
      environment: {
        capabilities: { workstreamOperations: { contract: WORKSTREAM_OPERATIONS_CONTRACT } },
      },
    } as unknown as ServerConfig);
});
afterEach(async () => {
  await act(async () => root.unmount());
  registry.dispose();
  container.remove();
  vi.unstubAllGlobals();
});
describe("capability-gated application operations", () => {
  it("does not use operation endpoints when the capability is absent", async () => {
    registry.set(config(primary), null);
    registry.set(config(remote), null);
    await mount();
    expect(latest.supports(primary)).toBe(false);
    expect(fixture.read).not.toHaveBeenCalled();
  });
  it("routes a linked thread to its server and reloads only after a committed result", async () => {
    await mount();
    let resolve!: (result: ReturnType<typeof operationFixtureResult>) => void;
    fixture.submit.mockReturnValue(
      new Promise((finish) => {
        resolve = finish;
      }),
    );
    let pending!: Promise<unknown>;
    await act(async () => {
      pending = latest.assign(
        { environmentId: remote, id: "thread" },
        "beta",
        { kind: "unassigned" },
        { commandId: async () => "operation-fixture-0001" },
      );
    });
    expect(fixture.selected).toEqual([remote]);
    expect(refresh).not.toHaveBeenCalled();
    expect(operationFixtureData.items.map((item) => item.workstreamId)).toEqual(["alpha", "beta"]);
    await act(async () => {
      resolve(operationFixtureResult());
      await pending;
    });
    expect(refresh).toHaveBeenCalled();
    expect(fixture.submit.mock.calls[0]![0].request.target.registry_id).toBe(
      "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
    );
  });
  it("keeps a capable server rejection on the operations path", async () => {
    await mount();
    fixture.submit.mockRejectedValue({ code: "operation_id_conflict" });
    await act(async () => {
      await expect(
        latest.assign(
          { environmentId: remote, id: "thread" },
          "beta",
          { kind: "any" },
          { commandId: async () => "operation-fixture-0001" },
        ),
      ).rejects.toThrow("ID conflict");
    });
    expect(fixture.submit).toHaveBeenCalledTimes(1);
    expect(refresh).not.toHaveBeenCalled();
  });
  it.each(["create", "assign", "unassigned"] as const)(
    "uses the signed UUID, not the gateway origin, for %s",
    async (action) => {
      await mount();
      fixture.submit.mockImplementation(async ({ request }) => ({
        ...operationFixtureResult(),
        kind: request.kind,
        observed: request.kind === "create_workstream" ? null : operationFixtureResult().observed,
      }));
      const options = { commandId: async () => "operation-fixture-0001" };
      await act(async () => {
        if (action === "create") await latest.create("New name", options);
        else
          await latest.assign(
            { environmentId: remote, id: "thread" },
            action === "unassigned" ? null : "beta",
            { kind: "any" },
            options,
          );
      });
      const request = fixture.submit.mock.calls[0]![0].request;
      const identity = request.kind === "create_workstream" ? request.registry : request.target;
      expect(identity.registry_id).toBe("8cdd84b6-81c5-4c88-9d13-5d324abaf662");
      expect(identity.registry_id).not.toBe(operationFixtureData.binding.registryId);
      expect(fixture.selected).toEqual([action === "create" ? primary : remote]);
    },
  );
  it.each(["create", "assign", "unassigned"] as const)(
    "does not dispatch %s without setup identity",
    async (action) => {
      fixture.setup.mockResolvedValue({
        ...setupFixture,
        registry: { state: "not_configured", registry_id: null, owner_id: null, can_write: false },
      });
      await mount();
      const options = { commandId: vi.fn(async () => "operation-fixture-0001") };
      await act(async () => {
        const result =
          action === "create"
            ? latest.create("New name", options)
            : latest.assign(
                { environmentId: remote, id: "thread" },
                action === "unassigned" ? null : "beta",
                { kind: "any" },
                options,
              );
        await expect(result).rejects.toThrow("not_configured");
      });
      expect(fixture.submit).not.toHaveBeenCalled();
      expect(options.commandId).not.toHaveBeenCalled();
    },
  );
});
