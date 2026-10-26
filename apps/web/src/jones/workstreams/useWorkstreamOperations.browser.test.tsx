import { RegistryContext } from "@effect/atom-react";
import { AtomRegistry } from "effect/reactivity";
import { useEffect } from "react";
import { act, render } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import type { EnvironmentCatalogState } from "@t3tools/client-runtime/state/connections";
import type { ServerConfig, EnvironmentId } from "@t3tools/contracts";
import { EnvironmentId as Id } from "@t3tools/contracts";
import { useWorkstreamOperations } from "./useWorkstreamOperations";
import { operationFixtureData, operationFixtureResult, setupFixture } from "./operations.fixtures";
import { WORKSTREAM_OPERATIONS_CONTRACT } from "@t3tools/contracts/jones/workstreamOperations";
const remote = Id.make("env:remote");
const primary = Id.make("env:a");
const { catalog, config } = await vi.hoisted(async () => {
  const { Atom } = await import("effect/reactivity");
  return {
    catalog: Atom.make<EnvironmentCatalogState>({ isReady: true, entries: new Map() }).pipe(
      Atom.keepAlive,
    ),
    config: Atom.family((_id: EnvironmentId) =>
      Atom.make<ServerConfig | null>(null).pipe(Atom.keepAlive),
    ),
  };
});
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
vi.mock("./environmentWorkstreamsRequest", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./environmentWorkstreamsRequest")>()),
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
let latest: ReturnType<typeof useWorkstreamOperations>;
const refresh = vi.fn();
function Mounted() {
  const operations = useWorkstreamOperations({
    data: operationFixtureData,
    primaryEnvironmentId: primary,
    refresh,
  });
  useEffect(() => {
    latest = operations;
  }, [operations]);
  return null;
}
const mount = () =>
  render(
    <RegistryContext.Provider value={registry}>
      <Mounted />
    </RegistryContext.Provider>,
  );
function setup() {
  registry = AtomRegistry.make();
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
  return {
    [Symbol.dispose]() {
      registry.dispose();
      window.localStorage.clear();
    },
  };
}
describe("capability-gated application operations", () => {
  it("keeps native request and result projection plaintext out of local storage", async () => {
    using _setup = setup();
    registry.set(config(primary), {
      environment: {
        capabilities: {
          workstreamOperations: { contract: WORKSTREAM_OPERATIONS_CONTRACT },
          workstreamsNative: {
            contract: "workstreams-native/1.0.0",
            mode: "native-home",
            reads: ["workstreams-read/1.0.0"],
            placement_proofs: ["workstreams-t3-placement/1.0.0"],
            registry: {
              registry_instance_id: setupFixture.registry.registry_id,
              owner_id: operationFixtureData.binding.ownerId,
              canonical_origin: operationFixtureData.binding.registryId,
              home_environment_id: primary,
              server_generation: operationFixtureData.binding.serverGeneration,
            },
          },
        },
      },
    } as unknown as ServerConfig);
    await using _screen = await mount();
    fixture.submit.mockImplementation(async ({ request }) => ({
      ...operationFixtureResult(),
      kind: request.kind,
      observed: null,
      result: { workstream_id: "beta", membership_id: null, registry_version: 12 },
    }));
    await act(async () => {
      await latest.create("Protected native name", {
        commandId: async () => "operation-fixture-0001",
      });
    });
    expect(fixture.submit).toHaveBeenCalledTimes(1);
    expect(window.localStorage.length).toBe(0);
    expect(latest.capability?.mode).toBe("native-home");
  });

  it("does not use operation endpoints when the capability is absent", async () => {
    using _setup = setup();
    registry.set(config(primary), null);
    registry.set(config(remote), null);
    await using _screen = await mount();
    expect(latest.supports(primary)).toBe(false);
    expect(fixture.read).not.toHaveBeenCalled();
  });
  it("routes a linked thread to its server and reloads only after a committed result", async () => {
    using _setup = setup();
    await using _screen = await mount();
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
    using _setup = setup();
    await using _screen = await mount();
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
      using _setup = setup();
      await using _screen = await mount();
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
      using _setup = setup();
      fixture.setup.mockResolvedValue({
        ...setupFixture,
        registry: { state: "not_configured", registry_id: null, owner_id: null, can_write: false },
      });
      await using _screen = await mount();
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
