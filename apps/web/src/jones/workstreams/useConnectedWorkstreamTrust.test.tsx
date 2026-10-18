import { RegistryContext } from "@effect/atom-react";
import type { ConnectionCatalogEntry } from "@t3tools/client-runtime/connection";
import type { EnvironmentCatalogState } from "@t3tools/client-runtime/state/connections";
import type { ConnectedEnvironmentTrust } from "@t3tools/client-runtime/state/workstreams";
import {
  EnvironmentId,
  type ServerConfig,
  type TrustedT3PlacementEnvironment,
} from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

vi.mock("../../connection/catalog", () => ({
  environmentCatalog: {
    get catalogValueAtom() {
      return catalog;
    },
  },
}));
vi.mock("../../state/session", () => ({
  environmentSession: {
    initialConfigValueAtom: (environmentId: EnvironmentId) => initialConfig(environmentId),
  },
}));

// Kept alive so values set before the hook subscribes are not dropped.
const catalog = Atom.make<EnvironmentCatalogState>({ isReady: true, entries: new Map() }).pipe(
  Atom.keepAlive,
);
const initialConfig = Atom.family((environmentId: EnvironmentId) =>
  Atom.make<ServerConfig | null>(null).pipe(
    Atom.keepAlive,
    Atom.withLabel(`test-initial-config:${environmentId}`),
  ),
);

const primary = EnvironmentId.make("env:a");
const remote = EnvironmentId.make("env:b");
const descriptor = (environmentId: string, storeGeneration = 1): TrustedT3PlacementEnvironment => ({
  environmentId,
  authorityNamespace: `authority:${environmentId}`,
  storeGeneration,
});
const liveConfig = (environmentId: string, storeGeneration = 1) =>
  ({
    environment: { environmentId },
    nativeStoreAuthority: descriptor(environmentId, storeGeneration),
  }) as unknown as ServerConfig;
const entries = (enabled: Record<string, boolean>): EnvironmentCatalogState => ({
  isReady: true,
  entries: new Map(
    Object.entries(enabled).map(([id, isEnabled]) => [
      EnvironmentId.make(id),
      { enabled: isEnabled } as unknown as ConnectionCatalogEntry,
    ]),
  ),
});

let registry: ReturnType<typeof AtomRegistry.make>;
let renderer: ReactTestRenderer | undefined;
let latest: ConnectedEnvironmentTrust;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  registry = AtomRegistry.make();
  registry.set(catalog, entries({ [primary]: true, [remote]: true }));
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

async function render(placement: readonly TrustedT3PlacementEnvironment[] | undefined) {
  const { useConnectedWorkstreamTrust } = await import("./useConnectedWorkstreamTrust");
  function Probe() {
    const value = useConnectedWorkstreamTrust(placement);
    useLayoutEffect(() => {
      latest = value;
    });
    return null;
  }
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <Probe />
      </RegistryContext.Provider>,
    );
  });
}

const view = () => ({ trusted: [...latest.trusted.keys()], conflicts: [...latest.conflicts] });

it("trusts the primary's placements and each live environment's own descriptor", async () => {
  registry.set(initialConfig(remote), liveConfig(remote));
  await render([descriptor(primary)]);
  expect(view()).toEqual({ trusted: [primary, remote], conflicts: [] });
  expect(latest.trusted.get(remote)).toEqual({
    authorityNamespace: "authority:env:b",
    storeGeneration: 1,
  });
});

it("ignores environments without a live session config, and disabled ones", async () => {
  registry.set(catalog, entries({ [primary]: true, [remote]: false }));
  registry.set(initialConfig(remote), liveConfig(remote));
  await render(undefined);
  expect(view()).toEqual({ trusted: [], conflicts: [] });
});

it("ignores a config that describes a different environment", async () => {
  registry.set(initialConfig(remote), liveConfig(primary));
  await render(undefined);
  expect(view()).toEqual({ trusted: [], conflicts: [] });
});

it("reports a live descriptor that disagrees with placement trust as a conflict", async () => {
  registry.set(initialConfig(primary), liveConfig(primary, 2));
  registry.set(initialConfig(remote), liveConfig(remote));
  await render([descriptor(primary, 1)]);
  expect(view()).toEqual({ trusted: [remote], conflicts: [primary] });
});

it("removes a remote environment's trust when its session disconnects", async () => {
  registry.set(initialConfig(remote), liveConfig(remote));
  await render([descriptor(primary)]);
  expect(view().trusted).toEqual([primary, remote]);
  await act(() => registry.set(initialConfig(remote), null));
  expect(view()).toEqual({ trusted: [primary], conflicts: [] });
});
