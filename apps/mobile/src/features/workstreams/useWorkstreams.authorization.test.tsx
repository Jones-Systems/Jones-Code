// @vitest-environment jsdom

import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentHttpForbiddenError,
  EnvironmentHttpUnauthorizedError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { AtomRegistry } from "effect/reactivity";
import { act, useLayoutEffect } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { data, membershipFor, reference, thread } from "./actions.fixtures";
import type { MobileWorkstreamSnapshot } from "./projection";

const fixture = vi.hoisted(() => ({
  failures: new Map<string, unknown>(),
  pending: new Set<string>(),
  calls: [] as string[],
}));

vi.mock("react-native", () => ({
  AppState: { currentState: "background", addEventListener: () => ({ remove: () => {} }) },
  Pressable: "Pressable",
  Text: "Text",
}));
vi.mock("./Controls", () => ({ MobileWorkstreamControls: () => null }));
vi.mock("../../persistence/mobile-secure-storage", () => ({
  MobileSecureStorage: { pipe: () => ({ storage: true }) },
}));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ environments: [{ environmentId: "env:a" }] }),
}));
vi.mock("../../connection/runtime", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  return { connectionAtomRuntime: Atom.make(AsyncResult.success({})) };
});
vi.mock("../../connection/catalog", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  const connected = Atom.make(AsyncResult.success({ phase: "connected", generation: 1 }));
  return { environmentCatalog: { stateAtom: () => connected } };
});
vi.mock("../../state/session", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  const Option = await import("effect/Option");
  const { EnvironmentId } = await import("@t3tools/contracts");
  const environmentId = EnvironmentId.make("env:a");
  const prepared = Atom.make(
    Option.some({
      environmentId,
      label: "env:a",
      httpBaseUrl: "https://test.invalid",
      socketUrl: "wss://test.invalid",
      httpAuthorization: null,
      target: {
        _tag: "PrimaryConnectionTarget",
        environmentId,
        label: "env:a",
        httpBaseUrl: "https://test.invalid",
        wsBaseUrl: "wss://test.invalid",
      },
    }),
  );
  const session = Atom.make(AsyncResult.success({ authenticated: true }));
  const config = Atom.make(null);
  return {
    environmentSession: {
      preparedConnectionValueAtom: () => prepared,
      sessionStateAtom: () => session,
      initialConfigValueAtom: () => config,
    },
  };
});
// Requests resolve by endpoint name; digests and storage reads get inert values.
vi.mock("./gateway", () => {
  const urls = new Proxy(
    {},
    {
      get: (_, name) => (input?: { readonly params?: { readonly workstreamId?: string } }) =>
        input?.params?.workstreamId ? `${String(name)}:${input.params.workstreamId}` : String(name),
    },
  );
  return {
    workstreamPageUrl: (endpoint: string) => endpoint,
    workstreamRequest: (_prepared: unknown, _method: string, url: (value: unknown) => string) => {
      const endpoint = url(urls);
      return { pipe: () => ({ endpoint }) };
    },
  };
});
vi.mock("../../lib/runtime", () => ({
  runtime: {
    runPromise: async (effect: { readonly endpoint?: string; readonly storage?: boolean }) => {
      if (effect.storage) return null;
      if (effect.endpoint === undefined) return new Uint8Array(32);
      const endpoint = effect.endpoint;
      fixture.calls.push(endpoint);
      if (fixture.pending.has(endpoint)) return new Promise(() => {});
      if (fixture.failures.has(endpoint)) throw fixture.failures.get(endpoint);
      return respond(endpoint);
    },
  },
}));

const context = {
  owner_id: data.binding.ownerId,
  server_generation: data.binding.serverGeneration,
  registry_version: data.binding.registryVersion,
};
function respond(endpoint: string): unknown {
  switch (endpoint) {
    case "list":
      return data;
    case "references":
      return { context, items: [reference], next_cursor: null };
    case "memberships:alpha":
      return {
        context,
        items: [membershipFor("membership", "alpha", reference.native_reference_id)],
        next_cursor: null,
      };
    case "memberships:beta":
      return { context, items: [], next_cursor: null };
    case "appearanceRead":
      return { supported: false };
    case "threadPlacements":
      throw transport;
    default:
      return {};
  }
}

const transport = new Error("Network request failed");
const unauthorized = new EnvironmentHttpUnauthorizedError({ message: "unauthorized" });
const forbidden = new EnvironmentHttpForbiddenError({ message: "forbidden" });
const key = JSON.stringify(["env:a", "thread"]);

type Api = ReturnType<typeof import("./useWorkstreams").useMobileWorkstreams>;
let latest: Api;
let root: Root;
let container: HTMLDivElement;

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  fixture.failures.clear();
  fixture.pending.clear();
  fixture.calls.length = 0;
  const { useMobileWorkstreams } = await import("./useWorkstreams");
  const threads = [thread];
  function Probe() {
    const api = useMobileWorkstreams(threads);
    useLayoutEffect(() => {
      latest = api;
    });
    return null;
  }
  const registry = AtomRegistry.make();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  await act(async () =>
    root.render(
      <RegistryContext.Provider value={registry}>
        <Probe />
      </RegistryContext.Provider>,
    ),
  );
  await flush();
  expect(latest.snapshots).toHaveLength(1);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function flush() {
  for (let turn = 0; turn < 10; turn += 1)
    await act(() => new Promise<void>((resolve) => setTimeout(resolve, 0)));
}

async function reloadWith(endpoint: string, cause: unknown) {
  fixture.failures.set(endpoint, cause);
  await act(async () => latest.refresh());
  await flush();
}

const recorded = () => ({
  snapshots: latest.snapshots.length,
  stale: latest.stale,
  grouped: latest.groups.find((group) => group.workstream.workstreamId === "alpha")?.threadKeys,
  assurance: latest.assuranceByKey.get(key),
});

describe("mobile Workstreams authorization loss after the core index load", () => {
  it.each([
    ["401", "placement", "threadPlacements", unauthorized],
    ["403", "placement", "threadPlacements", forbidden],
    ["401", "appearance", "appearanceRead", unauthorized],
    ["403", "appearance", "appearanceRead", forbidden],
  ] as const)("purges on a %s %s read", async (_status, _read, endpoint, cause) => {
    await reloadWith(endpoint, cause);
    expect(latest.snapshots).toHaveLength(0);
    expect(latest.groups).toHaveLength(0);
  });

  it("keeps groups recorded after a transport failure at the placement or appearance read", async () => {
    for (const endpoint of ["threadPlacements", "appearanceRead"]) {
      await reloadWith(endpoint, transport);
      expect(recorded(), endpoint).toEqual({
        snapshots: 1,
        stale: false,
        grouped: new Set([key]),
        assurance: "recorded",
      });
    }
  });
});

describe("mobile Workstreams detail reads", () => {
  const detail = (snapshot: MobileWorkstreamSnapshot) =>
    latest.read(
      snapshot,
      (urls) => urls.detail({ params: { workstreamId: "alpha" } }),
      () => Effect.succeed(null),
    );

  it("purges immediately on a 401, before and regardless of any reload", async () => {
    fixture.pending.add("list");
    fixture.failures.set("detail:alpha", unauthorized);
    await act(async () => {
      await expect(detail(latest.snapshots[0]!)).rejects.toBe(unauthorized);
    });
    expect(latest.snapshots).toHaveLength(0);
    fixture.pending.delete("list");
    await reloadWith("list", transport);
    expect(latest.snapshots).toHaveLength(0);
    expect(latest.groups).toHaveLength(0);
  });

  it("keeps the last load as stale after a transport failure and a failed reload", async () => {
    fixture.failures.set("detail:alpha", transport);
    fixture.failures.set("list", transport);
    await act(async () => {
      await expect(detail(latest.snapshots[0]!)).rejects.toBe(transport);
    });
    await flush();
    expect(recorded()).toEqual({
      snapshots: 1,
      stale: true,
      grouped: new Set([key]),
      assurance: "recorded",
    });
  });
});
