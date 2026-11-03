import { it } from "@effect/vitest";
import { makeNativeWorkstreamsTestRunner } from "@t3tools/client-runtime/jones/workstreams/native-lifetime-testing";
import { RegistryContext } from "@effect/atom-react";
import { EnvironmentId, type ServerConfig, type AuthSessionState } from "@t3tools/contracts";
import type {
  NativeWorkstreamsRequestError,
  NativeWorkstreamsEffectRunner,
} from "@t3tools/client-runtime/jones/workstreams/native-lifetime";
import type { HttpClient } from "effect/http";
import type {
  PreparedConnection,
  SupervisorConnectionState,
} from "@t3tools/client-runtime/connection";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { act, useLayoutEffect } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, vi } from "vite-plus/test";
import { data } from "./actions.fixtures";
import { useMobileWorkstreams } from "./useWorkstreams";
import type { MobileWorkstreams } from "./types";

vi.mock("react-native", () => ({
  AppState: { currentState: "background", addEventListener: () => ({ remove() {} }) },
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
vi.mock("../../connection/runtime", () => ({
  get connectionAtomRuntime() {
    return contextAtom;
  },
}));
vi.mock("../../connection/catalog", () => ({
  environmentCatalog: { stateAtom: () => connection },
}));
vi.mock("../../state/session", () => ({
  environmentSession: {
    preparedConnectionValueAtom: () => preparedState,
    sessionStateAtom: () => session,
    initialConfigValueAtom: () => config,
  },
}));
vi.mock("../../lib/runtime", () => ({
  runtime: {
    runPromise: (effect: unknown, options?: Effect.RunOptions) => {
      if (effect && typeof effect === "object" && "storage" in effect) return Promise.resolve(null);
      const job = run(
        effect as Effect.Effect<unknown, NativeWorkstreamsRequestError, HttpClient.HttpClient>,
        options?.signal ?? new AbortController().signal,
      );
      pendingJobs.push(job);
      return job;
    },
  },
}));

const id = EnvironmentId.make("env:a");
const prepared: PreparedConnection = {
  environmentId: id,
  label: "Member",
  httpBaseUrl: "https://member.invalid",
  socketUrl: "wss://member.invalid/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture" },
  target: {
    _tag: "PrimaryConnectionTarget",
    environmentId: id,
    label: "Member",
    httpBaseUrl: "https://member.invalid",
    wsBaseUrl: "wss://member.invalid",
  },
};
const connected: SupervisorConnectionState = {
  desired: true,
  network: "online",
  phase: "connected",
  stage: null,
  attempt: 1,
  generation: 1,
  lastFailure: null,
  retryAt: null,
};
const capability = {
  contract: "workstreams-native/1.0.0",
  mode: "native-member",
  home_link: "ok",
  reads: ["workstreams-read/1.0.0"],
  placement_proofs: ["workstreams-t3-placement/2.0.0"],
  registry: {
    registry_instance_id: "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
    owner_id: "owner",
    canonical_origin: "https://registry.invalid",
    home_environment_id: "env:home",
    server_generation: 7,
  },
};
const publicData = {
  ...data,
  binding: { ...data.binding, registryId: "https://registry.invalid" },
};
const configFor = (reads = ["workstreams-read/1.0.0"]) =>
  ({
    environment: {
      environmentId: id,
      capabilities: { workstreamsNative: { ...capability, reads } },
    },
  }) as unknown as ServerConfig;
const authenticated = { authenticated: true } as AuthSessionState;
const connection = Atom.make(AsyncResult.success(connected)).pipe(Atom.keepAlive);
const preparedState = Atom.make(Option.some(prepared)).pipe(Atom.keepAlive);
const session = Atom.make(AsyncResult.success(authenticated)).pipe(Atom.keepAlive);
const config = Atom.make<ServerConfig | null>(configFor()).pipe(Atom.keepAlive);
const contextAtom = Atom.make(AsyncResult.success(Context.empty())).pipe(Atom.keepAlive);
const readContext = { owner_id: "owner", server_generation: 7, registry_version: 11 };
const appearance = {
  supported: true,
  page: {
    owner_id: "owner",
    server_generation: 7,
    permissions: ["workstreams:read"],
    items: publicData.items.map((item) => ({
      workstream_id: item.workstreamId,
      border_color: "#123ABC",
      version: 1,
      updated_at: "2026-11-03T00:00:00Z",
      updated_by: { principal_id: "principal" },
    })),
  },
};
const nativeAppearance = () =>
  Response.json({
    contract: "workstreams-native-home-read-result/1.0.0",
    route: "appearance",
    value: appearance,
    authorization_revision: 1,
    envelope: {
      contract: "workstreams-read/1.0.0",
      registry: {
        registry_instance_id: capability.registry.registry_instance_id,
        owner_id: "owner",
        canonical_origin: "https://registry.invalid",
        server_generation: 7,
        registry_version: 11,
      },
      provenance: {
        authority: "native-home",
        home_environment_id: "env:home",
        served_by_environment_id: "env:home",
        source: "live",
        observed_at: "2026-11-03T00:00:00Z",
        stale: false,
      },
      coverage: { registry: "complete", sources: [] },
    },
  });
let registry: ReturnType<typeof AtomRegistry.make>;
let root: Root | undefined;
let container: HTMLDivElement;
let latest: MobileWorkstreams;
let nativeReply: () => Response | Promise<Response>;
let httpLayer: ReturnType<typeof layerRemoteHttpClient>;
const calls: { path: string; init: RequestInit }[] = [];
function Probe() {
  const value = useMobileWorkstreams([]);
  useLayoutEffect(() => {
    latest = value;
  }, [value]);
  return null;
}
async function mount(settle = true) {
  await act(async () => {
    root = createRoot(container);
    root.render(
      <RegistryContext.Provider value={registry}>
        <Probe />
      </RegistryContext.Provider>,
    );
  });
  if (settle) {
    for (let round = 0; round < 20; round++) {
      await act(drainRequests);
      if (latest.snapshots.length > 0 || latest.error !== null) return;
    }
    throw new Error("The mounted Workstreams load did not settle.");
  }
}
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  registry = AtomRegistry.make();
  registry.set(connection, AsyncResult.success(connected));
  registry.set(preparedState, Option.some(prepared));
  registry.set(session, AsyncResult.success(authenticated));
  registry.set(config, configFor());
  calls.length = 0;
  pendingJobs.length = 0;
  nativeReply = nativeAppearance;
  httpLayer = layerRemoteHttpClient(async (url, init) => {
    const path = new URL(String(url)).pathname;
    calls.push({ path, init: init ?? {} });
    if (path === "/api/workstreams/native/v1/read") return nativeReply();
    if (path === "/api/workstreams") return Response.json(publicData);
    if (path === "/api/workstreams/references" || path.endsWith("/memberships"))
      return Response.json({ context: readContext, items: [], next_cursor: null });
    if (path === "/api/workstreams/appearance") return Response.json(appearance);
    return Response.json({ missing: true }, { status: 404 });
  });
  container = document.createElement("div");
  document.body.appendChild(container);
});
afterEach(async () => {
  await act(() => root?.unmount());
  root = undefined;
  container.remove();
  registry.dispose();
  vi.unstubAllGlobals();
});
caseEffect(
  "loads native appearance through the real gateway, HTTP auth and captured snapshot",
  async () => {
    await mount();
    expect(latest.error).toBeNull();
    expect(latest.snapshots).toHaveLength(1);
    expect(latest.snapshots[0]!.appearance?.items).toHaveLength(2);
    const native = calls.find((call) => call.path === "/api/workstreams/native/v1/read");
    expect(native).toBeDefined();
    expect(new Headers(native!.init.headers).get("authorization")).toBe("Bearer fixture");
    expect(calls.some((call) => call.path === "/api/workstreams/appearance")).toBe(false);
  },
);
caseEffect("keeps public behavior when native reads have not been advertised", async () => {
  registry.set(config, configFor([]));
  await mount();
  expect(latest.error).toBeNull();
  expect(latest.snapshots).toHaveLength(1);
  expect(calls.some((call) => call.path === "/api/workstreams/native/v1/read")).toBe(false);
});
caseEffect("cancels old native reads when the authenticated session is lost", async () => {
  const started = deferred<void>();
  const pending = deferred<Response>();
  nativeReply = () => {
    started.resolve();
    return pending.promise;
  };
  await mount(false);
  await act(() => started.promise);
  await act(async () => {
    registry.set(session, AsyncResult.success({ ...authenticated, authenticated: false }));
    await drainRequests();
  });
  const native = calls.find((call) => call.path === "/api/workstreams/native/v1/read")!;
  expect(native.init.signal?.aborted).toBe(true);
  await act(async () => {
    pending.resolve(nativeAppearance());
    await drainRequests();
  });
  expect(latest.snapshots).toHaveLength(0);
});
caseEffect(
  "old snapshot export refuses after replacement without issuing an export request",
  async () => {
    await mount();
    expect(latest.snapshots).toHaveLength(1);
    const snapshot = latest.snapshots[0]!;
    await act(() => registry.set(connection, AsyncResult.success({ ...connected, generation: 2 })));
    await expect(latest.exportNative!(snapshot)).rejects.toThrow();
    expect(calls.some((call) => call.path.includes("/export"))).toBe(false);
  },
);
function deferred<A>() {
  let resolve!: (value: A | PromiseLike<A>) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

let run: NativeWorkstreamsEffectRunner;
const underTest = (use: () => Promise<void>) =>
  Effect.gen(function* () {
    const bridge = yield* makeNativeWorkstreamsTestRunner;
    run = (effect, signal) => bridge(effect.pipe(Effect.provide(httpLayer)), signal);
    yield* Effect.promise(use).pipe(
      Effect.ensuring(
        Effect.promise(async () => {
          await act(() => root?.unmount());
          root = undefined;
        }),
      ),
    );
  });
function caseEffect(name: string, use: () => Promise<void>) {
  return it.effect(name, () => underTest(use));
}

const pendingJobs: Promise<unknown>[] = [];
async function drainRequests() {
  for (let round = 0; round < 20; round++) {
    const jobs = pendingJobs.splice(0);
    if (jobs.length === 0) return;
    await Promise.allSettled(jobs);
  }
  throw new Error("Owned application requests did not drain.");
}
