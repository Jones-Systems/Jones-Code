import { it } from "@effect/vitest";
import { makeNativeWorkstreamsTestRunner } from "@t3tools/client-runtime/jones/workstreams/native-lifetime-testing";
import { act } from "../testing/nodeRenderer";
import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  t3PlacementInventoryJson,
  type ServerConfig,
  type AuthSessionState,
  type T3WorkstreamBinding,
} from "@t3tools/contracts";
import {
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "@t3tools/client-runtime/connection";
import type {
  NativeWorkstreamsEffectRunner,
  EnvironmentNativeWorkstreamsLifetime,
} from "@t3tools/client-runtime/jones/workstreams/native-lifetime";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import {
  nativeReadRequest,
  liveT3PlacementsCurrent,
} from "@t3tools/client-runtime/state/workstreams";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { StrictMode, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, vi } from "vite-plus/test";
import { useNativeWorkstreamReads } from "./useNativeWorkstreamReads";
import { useWorkstreams, type WorkstreamListView } from "../../state/workstreams";
import { data } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { routeWorkstreamsHttp, syntheticWorkstreams } from "./workstreamsHttp.fixtures";

vi.mock("../../lib/runtime", () => ({
  runPrimaryHttp: (effect: unknown, options?: { readonly signal?: AbortSignal }) => {
    const job = publicHttp(effect, options);
    pendingJobs.push(job);
    return job;
  },
}));
vi.mock("./useWorkstreamOperations", () => ({
  useWorkstreamOperations: () => ({
    entries: [],
    error: null,
    supports: () => false,
    capability,
    capabilityFor: () => capability,
  }),
}));
vi.mock("../../state/primaryEnvironment", async () => {
  const { Atom } = await import("effect/reactivity");
  const { EnvironmentId } = await import("@t3tools/contracts");
  return { primaryEnvironmentIdAtom: Atom.make(EnvironmentId.make("env:member")) };
});
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
vi.mock("./environmentWorkstreamsRequest", () => ({
  runEnvironmentWorkstreamsEffect: (
    p: PreparedConnection,
    effect: Parameters<NativeWorkstreamsEffectRunner>[0],
    signal: AbortSignal,
  ) => {
    sent.push(p);
    const job = run(effect, signal);
    pendingJobs.push(job);
    return job;
  },
}));

const id = EnvironmentId.make("env:member");
const prepared: PreparedConnection = {
  environmentId: id,
  label: "Member",
  httpBaseUrl: "https://member.invalid",
  socketUrl: "wss://member.invalid/ws",
  httpAuthorization: { _tag: "Bearer", token: "fixture" },
  target: new PrimaryConnectionTarget({
    environmentId: id,
    label: "Member",
    httpBaseUrl: "https://member.invalid",
    wsBaseUrl: "wss://member.invalid",
  }),
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
const originalBinding: T3WorkstreamBinding = {
  registryId: "https://registry.invalid",
  ownerId: "owner",
  principalId: "principal",
  authorizationRevision: 1,
  serverGeneration: 7,
  registryVersion: 11,
  permissions: ["workstreams:read"],
  contractVersion: "workstreams/1.0.0",
  contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
};
const initial = (native = capability) =>
  ({
    environment: { environmentId: id, capabilities: { workstreamsNative: native } },
  }) as unknown as ServerConfig;
const authenticated = { authenticated: true } as AuthSessionState;
const connection = Atom.make(AsyncResult.success(connected)).pipe(Atom.keepAlive);
const preparedState = Atom.make(Option.some(prepared)).pipe(Atom.keepAlive);
const session = Atom.make(AsyncResult.success(authenticated)).pipe(Atom.keepAlive);
const config = Atom.make<ServerConfig | null>(initial()).pipe(Atom.keepAlive);
const request = nativeReadRequest({ route: "counts", params: {} });
const response = () =>
  Response.json({
    contract: "workstreams-native-home-read-result/1.0.0",
    route: "counts",
    authorization_revision: 1,
    value: {
      context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
      principal_id: "principal",
      observed_at: "2026-11-03T00:00:00Z",
      active: 0,
      total: 0,
      unknown_lifecycle: 0,
      authority_effect: "none",
    },
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
let renderer: ReactTestRenderer | undefined;
let latest: EnvironmentNativeWorkstreamsLifetime | null;
let application: WorkstreamListView;
let publicHttp: ReturnType<typeof routeWorkstreamsHttp>;
const applicationThreads = [{ environmentId: id, id: "thread:a" }];
function ApplicationProbe() {
  const value = useWorkstreams(true, applicationThreads);
  useLayoutEffect(() => {
    application = value;
  }, [value]);
  return null;
}
let binding: T3WorkstreamBinding;
let run: NativeWorkstreamsEffectRunner;
const pendingJobs: Promise<unknown>[] = [];
const sent: PreparedConnection[] = [];
const calls: RequestInit[] = [];
let reply: () => Response | Promise<Response>;
function Probe() {
  const reader = useNativeWorkstreamReads(id, binding);
  useLayoutEffect(() => {
    latest = reader;
  }, [reader]);
  return null;
}
async function mount() {
  await act(() => {
    renderer = create(
      <StrictMode>
        <RegistryContext.Provider value={registry}>
          <Probe />
        </RegistryContext.Provider>
      </StrictMode>,
    );
  });
}
const placementNow = Date.parse("2026-11-03T00:00:00Z");
beforeEach(() => {
  vi.stubGlobal("window", globalThis);
  vi.useFakeTimers({ toFake: ["Date"], now: placementNow });
  pendingJobs.length = 0;
  const digest = crypto.subtle.digest.bind(crypto.subtle);
  vi.spyOn(crypto.subtle, "digest").mockImplementation((algorithm, bytes) => {
    const job = digest(algorithm, bytes);
    pendingJobs.push(job);
    return job;
  });
  registry = AtomRegistry.make();
  binding = structuredClone(originalBinding);
  latest = null;
  registry.set(connection, AsyncResult.success(connected));
  registry.set(preparedState, Option.some(prepared));
  registry.set(session, AsyncResult.success(authenticated));
  registry.set(config, initial());
  sent.length = 0;
  calls.length = 0;
  reply = response;
  const layer = layerRemoteHttpClient((_url, init) => {
    calls.push(init ?? {});
    return Promise.resolve(reply());
  });
  httpLayer = layer;
});
afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  registry.dispose();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});
caseEffect("uses the actual mounted registry and survives StrictMode lifetime replay", async () => {
  await mount();
  expect(latest).not.toBeNull();
  expect((await latest!.read(request)).route).toBe("counts");
  expect(sent[0]).toBe(prepared);
});
it.effect.each(["prepared", "generation", "session", "capability", "binding"] as const)(
  "closes old reads on actual %s replacement",
  (kind) =>
    underTest(async () => {
      await mount();
      const old = latest!;
      const started = deferred<void>();
      const waiting = deferred<Response>();
      reply = () => {
        started.resolve();
        return waiting.promise;
      };
      const pending = old.read(request).catch((cause: unknown) => cause);
      await started.promise;
      await act(() => {
        if (kind === "prepared")
          registry.set(
            preparedState,
            Option.some({ ...prepared, httpBaseUrl: "https://replacement.invalid" }),
          );
        if (kind === "generation")
          registry.set(connection, AsyncResult.success({ ...connected, generation: 2 }));
        if (kind === "session")
          registry.set(session, AsyncResult.success({ ...authenticated, authenticated: false }));
        if (kind === "capability") registry.set(config, initial({ ...capability, reads: [] }));
        if (kind === "binding") {
          binding = { ...binding, authorizationRevision: 2 };
          renderer!.update(
            <StrictMode>
              <RegistryContext.Provider value={registry}>
                <Probe />
              </RegistryContext.Provider>
            </StrictMode>,
          );
        }
      });
      expect(calls[0]!.signal?.aborted).toBe(true);
      waiting.resolve(response());
      await pending;
      await expect(old.read(request)).rejects.toMatchObject({ code: "scope_changed" });
      expect(calls).toHaveLength(1);
      if (kind === "session" || kind === "capability") expect(latest).toBeNull();
      else expect(latest).not.toBe(old);
    }),
);
caseEffect("does not create a reader when the native read protocol is withheld", async () => {
  registry.set(config, initial({ ...capability, reads: [] }));
  await mount();
  expect(latest).toBeNull();
  expect(sent).toHaveLength(0);
});
caseEffect("unmount aborts pending transport and prevents retained-reader reuse", async () => {
  await mount();
  const reader = latest!;
  const started = deferred<void>();
  const waiting = deferred<Response>();
  reply = () => {
    started.resolve();
    return waiting.promise;
  };
  const pending = reader.read(request).catch((cause: unknown) => cause);
  await started.promise;
  await act(() => renderer!.unmount());
  renderer = undefined;
  expect(calls[0]!.signal?.aborted).toBe(true);
  waiting.resolve(response());
  await pending;
  await expect(reader.read(request)).rejects.toMatchObject({ code: "scope_changed" });
});

function deferred<A>() {
  let resolve!: (value: A | PromiseLike<A>) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

let httpLayer: ReturnType<typeof layerRemoteHttpClient>;
const underTest = (use: () => Promise<void>) =>
  Effect.gen(function* () {
    const bridge = yield* makeNativeWorkstreamsTestRunner;
    run = (effect, signal) => bridge(effect.pipe(Effect.provide(httpLayer)), signal);
    yield* Effect.promise(use).pipe(
      Effect.ensuring(
        Effect.promise(async () => {
          await act(() => renderer?.unmount());
          renderer = undefined;
        }),
      ),
    );
  });
function caseEffect(name: string, use: () => Promise<void>) {
  return it.effect(name, () => underTest(use));
}

async function nativePlacementResponse(
  identities: readonly { readonly source_instance_id: string; readonly native_thread_id: string }[],
  namespace = "namespace:source",
) {
  const observed = new Date(placementNow).toISOString();
  const bytes = new TextEncoder().encode(t3PlacementInventoryJson(identities));
  const inventory = Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
  const provenance = {
    authority: "native-home",
    registry_instance_id: capability.registry.registry_instance_id,
    home_environment_id: "env:home",
    home_generation: 7,
    served_by_environment_id: "env:home",
    observed_at: observed,
  };
  return Response.json({
    contract: "workstreams-native-home-read-result/1.0.0",
    route: "placements",
    placement_contract: "workstreams-t3-placement/2.0.0",
    authorization_revision: 1,
    value: {
      inventory_sha256: inventory,
      context: {
        owner_id: "owner",
        principal_id: "principal",
        authorization_revision: 1,
        server_generation: 7,
        registry_version: 11,
      },
      items: identities.map((identity) => ({
        ...identity,
        membership_id: "membership-00000",
        native_reference_id: "reference-00000",
        workstream_id: "alpha",
        kind: "primary",
        authority_namespace: namespace,
        store_generation: 3,
        source_binding_version: 1,
        attestation_version: 1,
        evidence_sha256: "a".repeat(64),
        attestation_source: "native",
        evidence_contract: "workstreams-t3-provider/2.0.0",
        attested_at: new Date(placementNow - 1000).toISOString(),
        expires_at: new Date(placementNow + 300000).toISOString(),
      })),
      next_cursor: null,
      provenance,
    },
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
        observed_at: observed,
        source: "live",
        stale: false,
      },
      coverage: {
        registry: "complete",
        sources: identities.map((identity) => ({
          source_instance_id: identity.source_instance_id,
          mapping: "mapped",
          attestation: "current",
          observed_at: observed,
        })),
      },
    },
  });
}

caseEffect("loads native placement v2 using the original mounted source descriptor", async () => {
  registry.set(config, {
    ...initial(),
    nativeStoreAuthority: {
      environmentId: id,
      authorityNamespace: "namespace:source",
      storeGeneration: 3,
    },
  });
  await mount();
  const identities = [{ source_instance_id: id, native_thread_id: "thread:a" }];
  reply = () => nativePlacementResponse(identities);
  const proof = await latest!.placements(
    { binding, items: [], nextCursor: null, source: "live", stale: false },
    identities,
  );
  expect(proof.items).toHaveLength(1);
  expect(proof.items[0]).toMatchObject({
    attestation_source: "native",
    evidence_contract: "workstreams-t3-provider/2.0.0",
  });
  expect(liveT3PlacementsCurrent(proof)).toBe(true);
  expect(new Headers(calls[0]!.headers).get("authorization")).toBe("Bearer fixture");
  await act(() => registry.set(config, initial()));
  expect(liveT3PlacementsCurrent(proof)).toBe(false);
});
caseEffect(
  "refuses native placements before HTTP when the live descriptor is missing",
  async () => {
    await mount();
    await expect(
      latest!.placements({ binding, items: [], nextCursor: null, source: "live", stale: false }, [
        { source_instance_id: id, native_thread_id: "thread:a" },
      ]),
    ).rejects.toMatchObject({ code: "scope_changed" });
    expect(calls).toHaveLength(0);
  },
);
caseEffect(
  "does not accept a placement response as the source's authority descriptor",
  async () => {
    registry.set(config, {
      ...initial(),
      nativeStoreAuthority: {
        environmentId: id,
        authorityNamespace: "namespace:source",
        storeGeneration: 3,
      },
    });
    await mount();
    const identities = [{ source_instance_id: id, native_thread_id: "thread:a" }];
    reply = () => nativePlacementResponse(identities, "namespace:forged");
    await expect(
      latest!.placements(
        { binding, items: [], nextCursor: null, source: "live", stale: false },
        identities,
      ),
    ).rejects.toMatchObject({ code: "invalid_response" });
    expect(calls).toHaveLength(1);
  },
);

async function mountApplication(namespace = "namespace:source") {
  const server = syntheticWorkstreams({
    now: () => placementNow,
    assignments: [{ environmentId: id, threadId: "thread:a", workstreamId: "alpha" }],
  });
  server.state.intercept = (call) =>
    call.route === "list" ? { ...data, binding: originalBinding } : undefined;
  publicHttp = routeWorkstreamsHttp(server.handle);
  registry.set(config, {
    ...initial(),
    nativeStoreAuthority: {
      environmentId: id,
      authorityNamespace: "namespace:source",
      storeGeneration: 3,
    },
  });
  reply = () =>
    nativePlacementResponse([{ source_instance_id: id, native_thread_id: "thread:a" }], namespace);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <ApplicationProbe />
      </RegistryContext.Provider>,
    );
  });
  return server;
}
async function settleApplication(check: () => void) {
  for (let turn = 0; ; turn++) {
    await act(async () => {
      for (let round = 0; round < 20; round++) {
        const jobs = pendingJobs.splice(0);
        if (jobs.length === 0) return;
        await Promise.allSettled(jobs);
      }
      throw new Error("Owned application requests and genuine digest jobs did not drain.");
    });
    try {
      check();
      return;
    } catch (cause) {
      if (turn === 199) throw cause;
    }
  }
}
caseEffect(
  "the mounted web controller bootstraps metadata then loads native placements without a v1 request",
  async () => {
    const server = await mountApplication();
    await settleApplication(() => {
      expect(application.loading).toBe(false);
      expect(application.data?.items).toHaveLength(2);
      expect(application.placements?.items).toHaveLength(1);
    });
    const proof = application.placements!;
    expect(proof.items[0]).toMatchObject({
      attestation_source: "native",
      evidence_contract: "workstreams-t3-provider/2.0.0",
    });
    expect(liveT3PlacementsCurrent(proof)).toBe(true);
    expect(server.state.placementRequests).toHaveLength(0);
    expect(calls.length).toBeGreaterThan(0);
    await act(() => registry.set(session, AsyncResult.success({ ...authenticated })));
    expect(liveT3PlacementsCurrent(proof)).toBe(false);
  },
);
caseEffect(
  "the mounted web controller retains stored assignments when native source trust disagrees",
  async () => {
    const server = await mountApplication("namespace:forged");
    await settleApplication(() => {
      expect(application.loading).toBe(false);
      expect(application.data?.items).toHaveLength(2);
      expect(calls.length).toBeGreaterThan(0);
      expect(application.placementInventory.coverage).toBe("partial");
    });
    expect(application.placements).toBeNull();
    expect(application.assignmentIndex?.entries.size).toBe(1);
    expect(server.state.placementRequests).toHaveLength(0);
  },
);
