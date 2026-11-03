import { it } from "@effect/vitest";
import { makeNativeWorkstreamsTestRunner } from "@t3tools/client-runtime/jones/workstreams/native-lifetime-testing";
import { act } from "../testing/nodeRenderer";
import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  type ServerConfig,
  type AuthSessionState,
  type T3WorkstreamBinding,
} from "@t3tools/contracts";
import type {
  PreparedConnection,
  SupervisorConnectionState,
} from "@t3tools/client-runtime/connection";
import type {
  NativeWorkstreamsEffectRunner,
  EnvironmentNativeWorkstreamsLifetime,
} from "@t3tools/client-runtime/jones/workstreams/native-lifetime";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import { nativeReadRequest } from "@t3tools/client-runtime/state/workstreams";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { StrictMode, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, vi } from "vite-plus/test";
import { useNativeWorkstreamReads } from "./useNativeWorkstreamReads";

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
    return run(effect, signal);
  },
}));

const id = EnvironmentId.make("env:member");
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
let binding: T3WorkstreamBinding;
let run: NativeWorkstreamsEffectRunner;
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
beforeEach(() => {
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
    yield* Effect.promise(use);
  });
function caseEffect(name: string, use: () => Promise<void>) {
  return it.effect(name, () => underTest(use));
}
