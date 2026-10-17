import { RegistryContext } from "@effect/atom-react";
import type { SupervisorConnectionState } from "@t3tools/client-runtime/connection";
import type { WorkstreamsRegistrationContextAcceptedResponse } from "@t3tools/contracts";
import {
  EnvironmentHttpBadRequestError,
  EnvironmentHttpConflictError,
  EnvironmentHttpUnauthorizedError,
  EnvironmentInternalError,
  t3PlacementInventoryJson,
  type T3PlacementIdentity,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import {
  data,
  thread,
  placements,
  reference,
  now,
} from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import {
  useWorkstreams,
  nativePlacementInventory,
  type WorkstreamListView,
} from "../../state/workstreams";

const http = vi.hoisted(() => ({
  run: vi.fn<(effect: unknown, options?: { readonly signal?: AbortSignal }) => Promise<unknown>>(),
}));
vi.mock("../../lib/runtime", () => ({ runPrimaryHttp: http.run }));
vi.mock("../../connection/catalog", () => ({
  environmentCatalog: { stateAtom: () => connection },
}));
vi.mock("../../state/primaryEnvironment", async () => {
  const { Atom } = await import("effect/reactivity");
  const { EnvironmentId } = await import("@t3tools/contracts");
  return { primaryEnvironmentIdAtom: Atom.make(EnvironmentId.make("env:a")) };
});

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
const connection = Atom.make(AsyncResult.success(connected));
const nativeThreads = [thread];
const registration: WorkstreamsRegistrationContextAcceptedResponse = {
  protocol: "workstreams-registration-context/2.0.0",
  state: "ready",
  owner_id: data.binding.ownerId,
  principal_id: data.binding.principalId,
  grant_id: "grant",
  authorization_revision: data.binding.authorizationRevision,
  server_generation: data.binding.serverGeneration,
  registry_version: data.binding.registryVersion,
  sources: [],
};
const unavailable = new EnvironmentInternalError({
  code: "internal_error",
  reason: "internal_error",
  traceId: "cp-outage",
});
let registry: ReturnType<typeof AtomRegistry.make>;
let renderer: ReactTestRenderer | undefined;
let latest: WorkstreamListView;

function MountedWorkstreams({
  enabled = false,
  threads = nativeThreads,
}: {
  enabled?: boolean;
  threads?: readonly { environmentId: string; id: string }[];
}) {
  const view = useWorkstreams(enabled, threads);
  useLayoutEffect(() => {
    latest = view;
  }, [view]);
  return null;
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  http.run.mockReset();
  registry = AtomRegistry.make();
  registry.set(connection, AsyncResult.success(connected));
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  registry.dispose();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("recovers list and registration reads after CP outages without replacing the mounted RPC generation", async () => {
  vi.useFakeTimers();
  const initialConnection = registry.get(connection);
  http.run
    .mockRejectedValueOnce(unavailable)
    .mockResolvedValueOnce(data)
    .mockRejectedValueOnce(unavailable)
    .mockResolvedValueOnce(registration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  expect(latest.data).toBeNull();
  expect(http.run).toHaveBeenCalledTimes(1);

  await act(() => vi.advanceTimersByTimeAsync(1_000));
  expect(latest.data).toEqual(data);
  expect(latest.registrationContext).toBeNull();
  expect(http.run).toHaveBeenCalledTimes(3);

  await act(() => vi.advanceTimersByTimeAsync(1_000));
  expect(latest.error).toBeNull();
  expect(latest.loading).toBe(false);
  expect(latest.registrationContext).toEqual(registration);
  expect(http.run).toHaveBeenCalledTimes(4);
  expect(registry.get(connection)).toBe(initialConnection);
});

it("ignores a late registration failure from a superseded read without starting retries", async () => {
  vi.useFakeTimers();
  let rejectRegistration!: (cause: Error) => void;
  const interrupted = new Promise<never>((_resolve, reject) => {
    rejectRegistration = reject;
  });
  http.run
    .mockResolvedValueOnce(data)
    .mockReturnValueOnce(interrupted)
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce(registration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  const signal = http.run.mock.calls[1]?.[1]?.signal;
  await act(() => latest.refresh());
  expect(signal?.aborted).toBe(true);
  expect(latest.registrationContext).toEqual(registration);
  await act(() => rejectRegistration(unavailable));
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(latest.registrationContext).toEqual(registration);
  expect(http.run).toHaveBeenCalledTimes(4);
});

it("reloads the list and context together when registration recovery observes a newer registry", async () => {
  vi.useFakeTimers();
  const initialConnection = registry.get(connection);
  const nextVersion = data.binding.registryVersion + 1;
  const updatedData = { ...data, binding: { ...data.binding, registryVersion: nextVersion } };
  const updatedRegistration = { ...registration, registry_version: nextVersion };
  http.run
    .mockResolvedValueOnce(data)
    .mockRejectedValueOnce(unavailable)
    .mockResolvedValueOnce(updatedRegistration)
    .mockResolvedValueOnce(updatedData)
    .mockResolvedValueOnce(updatedRegistration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  await act(() => vi.advanceTimersByTimeAsync(1_000));
  expect(latest.registrationContext).toBeNull();
  await act(() => vi.advanceTimersByTimeAsync(2_000));
  expect(latest.data).toEqual(updatedData);
  expect(latest.registrationContext).toEqual(updatedRegistration);
  expect(latest.loading).toBe(false);
  expect(latest.error).toBeNull();
  expect(http.run).toHaveBeenCalledTimes(5);
  expect(registry.get(connection)).toBe(initialConnection);
});

it("shares the finite recovery budget between outages and repeated registry changes", async () => {
  vi.useFakeTimers();
  http.run.mockResolvedValueOnce(data).mockRejectedValueOnce(unavailable);
  for (let advance = 1; advance <= 5; advance += 1) {
    const version = data.binding.registryVersion + advance;
    http.run.mockResolvedValueOnce({ ...registration, registry_version: version });
    if (advance < 5)
      http.run.mockResolvedValueOnce({
        ...data,
        binding: { ...data.binding, registryVersion: version },
      });
  }
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  await act(() => vi.advanceTimersByTimeAsync(31_000));
  expect(http.run).toHaveBeenCalledTimes(11);
  expect(latest.data?.binding.registryVersion).toBe(data.binding.registryVersion + 4);
  expect(latest.registrationContext).toBeNull();
  expect(latest.loading).toBe(false);
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(http.run).toHaveBeenCalledTimes(11);
});

it.each([
  { owner_id: `${registration.owner_id}-other` },
  { principal_id: `${registration.principal_id}-other` },
  { authorization_revision: registration.authorization_revision + 1 },
  { server_generation: `${registration.server_generation}-other` },
])(
  "does not turn a registry change with an authority mismatch into a retry: %j",
  async (mismatch) => {
    vi.useFakeTimers();
    http.run.mockResolvedValueOnce(data).mockResolvedValueOnce({
      ...registration,
      registry_version: registration.registry_version + 1,
      ...mismatch,
    });
    await act(() => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <MountedWorkstreams />
        </RegistryContext.Provider>,
      );
    });
    await act(() => vi.advanceTimersByTimeAsync(60_000));
    expect(http.run).toHaveBeenCalledTimes(2);
    expect(latest.registrationContext).toBeNull();
    expect(latest.loading).toBe(false);
  },
);

it("fences a late list reload after a registry mismatch when refresh supersedes it", async () => {
  vi.useFakeTimers();
  let resolveReload!: (value: typeof data) => void;
  const interrupted = new Promise<typeof data>((resolve) => {
    resolveReload = resolve;
  });
  http.run
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce({ ...registration, registry_version: registration.registry_version + 1 })
    .mockReturnValueOnce(interrupted)
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce(registration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  await act(() => vi.advanceTimersByTimeAsync(1_000));
  const signal = http.run.mock.calls[2]?.[1]?.signal;
  await act(() => latest.refresh());
  expect(signal?.aborted).toBe(true);
  expect(latest.registrationContext).toEqual(registration);
  await act(() => resolveReload(data));
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(latest.data).toEqual(data);
  expect(latest.registrationContext).toEqual(registration);
  expect(http.run).toHaveBeenCalledTimes(5);
});

it("hides the previous list when authorization rejects a coherent reload", async () => {
  vi.useFakeTimers();
  const denied = new EnvironmentHttpUnauthorizedError({ message: "authorization revoked" });
  http.run
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce({ ...registration, registry_version: registration.registry_version + 1 })
    .mockRejectedValueOnce(denied);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(latest.data).toBeNull();
  expect(latest.registrationContext).toBeNull();
  expect(latest.error).toBe(denied.message);
  expect(http.run).toHaveBeenCalledTimes(3);
});

it("stops automatic reads after a bounded outage and permits an explicit retry", async () => {
  vi.useFakeTimers();
  http.run.mockRejectedValue(unavailable);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  await act(() => vi.advanceTimersByTimeAsync(31_000));
  expect(http.run).toHaveBeenCalledTimes(6);
  expect(latest.loading).toBe(false);
  expect(latest.error).toBe(unavailable.message);
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(http.run).toHaveBeenCalledTimes(6);

  http.run.mockResolvedValueOnce(data).mockResolvedValueOnce(registration);
  await act(() => latest.refresh());
  expect(latest.data).toEqual(data);
  expect(latest.registrationContext).toEqual(registration);
  expect(latest.error).toBeNull();
});

it.each([
  new EnvironmentHttpUnauthorizedError({ message: "unauthorized" }),
  new EnvironmentHttpBadRequestError({ message: "invalid contract" }),
  new EnvironmentHttpConflictError({ message: "workstreams_not_configured" }),
  new Error("Client response decoding failed"),
])(
  "does not automatically retry explicit auth, contract or unconfigured failures: %s",
  async (cause) => {
    vi.useFakeTimers();
    http.run.mockRejectedValue(cause);
    await act(() => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <MountedWorkstreams />
        </RegistryContext.Provider>,
      );
    });
    await act(() => vi.advanceTimersByTimeAsync(60_000));
    expect(http.run).toHaveBeenCalledTimes(1);
    expect(latest.loading).toBe(false);
    expect(latest.data).toBeNull();
  },
);

it.each(["refresh", "reconnect", "unmount"] as const)(
  "cancels a pending CP read retry on %s without duplicate recovery reads",
  async (action) => {
    vi.useFakeTimers();
    http.run
      .mockRejectedValueOnce(unavailable)
      .mockResolvedValueOnce(data)
      .mockResolvedValueOnce(registration);
    await act(() => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <MountedWorkstreams />
        </RegistryContext.Provider>,
      );
    });
    const signal = http.run.mock.calls[0]?.[1]?.signal;
    await act(() => {
      if (action === "refresh") latest.refresh();
      else if (action === "reconnect")
        registry.set(connection, AsyncResult.success({ ...connected, generation: 2 }));
      else {
        renderer?.unmount();
        renderer = undefined;
      }
    });
    expect(signal?.aborted).toBe(true);
    await act(() => vi.advanceTimersByTimeAsync(60_000));
    expect(http.run).toHaveBeenCalledTimes(action === "unmount" ? 1 : 3);
    if (action !== "unmount") {
      expect(latest.error).toBeNull();
      expect(latest.data).toEqual(data);
      expect(latest.registrationContext).toEqual(registration);
    }
  },
);

it("refreshes the same mounted Workstreams view after an interrupted read and ordinary reconnect", async () => {
  http.run
    .mockRejectedValueOnce(new Error("Connection interrupted"))
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce(registration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  expect(latest.error).toBe("Connection interrupted");
  expect(latest.data).toBeNull();
  expect(http.run).toHaveBeenCalledTimes(1);

  await act(() => {
    registry.set(connection, AsyncResult.success({ ...connected, phase: "backoff" }));
  });
  expect(http.run).toHaveBeenCalledTimes(1);
  await act(() => {
    registry.set(connection, AsyncResult.success({ ...connected, generation: 2 }));
  });

  expect(latest.error).toBeNull();
  expect(latest.loading).toBe(false);
  expect(latest.data).toEqual(data);
  expect(latest.registrationContext).toEqual(registration);
  expect(http.run).toHaveBeenCalledTimes(3);
});

it("does not duplicate the mount read or refresh for same-generation connection updates", async () => {
  http.run.mockResolvedValueOnce(data).mockResolvedValueOnce(registration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  expect(latest.data).toEqual(data);
  expect(http.run).toHaveBeenCalledTimes(2);
  await act(() => {
    registry.set(connection, AsyncResult.success({ ...connected, attempt: 2 }));
  });
  expect(http.run).toHaveBeenCalledTimes(2);
});

it("aborts an interrupted read and ignores its late failure after reconnect recovery", async () => {
  vi.useFakeTimers();
  let rejectInterrupted!: (cause: Error) => void;
  const interrupted = new Promise<never>((_resolve, reject) => {
    rejectInterrupted = reject;
  });
  http.run
    .mockReturnValueOnce(interrupted)
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce(registration);
  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  const interruptedSignal = http.run.mock.calls[0]?.[1]?.signal;
  expect(latest.loading).toBe(true);
  await act(() => {
    registry.set(connection, AsyncResult.success({ ...connected, generation: 2 }));
  });
  expect(interruptedSignal?.aborted).toBe(true);
  expect(latest.data).toEqual(data);
  await act(() => rejectInterrupted(unavailable));
  await act(() => vi.advanceTimersByTimeAsync(60_000));
  expect(latest.error).toBeNull();
  expect(latest.data).toEqual(data);
  expect(latest.registrationContext).toEqual(registration);
  expect(http.run).toHaveBeenCalledTimes(3);
});

it.each([11, 12])(
  "assigns a chosen tail thread at fresh registry %s and retains its verified placement after refresh",
  async (freshVersion) => {
    vi.spyOn(Date, "now").mockReturnValue(now);
    vi.stubGlobal("window", globalThis);
    const threads = Array.from({ length: 4501 }, (_, i) => ({
      environmentId: thread.environmentId,
      id: String(i).padStart(5, "0"),
    }));
    const chosen = threads[4500]!;
    const chosenReference = {
      ...reference,
      identity: { ...reference.identity, native_id: chosen.id },
      registration: {
        ...reference.registration,
        evidence: { ...reference.registration.evidence!, native_id: chosen.id },
      },
    };
    const quarantined = {
      ...chosenReference,
      registration: {
        state: "quarantined",
        attestation_version: 0,
        attested_at: null,
        expires_at: null,
        evidence: null,
      },
    };
    const selected = [{ source_instance_id: chosen.environmentId, native_thread_id: chosen.id }];
    const context = (version: number) => ({
      ...registration,
      registry_version: version,
      sources: [
        {
          provider: "t3",
          source_instance_id: chosen.environmentId,
          authority_namespace: "authority",
          store_generation: 1,
          resource_kind: "thread",
          id_kind: "internal",
          account_provenance: { kind: "not_account_scoped" },
          native_protocol: "workstreams-t3-provider/1.0.0",
          build: {
            repository: "Jones-Systems/Jones-Code",
            sha: "a".repeat(40),
            tree: "b".repeat(40),
          },
        },
      ],
    });
    const list = (version: number) => ({
      ...data,
      binding: { ...data.binding, registryVersion: version },
    });
    const readContext = (version: number) => ({
      owner_id: "owner",
      server_generation: 7,
      registry_version: version,
    });
    const projection = async (
      version: number,
      identities: readonly T3PlacementIdentity[],
      assigned = false,
    ) => ({
      page: {
        inventory_sha256: Array.from(
          new Uint8Array(
            await crypto.subtle.digest(
              "SHA-256",
              new TextEncoder().encode(t3PlacementInventoryJson(identities)),
            ),
          ),
          (value) => value.toString(16).padStart(2, "0"),
        ).join(""),
        context: { ...placements.context, registry_version: version },
        next_cursor: null,
        items: assigned
          ? [{ ...placements.items[0]!, native_thread_id: chosen.id, workstream_id: "beta" }]
          : [],
      },
      trustedEnvironments: placements.trustedEnvironments,
      readiness: "ready",
    });
    const memberships = (version: number) => ({
      context: readContext(version),
      items: [],
      next_cursor: null,
    });
    const receipt = (
      version: number,
      operation: "register_reference" | "verify_reference" | "attach_primary",
    ): WorkstreamReceipt => ({
      command_id: `command-${version - freshVersion}`,
      owner_id: "owner",
      actor: { principal_id: "principal" },
      operation,
      request_sha256: "c".repeat(64),
      server_generation: 7,
      accepted_at: new Date(now).toISOString(),
      completed_at: new Date(now).toISOString(),
      state: "committed",
      registry_version: version,
      changed: true,
      effects: {
        native_reference_id: "reference",
        registration: null,
        workstream_versions: [],
        membership_ids: [],
        declaration_id: null,
        declaration_revision: null,
        edge_id: null,
        observation: null,
        lifecycle_declaration: null,
        coordination_disposition: null,
        native_settlement: null,
      },
    });
    const responses = [
      list(11),
      context(11),
      await projection(11, nativePlacementInventory(threads).identities),
      list(freshVersion),
      context(freshVersion),
      await projection(freshVersion, selected),
      memberships(freshVersion),
      receipt(freshVersion + 1, "register_reference"),
      list(freshVersion + 1),
      context(freshVersion + 1),
      await projection(freshVersion + 1, selected),
      { context: readContext(freshVersion + 1), reference: quarantined, latest_observation: null },
      memberships(freshVersion + 1),
      receipt(freshVersion + 2, "verify_reference"),
      list(freshVersion + 2),
      context(freshVersion + 2),
      await projection(freshVersion + 2, selected),
      {
        context: readContext(freshVersion + 2),
        reference: chosenReference,
        latest_observation: null,
      },
      memberships(freshVersion + 2),
      receipt(freshVersion + 3, "attach_primary"),
      list(freshVersion + 3),
      context(freshVersion + 3),
      await projection(
        freshVersion + 3,
        nativePlacementInventory(threads, chosen).identities,
        true,
      ),
    ];
    http.run.mockImplementation(async () => {
      const response = responses.shift();
      if (!response) throw new Error("Unexpected additional HTTP request");
      return response;
    });
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <MountedWorkstreams enabled threads={threads} />
        </RegistryContext.Provider>,
      );
    });
    await act(async () => {
      await vi.waitFor(() => expect(latest.loading).toBe(false));
    });
    expect(latest.placementInventory.identities).not.toContainEqual(selected[0]);
    expect(latest.data?.binding.registryVersion).toBe(11);
    const mountedConnection = registry.get(connection);
    let nextId = 0;
    await act(async () => {
      await latest.assignNativeThread!(chosen, "beta", {
        commandId: async () => `command-${++nextId}`,
      });
    });
    await act(async () => {
      await vi.waitFor(() => expect(latest.placements?.items[0]?.native_thread_id).toBe(chosen.id));
    });
    expect(latest.placements?.items[0]?.workstream_id).toBe("beta");
    expect(latest.placementInventory.identities).toHaveLength(1000);
    expect(latest.placementInventory.identities).toContainEqual(selected[0]);
    expect(latest.data?.binding.registryVersion).toBe(freshVersion + 3);
    expect(nextId).toBe(3);
    expect(registry.get(connection)).toBe(mountedConnection);
    expect(responses).toHaveLength(0);
  },
);

it("keeps an explicit recovery path after a preflight refusal without submitting any command", async () => {
  http.run.mockResolvedValueOnce(data).mockResolvedValueOnce(registration);
  await act(async () => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams />
      </RegistryContext.Provider>,
    );
  });
  expect(latest.data).toEqual(data);
  await act(async () => {
    await expect(
      latest.runBindingOperation(async () => {
        throw new Error("preflight refused");
      }),
    ).rejects.toThrow("preflight refused");
  });
  expect(latest.data).toBeNull();
  expect(latest.registrationContext).toBeNull();
  expect(latest.placements).toBeNull();
  expect(latest.error).toBeTruthy();
  expect(http.run).toHaveBeenCalledTimes(2);
  const fresh = { ...data, binding: { ...data.binding, registryVersion: 12 } };
  http.run
    .mockResolvedValueOnce(fresh)
    .mockResolvedValueOnce({ ...registration, registry_version: 12 });
  await act(async () => latest.refresh());
  expect(latest.data).toEqual(fresh);
  expect(latest.error).toBeNull();
  expect(http.run).toHaveBeenCalledTimes(4);
});

it("automatically starts verify-only renewal before the original native proof expires", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(now);
  vi.stubGlobal("window", globalThis);
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => storage.set(key, value),
  });
  const source = {
    provider: "t3" as const,
    source_instance_id: thread.environmentId,
    authority_namespace: "authority",
    store_generation: 1,
    resource_kind: "thread" as const,
    id_kind: "internal" as const,
    account_provenance: { kind: "not_account_scoped" as const },
    native_protocol: "workstreams-t3-provider/1.0.0" as const,
    build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
  };
  const identities = nativePlacementInventory(nativeThreads).identities;
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(t3PlacementInventoryJson(identities)),
  );
  const projection = {
    page: {
      context: placements.context,
      items: placements.items.map((item) => ({
        ...item,
        expires_at: new Date(now + 300_000).toISOString(),
      })),
      next_cursor: null,
      inventory_sha256: Array.from(new Uint8Array(digest), (value) =>
        value.toString(16).padStart(2, "0"),
      ).join(""),
    },
    trustedEnvironments: placements.trustedEnvironments,
    readiness: "ready",
  };
  http.run
    .mockResolvedValueOnce(data)
    .mockResolvedValueOnce({ ...registration, sources: [source] })
    .mockResolvedValueOnce(projection);
  await act(async () => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <MountedWorkstreams enabled />
      </RegistryContext.Provider>,
    );
  });
  expect(latest.placements?.items).toHaveLength(2);
  http.run.mockRejectedValue(new Error("synthetic renewal read outage"));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(240_000);
  });
  expect(http.run.mock.calls.length).toBeGreaterThan(3);
  expect(latest.placements?.items).toHaveLength(2);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  expect(latest.placements).toBeNull();
});

it.each(["cold load", "reconnect"])(
  "restores expired grouping on %s and keeps the mounted membership through two expiries",
  async (mode) => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    vi.stubGlobal("window", globalThis);
    const storage = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => storage.get(key) ?? null,
      setItem: (key: string, value: string) => storage.set(key, value),
    });
    const source = {
      provider: "t3" as const,
      source_instance_id: thread.environmentId,
      authority_namespace: "authority",
      store_generation: 1,
      resource_kind: "thread" as const,
      id_kind: "internal" as const,
      account_provenance: { kind: "not_account_scoped" as const },
      native_protocol: "workstreams-t3-provider/1.0.0" as const,
      build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
    };
    const identities = nativePlacementInventory(nativeThreads).identities;
    const hash = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(t3PlacementInventoryJson(identities)),
    );
    const digest = Array.from(new Uint8Array(hash), (value) =>
      value.toString(16).padStart(2, "0"),
    ).join("");
    const readContext = (version: number) => ({ ...placements.context, registry_version: version });
    const list = (version: number) => ({
      ...data,
      binding: { ...data.binding, registryVersion: version },
    });
    const context = (version: number) => ({
      ...registration,
      registry_version: version,
      sources: [source],
    });
    const native = (proof: number, at: number) => ({
      ...reference,
      registration: {
        ...reference.registration,
        attestation_version: proof,
        attested_at: new Date(at).toISOString(),
        expires_at: new Date(at + 300_000).toISOString(),
      },
    });
    const projection = (version: number, proof: number, at: number, expired = false) => ({
      page: {
        context: readContext(version),
        inventory_sha256: digest,
        next_cursor: null,
        items: expired
          ? []
          : [
              {
                ...placements.items[0]!,
                attestation_version: proof,
                attested_at: new Date(at).toISOString(),
                expires_at: new Date(at + 300_000).toISOString(),
              },
            ],
      },
      readiness: "ready",
      trustedEnvironments: placements.trustedEnvironments,
    });
    const member = {
      membership_id: "membership",
      workstream_id: "alpha",
      native_reference_id: "reference",
      kind: "primary",
      opened: {
        at: new Date(now).toISOString(),
        actor: { principal_id: "principal" },
        command_id: "original",
        registry_version: 1,
      },
      closed: null,
    };
    const members = (version: number, present = true) => ({
      context: readContext(version),
      items: present ? [member] : [],
      next_cursor: null,
    });
    const expiredAt = now - 300_001;
    const responses: unknown[] = [];
    if (mode === "reconnect")
      responses.push(list(11), registration, projection(11, 2, expiredAt, true));
    responses.push(
      list(11),
      context(11),
      projection(11, 2, expiredAt, true),
      { context: readContext(11), items: [native(2, expiredAt)], next_cursor: null },
      members(11),
      members(11, false),
    );
    for (let round = 0; round < 3; round++) {
      const version = 11 + round;
      const proof = 2 + round;
      const at = round === 0 ? expiredAt : now + (round - 1) * 240_000;
      const freshAt = now + round * 240_000;
      responses.push(
        list(version),
        context(version),
        projection(version, proof, at, round === 0),
        { context: readContext(version), reference: native(proof, at), latest_observation: null },
        members(version),
      );
      const commandId = `renewal-mounted-${round}`;
      responses.push({
        command_id: commandId,
        owner_id: "owner",
        actor: { principal_id: "principal" },
        operation: "verify_reference",
        request_sha256: "c".repeat(64),
        server_generation: 7,
        accepted_at: new Date(freshAt).toISOString(),
        completed_at: new Date(freshAt).toISOString(),
        state: "committed",
        registry_version: version + 1,
        changed: true,
        effects: {
          native_reference_id: "reference",
          registration: native(proof + 1, freshAt).registration,
          workstream_versions: [],
          membership_ids: [],
          declaration_id: null,
          declaration_revision: null,
          edge_id: null,
          observation: null,
          lifecycle_declaration: null,
          coordination_disposition: null,
          native_settlement: null,
        },
      });
      responses.push(
        list(version + 1),
        context(version + 1),
        projection(version + 1, proof + 1, freshAt),
        {
          context: readContext(version + 1),
          reference: native(proof + 1, freshAt),
          latest_observation: null,
        },
        members(version + 1),
      );
    }
    // Command IDs are generated by the application's UUID helper; bind each synthetic receipt to the persisted intent.
    http.run.mockImplementation(async () => {
      const response = responses.shift();
      if (response && typeof response === "object" && "command_id" in response) {
        const records: { commandId: string }[] = JSON.parse([...storage.values()][0]!);
        return { ...response, command_id: records[0]!.commandId };
      }
      if (!response) throw new Error("unexpected HTTP request");
      return response;
    });
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <MountedWorkstreams enabled />
        </RegistryContext.Provider>,
      );
    });
    if (mode === "reconnect")
      await act(async () => {
        registry.set(connection, AsyncResult.success({ ...connected, generation: 2 }));
      });
    await act(async () => {
      await vi.waitFor(() =>
        expect(http.run).toHaveBeenCalledTimes(mode === "cold load" ? 17 : 20),
      );
    });
    expect(latest.placements?.items[0]).toMatchObject({
      membership_id: "membership",
      native_reference_id: "reference",
      attestation_version: 3,
    });
    const sameConnection = registry.get(connection);
    for (const proof of [4, 5]) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(240_000);
      });
      await act(async () => {
        await vi.waitFor(() =>
          expect(http.run).toHaveBeenCalledTimes(
            (mode === "cold load" ? 17 : 20) + (proof - 3) * 11,
          ),
        );
      });
    }
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120_000);
    });
    expect(latest.placements?.items[0]).toMatchObject({
      membership_id: "membership",
      native_reference_id: "reference",
      attestation_version: 5,
    });
    expect(latest.error).toBeNull();
    expect(registry.get(connection)).toBe(sameConnection);
    expect(responses).toHaveLength(0);
  },
);
