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

import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";
import {
  data,
  thread,
  placements,
  reference,
  now,
} from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { useWorkstreams, type WorkstreamListView } from "../../state/workstreams";
import { routeWorkstreamsHttp } from "./workstreamsHttp.fixtures";

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
  // The list commits together with its registration context, never ahead of it.
  expect(latest.data).toBeNull();
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
    const memberships = (version: number, items: readonly unknown[] = []) => ({
      context: readContext(version),
      items,
      next_cursor: null,
    });
    const attached = {
      membership_id: "membership",
      workstream_id: "beta",
      native_reference_id: chosenReference.native_reference_id,
      kind: "primary",
      opened: {
        at: new Date(now).toISOString(),
        actor: { principal_id: "principal" },
        command_id: "command-3",
        registry_version: freshVersion + 3,
      },
      closed: null,
    };
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
    // Index reads run concurrently, so each route keeps its own strict response queue.
    const responses: Record<string, unknown[]> = {
      list: [
        list(11),
        list(freshVersion),
        list(freshVersion + 1),
        list(freshVersion + 2),
        list(freshVersion + 3),
      ],
      registrationContext: [
        context(11),
        context(freshVersion),
        context(freshVersion + 1),
        context(freshVersion + 2),
        context(freshVersion + 3),
      ],
      references: [
        { context: readContext(11), items: [], next_cursor: null },
        { context: readContext(freshVersion + 3), items: [chosenReference], next_cursor: null },
      ],
      "memberships:alpha": [memberships(11), memberships(freshVersion + 3)],
      "memberships:beta": [
        memberships(11),
        memberships(freshVersion),
        memberships(freshVersion + 1),
        memberships(freshVersion + 2),
        memberships(freshVersion + 3, [attached]),
      ],
      threadPlacements: [
        await projection(freshVersion, selected),
        await projection(freshVersion + 1, selected),
        await projection(freshVersion + 2, selected),
        await projection(freshVersion + 3, selected, true),
      ],
      reference: [
        {
          context: readContext(freshVersion + 1),
          reference: quarantined,
          latest_observation: null,
        },
        {
          context: readContext(freshVersion + 2),
          reference: chosenReference,
          latest_observation: null,
        },
      ],
      submit: [
        receipt(freshVersion + 1, "register_reference"),
        receipt(freshVersion + 2, "verify_reference"),
        receipt(freshVersion + 3, "attach_primary"),
      ],
    };
    const placementRequests: (readonly T3PlacementIdentity[])[] = [];
    const completion = () => {
      let resolve!: () => void;
      const promise = new Promise<void>((done) => {
        resolve = done;
      });
      return { promise, resolve };
    };
    const initialRead = { requested: completion(), response: completion() };
    const refreshedRead = { requested: completion(), response: completion() };
    const completePlacementRead = async (read: typeof initialRead, assertCommitted: () => void) => {
      await read.requested.promise;
      // Waiting for rendered state inside act would block the commit being observed.
      await act(async () => {
        read.response.resolve();
      });
      assertCommitted();
    };
    http.run.mockImplementation(
      routeWorkstreamsHttp(async (call) => {
        const key =
          call.route === "memberships"
            ? `memberships:${call.request.params?.workstreamId}`
            : call.route;
        const response = responses[key]?.shift();
        if (!response) throw new Error(`Unexpected additional HTTP request: ${key}`);
        if (call.route === "threadPlacements")
          placementRequests.push(call.request.payload?.identities as T3PlacementIdentity[]);
        // The initial read ends with registration context: no indexed identity needs proof yet.
        const read =
          key === "registrationContext" && responses.registrationContext!.length === 4
            ? initialRead
            : key === "threadPlacements" && responses.threadPlacements!.length === 0
              ? refreshedRead
              : null;
        if (read) {
          read.requested.resolve();
          await read.response.promise;
        }
        return response;
      }),
    );
    try {
      await act(async () => {
        renderer = create(
          <RegistryContext.Provider value={registry}>
            <MountedWorkstreams enabled threads={threads} />
          </RegistryContext.Provider>,
        );
      });
      await completePlacementRead(initialRead, () => expect(latest.loading).toBe(false));
      expect(latest.placementInventory.identities).not.toContainEqual(selected[0]);
      expect(latest.assignmentIndex?.entries.size).toBe(0);
      expect(placementRequests).toEqual([]);
      expect(latest.data?.binding.registryVersion).toBe(11);
      const mountedConnection = registry.get(connection);
      let nextId = 0;
      await act(async () => {
        await latest.assignNativeThread!(chosen, "beta", {
          commandId: async () => `command-${++nextId}`,
        });
      });
      await completePlacementRead(refreshedRead, () =>
        expect(latest.placements?.items[0]?.native_thread_id).toBe(chosen.id),
      );
      expect(latest.placements?.items[0]?.workstream_id).toBe("beta");
      // Proof is requested only for the indexed identity, outside any capped inventory.
      expect(placementRequests.at(-1)).toEqual(selected);
      expect(latest.placementInventory.identities).toEqual(selected);
      expect(
        latest.assignmentIndex?.entries.get(JSON.stringify([chosen.environmentId, chosen.id]))
          ?.primary?.workstreamId,
      ).toBe("beta");
      expect(latest.data?.binding.registryVersion).toBe(freshVersion + 3);
      expect(nextId).toBe(3);
      expect(registry.get(connection)).toBe(mountedConnection);
      expect(Object.values(responses).flat()).toHaveLength(0);
    } finally {
      initialRead.response.resolve();
      refreshedRead.response.resolve();
    }
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
  // A refusal that is not lost access keeps the last good list, stale and read-only.
  expect(latest.data).toEqual({ ...data, stale: true });
  expect(latest.stale).toBe(true);
  expect(canEditWorkstreams(latest.data)).toBe(false);
  expect(latest.registrationContext).toEqual(registration);
  expect(latest.placements).toBeNull();
  expect(latest.error).toBeTruthy();
  expect(http.run).toHaveBeenCalledTimes(2);
  const fresh = { ...data, binding: { ...data.binding, registryVersion: 12 } };
  http.run
    .mockResolvedValueOnce(fresh)
    .mockResolvedValueOnce({ ...registration, registry_version: 12 });
  await act(async () => latest.refresh());
  expect(latest.data).toEqual(fresh);
  expect(latest.stale).toBe(false);
  expect(latest.error).toBeNull();
  expect(http.run).toHaveBeenCalledTimes(4);
});
