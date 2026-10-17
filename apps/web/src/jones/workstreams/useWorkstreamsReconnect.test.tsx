import { RegistryContext } from "@effect/atom-react";
import type { SupervisorConnectionState } from "@t3tools/client-runtime/connection";
import type { WorkstreamsRegistrationContextAcceptedResponse } from "@t3tools/contracts";
import {
  EnvironmentHttpBadRequestError,
  EnvironmentHttpConflictError,
  EnvironmentHttpUnauthorizedError,
  EnvironmentInternalError,
} from "@t3tools/contracts";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { data, thread } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { useWorkstreams, type WorkstreamListView } from "../../state/workstreams";

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

function MountedWorkstreams() {
  const view = useWorkstreams(false, nativeThreads);
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
