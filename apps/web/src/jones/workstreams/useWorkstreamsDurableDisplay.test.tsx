import { act } from "../testing/nodeRenderer";
import { RegistryContext } from "@effect/atom-react";
import type { SupervisorConnectionState } from "@t3tools/client-runtime/connection";
import { RemoteEnvironmentAuthFetchError } from "@t3tools/client-runtime/rpc";
import {
  groupNativeThreadsByWorkstream,
  nativeWorkstreamThreadKey,
  type NativeWorkstreamThreadGrouping,
} from "@t3tools/client-runtime/state/workstreams";
import {
  EnvironmentHttpConflictError,
  EnvironmentHttpUnauthorizedError,
  EnvironmentInternalError,
  T3_PLACEMENT_MAX_IDENTITIES,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
  type T3PlacementIdentity,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";
import { data } from "../../components/workstreams/nativeWorkstreamActions.fixtures";
import { useWorkstreamAppearance } from "../workstreamAppearance/useWorkstreamAppearance";
import { useWorkstreams, type WorkstreamListView } from "../../state/workstreams";
import {
  routeWorkstreamsHttp,
  syntheticWorkstreams,
  type SyntheticAssignment,
} from "./workstreamsHttp.fixtures";

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
const realSetTimeout = globalThis.setTimeout;
const start = Date.parse("2026-09-30T12:00:00Z");
// The sidebar's coarse snooze clock: it lags the hook's verification clock.
const coarseNow = "2026-09-30T12:00:00.000Z";

interface Rendered {
  readonly view: WorkstreamListView;
  readonly grouping: NativeWorkstreamThreadGrouping<{ environmentId: string; id: string }>;
}

let registry: ReturnType<typeof AtomRegistry.make>;
let renderer: ReactTestRenderer | undefined;
let renders: Rendered[];
let latest: Rendered;

const nativeThreads = (count: number) =>
  Array.from({ length: count }, (_, index) => ({
    environmentId: "env:a",
    id: String(index).padStart(5, "0"),
  }));
const keyOf = (thread: { readonly environmentId: string; readonly id: string }) =>
  nativeWorkstreamThreadKey(thread.environmentId, thread.id);
const assign = (
  thread: { readonly environmentId: string; readonly id: string },
  workstreamId: string,
  overrides: Partial<SyntheticAssignment> = {},
): SyntheticAssignment => ({
  environmentId: thread.environmentId,
  threadId: thread.id,
  workstreamId,
  ...overrides,
});

function Mounted({
  threads,
}: {
  readonly threads: readonly { environmentId: string; id: string }[];
}) {
  const view = useWorkstreams(true, threads);
  // Mirrors the Sidebar hook: stored assignments group; proof assurance uses the hook clock.
  const grouping = groupNativeThreadsByWorkstream({
    workstreams: view.data?.items ?? [],
    placements: view.placements?.items ?? [],
    threads,
    trustedNow: coarseNow,
    trustedEnvironments: new Map(
      (view.placements?.trustedEnvironments ?? []).map((entry) => [entry.environmentId, entry]),
    ),
    ...(view.assignmentIndex
      ? {
          assignmentIndex: view.assignmentIndex,
          verificationNow: view.verificationNow ?? coarseNow,
        }
      : {}),
  });
  useLayoutEffect(() => {
    latest = { view, grouping };
    renders.push(latest);
  });
  return null;
}

const mount = async (threads: readonly { environmentId: string; id: string }[]) => {
  await act(async () => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <Mounted threads={threads} />
      </RegistryContext.Provider>,
    );
  });
};

/** Lets real crypto digests finish inside bounded act turns, then asserts outside act. */
async function settle(check: () => void, attempts = 3_000): Promise<void> {
  for (let attempt = 0; ; attempt += 1) {
    await act(async () => {
      await new Promise((resolve) => realSetTimeout(resolve, 0));
    });
    try {
      check();
      return;
    } catch (cause) {
      if (attempt >= attempts) throw cause;
    }
  }
}

const loaded = (version = data.binding.registryVersion) => {
  expect(latest.view.loading).toBe(false);
  expect(latest.view.data?.binding.registryVersion).toBe(version);
};

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"], now: start });
  vi.stubGlobal("window", globalThis);
  http.run.mockReset();
  registry = AtomRegistry.make();
  registry.set(connection, AsyncResult.success(connected));
  renders = [];
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  registry.dispose();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("durable Workstream assignment display", () => {
  it("groups 4,600 native threads with assignments beyond 1,000 and bounds every proof batch", async () => {
    const threads = nativeThreads(4_600);
    const assigned = threads.filter((_, index) => index % 2 === 1);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: assigned.map((thread, index) => assign(thread, index % 2 ? "beta" : "alpha")),
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());

    const { grouping, view } = latest;
    expect(view.assignmentIndex?.complete).toBe(true);
    expect(grouping.groupedKeys.size).toBe(2_300);
    for (const thread of assigned)
      expect(grouping.assuranceByKey?.get(keyOf(thread))).toBe("verified");
    expect(grouping.assuranceByKey?.get(keyOf(threads[4_598]!))).toBe("unassigned");
    expect(grouping.groups.map((group) => group.threads.length)).toEqual([1_150, 1_150]);
    expect(grouping.groupedKeys.has(keyOf(threads[4_599]!))).toBe(true);

    const requests = server.state.placementRequests;
    expect(requests.map((batch) => batch.length)).toEqual([1_000, 1_000, 300]);
    for (const batch of requests)
      expect(
        new TextEncoder().encode(JSON.stringify({ identities: batch })).byteLength,
      ).toBeLessThanOrEqual(T3_PLACEMENT_MAX_REQUEST_BYTES);
    expect(Math.max(...requests.map((batch) => batch.length))).toBeLessThanOrEqual(
      T3_PLACEMENT_MAX_IDENTITIES,
    );
    // Only stored identities are requested; every batch was accepted against its own hash.
    const requested = new Set(requests.flat().map((entry) => entry.native_thread_id));
    expect(requested).toEqual(new Set(assigned.map((thread) => thread.id)));
    expect(view.placementInventory).toMatchObject({ coverage: "complete", totalIdentities: 2_300 });
    expect(view.placements?.items).toHaveLength(2_300);
  });

  it("keeps groups when the clock passes proof expiry, downgrading them to recorded", async () => {
    const threads = nativeThreads(3);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "alpha"), assign(threads[1]!, "alpha")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("verified");
    const calls = server.state.calls.length;
    const committed = renders.length;

    await act(() => vi.advanceTimersByTimeAsync(4 * 60_000));
    await settle(() =>
      expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("recorded"),
    );
    expect(latest.grouping.groups[0]?.threads.map((thread) => thread.id)).toEqual([
      "00000",
      "00001",
    ]);
    expect(latest.view.placements).not.toBeNull();
    expect(server.state.calls).toHaveLength(calls);
    for (const render of renders.slice(committed - 1))
      expect(render.grouping.groupedKeys.has(keyOf(threads[1]!))).toBe(true);
  });

  it("never renders an assigned thread as Unassigned during refresh or reconnect", async () => {
    const threads = nativeThreads(3);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "alpha")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const firstCommit = renders.findIndex((render) => render.view.data !== null);

    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    server.state.intercept = async (call) => {
      if (call.route === "list") await held;
      return undefined;
    };
    server.state.version += 1;
    await act(async () => latest.view.refresh());
    await settle(() => expect(server.state.calls.at(-1)?.route).toBe("list"));
    // The reload is in flight and the last good groups are still rendered.
    expect(latest.view.loading).toBe(true);
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    expect(latest.view.assignmentIndex?.registryVersion).toBe(11);
    server.state.intercept = null;
    release();
    await settle(() => loaded(12));

    server.state.version += 1;
    await act(async () => {
      registry.set(connection, AsyncResult.success({ ...connected, generation: 2 }));
    });
    await settle(() => loaded(13));
    for (const render of renders.slice(firstCommit)) {
      expect(render.view.data).not.toBeNull();
      expect(render.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
      expect(render.grouping.assuranceByKey?.get(keyOf(threads[1]!))).toBe("unassigned");
    }
  });

  it.each([
    ["a transport failure", new Error("Connection interrupted")],
    [
      "an auth transport failure",
      new RemoteEnvironmentAuthFetchError({ message: "fetch failed", cause: null }),
    ],
  ])("keeps the last good index as stale after %s", async (_label, failure) => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const index = latest.view.assignmentIndex;

    server.state.intercept = (call) => {
      if (call.route === "list") throw failure;
      return undefined;
    };
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.loading).toBe(false));
    expect(latest.view.stale).toBe(true);
    expect(latest.view.error).toBe(failure.message);
    expect(latest.view.data).not.toBeNull();
    expect(latest.view.assignmentIndex).toBe(index);
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    expect(
      renders.every(
        (render) => render.view.data === null || render.grouping.groupedKeys.size === 1,
      ),
    ).toBe(true);

    server.state.intercept = null;
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.stale).toBe(false));
    expect(latest.view.error).toBeNull();
  });

  it("treats an opaque control-plane failure as transient after its bounded retries", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const outage = new EnvironmentInternalError({
      code: "internal_error",
      reason: "internal_error",
      traceId: "cp-outage",
    });
    server.state.intercept = (call) => {
      if (call.route === "list") throw outage;
      return undefined;
    };
    await act(async () => latest.view.refresh());
    for (let second = 0; second < 32; second += 1) {
      await act(() => vi.advanceTimersByTimeAsync(1_000));
      await settle(() => undefined, 1);
    }
    await settle(() => expect(latest.view.loading).toBe(false));
    expect(latest.view.stale).toBe(true);
    expect(latest.view.assignmentIndex).not.toBeNull();
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
  });

  it("hides previously loaded Workstreams when authorization is rejected", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const denied = new EnvironmentHttpUnauthorizedError({ message: "authorization revoked" });
    server.state.intercept = (call) => {
      if (call.route === "list") throw denied;
      return undefined;
    };
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.loading).toBe(false));
    expect(latest.view.data).toBeNull();
    expect(latest.view.assignmentIndex).toBeNull();
    expect(latest.view.placements).toBeNull();
    expect(latest.view.registrationContext).toBeNull();
    expect(latest.view.stale).toBe(false);
    expect(latest.view.error).toBe(denied.message);
  });

  it.each([
    ["not configured", new EnvironmentHttpConflictError({ message: "workstreams_not_configured" })],
    [
      "not found",
      Object.assign(new Error("Not Found"), { _tag: "ResponseError", response: { status: 404 } }),
    ],
  ])(
    "keeps the last good groups as stale and read-only when the server answers %s",
    async (_label, failure) => {
      const threads = nativeThreads(2);
      const server = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [assign(threads[0]!, "beta")],
      });
      http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
      await mount(threads);
      await settle(() => loaded());
      const index = latest.view.assignmentIndex;
      server.state.intercept = (call) => {
        if (call.route === "list") throw failure;
        return undefined;
      };
      await act(async () => latest.view.refresh());
      await settle(() => expect(latest.view.loading).toBe(false));
      expect(latest.view.stale).toBe(true);
      expect(latest.view.data).not.toBeNull();
      expect(latest.view.assignmentIndex).toBe(index);
      expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    },
  );

  it("keeps other proof batches and every stored group when one batch fails", async () => {
    const threads = nativeThreads(1_500);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: threads.map((thread) => assign(thread, "alpha")),
    });
    server.state.intercept = (call) => {
      const identities = call.request.payload?.identities as
        | readonly T3PlacementIdentity[]
        | undefined;
      if (call.route === "threadPlacements" && identities?.length === 500)
        throw new Error("proof outage");
      return undefined;
    };
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const assurance = latest.grouping.assuranceByKey!;
    expect(latest.grouping.groupedKeys.size).toBe(1_500);
    expect(threads.every((thread) => assurance.get(keyOf(thread)) !== "unknown")).toBe(true);
    // Only the failed batch lacks placement proof; the successful batch is kept whole.
    expect(new Set(latest.view.placements?.items.map((item) => item.native_thread_id))).toEqual(
      new Set(threads.slice(0, 1_000).map((thread) => thread.id)),
    );
    expect(latest.view.placementInventory).toMatchObject({
      coverage: "partial",
      totalIdentities: 1_500,
    });
    expect(latest.view.placementInventory.identities).toHaveLength(1_000);
    expect(latest.view.placementInventory.proofsUnavailable).toBe(false);
  });

  it.each([
    [
      "not configured",
      () => new EnvironmentHttpConflictError({ message: "workstreams_not_configured" }),
    ],
    [
      "missing (404)",
      () =>
        Object.assign(new Error("Not Found"), { _tag: "ResponseError", response: { status: 404 } }),
    ],
  ])(
    "keeps recorded groups and reports proofs unavailable when placements are %s",
    async (_label, failure) => {
      const threads = nativeThreads(2);
      const server = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [assign(threads[0]!, "beta")],
      });
      server.state.intercept = (call) => {
        if (call.route === "threadPlacements") throw failure();
        return undefined;
      };
      http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
      await mount(threads);
      await settle(() => loaded());
      expect(latest.view.stale).toBe(false);
      expect(latest.view.error).toBeNull();
      expect(latest.view.placements).toBeNull();
      expect(latest.grouping.assuranceByKey!.get(keyOf(threads[0]!))).toBe("recorded");
      expect(latest.view.placementInventory).toMatchObject({
        coverage: "partial",
        proofsUnavailable: true,
        identities: [],
      });
    },
  );

  it("displays a newly attested assignment immediately after the post-command reload", async () => {
    vi.setSystemTime(Date.parse("2026-09-30T12:00:45Z"));
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("unassigned");

    // Attested after the sidebar's coarse clock, as observed in the root Browser regression.
    server.state.assignments.push(
      assign(threads[0]!, "alpha", {
        attestedAt: "2026-09-30T12:00:30Z",
        expiresAt: "2026-09-30T12:05:30Z",
      }),
    );
    server.state.version += 1;
    await act(async () => latest.view.refresh());
    await settle(() => loaded(12));
    expect(latest.grouping.groups[0]?.workstream.workstreamId).toBe("alpha");
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("verified");
    // Grouping only from proof at the coarse clock would still leave it ungrouped.
    const proofOnly = groupNativeThreadsByWorkstream({
      workstreams: latest.view.data!.items,
      placements: latest.view.placements!.items,
      threads,
      trustedNow: coarseNow,
      trustedEnvironments: new Map(
        latest.view.placements!.trustedEnvironments.map((entry) => [entry.environmentId, entry]),
      ),
    });
    expect(proofOnly.groupedKeys.size).toBe(0);
  });

  it("never confirms Unassigned from an incomplete index", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "alpha")],
    });
    server.state.intercept = (call) =>
      call.route === "list"
        ? {
            ...data,
            binding: { ...data.binding, registryVersion: server.state.version },
            stale: true,
          }
        : undefined;
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    expect(latest.view.assignmentIndex?.complete).toBe(false);
    expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("recorded");
    expect(latest.grouping.assuranceByKey?.get(keyOf(threads[1]!))).toBe("unknown");
  });

  it("refreshes only proofs when a stored thread appears natively", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "alpha"), assign(threads[1]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount([threads[0]!]);
    await settle(() => loaded());
    expect(server.state.placementRequests).toEqual([
      [{ source_instance_id: "env:a", native_thread_id: "00000" }],
    ]);
    const lists = server.state.calls.filter((call) => call.route === "list").length;
    const before = renders.length;

    await act(async () => {
      renderer!.update(
        <RegistryContext.Provider value={registry}>
          <Mounted threads={threads} />
        </RegistryContext.Provider>,
      );
    });
    // Stored assignments group the new row at once, before its proof request completes.
    expect(latest.grouping.groupedKeys.has(keyOf(threads[1]!))).toBe(true);
    await settle(() => expect(server.state.placementRequests).toHaveLength(2));
    await settle(() =>
      expect(latest.view.placements?.items.map((item) => item.native_thread_id)).toEqual([
        "00000",
        "00001",
      ]),
    );
    expect(server.state.placementRequests[1]).toHaveLength(2);
    expect(server.state.calls.filter((call) => call.route === "list")).toHaveLength(lists);
    for (const render of renders.slice(before))
      expect(render.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
  });

  it("refuses to plan a first attach when a stored primary has no current proof", async () => {
    const threads = nativeThreads(1);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [
        assign(threads[0]!, "alpha", {
          attestedAt: "2026-09-30T11:00:00Z",
          expiresAt: "2026-09-30T11:05:00Z",
        }),
      ],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("recorded");

    const reference = {
      native_reference_id: "reference-00000",
      owner_id: "owner",
      identity: {
        provider: "t3",
        source_instance_id: "env:a",
        native_id: "00000",
        resource_kind: "thread",
        id_kind: "internal",
        account_provenance: { kind: "not_account_scoped" },
      },
      pr_locator: null,
      registration: {
        state: "attested",
        attestation_version: 3,
        attested_at: "2026-09-30T11:59:00Z",
        expires_at: "2026-09-30T12:04:00Z",
        evidence: {
          provider: "t3",
          source_instance_id: "env:a",
          native_id: "00000",
          authority_namespace: "authority",
          store_generation: 1,
          evidence_sha256: "a".repeat(64),
        },
      },
      created_at: "2026-09-30T11:00:00Z",
      created_by: { principal_id: "owner" },
      created_registry_version: 1,
    };
    const submitted: string[] = [];
    server.state.intercept = (call) => {
      if (call.route === "registrationContext")
        return {
          ...server.registration(),
          sources: [
            {
              provider: "t3",
              source_instance_id: "env:a",
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
        };
      if (call.route === "reference")
        return {
          context: {
            owner_id: "owner",
            server_generation: 7,
            registry_version: server.state.version,
          },
          reference,
          latest_observation: null,
        };
      if (call.route === "submit") {
        const command = call.request.payload?.command as {
          command_id: string;
          action: { operation: string };
        };
        submitted.push(command.action.operation);
        server.state.version += 1;
        return {
          command_id: command.command_id,
          owner_id: "owner",
          actor: { principal_id: "principal" },
          operation: command.action.operation,
          request_sha256: "c".repeat(64),
          server_generation: 7,
          accepted_at: vi.getMockedSystemTime()!.toISOString(),
          completed_at: vi.getMockedSystemTime()!.toISOString(),
          state: "committed",
          registry_version: server.state.version,
          changed: true,
          effects: { native_reference_id: "reference-00000" },
        } as unknown as WorkstreamReceipt;
      }
      return undefined;
    };
    let nextId = 0;
    let failure: unknown;
    await act(async () => {
      await latest.view.assignNativeThread!(threads[0]!, "beta", {
        commandId: async () => `command-${++nextId}`,
      }).catch((cause: unknown) => {
        failure = cause;
      });
    });
    // The stored reference is registered (idempotently) and re-verified, but its proof still
    // does not show the stored primary, so no membership command is planned.
    expect(failure).toMatchObject({ reason: "stale" });
    expect(submitted).toEqual(["register_reference"]);
    expect(submitted).not.toContain("attach_primary");
  });

  it("refuses every registry write while the last good snapshot is stale", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    expect(canEditWorkstreams(latest.view.data)).toBe(true);
    server.state.intercept = (call) => {
      if (call.route === "list") throw new Error("Connection interrupted");
      return undefined;
    };
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.stale).toBe(true));
    server.state.intercept = null;

    // Every edit affordance fed by the hook's list reads the snapshot as not current.
    expect(canEditWorkstreams(latest.view.data)).toBe(false);
    const command = {
      command_id: "command-stale",
      expected_server_generation: data.binding.serverGeneration,
      expected_registry_version: data.binding.registryVersion,
      action: { operation: "update_workstream" },
    } as unknown as Parameters<WorkstreamListView["submit"]>[0];
    const operation = vi.fn(async () => undefined);
    const failures: unknown[] = [];
    await act(async () => {
      await latest.view.submit(command).catch((cause: unknown) => failures.push(cause));
      await latest.view
        .runBindingOperation(operation)
        .catch((cause: unknown) => failures.push(cause));
      await latest.view.assignNativeThread!(threads[1]!, "beta", {
        commandId: async () => "command-assign",
      }).catch((cause: unknown) => failures.push(cause));
    });
    expect(failures).toHaveLength(3);
    for (const failure of failures) expect(failure).toMatchObject({ reason: "stale" });
    expect(operation).not.toHaveBeenCalled();
    expect(server.state.calls.filter((call) => call.route === "submit")).toHaveLength(0);
    // Refusals keep the last good groups visible.
    expect(latest.view.stale).toBe(true);
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
  });

  it("purges when the registration context rejects authorization after list and index load", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const denied = new EnvironmentHttpUnauthorizedError({ message: "authorization revoked" });
    server.state.intercept = (call) => {
      if (call.route === "registrationContext") throw denied;
      return undefined;
    };
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.loading).toBe(false));
    expect(latest.view.data).toBeNull();
    expect(latest.view.assignmentIndex).toBeNull();
    expect(latest.view.placements).toBeNull();
    expect(latest.view.registrationContext).toBeNull();
    expect(latest.view.stale).toBe(false);
    expect(latest.view.error).toBe(denied.message);
  });

  it("keeps the last good snapshot stale when the registration context read fails", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const context = latest.view.registrationContext;
    const index = latest.view.assignmentIndex;
    expect(context).not.toBeNull();
    server.state.version += 1;
    server.state.intercept = (call) => {
      if (call.route === "registrationContext") throw new Error("Connection interrupted");
      return undefined;
    };
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.loading).toBe(false));
    // The newer list and index were read but never published without a validated context.
    expect(latest.view.stale).toBe(true);
    expect(latest.view.error).toBe("Connection interrupted");
    expect(latest.view.data?.binding.registryVersion).toBe(11);
    expect(latest.view.assignmentIndex).toBe(index);
    expect(latest.view.registrationContext).toBe(context);
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
  });

  it("shows the error instead of a snapshot when the first registration context read fails", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    server.state.intercept = (call) => {
      if (call.route === "registrationContext") throw new Error("Connection interrupted");
      return undefined;
    };
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => expect(latest.view.loading).toBe(false));
    expect(latest.view.data).toBeNull();
    expect(latest.view.assignmentIndex).toBeNull();
    expect(latest.view.stale).toBe(false);
    expect(latest.view.error).toBe("Connection interrupted");
  });

  it("publishes without a registration context when the server does not provide one", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    server.state.intercept = (call) => {
      if (call.route === "registrationContext")
        throw Object.assign(new Error("Not Found"), {
          _tag: "ResponseError",
          response: { status: 404 },
        });
      return undefined;
    };
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    expect(latest.view.registrationContext).toBeNull();
    expect(latest.view.stale).toBe(false);
    expect(latest.view.error).toBeNull();
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    // An older server without the endpoint stays writable, as before registration contexts.
    expect(canEditWorkstreams(latest.view.data)).toBe(true);
  });

  const authorityMismatches = [
    ["owner", { owner_id: "another-owner" }],
    ["principal", { principal_id: "another-principal" }],
    ["authorization revision", { authorization_revision: data.binding.authorizationRevision + 1 }],
    ["server generation", { server_generation: data.binding.serverGeneration + 1 }],
  ] as const;

  it.each(authorityMismatches)(
    "shows no snapshot when a cold load's context has a different %s",
    async (_label, mismatch) => {
      const threads = nativeThreads(2);
      const server = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [assign(threads[0]!, "beta")],
      });
      server.state.intercept = (call) =>
        call.route === "registrationContext"
          ? { ...server.registration(), ...mismatch }
          : undefined;
      http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
      await mount(threads);
      await settle(() => expect(latest.view.loading).toBe(false));
      expect(latest.view.data).toBeNull();
      expect(latest.view.assignmentIndex).toBeNull();
      expect(latest.view.registrationContext).toBeNull();
      expect(latest.view.error).toBeTruthy();
      expect(renders.every((render) => render.view.data === null)).toBe(true);
      // A different authority is never retried into.
      expect(
        server.state.calls.filter((call) => call.route === "registrationContext"),
      ).toHaveLength(1);
    },
  );

  it.each(authorityMismatches)(
    "purges loaded Workstreams when a reload's context has a different %s",
    async (_label, mismatch) => {
      const threads = nativeThreads(2);
      const server = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [assign(threads[0]!, "beta")],
      });
      http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
      await mount(threads);
      await settle(() => loaded());
      const contextReads = server.state.calls.filter(
        (call) => call.route === "registrationContext",
      ).length;
      server.state.intercept = (call) =>
        call.route === "registrationContext"
          ? { ...server.registration(), ...mismatch }
          : undefined;
      await act(async () => {
        registry.set(connection, AsyncResult.success({ ...connected, generation: 5 }));
      });
      await settle(() => expect(latest.view.data).toBeNull());
      await settle(() => expect(latest.view.loading).toBe(false));
      expect(latest.view.assignmentIndex).toBeNull();
      expect(latest.view.placements).toBeNull();
      expect(latest.view.registrationContext).toBeNull();
      expect(latest.view.stale).toBe(false);
      expect(latest.view.error).toBeTruthy();
      expect(
        server.state.calls.filter((call) => call.route === "registrationContext"),
      ).toHaveLength(contextReads + 1);
      expect(server.state.calls.filter((call) => call.route === "submit")).toHaveLength(0);
    },
  );

  it("shows the list read-only without a context after registry-version recovery runs out", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    // The same authority keeps reporting a newer registry than the list just read.
    server.state.intercept = (call) =>
      call.route === "registrationContext"
        ? { ...server.registration(), registry_version: server.state.version + 1 }
        : undefined;
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    for (let second = 0; second < 32; second += 1) {
      await act(() => vi.advanceTimersByTimeAsync(1_000));
      await settle(() => undefined, 1);
    }
    await settle(() => loaded());
    expect(latest.view.registrationContext).toBeNull();
    expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    expect(latest.view.stale).toBe(true);
    expect(canEditWorkstreams(latest.view.data)).toBe(false);
    const failures: unknown[] = [];
    await act(async () => {
      await latest.view
        .submit({
          command_id: "command-without-context",
          expected_server_generation: data.binding.serverGeneration,
          expected_registry_version: data.binding.registryVersion,
          action: { operation: "update_workstream" },
        } as unknown as Parameters<WorkstreamListView["submit"]>[0])
        .catch((cause: unknown) => failures.push(cause));
      await latest.view
        .runBindingOperation(async () => undefined)
        .catch((cause: unknown) => failures.push(cause));
    });
    expect(failures).toHaveLength(2);
    for (const failure of failures) expect(failure).toMatchObject({ reason: "stale" });
    expect(server.state.calls.filter((call) => call.route === "submit")).toHaveLength(0);

    // A fresh context restores writes.
    server.state.intercept = null;
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.stale).toBe(false));
    expect(latest.view.registrationContext).not.toBeNull();
    expect(canEditWorkstreams(latest.view.data)).toBe(true);
  });

  it("refuses an open color save once the snapshot turns stale", async () => {
    vi.stubGlobal("document", {
      hidden: false,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
    });
    vi.stubGlobal("addEventListener", () => undefined);
    vi.stubGlobal("removeEventListener", () => undefined);
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    const saves: unknown[] = [];
    server.state.intercept = (call) => {
      if (call.route === "appearanceRead") {
        const ids = call.request.payload?.workstream_ids as readonly string[];
        return {
          supported: true,
          page: {
            owner_id: data.binding.ownerId,
            server_generation: data.binding.serverGeneration,
            permissions: ["workstreams:read", "workstreams:write"],
            items: ids.map((id) => ({ workstream_id: id, border_color: null, version: 1 })),
          },
        };
      }
      if (call.route === "appearanceSave") {
        saves.push(call.request.payload);
        return { workstream_id: "alpha", border_color: "#2563EB", version: 2 };
      }
      return undefined;
    };
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    let appearance!: ReturnType<typeof useWorkstreamAppearance>;
    function WithAppearance() {
      const view = useWorkstreams(true, threads);
      const current = useWorkstreamAppearance(view.data);
      useLayoutEffect(() => {
        latest = { ...latest, view };
        appearance = current;
      });
      return null;
    }
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <WithAppearance />
        </RegistryContext.Provider>,
      );
    });
    await settle(() => loaded());
    await settle(() => expect(appearance.writable).toBe(true));
    // The dialog captured this save while the snapshot was writable.
    const save = appearance.save;
    const previous = server.state.intercept;
    server.state.intercept = (call) => {
      if (call.route === "list") throw new Error("Connection interrupted");
      return previous(call);
    };
    await act(async () => latest.view.refresh());
    await settle(() => expect(latest.view.stale).toBe(true));
    const failures: unknown[] = [];
    for (const attempt of [save, appearance.save])
      await act(async () => {
        await attempt({
          command_id: "appearance-command-0001",
          workstream_id: "alpha",
          expected_server_generation: data.binding.serverGeneration,
          expected_version: 1,
          border_color: "#2563EB",
        }).catch((cause: unknown) => failures.push(cause));
      });
    expect(failures).toHaveLength(2);
    for (const failure of failures) expect(failure).toMatchObject({ reason: "stale" });
    expect(saves).toHaveLength(0);
  });

  it("purges at once when a proof-only refresh rejects authorization", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "alpha"), assign(threads[1]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount([threads[0]!]);
    await settle(() => loaded());
    const denied = new EnvironmentHttpUnauthorizedError({ message: "authorization revoked" });
    server.state.intercept = (call) => {
      if (call.route === "threadPlacements") throw denied;
      // A later list read would fail as an outage, which alone would only mark it stale.
      if (call.route === "list") throw new Error("Connection interrupted");
      return undefined;
    };
    await act(async () => {
      renderer!.update(
        <RegistryContext.Provider value={registry}>
          <Mounted threads={threads} />
        </RegistryContext.Provider>,
      );
    });
    await settle(() => expect(latest.view.data).toBeNull());
    await settle(() => expect(latest.view.loading).toBe(false));
    expect(latest.view.assignmentIndex).toBeNull();
    expect(latest.view.placements).toBeNull();
    expect(latest.view.stale).toBe(false);
    expect(latest.view.error).toBe(denied.message);
  });

  it("installs a retry's list, context and index together at one registry version", async () => {
    const threads = nativeThreads(2);
    const server = syntheticWorkstreams({
      now: () => vi.getMockedSystemTime()!.getTime(),
      assignments: [assign(threads[0]!, "beta")],
    });
    http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
    await mount(threads);
    await settle(() => loaded());
    const firstCommit = renders.findIndex((render) => render.view.data !== null);

    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    // Only the assignment index reads memberships, so this gates the index alone.
    server.state.intercept = async (call) => {
      if (call.route === "memberships") await held;
      return undefined;
    };
    server.state.version += 1;
    let retried!: Promise<void>;
    await act(async () => {
      retried = latest.view.retry!();
    });
    await settle(() => expect(server.state.calls.at(-1)?.route).toBe("memberships"));
    await act(async () => retried);
    expect(latest.view.assignmentIndex?.registryVersion).toBe(11);
    server.state.intercept = null;
    release();
    await settle(() => loaded(12));
    expect(latest.view.assignmentIndex?.registryVersion).toBe(12);
    expect(latest.view.registrationContext?.registry_version).toBe(12);
    expect(latest.view.error).toBeNull();
    for (const render of renders.slice(firstCommit)) {
      const version = render.view.data?.binding.registryVersion;
      expect(render.view.assignmentIndex?.registryVersion).toBe(version);
      expect(render.view.registrationContext?.registry_version).toBe(version);
      expect(render.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
    }
  });

  describe("after reference commands commit and the next snapshot read fails", () => {
    const expiredReference = {
      native_reference_id: "reference-00000",
      owner_id: "owner",
      identity: {
        provider: "t3",
        source_instance_id: "env:a",
        native_id: "00000",
        resource_kind: "thread",
        id_kind: "internal",
        account_provenance: { kind: "not_account_scoped" },
      },
      pr_locator: null,
      registration: {
        state: "attested",
        attestation_version: 3,
        attested_at: "2026-09-30T11:00:00Z",
        expires_at: "2026-09-30T11:05:00Z",
        evidence: {
          provider: "t3",
          source_instance_id: "env:a",
          native_id: "00000",
          authority_namespace: "authority",
          store_generation: 1,
          evidence_sha256: "a".repeat(64),
        },
      },
      created_at: "2026-09-30T11:00:00Z",
      created_by: { principal_id: "owner" },
      created_registry_version: 1,
    };
    const readySource = {
      provider: "t3",
      source_instance_id: "env:a",
      authority_namespace: "authority",
      store_generation: 1,
      resource_kind: "thread",
      id_kind: "internal",
      account_provenance: { kind: "not_account_scoped" },
      native_protocol: "workstreams-t3-provider/1.0.0",
      build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
    };

    /**
     * Moves a thread whose stored primary is beta but whose proof has expired, so the
     * assignment registers and re-verifies its reference before the next snapshot read.
     */
    const startMove = async (input: {
      readonly failure: (call: { readonly route: string }) => unknown;
      readonly verifyState?: "committed" | "pending";
    }) => {
      const threads = nativeThreads(2);
      const server = syntheticWorkstreams({
        now: () => vi.getMockedSystemTime()!.getTime(),
        assignments: [
          assign(threads[0]!, "beta", {
            attestedAt: "2026-09-30T11:00:00Z",
            expiresAt: "2026-09-30T11:05:00Z",
          }),
        ],
      });
      http.run.mockImplementation(routeWorkstreamsHttp(server.handle));
      await mount(threads);
      await settle(() => loaded());
      expect(latest.grouping.assuranceByKey?.get(keyOf(threads[0]!))).toBe("recorded");
      const submitted: string[] = [];
      const failing = { active: false };
      const receipt = (command: { command_id: string; action: { operation: string } }) =>
        ({
          command_id: command.command_id,
          owner_id: "owner",
          actor: { principal_id: "principal" },
          operation: command.action.operation,
          request_sha256: "c".repeat(64),
          server_generation: 7,
          accepted_at: vi.getMockedSystemTime()!.toISOString(),
          completed_at: vi.getMockedSystemTime()!.toISOString(),
          state: "committed",
          registry_version: server.state.version,
          changed: true,
          effects: { native_reference_id: "reference-00000" },
        }) as unknown as WorkstreamReceipt;
      let pendingCommand: { command_id: string; action: { operation: string } } | null = null;
      server.state.intercept = (call) => {
        if (failing.active) {
          const failed = input.failure(call);
          if (failed !== undefined) return failed;
        }
        if (call.route === "registrationContext")
          return { ...server.registration(), sources: [readySource] };
        if (call.route === "reference")
          return {
            context: {
              owner_id: "owner",
              server_generation: 7,
              registry_version: server.state.version,
            },
            reference: expiredReference,
            latest_observation: null,
          };
        if (call.route === "command" && pendingCommand !== null) return receipt(pendingCommand);
        if (call.route === "submit") {
          const command = call.request.payload?.command as {
            command_id: string;
            action: { operation: string };
          };
          submitted.push(command.action.operation);
          server.state.version += 1;
          if (submitted.length === 2) {
            failing.active = true;
            if (input.verifyState === "pending") {
              pendingCommand = command;
              return {
                ...receipt(command),
                state: "pending",
                completed_at: null,
                retry_after_seconds: 1,
              };
            }
          }
          return receipt(command);
        }
        return undefined;
      };
      let nextId = 0;
      let failure: unknown;
      await act(async () => {
        void latest.view.assignNativeThread!(threads[0]!, "alpha", {
          commandId: async () => `command-${++nextId}`,
        }).catch((cause: unknown) => {
          failure = cause;
        });
      });
      // A pending receipt is polled after its retry delay.
      for (let attempt = 0; attempt < 200; attempt += 1) {
        if (failure !== undefined) break;
        await act(() => vi.advanceTimersByTimeAsync(250));
        await settle(() => undefined, 1);
      }
      expect(failure).toBeDefined();
      await settle(() => expect(latest.view.loading).toBe(false));
      expect(submitted).toEqual(["register_reference", "verify_reference"]);
      return { threads, server, submitted, failing };
    };

    const transport = (call: { readonly route: string }) => {
      if (call.route === "list" || call.route === "command")
        throw new Error("Connection interrupted");
      return undefined;
    };

    it("keeps the stored beta group stale and read-only and reconciles only by reads", async () => {
      const { threads, submitted, failing } = await startMove({ failure: transport });
      expect(latest.view.data).not.toBeNull();
      expect(latest.view.stale).toBe(true);
      expect(latest.view.error).not.toBeNull();
      expect(canEditWorkstreams(latest.view.data)).toBe(false);
      expect(
        latest.view.assignmentIndex?.entries.get(keyOf(threads[0]!))?.primary?.workstreamId,
      ).toBe("beta");
      expect(
        latest.grouping.groups
          .find((group) => group.workstream.workstreamId === "beta")
          ?.threads.map((thread) => thread.id),
      ).toEqual(["00000"]);

      await act(async () => {
        await latest.view.retry!().catch(() => undefined);
        latest.view.refresh();
      });
      await settle(() => expect(latest.view.loading).toBe(false));
      expect(latest.view.stale).toBe(true);
      expect(submitted).toHaveLength(2);

      failing.active = false;
      await act(async () => latest.view.refresh());
      await settle(() => expect(latest.view.stale).toBe(false));
      expect(latest.view.error).toBeNull();
      expect(canEditWorkstreams(latest.view.data)).toBe(true);
      expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
      expect(submitted).toHaveLength(2);
    });

    it("fences an unproved command and resolves it through retry reads without resubmitting", async () => {
      const { threads, submitted, failing, server } = await startMove({
        failure: transport,
        verifyState: "pending",
      });
      expect(latest.view.data).not.toBeNull();
      expect(latest.view.stale).toBe(true);
      expect(latest.grouping.groupedKeys.has(keyOf(threads[0]!))).toBe(true);
      expect(submitted).toHaveLength(2);

      failing.active = false;
      const commandReads = server.state.calls.filter((call) => call.route === "command").length;
      await act(async () => latest.view.retry!());
      await settle(() => expect(latest.view.stale).toBe(false));
      expect(latest.view.error).toBeNull();
      expect(server.state.calls.filter((call) => call.route === "command").length).toBe(
        commandReads + 1,
      );
      expect(submitted).toHaveLength(2);
    });

    it("still purges when the next snapshot read rejects authorization", async () => {
      const denied = new EnvironmentHttpUnauthorizedError({ message: "authorization revoked" });
      const { submitted } = await startMove({
        failure: (call) => {
          if (call.route === "list") throw denied;
          return undefined;
        },
      });
      expect(latest.view.data).toBeNull();
      expect(latest.view.assignmentIndex).toBeNull();
      expect(latest.view.stale).toBe(false);
      expect(latest.view.error).not.toBeNull();
      expect(submitted).toHaveLength(2);
    });
  });
});
