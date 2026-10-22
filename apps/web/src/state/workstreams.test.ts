import {
  EnvironmentHttpConflictError,
  T3_PLACEMENT_MAX_IDENTITIES,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
  type T3PlacementIdentity,
  type WorkstreamDetail,
  type WorkstreamReadContext,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  loadCompleteWorkstreamDetail,
  loadCompleteWorkstreamList,
  loadCompleteWorkstreamReferences,
  mergeLivePlacements,
  nativePlacementBatches,
  reconcileWorkstreamCommands,
  reuseNativePlacementIdentitySnapshot,
  workstreamListErrorMessage,
  type WorkstreamDetailLoaders,
  type NativePlacementThread,
} from "./workstreams";
import { registrationAvailability } from "../jones/workstreams/registrationAvailability";
import {
  WorkstreamActionError,
  workstreamFailureMessage,
  workstreamOperationFailure,
} from "../jones/workstreams/workstreamActionSnapshot";
import { placements } from "../components/workstreams/nativeWorkstreamActions.fixtures";

const binding = {
  registryId: "registry",
  ownerId: "owner",
  principalId: "principal",
  authorizationRevision: 1,
  serverGeneration: 7,
  registryVersion: 11,
  permissions: ["workstreams:read" as const],
  contractVersion: "workstreams/1.0.0" as const,
  contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557" as const,
};

const context: WorkstreamReadContext = {
  owner_id: "owner",
  server_generation: 7,
  registry_version: 11,
};
const detail = { context, workstream: {} } as WorkstreamDetail;
const emptyPage = { context, items: [], next_cursor: null } as const;

const detailLoaders = (
  calls: Record<keyof WorkstreamDetailLoaders, Array<string | undefined>>,
  memberships: WorkstreamDetailLoaders["memberships"],
): WorkstreamDetailLoaders => ({
  detail: async () => {
    calls.detail.push(undefined);
    return detail;
  },
  memberships,
  declarations: async (cursor) => {
    calls.declarations.push(cursor);
    return emptyPage;
  },
  edges: async (cursor) => {
    calls.edges.push(cursor);
    return emptyPage;
  },
  history: async (cursor) => {
    calls.history.push(cursor);
    return emptyPage;
  },
  references: async (cursor) => {
    calls.references.push(cursor);
    return emptyPage;
  },
});

const detailCalls = (): Record<keyof WorkstreamDetailLoaders, Array<string | undefined>> => ({
  detail: [],
  memberships: [],
  declarations: [],
  edges: [],
  history: [],
  references: [],
});

describe("proof request batching", () => {
  const requestBytes = (identities: readonly T3PlacementIdentity[]) =>
    new TextEncoder().encode(JSON.stringify({ identities })).byteLength;
  const canonical = (identities: readonly T3PlacementIdentity[]) =>
    identities.map((entry) => JSON.stringify([entry.source_instance_id, entry.native_thread_id]));
  const expectWithinCeilings = (batches: readonly (readonly T3PlacementIdentity[])[]) => {
    for (const batch of batches) {
      expect(batch.length).toBeGreaterThan(0);
      expect(batch.length).toBeLessThanOrEqual(T3_PLACEMENT_MAX_IDENTITIES);
      expect(requestBytes(batch)).toBeLessThanOrEqual(T3_PLACEMENT_MAX_REQUEST_BYTES);
      expect(canonical(batch)).toEqual(canonical(batch).toSorted());
    }
  };

  it("projects only stable identity pairs, preserves colon IDs and deduplicates", () => {
    const threads = [
      { environmentId: "a:b", id: "c", title: "private" },
      { environmentId: "a", id: "b:c", title: "private" },
    ];
    const batches = nativePlacementBatches([...threads, threads[0]!]);
    expect(batches).toEqual([
      [
        { source_instance_id: "a", native_thread_id: "b:c" },
        { source_instance_id: "a:b", native_thread_id: "c" },
      ],
    ]);
    expect(JSON.stringify(batches)).not.toContain("private");
    expect(nativePlacementBatches([...threads].reverse())).toEqual(batches);
    expect(nativePlacementBatches([])).toEqual([]);
  });

  it("requests every identity of a large inventory within the count ceiling", () => {
    const threads = Array.from({ length: 4_600 }, (_, i) => ({
      environmentId: "env",
      id: String(i).padStart(5, "0"),
    }));
    const batches = nativePlacementBatches(threads);
    expect(batches.map((batch) => batch.length)).toEqual([1_000, 1_000, 1_000, 1_000, 600]);
    expectWithinCeilings(batches);
    expect(new Set(batches.flat().map((entry) => entry.native_thread_id)).size).toBe(4_600);
    expect(nativePlacementBatches(threads.toReversed())).toEqual(batches);
  });

  it("splits by encoded bytes before the identity ceiling", () => {
    const threads = Array.from({ length: 900 }, (_, i) => ({
      environmentId: `environment-${String(i).padStart(3, "0")}`,
      id: `${String(i).padStart(3, "0")}-${"x".repeat(508)}`,
    }));
    const batches = nativePlacementBatches(threads);
    expect(batches.length).toBeGreaterThan(1);
    expectWithinCeilings(batches);
    expect(batches.flat()).toHaveLength(900);
    const [first, second] = batches;
    // The first batch is full: one more identity would exceed the byte ceiling.
    expect(requestBytes([...first!, second![0]!])).toBeGreaterThan(T3_PLACEMENT_MAX_REQUEST_BYTES);
  });

  it("requests only accepted identities and always includes the preferred thread first", () => {
    const threads = Array.from({ length: 4_501 }, (_, i) => ({
      environmentId: "env",
      id: String(i).padStart(5, "0"),
    }));
    const chosen = threads[4_500]!;
    const indexed = new Set(
      threads.slice(0, 2_000).map((entry) => JSON.stringify(["env", entry.id])),
    );
    const include = (key: string) => indexed.has(key);
    const batches = nativePlacementBatches(threads, { include, preferred: chosen });
    expectWithinCeilings(batches);
    expect(batches.flat()).toHaveLength(2_001);
    expect(batches[0]!.some((entry) => entry.native_thread_id === chosen.id)).toBe(true);
    expect(nativePlacementBatches(threads.toReversed(), { include, preferred: chosen })).toEqual(
      batches,
    );
    // A preferred thread that is not present natively is never admitted.
    expect(
      nativePlacementBatches(threads, {
        include,
        preferred: { ...chosen, environmentId: "remote" },
      }).flat(),
    ).toHaveLength(2_000);
  });

  it("orders batches by preference, activity and recency", () => {
    const settled = Array.from({ length: 1_005 }, (_, index) => ({
      environmentId: "env",
      id: `old-${String(index).padStart(4, "0")}`,
      settledOverride: "settled" as const,
      settledAt: "2026-01-01T00:00:00Z",
    }));
    const recent = {
      environmentId: "env",
      id: "zzz-recent",
      settledOverride: "settled" as const,
      settledAt: "2026-10-17T12:00:00Z",
    };
    const active = {
      environmentId: "env",
      id: "zzz-active",
      settledOverride: "active" as const,
      settledAt: "2025-01-01T00:00:00Z",
    };
    const preferred = { ...settled[1_004]!, id: "zzz-preferred-old" };
    const threads = [...settled, recent, active, preferred];
    const batches = nativePlacementBatches(threads, { preferred });
    expectWithinCeilings(batches);
    const first = batches[0]!.map((entry) => entry.native_thread_id);
    expect(first).toContain(preferred.id);
    expect(first).toContain(active.id);
    expect(first).toContain(recent.id);
    expect(first).not.toContain("old-1004");
    expect(batches.flat()).toHaveLength(threads.length);
    expect(nativePlacementBatches(threads.toReversed(), { preferred })).toEqual(batches);
  });

  it("merges proof batches only when their contexts and trust snapshots agree", () => {
    const first = { ...placements, items: [placements.items[0]!] };
    const second = { ...placements, items: [placements.items[1]!] };
    expect(mergeLivePlacements([])).toBeNull();
    expect(mergeLivePlacements([first, second])?.items).toEqual(placements.items);
    for (const changed of [
      { ...second, context: { ...second.context, registry_version: 12 } },
      { ...second, context: { ...second.context, authorization_revision: 2 } },
      { ...second, readiness: "trust-provider-required" as const, trustedEnvironments: [] },
      {
        ...second,
        trustedEnvironments: [{ ...second.trustedEnvironments[0]!, storeGeneration: 2 }],
      },
    ])
      expect(mergeLivePlacements([first, changed])).toBeNull();
  });

  it("refreshes priority when settlement changes and keeps irrelevant shell updates stable", () => {
    const original: NativePlacementThread[] = [
      {
        environmentId: "env",
        id: "thread",
        settledOverride: "active" as const,
        settledAt: null as string | null,
      },
    ];
    expect(
      reuseNativePlacementIdentitySnapshot(
        original,
        original.map((thread) => ({ ...thread, title: "new" })),
      ),
    ).toBe(original);
    const settled = [
      { ...original[0]!, settledOverride: "settled" as const, settledAt: "2026-10-17T12:00:00Z" },
    ];
    expect(reuseNativePlacementIdentitySnapshot(original, settled)).toBe(settled);
    const recent = [{ ...settled[0]!, settledAt: "2026-10-17T13:00:00Z" }];
    expect(reuseNativePlacementIdentitySnapshot(settled, recent)).toBe(recent);
  });

  it("reuses a large placement identity snapshot until identity membership changes", () => {
    const original = Array.from({ length: 100_001 }, (_, index) => ({
      environmentId: `environment-${index % 7}`,
      id: `thread-${index}`,
      activeAt: 1,
    }));
    const minuteTick = original.map((thread) => ({ ...thread, activeAt: 2 }));
    const changed = minuteTick.map((thread, index) =>
      index === minuteTick.length - 1 ? { ...thread, id: "replacement-thread" } : thread,
    );

    expect(reuseNativePlacementIdentitySnapshot(original, minuteTick)).toBe(original);
    expect(reuseNativePlacementIdentitySnapshot(original, changed)).toBe(changed);
  });
});

describe("complete Workstream list loading", () => {
  it("loads across page boundaries before exposing an owner list", async () => {
    const cursors: Array<string | undefined> = [];
    const result = await loadCompleteWorkstreamList(async (cursor) => {
      cursors.push(cursor);
      return {
        binding,
        items: [],
        nextCursor: cursor === undefined ? "second-page" : null,
        source: "live",
        stale: false,
      };
    });
    expect(cursors).toEqual([undefined, "second-page"]);
    expect(result.nextCursor).toBeNull();
  });

  it("rejects a partial list when a later page fails", async () => {
    await expect(
      loadCompleteWorkstreamList(async (cursor) => {
        if (cursor) throw new Error("later-page-conflict");
        return {
          binding,
          items: [],
          nextCursor: "second-page",
          source: "live",
          stale: false,
        };
      }),
    ).rejects.toThrow("later-page-conflict");
  });

  it("does not request another page after cancellation", async () => {
    const controller = new AbortController();
    const cancelled = new Error("list-cancelled");
    const cursors: Array<string | undefined> = [];
    await expect(
      loadCompleteWorkstreamList(
        async (cursor) => {
          cursors.push(cursor);
          controller.abort(cancelled);
          return {
            binding,
            items: [],
            nextCursor: "must-not-load",
            source: "live" as const,
            stale: false,
          };
        },
        { signal: controller.signal },
      ),
    ).rejects.toBe(cancelled);
    expect(cursors).toEqual([undefined]);
  });

  it("restarts a stale page sequence with bounded backoff before exposing data", async () => {
    const cursors: Array<string | undefined> = [];
    const wait = vi.fn(async () => undefined);
    let attempt = 0;
    const result = await loadCompleteWorkstreamList(
      async (cursor) => {
        cursors.push(cursor);
        if (cursor === undefined) {
          attempt += 1;
          return {
            binding,
            items: [],
            nextCursor: attempt === 1 ? "stale" : "fresh",
            source: "live" as const,
            stale: false,
          };
        }
        if (cursor === "stale")
          throw new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });
        return { binding, items: [], nextCursor: null, source: "live" as const, stale: false };
      },
      { wait },
    );
    expect(cursors).toEqual([undefined, "stale", undefined, "fresh"]);
    expect(wait).toHaveBeenCalledWith(50);
    expect(result.nextCursor).toBeNull();
  });

  it("fails closed after three stale page sequences", async () => {
    const wait = vi.fn(async () => undefined);
    let firstPages = 0;
    await expect(
      loadCompleteWorkstreamList(
        async (cursor) => {
          if (cursor !== undefined)
            throw new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });
          firstPages += 1;
          return {
            binding,
            items: [],
            nextCursor: `stale-${firstPages}`,
            source: "live" as const,
            stale: false,
          };
        },
        { wait },
      ),
    ).rejects.toMatchObject({ message: "workstream_cursor_stale" });
    expect(firstPages).toBe(3);
    expect(wait.mock.calls).toEqual([[50], [100]]);
  });

  it("interrupts cursor backoff without starting another attempt", async () => {
    const controller = new AbortController();
    const cancelled = new Error("retry-cancelled");
    const load = vi.fn(async () => {
      throw new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });
    });
    const wait = vi.fn(async (_delayMs: number, signal?: AbortSignal) => {
      expect(signal).toBe(controller.signal);
      controller.abort(cancelled);
    });

    await expect(
      loadCompleteWorkstreamList(load, { signal: controller.signal, wait }),
    ).rejects.toBe(cancelled);
    expect(load).toHaveBeenCalledTimes(1);
    expect(wait).toHaveBeenCalledTimes(1);
  });
});

describe("complete Workstream detail loading", () => {
  it("interrupts every parallel detail branch", async () => {
    const controller = new AbortController();
    const cancelled = new Error("detail-cancelled");
    const calls = detailCalls();
    const pending = <A>(name: keyof WorkstreamDetailLoaders): Promise<A> => {
      calls[name].push(undefined);
      return new Promise<A>((_resolve, reject) => {
        controller.signal.addEventListener("abort", () => reject(controller.signal.reason), {
          once: true,
        });
      });
    };
    const loaders: WorkstreamDetailLoaders = {
      detail: () => pending("detail"),
      memberships: () => pending("memberships"),
      declarations: () => pending("declarations"),
      edges: () => pending("edges"),
      history: () => pending("history"),
      references: () => pending("references"),
    };
    const result = loadCompleteWorkstreamDetail(loaders, { signal: controller.signal });
    await Promise.resolve();
    controller.abort(cancelled);

    await expect(result).rejects.toBe(cancelled);
    for (const name of [
      "detail",
      "memberships",
      "declarations",
      "edges",
      "history",
      "references",
    ] as const)
      expect(calls[name]).toHaveLength(1);
  });

  it("restarts every detail component when a later page becomes stale", async () => {
    const calls = detailCalls();
    const wait = vi.fn(async () => undefined);
    let round = 0;
    const loaders = detailLoaders(calls, async (cursor) => {
      calls.memberships.push(cursor);
      if (cursor === undefined) {
        round += 1;
        return { ...emptyPage, next_cursor: round === 1 ? "stale" : null };
      }
      throw new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });
    });

    const result = await loadCompleteWorkstreamDetail(loaders, { wait });

    expect(result.detail).toBe(detail);
    expect(result.history.coverage).toBe("complete");
    expect(result.history.next_cursor).toBeNull();
    expect(calls.memberships).toEqual([undefined, "stale", undefined]);
    for (const name of ["detail", "declarations", "edges", "history", "references"] as const)
      expect(calls[name]).toHaveLength(2);
    expect(wait.mock.calls).toEqual([[50]]);
  });

  it("fails closed after three complete detail rounds under sustained churn", async () => {
    const calls = detailCalls();
    const wait = vi.fn(async () => undefined);
    const loaders = detailLoaders(calls, async (cursor) => {
      calls.memberships.push(cursor);
      if (cursor === undefined) return { ...emptyPage, next_cursor: "stale" };
      throw new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });
    });

    await expect(loadCompleteWorkstreamDetail(loaders, { wait })).rejects.toMatchObject({
      message: "workstream_cursor_stale",
    });

    expect(calls.memberships).toEqual([undefined, "stale", undefined, "stale", undefined, "stale"]);
    for (const name of ["detail", "declarations", "edges", "history", "references"] as const)
      expect(calls[name]).toHaveLength(3);
    expect(wait.mock.calls).toEqual([[50], [100]]);
  });

  it("does not retry or mask a non-stale failure from the same detail round", async () => {
    const calls = detailCalls();
    const wait = vi.fn(async () => undefined);
    const permissionFailure = new Error("permission-revoked");
    const loaders = detailLoaders(calls, async (cursor) => {
      calls.memberships.push(cursor);
      if (cursor === undefined)
        throw new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });
      return emptyPage;
    });

    await expect(
      loadCompleteWorkstreamDetail(
        {
          ...loaders,
          declarations: async (cursor) => {
            calls.declarations.push(cursor);
            throw permissionFailure;
          },
        },
        { wait },
      ),
    ).rejects.toBe(permissionFailure);

    for (const name of [
      "detail",
      "memberships",
      "declarations",
      "edges",
      "history",
      "references",
    ] as const)
      expect(calls[name]).toHaveLength(1);
    expect(wait).not.toHaveBeenCalled();
  });
});

describe("Workstream reference action reads", () => {
  it("collects every reference page and refuses mixed registry versions", async () => {
    const load = vi.fn(async (cursor?: string) =>
      cursor === undefined ? { ...emptyPage, items: [], next_cursor: "next-page" } : emptyPage,
    );
    await expect(loadCompleteWorkstreamReferences(load)).resolves.toEqual(emptyPage);
    expect(load.mock.calls).toEqual([[], ["next-page"]]);
    const mixed = vi.fn(async (cursor?: string) =>
      cursor === undefined
        ? { ...emptyPage, next_cursor: "next-page" }
        : { ...emptyPage, context: { ...context, registry_version: 12 } },
    );
    await expect(loadCompleteWorkstreamReferences(mixed)).rejects.toThrow();
  });
  it("removes only terminal observed IDs and retains unknown effects without replay", async () => {
    const retained = new Set(["committed-command", "unknown-command", "unprocessed-command"]);
    const observe = vi.fn(
      async (id: string): Promise<WorkstreamReceipt> =>
        ({ state: id === "committed-command" ? "committed" : "unresolved" }) as WorkstreamReceipt,
    );
    await expect(
      reconcileWorkstreamCommands({
        commandIds: [...retained],
        observe,
        resolved: (id) => retained.delete(id),
      }),
    ).rejects.toMatchObject({ reason: "unknown" });
    expect([...retained]).toEqual(["unknown-command", "unprocessed-command"]);
    expect(observe.mock.calls).toEqual([["committed-command"], ["unknown-command"]]);
    observe.mockResolvedValue({ state: "rejected" } as WorkstreamReceipt);
    await reconcileWorkstreamCommands({
      commandIds: [...retained],
      observe,
      resolved: (id) => retained.delete(id),
    });
    expect(retained.size).toBe(0);
    expect(observe.mock.calls.slice(2)).toEqual([["unknown-command"], ["unprocessed-command"]]);
  });
  it("retains the command when GET observation loses its response", async () => {
    const resolved = vi.fn();
    const observe = vi.fn(async () => {
      throw new Error("lost-read-response");
    });
    await expect(
      reconcileWorkstreamCommands({ commandIds: ["exact-command"], observe, resolved }),
    ).rejects.toThrow("lost-read-response");
    expect(observe).toHaveBeenCalledExactlyOnceWith("exact-command");
    expect(resolved).not.toHaveBeenCalled();
  });
});

describe("Workstream list error classification", () => {
  it("treats an unconfigured server as no Workstreams rather than an error", () => {
    expect(
      workstreamListErrorMessage(
        new EnvironmentHttpConflictError({ message: "workstreams_not_configured" }),
      ),
    ).toBeNull();
  });

  it("keeps every other list failure visible", () => {
    expect(
      workstreamListErrorMessage(
        new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" }),
      ),
    ).toBe("workstream_cursor_stale");
    expect(workstreamListErrorMessage(new Error("internal failure"))).toBe("internal failure");
    expect(workstreamListErrorMessage("opaque")).toBe("Workstreams are unavailable.");
  });
});

describe("Workstream assignment diagnostics", () => {
  it.each([
    undefined,
    "",
    "ready;reason=source_mismatch",
    "unavailable;reason=private-value",
    "unavailable;reason=source_mismatch;extra=x",
  ])("keeps absent or malformed availability unknown", (value) => {
    expect(registrationAvailability({ "x-jones-workstreams-t3-availability": value })).toEqual({
      state: "unknown",
      reason: null,
    });
  });
  it("uses only sanitized closed reasons", () => {
    expect(
      registrationAvailability({
        "x-jones-workstreams-t3-availability": "unavailable;reason=contract_mismatch",
      }),
    ).toEqual({
      state: "unavailable",
      reason:
        "the running app build differs from the enrolled build; refresh enrollment for this running build, then refresh here",
    });
    expect(
      registrationAvailability({
        "x-jones-workstreams-t3-availability": "unavailable;reason=source_mismatch",
      }),
    ).toEqual({
      state: "unavailable",
      reason: "this environment is not enrolled; enroll this environment, then refresh here",
    });
    expect(registrationAvailability({ "x-jones-workstreams-t3-availability": "ready" })).toEqual({
      state: "ready",
      reason: null,
    });
  });
  it("distinguishes pre-submit loading failures from failures after a command attempt", () => {
    const failure = new Error("private backend detail");
    expect(workstreamFailureMessage(workstreamOperationFailure(failure, false))).toContain(
      "Refresh before trying again",
    );
    expect(workstreamFailureMessage(workstreamOperationFailure(failure, true))).toContain(
      "effect is unknown",
    );
    expect(workstreamFailureMessage(failure)).toBe(
      "Could not complete the action. Refresh before trying again.",
    );
    expect(workstreamFailureMessage(failure)).not.toContain("private backend detail");
    expect(workstreamFailureMessage(new WorkstreamActionError("unknown"))).toContain(
      "effect is unknown",
    );
  });
});
