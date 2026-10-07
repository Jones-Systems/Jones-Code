import {
  EnvironmentHttpConflictError,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
  type WorkstreamDetail,
  type WorkstreamReadContext,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  loadCompleteWorkstreamDetail,
  loadCompleteWorkstreamList,
  loadCompleteWorkstreamReferences,
  reconcileWorkstreamCommands,
  nativePlacementInventory,
  nativePlacementInventoryJson,
  reuseNativePlacementIdentitySnapshot,
  type WorkstreamDetailLoaders,
} from "./workstreams";

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

describe("complete Workstream list loading", () => {
  it("projects only stable native identity pairs, preserves colon IDs and caps excess inventory", () => {
    const threads = [
      { environmentId: "a:b", id: "c", title: "private" },
      { environmentId: "a", id: "b:c", title: "private" },
    ];
    const inventory = nativePlacementInventoryJson([...threads, threads[0]!]);
    expect(JSON.parse(inventory)).toHaveLength(2);
    expect(inventory).not.toContain("private");
    expect(
      nativePlacementInventoryJson(
        [...threads].reverse().map((value) => ({ ...value, title: "changed" })),
      ),
    ).toBe(inventory);
    const excessThreads = Array.from({ length: 20_000 }, (_, i) => ({
      environmentId: "env",
      id: String(i),
    }));
    const excessInventory = nativePlacementInventory(excessThreads);
    expect(excessInventory.identities).toHaveLength(1_000);
    expect(excessInventory.totalIdentities).toBe(20_000);
    expect(excessInventory.coverage).toBe("partial");
    expect(nativePlacementInventory([...excessThreads].reverse())).toEqual(excessInventory);
    expect(nativePlacementInventory(threads).coverage).toBe("complete");

    const byteBoundInventory = nativePlacementInventory(
      Array.from({ length: 900 }, (_, i) => ({
        environmentId: `environment-${String(i).padStart(3, "0")}`,
        id: `${String(i).padStart(3, "0")}-${"x".repeat(508)}`,
      })),
    );
    expect(byteBoundInventory.identities.length).toBeLessThan(900);
    expect(byteBoundInventory.coverage).toBe("partial");
    expect(
      new TextEncoder().encode(JSON.stringify({ identities: byteBoundInventory.identities }))
        .byteLength,
    ).toBeLessThanOrEqual(T3_PLACEMENT_MAX_REQUEST_BYTES);
    const nextIdentity = {
      source_instance_id: `environment-${String(byteBoundInventory.identities.length).padStart(3, "0")}`,
      native_thread_id: `${String(byteBoundInventory.identities.length).padStart(3, "0")}-${"x".repeat(508)}`,
    };
    expect(
      new TextEncoder().encode(
        JSON.stringify({ identities: [...byteBoundInventory.identities, nextIdentity] }),
      ).byteLength,
    ).toBeGreaterThan(T3_PLACEMENT_MAX_REQUEST_BYTES);
  });

  it("keeps a deterministic 1,000-identity selection for a large reversed inventory", () => {
    const threads = Array.from({ length: 100_001 }, (_, index) => ({
      environmentId: `environment-${String(index % 7).padStart(2, "0")}`,
      id: `thread-${String(index).padStart(6, "0")}`,
    }));
    const inventory = nativePlacementInventory([...threads].reverse());
    const ordered = nativePlacementInventory(threads);

    expect(inventory).toEqual(ordered);
    expect(inventory.identities).toHaveLength(1_000);
    expect(inventory.totalIdentities).toBe(100_001);
    expect(inventory.coverage).toBe("partial");
  });

  it("never sorts more placement candidates than the identity cap", () => {
    const originalSort = Array.prototype.sort;
    const sortedLengths: number[] = [];
    const sort = vi.spyOn(Array.prototype, "sort").mockImplementation(function (
      this: unknown[],
      compareFn?: (a: unknown, b: unknown) => number,
    ) {
      sortedLengths.push(this.length);
      if (this.length > 1_000) throw new Error(`sorted ${this.length} placement candidates`);
      return originalSort.call(this, compareFn);
    });

    try {
      const inventory = nativePlacementInventory(
        Array.from({ length: 20_000 }, (_, index) => ({
          environmentId: "environment",
          id: String(20_000 - index),
        })),
      );
      expect(inventory.totalIdentities).toBe(20_000);
      expect(inventory.identities).toHaveLength(1_000);
      expect(sortedLengths).toEqual([1_000]);
    } finally {
      sort.mockRestore();
    }
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
    const load = vi.fn(async (cursor?: string) => cursor === undefined
      ? { ...emptyPage, items: [], next_cursor: "next-page" }
      : emptyPage);
    await expect(loadCompleteWorkstreamReferences(load)).resolves.toEqual(emptyPage);
    expect(load.mock.calls).toEqual([[], ["next-page"]]);
    const mixed = vi.fn(async (cursor?: string) => cursor === undefined
      ? { ...emptyPage, next_cursor: "next-page" }
      : { ...emptyPage, context: { ...context, registry_version: 12 } });
    await expect(loadCompleteWorkstreamReferences(mixed)).rejects.toThrow();
  });
  it("removes only terminal observed IDs and retains unknown effects without replay", async () => {
    const retained = new Set(["committed-command", "unknown-command", "unprocessed-command"]);
    const observe = vi.fn(async (id: string): Promise<WorkstreamReceipt> =>
      ({ state: id === "committed-command" ? "committed" : "unresolved" }) as WorkstreamReceipt);
    await expect(reconcileWorkstreamCommands({
      commandIds: [...retained], observe, resolved: (id) => retained.delete(id),
    })).rejects.toMatchObject({ reason: "unknown" });
    expect([...retained]).toEqual(["unknown-command", "unprocessed-command"]);
    expect(observe.mock.calls).toEqual([["committed-command"], ["unknown-command"]]);
    observe.mockResolvedValue({ state: "rejected" } as WorkstreamReceipt);
    await reconcileWorkstreamCommands({ commandIds: [...retained], observe, resolved: (id) => retained.delete(id) });
    expect(retained.size).toBe(0);
    expect(observe.mock.calls.slice(2)).toEqual([["unknown-command"], ["unprocessed-command"]]);
  });
  it("retains the command when GET observation loses its response", async () => {
    const resolved = vi.fn();
    const observe = vi.fn(async () => { throw new Error("lost-read-response"); });
    await expect(reconcileWorkstreamCommands({ commandIds: ["exact-command"], observe, resolved })).rejects.toThrow("lost-read-response");
    expect(observe).toHaveBeenCalledExactlyOnceWith("exact-command");
    expect(resolved).not.toHaveBeenCalled();
  });
});
