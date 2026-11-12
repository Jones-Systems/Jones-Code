import { describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentAuthInvalidError,
  EnvironmentAuthorizationError,
  EnvironmentHttpForbiddenError,
  EnvironmentHttpUnauthorizedError,
  EnvironmentInternalError,
  EnvironmentScopeRequiredError,
} from "@t3tools/contracts";
import type { WorkstreamAssignmentIndexFetchers } from "@t3tools/client-runtime/state/workstreams";
import {
  classifyMobileWorkstreamFailure,
  loadCompleteWorkstreamList,
  loadMobileWorkstreamRegistry,
  settleMobileWorkstreamSnapshots,
  type MobileWorkstreamLoadOutcome,
} from "./loaders";
import { data, membershipFor, reference } from "./actions.fixtures";
describe("mobile Workstream metadata paging", () => {
  it("completes all pages before publishing metadata", async () => {
    const load = vi.fn(async (cursor?: string) =>
      cursor
        ? { ...data, items: [data.items[1]!] }
        : { ...data, items: [data.items[0]!], nextCursor: "next" },
    );
    expect((await loadCompleteWorkstreamList(load)).items).toEqual(data.items);
    expect(load).toHaveBeenNthCalledWith(2, "next");
  });
  it("rejects a changed principal or registry revision between pages", async () => {
    await expect(
      loadCompleteWorkstreamList(async (cursor) =>
        cursor
          ? { ...data, binding: { ...data.binding, principalId: "different" } }
          : { ...data, nextCursor: "next" },
      ),
    ).rejects.toThrow("binding changed");
  });
  it("does not follow a repeated cursor indefinitely", async () => {
    await expect(
      loadCompleteWorkstreamList(async () => ({ ...data, nextCursor: "next" })),
    ).rejects.toThrow("cursor repeated");
  });
  it("discards a response arriving after its connection was aborted", async () => {
    const controller = new AbortController();
    await expect(
      loadCompleteWorkstreamList(
        async () => {
          controller.abort();
          return data;
        },
        { signal: controller.signal },
      ),
    ).rejects.toThrow();
  });
});

const context = {
  owner_id: data.binding.ownerId,
  server_generation: data.binding.serverGeneration,
  registry_version: data.binding.registryVersion,
};
const fetchers = (
  overrides: Partial<WorkstreamAssignmentIndexFetchers> = {},
): WorkstreamAssignmentIndexFetchers => ({
  listWorkstreams: async () => data,
  references: async () => ({ context, items: [reference], next_cursor: null }),
  memberships: async ({ workstreamId }) => ({
    context,
    items:
      workstreamId === "alpha"
        ? [membershipFor("membership", "alpha", reference.native_reference_id)]
        : [],
    next_cursor: null,
  }),
  ...overrides,
});
const traceId = "trace-fixture";
const transportFailure = new Error("Network request failed");
const opaqueDenial = new EnvironmentInternalError({
  code: "internal_error",
  reason: "internal_error",
  traceId,
});
const authorizationFailures = [
  new EnvironmentAuthInvalidError({ code: "auth_invalid", reason: "invalid_credential", traceId }),
  new EnvironmentAuthorizationError({
    message: "denied",
    requiredScope: "workstreams:native:context",
  }),
  new EnvironmentHttpUnauthorizedError({ message: "unauthorized" }),
  new EnvironmentHttpForbiddenError({ message: "forbidden" }),
  new EnvironmentScopeRequiredError({
    code: "insufficient_scope",
    requiredScope: "workstreams:native:context",
    traceId,
  }),
  { status: 401 },
  { response: { status: 403 } },
];

describe("mobile Workstream failure classification", () => {
  it("treats only lost authorization as a reason to hide loaded Workstreams", () => {
    for (const cause of authorizationFailures)
      expect(classifyMobileWorkstreamFailure(cause)).toBe("authorization");
    for (const cause of [transportFailure, opaqueDenial, { status: 500 }, undefined, "offline"])
      expect(classifyMobileWorkstreamFailure(cause)).toBe("transient");
  });
});

describe("mobile Workstream registry loading", () => {
  const signal = new AbortController().signal;
  it("reads the list and stored assignments at one registry version", async () => {
    const loaded = await loadMobileWorkstreamRegistry(fetchers(), {
      signal,
      listOnlyFallback: false,
    });
    expect(loaded.data.items).toEqual(data.items);
    expect(loaded.index?.complete).toBe(true);
    expect(loaded.index?.entries.get('["env:a","thread"]')?.primary?.workstreamId).toBe("alpha");
  });
  it("falls back to the list alone only for a transient failure without a previous index", async () => {
    const failing = fetchers({
      references: async () => {
        throw opaqueDenial;
      },
    });
    await expect(
      loadMobileWorkstreamRegistry(failing, { signal, listOnlyFallback: false }),
    ).rejects.toBe(opaqueDenial);
    const listOnly = await loadMobileWorkstreamRegistry(failing, {
      signal,
      listOnlyFallback: true,
    });
    expect(listOnly).toEqual({ data, index: null });
    const signedOut = authorizationFailures[0]!;
    await expect(
      loadMobileWorkstreamRegistry(
        fetchers({
          memberships: async () => {
            throw signedOut;
          },
        }),
        { signal, listOnlyFallback: true },
      ),
    ).rejects.toBe(signedOut);
  });
});

describe("mobile Workstream snapshot settlement", () => {
  type Snapshot = { readonly id: string; readonly stale: boolean };
  const good: Snapshot = { id: "good", stale: false };
  const fresh: Snapshot = { id: "fresh", stale: false };
  const settle = (
    previous: ReadonlyMap<string, Snapshot>,
    outcomes: Record<string, MobileWorkstreamLoadOutcome<Snapshot>>,
    retained?: (key: string) => boolean,
  ) => settleMobileWorkstreamSnapshots(previous, new Map(Object.entries(outcomes)), retained);

  it("keeps the last good snapshot through refresh and reconnect until a load succeeds", () => {
    const previous = new Map([["env:a", good]]);
    expect(settle(previous, {}).get("env:a")).toBe(good);
    expect(settle(previous, { "env:a": { kind: "loaded", snapshot: fresh } }).get("env:a")).toBe(
      fresh,
    );
  });
  it("marks the last good snapshot stale after a transient failure", () => {
    for (const cause of [transportFailure, opaqueDenial]) {
      const next = settle(new Map([["env:a", good]]), { "env:a": { kind: "failed", cause } });
      expect(next.get("env:a")).toEqual({ id: "good", stale: true });
    }
    expect(settle(new Map(), { "env:a": { kind: "failed", cause: transportFailure } }).size).toBe(
      0,
    );
  });
  it("purges on authorization or session loss and keeps other environments", () => {
    const previous = new Map([
      ["env:a", good],
      ["env:b", good],
    ]);
    for (const cause of authorizationFailures) {
      const next = settle(previous, { "env:a": { kind: "failed", cause } });
      expect(next.has("env:a")).toBe(false);
      expect(next.get("env:b")).toBe(good);
    }
    const signedOut = settle(previous, {}, (key) => key !== "env:b");
    expect([...signedOut.keys()]).toEqual(["env:a"]);
  });
});
