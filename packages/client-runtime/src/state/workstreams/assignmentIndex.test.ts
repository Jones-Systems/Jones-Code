import {
  EnvironmentHttpConflictError,
  type MembershipEpisode,
  type NativeReference,
  type T3WorkstreamListResult,
  type WorkstreamMembershipPage,
  type WorkstreamReferencePage,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  buildWorkstreamAssignmentIndex,
  loadWorkstreamAssignmentIndex,
  WorkstreamAssignmentIndexVersionChanged,
  type WorkstreamAssignmentIndexFetchers,
} from "./assignmentIndex.ts";
import { nativeWorkstreamThreadKey } from "./nativeThreadGrouping.ts";

const ref = (
  nativeReferenceId: string,
  environmentId: string,
  threadId: string,
  overrides: Partial<NativeReference["registration"]> = {},
): NativeReference => ({
  native_reference_id: nativeReferenceId,
  owner_id: "owner",
  identity: {
    provider: "t3",
    source_instance_id: environmentId,
    resource_kind: "thread",
    id_kind: "internal",
    native_id: threadId,
    account_provenance: { kind: "not_account_scoped" },
  },
  pr_locator: null,
  registration: {
    state: "attested",
    attestation_version: 1,
    attested_at: "2026-09-12T11:00:00Z",
    expires_at: "2026-09-12T11:05:00Z",
    evidence: {
      provider: "t3",
      source_instance_id: environmentId,
      authority_namespace: environmentId,
      native_id: threadId,
      store_generation: 1,
      evidence_sha256: "a".repeat(64),
    },
    ...overrides,
  },
  created_at: "2026-09-12T11:00:00Z",
  created_by: { principal_id: "principal" },
  created_registry_version: 1,
});

const member = (
  id: string,
  workstreamId: string,
  referenceId: string,
  kind: "primary" | "secondary" = "primary",
): MembershipEpisode => ({
  membership_id: id,
  workstream_id: workstreamId,
  native_reference_id: referenceId,
  kind,
  opened: {
    at: "2026-09-12T11:00:00Z",
    actor: { principal_id: "principal" },
    command_id: "command-open-0001",
    registry_version: 1,
  },
  closed: null,
});

const closedMember = (
  id: string,
  workstreamId: string,
  referenceId: string,
): MembershipEpisode => ({
  ...member(id, workstreamId, referenceId),
  closed: {
    at: "2026-09-12T11:30:00Z",
    actor: { principal_id: "principal" },
    command_id: "command-close-0001",
    registry_version: 2,
    reason: "removed",
    other_reason: null,
  },
});

const build = (references: readonly NativeReference[], memberships: readonly MembershipEpisode[]) =>
  buildWorkstreamAssignmentIndex({
    ownerId: "owner",
    registryVersion: 11,
    references,
    memberships,
    complete: true,
  });

describe("buildWorkstreamAssignmentIndex", () => {
  it("keeps an open primary whose reference was once attested after its proof expires", () => {
    const expired = ref("ref-a", "env", "thread", { state: "expired" });
    const index = build([expired], [member("m-a", "ws-a", "ref-a")]);
    const entry = index.entries.get(nativeWorkstreamThreadKey("env", "thread"));
    expect(index.complete).toBe(true);
    expect(index.registryVersion).toBe(11);
    expect(entry).toMatchObject({
      sourceInstanceId: "env",
      nativeThreadId: "thread",
      state: "assigned",
      conflict: null,
      primary: {
        workstreamId: "ws-a",
        membershipId: "m-a",
        referenceId: "ref-a",
        kind: "primary",
        reference: expired,
      },
      secondaries: [],
    });
  });

  it("ignores closed episodes, unreferenced memberships, and references outside the T3 convention", () => {
    const base = ref("base", "env", "base");
    const variants: NativeReference[] = [
      ref("never", "env", "never", {
        state: "quarantined",
        attestation_version: 0,
        attested_at: null,
        expires_at: null,
        evidence: null,
      }),
      { ...ref("foreign", "env", "foreign"), owner_id: "other-owner" },
      {
        ...ref("provider", "env", "provider"),
        identity: { ...base.identity, native_id: "provider", provider: "github" },
      },
      {
        ...ref("project", "env", "project"),
        identity: { ...base.identity, native_id: "project", resource_kind: "project" },
      },
      {
        ...ref("external", "env", "external"),
        identity: { ...base.identity, native_id: "external", id_kind: "external" },
      },
      {
        ...ref("pr", "env", "pr"),
        pr_locator: {
          host: "github.com",
          repository_owner: "owner",
          repository_name: "repo",
          number: 1,
        },
      },
    ];
    const index = build(
      [ref("closed", "env", "closed"), ref("idle", "env", "idle"), ...variants],
      [
        closedMember("m-closed", "ws", "closed"),
        member("m-missing", "ws", "missing-reference"),
        ...variants.map((reference) =>
          member(`m-${reference.native_reference_id}`, "ws", reference.native_reference_id),
        ),
      ],
    );
    expect(index.entries.size).toBe(0);
  });

  it("marks two open primaries for one identity as a conflict instead of choosing one", () => {
    const index = build(
      [ref("ref-a", "env", "thread"), ref("ref-b", "env", "thread")],
      [member("m-b", "ws-b", "ref-b"), member("m-a", "ws-a", "ref-a")],
    );
    const entry = index.entries.get(nativeWorkstreamThreadKey("env", "thread"));
    expect(entry?.state).toBe("conflict");
    expect(entry?.conflict).toBe("multiple-primaries");
    expect(entry?.primary).toBeNull();
    expect(entry?.primaries.map((item) => item.membershipId)).toEqual(["m-a", "m-b"]);
  });

  it("marks evidence that names another native identity as a conflict", () => {
    const reference = ref("ref", "env", "thread");
    const index = build(
      [
        {
          ...reference,
          registration: {
            ...reference.registration,
            evidence: { ...reference.registration.evidence!, native_id: "other" },
          },
        },
      ],
      [member("m", "ws", "ref")],
    );
    expect(index.entries.get(nativeWorkstreamThreadKey("env", "thread"))).toMatchObject({
      state: "conflict",
      conflict: "evidence-mismatch",
      primary: null,
    });
  });

  it("records secondary links separately from the primary", () => {
    const index = build(
      [ref("ref", "env", "thread"), ref("secondary-only", "env", "other")],
      [
        member("s-2", "ws-c", "ref", "secondary"),
        member("p", "ws-a", "ref"),
        member("s-1", "ws-b", "ref", "secondary"),
        member("s-only", "ws-b", "secondary-only", "secondary"),
      ],
    );
    const entry = index.entries.get(nativeWorkstreamThreadKey("env", "thread"));
    expect(entry?.primary?.workstreamId).toBe("ws-a");
    expect(entry?.secondaries.map((item) => item.workstreamId)).toEqual(["ws-b", "ws-c"]);
    expect(index.entries.get(nativeWorkstreamThreadKey("env", "other"))).toMatchObject({
      state: "secondary-only",
      primary: null,
    });
  });
});

const binding: T3WorkstreamListResult["binding"] = {
  registryId: "https://registry.example",
  ownerId: "owner",
  principalId: "principal",
  authorizationRevision: 1,
  serverGeneration: 7,
  registryVersion: 11,
  permissions: ["workstreams:read"],
  contractVersion: "workstreams/1.0.0",
  contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
};
const context = (registryVersion = 11) => ({
  owner_id: "owner",
  server_generation: 7,
  registry_version: registryVersion,
});
const metadata = (workstreamId: string, sortOrder: number) => ({
  workstreamId,
  name: workstreamId,
  lifecycle: "active" as const,
  progress: { state: "progressing" as const },
  delivery: "none-observed" as const,
  freshness: "current" as const,
  sortOrder,
  version: 1,
  updatedAt: "2026-09-12T11:00:00Z",
});
const listPage = (
  items: T3WorkstreamListResult["items"],
  nextCursor: string | null,
  overrides: Partial<T3WorkstreamListResult> = {},
): T3WorkstreamListResult => ({
  binding,
  items,
  nextCursor,
  source: "live",
  stale: false,
  ...overrides,
});
const referencePage = (
  items: readonly NativeReference[],
  nextCursor: string | null,
  registryVersion = 11,
): WorkstreamReferencePage => ({
  context: context(registryVersion),
  items: [...items],
  next_cursor: nextCursor,
});
const membershipPage = (
  items: readonly MembershipEpisode[],
  nextCursor: string | null,
  registryVersion = 11,
): WorkstreamMembershipPage => ({
  context: context(registryVersion),
  items: [...items],
  next_cursor: nextCursor,
});
const cursorStale = () => new EnvironmentHttpConflictError({ message: "workstream_cursor_stale" });

type Fetchers = WorkstreamAssignmentIndexFetchers;

/** Two list pages, two reference pages, and paginated memberships for two workstreams. */
function registry() {
  const calls: string[] = [];
  const fetchers = {
    listWorkstreams: vi.fn(async ({ cursor }: Parameters<Fetchers["listWorkstreams"]>[0]) => {
      calls.push(`list:${cursor ?? ""}`);
      return cursor === undefined
        ? listPage([metadata("ws-a", 0)], "list-2")
        : listPage([metadata("ws-b", 1)], null);
    }),
    references: vi.fn(async ({ cursor }: Parameters<Fetchers["references"]>[0]) => {
      calls.push(`references:${cursor ?? ""}`);
      return cursor === undefined
        ? referencePage([ref("ref-a", "env", "a")], "references-2")
        : referencePage([ref("ref-b", "remote", "b")], null);
    }),
    memberships: vi.fn(async ({ workstreamId, cursor }: Parameters<Fetchers["memberships"]>[0]) => {
      calls.push(`memberships:${workstreamId}:${cursor ?? ""}`);
      if (workstreamId === "ws-a")
        return cursor === undefined
          ? membershipPage([closedMember("m-old", "ws-a", "ref-b")], "ws-a-2")
          : membershipPage([member("m-a", "ws-a", "ref-a")], null);
      return membershipPage([member("m-b", "ws-b", "ref-b")], null);
    }),
  } satisfies WorkstreamAssignmentIndexFetchers;
  return { calls, fetchers };
}

describe("loadWorkstreamAssignmentIndex", () => {
  it("reads every page at one registry version into a complete index", async () => {
    const { calls, fetchers } = registry();
    const loaded = await loadWorkstreamAssignmentIndex(fetchers);
    expect(loaded.incompleteReason).toBeNull();
    expect(loaded.index.complete).toBe(true);
    expect(loaded.index.registryVersion).toBe(11);
    expect(loaded.list.items.map((item) => item.workstreamId)).toEqual(["ws-a", "ws-b"]);
    expect(
      [...loaded.index.entries.values()]
        .map((entry) => [entry.key, entry.primary?.workstreamId])
        .sort(),
    ).toEqual([
      [nativeWorkstreamThreadKey("env", "a"), "ws-a"],
      [nativeWorkstreamThreadKey("remote", "b"), "ws-b"],
    ]);
    expect([...calls].sort()).toEqual([
      "list:",
      "list:list-2",
      "memberships:ws-a:",
      "memberships:ws-a:ws-a-2",
      "memberships:ws-b:",
      "references:",
      "references:references-2",
    ]);
  });

  it("restarts from the first page when a cursor goes stale", async () => {
    const { fetchers } = registry();
    const references = fetchers.references.getMockImplementation()!;
    let stale = true;
    fetchers.references.mockImplementation(async (request) => {
      if (request.cursor === "references-2" && stale) {
        stale = false;
        throw cursorStale();
      }
      return references(request);
    });
    const wait = vi.fn(async () => {});
    const loaded = await loadWorkstreamAssignmentIndex(fetchers, { wait });
    expect(loaded.index.complete).toBe(true);
    expect(loaded.index.entries.size).toBe(2);
    expect(wait.mock.calls).toEqual([[50, undefined]]);
    expect(
      fetchers.listWorkstreams.mock.calls.filter(([r]) => r.cursor === undefined),
    ).toHaveLength(2);
  });

  it("restarts when a page reports a different registry version", async () => {
    const { fetchers } = registry();
    let moved = true;
    fetchers.memberships.mockImplementationOnce(async () => {
      moved = false;
      return membershipPage([member("m-a", "ws-a", "ref-a")], null, 12);
    });
    const wait = vi.fn(async () => {});
    const loaded = await loadWorkstreamAssignmentIndex(fetchers, {
      wait,
      membershipConcurrency: 1,
    });
    expect(moved).toBe(false);
    expect(loaded.index.registryVersion).toBe(11);
    expect(loaded.index.complete).toBe(true);
    expect(wait.mock.calls).toEqual([[50, undefined]]);
  });

  it("restarts when the list binding advances between pages", async () => {
    const { fetchers } = registry();
    fetchers.listWorkstreams
      .mockResolvedValueOnce(listPage([metadata("ws-a", 0)], "list-2"))
      .mockResolvedValueOnce(
        listPage([metadata("ws-b", 1)], null, { binding: { ...binding, registryVersion: 12 } }),
      );
    const wait = vi.fn(async () => {});
    await expect(loadWorkstreamAssignmentIndex(fetchers, { wait })).resolves.toMatchObject({
      index: { registryVersion: 11, complete: true },
    });
    expect(wait).toHaveBeenCalledTimes(1);
  });

  it("stops after three attempts and surfaces the last restartable failure", async () => {
    const { fetchers } = registry();
    fetchers.references.mockImplementation(async () => referencePage([], null, 12));
    const wait = vi.fn(async () => {});
    await expect(loadWorkstreamAssignmentIndex(fetchers, { wait })).rejects.toBeInstanceOf(
      WorkstreamAssignmentIndexVersionChanged,
    );
    expect(wait.mock.calls).toEqual([
      [50, undefined],
      [100, undefined],
    ]);
    expect(fetchers.references).toHaveBeenCalledTimes(3);

    const stale = registry().fetchers;
    stale.references.mockImplementation(async () => {
      throw cursorStale();
    });
    await expect(loadWorkstreamAssignmentIndex(stale, { wait })).rejects.toMatchObject({
      message: "workstream_cursor_stale",
    });
    expect(stale.references).toHaveBeenCalledTimes(3);
  });

  it("does not restart for an owner change or an unrelated membership", async () => {
    const wait = vi.fn(async () => {});
    const owner = registry().fetchers;
    owner.references.mockResolvedValue({
      ...referencePage([], null),
      context: { ...context(), owner_id: "other" },
    });
    await expect(loadWorkstreamAssignmentIndex(owner, { wait })).rejects.toThrow(
      "Workstream owner changed while the assignment index was loading.",
    );
    const unrelated = registry().fetchers;
    unrelated.memberships.mockResolvedValue(membershipPage([member("m", "ws-z", "ref-a")], null));
    await expect(loadWorkstreamAssignmentIndex(unrelated, { wait })).rejects.toThrow(
      "Workstream membership page returned an unrelated membership.",
    );
    expect(wait).not.toHaveBeenCalled();
  });

  it("returns a partial index at the page ceiling instead of throwing or claiming coverage", async () => {
    const { fetchers } = registry();
    const loaded = await loadWorkstreamAssignmentIndex(fetchers, {
      maxPages: 4,
      membershipConcurrency: 1,
    });
    expect(loaded.incompleteReason).toBe("ceiling");
    expect(loaded.index.complete).toBe(false);
    expect(
      fetchers.listWorkstreams.mock.calls.length +
        fetchers.references.mock.calls.length +
        fetchers.memberships.mock.calls.length,
    ).toBe(4);
    expect([...loaded.index.entries.keys()]).not.toContain(
      nativeWorkstreamThreadKey("remote", "b"),
    );
  });

  it("never reports a cached or stale list as complete", async () => {
    const { fetchers } = registry();
    fetchers.listWorkstreams.mockImplementation(async ({ cursor }) =>
      cursor === undefined
        ? listPage([metadata("ws-a", 0)], "list-2", { source: "cache", stale: true })
        : listPage([metadata("ws-b", 1)], null),
    );
    const loaded = await loadWorkstreamAssignmentIndex(fetchers);
    expect(loaded.incompleteReason).toBe("stale-list");
    expect(loaded.index.complete).toBe(false);
  });

  it("stops reading and rejects with the caller's reason when aborted", async () => {
    const { fetchers } = registry();
    const controller = new AbortController();
    const reason = new Error("cancelled");
    fetchers.references.mockImplementation(
      ({ signal }) =>
        new Promise<WorkstreamReferencePage>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        }),
    );
    const cancelled = loadWorkstreamAssignmentIndex(fetchers, {
      signal: controller.signal,
      membershipConcurrency: 1,
    });
    await vi.waitFor(() => expect(fetchers.references).toHaveBeenCalledTimes(1));
    controller.abort(reason);
    await expect(cancelled).rejects.toBe(reason);
    expect(fetchers.listWorkstreams).toHaveBeenCalledTimes(2);

    const waiting = new AbortController();
    const stale = registry().fetchers;
    stale.references.mockRejectedValue(cursorStale());
    const wait = vi.fn(async () => {
      waiting.abort(reason);
      throw reason;
    });
    await expect(
      loadWorkstreamAssignmentIndex(stale, { signal: waiting.signal, wait }),
    ).rejects.toBe(reason);
    expect(stale.references).toHaveBeenCalledTimes(1);

    const before = new AbortController();
    before.abort(reason);
    const untouched = registry().fetchers;
    await expect(loadWorkstreamAssignmentIndex(untouched, { signal: before.signal })).rejects.toBe(
      reason,
    );
    expect(untouched.listWorkstreams).not.toHaveBeenCalled();
  });
});
