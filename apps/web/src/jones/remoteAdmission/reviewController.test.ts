import { expect, it, vi } from "vite-plus/test";
import {
  bindingKey,
  createActionFreeze,
  createReviewController,
  type Item,
  type Page,
  type ReviewClient,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import {
  createCanonicalReviewClient,
  type ReviewTransport,
} from "@t3tools/client-runtime/jones/remoteAdmission/adapter";
import { admissionFixture } from "./testFixtures";
import type { RemoteAdmissionReviewItem } from "@t3tools/contracts/jones/remoteAdmissionReview";

function item(id: string): Item {
  return {
    record: admissionFixture({ id }),
    attempt: "original",
    blocked: false,
    accessId: null,
    grantEligible: true,
    outcome: null,
    availability: "unknown",
    threadId: null,
    messageStage: null,
    reason: null,
    canRetry: false,
    canRecheck: false,
  };
}
const a = item("first"),
  b = item("second");
function fixture() {
  let records = [a, b];
  const page = (): Page => ({
    revision: "snapshot-1",
    policyRevision: "synthetic-policy",
    counts: {
      currentHeldAdmissions: records.length,
      blockedHeldAdmissions: 0,
      heldAdmissions: records.length,
      inFlightAdmissions: 0,
      failedAdmissions: 0,
      unresolvedAdmissions: 0,
      activeAccessScopes: 0,
      activeThreads: 0,
    },
    records,
    access: [],
    nextCursor: null,
    recordCount: records.length,
  });
  const client: ReviewClient = {
    capabilities: async () => ({
      canDecide: true,
      features: new Set(["reviewV2", "grants", "changeFeed"]),
    }),
    page: vi.fn(async (view) =>
      view === "access" || view === "inflight" || view === "unresolved"
        ? { ...page(), records: [] }
        : page(),
    ),
    lookup: async (id) => records.find((entry) => entry.record.id === id)!,
    lookupAccess: vi.fn(async () => {
      throw new Error("Access unavailable");
    }),
    mutate: vi.fn(async () => ({})),
    head: vi.fn(async () => "current-head"),
    changes: vi.fn(
      async (_after, signal) =>
        new Promise((_resolve, reject) =>
          signal.addEventListener("abort", () => reject(new Error("aborted"))),
        ),
    ),
  };
  return {
    client,
    controller: createReviewController(client),
    records: (next: Item[]) => {
      records = next;
    },
  };
}
it("serializes an exact binding but commits an unrelated denial during a slow approval", async () => {
  const f = fixture();
  await f.controller.load();
  let finish!: () => void;
  f.client.mutate = vi.fn((input) =>
    input.action === "decide" && input.input.id === a.record.id
      ? new Promise((resolve) => {
          finish = () => resolve({ item: { ...a, record: { ...a.record, status: "approved" } } });
        })
      : Promise.resolve({ item: { ...b, record: { ...b.record, status: "denied" } } }),
  );
  const first = f.controller.mutate({
    action: "decide",
    input: { ...a.record, decision: "approve" },
  });
  await f.controller.mutate({ action: "decide", input: { ...a.record, decision: "deny" } });
  await f.controller.mutate({ action: "decide", input: { ...b.record, decision: "deny" } });
  expect(f.client.mutate).toHaveBeenCalledTimes(2);
  expect(f.controller.getSnapshot().pending.has(bindingKey(a.record))).toBe(true);
  expect(
    f.controller.getSnapshot().page?.records.find((row) => row.record.id === b.record.id)?.record
      .status,
  ).toBe("denied");
  finish();
  await first;
});
it("keeps lost decisions uncertain across omission, and blocks policy changes until reconciled", async () => {
  const f = fixture();
  await f.controller.load();
  f.client.mutate = vi.fn(async () => {
    throw new Error("lost response");
  });
  f.records([]);
  await f.controller.mutate({ action: "decide", input: { ...a.record, decision: "approve" } });
  expect(f.controller.getSnapshot().uncertain.has(bindingKey(a.record))).toBe(true);
  const policy = vi.fn(async () => undefined);
  await f.controller.withPolicyBarrier(policy);
  expect(policy).not.toHaveBeenCalled();
  await f.controller.mutate({ action: "decide", input: { ...a.record, decision: "approve" } });
  expect(f.client.mutate).toHaveBeenCalledTimes(1);
});
it("holds policy exclusively and never admits a decision across its barrier", async () => {
  const f = fixture();
  await f.controller.load();
  let finish!: () => void;
  const policy = f.controller.withPolicyBarrier(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await f.controller.mutate({ action: "decide", input: { ...b.record, decision: "deny" } });
  expect(f.client.mutate).not.toHaveBeenCalled();
  finish();
  await policy;
  expect(f.controller.getSnapshot().policyBarrier).toBe(false);
});
it("freezes insertion/removal/order until pointer AND focus leave", () => {
  const freeze = createActionFreeze((value: string) => value);
  expect(freeze.reconcile(["a", "b"])).toEqual(["a", "b"]);
  freeze.pointer(true);
  freeze.focus(true);
  expect(freeze.reconcile(["c", "b"])).toEqual(["a", "b"]);
  freeze.pointer(false);
  expect(freeze.reconcile(["c", "b"])).toEqual(["a", "b"]);
  freeze.focus(false);
  expect(freeze.reconcile(["c", "b"])).toEqual(["c", "b"]);
});
it("starts result subscription at current head, without replaying retained history", async () => {
  const f = fixture();
  await f.controller.load();
  const watching = f.controller.watchResults();
  await vi.waitFor(() => expect(f.client.changes).toHaveBeenCalled());
  expect(f.client.head).toHaveBeenCalledTimes(1);
  expect(f.client.changes).toHaveBeenCalledWith("current-head", expect.any(AbortSignal));
  expect(f.controller.getSnapshot().results).toEqual([]);
  f.controller.stopResults();
  await watching;
});
it("rejects mixed-revision older pages without dropping the existing backlog", async () => {
  const f = fixture();
  await f.controller.load();
  const existing = f.controller.getSnapshot().page!;
  f.client.page = vi.fn(async () => ({
    ...existing,
    revision: "different-snapshot",
    records: [item("older")],
    nextCursor: null,
  }));
  await f.controller.load("current", true);
  expect(f.controller.getSnapshot().page?.records.map((row) => row.record.id)).toEqual([
    "first",
    "second",
  ]);
  expect(f.controller.getSnapshot().error).toContain("history changed");
});
it("prevents a page started before a decision from reviving the held control", async () => {
  const f = fixture();
  await f.controller.load();
  const old = f.controller.getSnapshot().page!;
  const finish: ((page: Page) => void)[] = [];
  f.client.page = vi.fn((view) =>
    view === "current"
      ? new Promise((resolve) => {
          finish.push(resolve);
        })
      : Promise.resolve({ ...old, records: [], access: [] }),
  );
  const reading = f.controller.load();
  f.client.mutate = vi.fn(async () => ({
    item: { ...a, record: { ...a.record, status: "approved" } },
  }));
  const decision = f.controller.mutate({
    action: "decide",
    input: { id: a.record.id, bindingDigest: a.record.bindingDigest, decision: "approve" },
  });
  await vi.waitFor(() => expect(finish).toHaveLength(2));
  for (const resolve of finish) resolve(old);
  await reading;
  await decision;
  f.client.page = vi.fn(async () => old);
  await f.controller.load();
  expect(
    f.controller.getSnapshot().page?.records.find((row) => row.record.id === a.record.id)?.record
      .status,
  ).toBe("approved");
});

const grant = {
  id: "exact-offpage",
  generation: 3,
  state: "active" as const,
  title: "Destination thread",
  origin: "origin",
  destinationThreadId: "destination",
  expiresAt: "2026-11-17T15:00:00Z",
};
it("reads an exact offpage grant and keeps it selected outside page membership", async () => {
  const f = fixture();
  await f.controller.load();
  f.client.lookupAccess = vi.fn(async () => grant);
  await f.controller.lookupAccess(grant.id);
  expect(f.client.lookupAccess).toHaveBeenCalledWith(grant.id);
  expect(f.controller.getSnapshot().selectedAccess).toEqual(grant);
  await f.controller.load("blocked");
  expect(f.controller.getSnapshot().selectedAccess).toEqual(grant);
});
it("keeps a lost access update uncertain until newer exact readback", async () => {
  const f = fixture();
  await f.controller.load();
  f.client.lookupAccess = vi.fn(async () => grant);
  f.client.mutate = vi.fn(async () => {
    throw new Error("lost response");
  });
  const input = {
    action: "update" as const,
    id: grant.id,
    expectedGeneration: 3,
    durationSeconds: 86400 as const,
    commandId: "update",
  };
  await f.controller.mutate(input);
  await f.controller.lookupAccess(grant.id);
  expect(f.controller.getSnapshot().uncertain.has(`access:${grant.id}`)).toBe(true);
  await f.controller.mutate({ ...input, commandId: "blind-retry" });
  expect(f.client.mutate).toHaveBeenCalledTimes(1);
  f.client.lookupAccess = vi.fn(async () => ({ ...grant, generation: 4 }));
  await f.controller.lookupAccess(grant.id);
  expect(f.controller.getSnapshot().uncertain.has(`access:${grant.id}`)).toBe(false);
});
it("uses a conflict chain head and invalidates the duration selection", async () => {
  const f = fixture();
  await f.controller.load();
  const head = { ...grant, id: "successor", generation: 4 };
  f.client.mutate = vi.fn(async () => {
    throw { code: "conflict", current: { grant, chainHead: head } };
  });
  const revision = f.controller.getSnapshot().accessSelectionRevision;
  await f.controller.mutate({
    action: "update",
    id: grant.id,
    expectedGeneration: 3,
    durationSeconds: 86400,
    commandId: "conflict",
  });
  expect(f.controller.getSnapshot().selectedAccess).toEqual(head);
  expect(f.controller.getSnapshot().accessSelectionRevision).toBeGreaterThan(revision);
  expect(f.controller.getSnapshot().uncertain.size).toBe(0);
});
it("does not unlock a lost Restore from a revoked predecessor readback", async () => {
  const f = fixture();
  f.client.capabilities = async () => ({
    canDecide: true,
    features: new Set(["reviewV2", "grants", "grantRestore"]),
  });
  await f.controller.load();
  f.client.lookupAccess = vi.fn(async () => ({ ...grant, state: "revoked", generation: 4 }));
  f.client.mutate = vi.fn(async () => {
    throw new Error("lost restore");
  });
  const input = {
    action: "restore" as const,
    id: grant.id,
    expectedGeneration: 3,
    durationSeconds: 86400 as const,
    commandId: "restore-original",
  };
  await f.controller.mutate(input);
  await f.controller.lookupAccess(grant.id);
  await f.controller.mutate({ ...input, commandId: "forbidden-fresh-command" });
  expect(f.client.mutate).toHaveBeenCalledTimes(1);
  expect(f.controller.getSnapshot().uncertain.has(`access:${grant.id}`)).toBe(true);
});
it("selects the returned successor after Restore", async () => {
  const f = fixture();
  f.client.capabilities = async () => ({
    canDecide: true,
    features: new Set(["reviewV2", "grants", "grantRestore"]),
  });
  await f.controller.load();
  f.client.mutate = vi.fn(async () => ({ access: { ...grant, id: "restored-successor" } }));
  await f.controller.mutate({
    action: "restore",
    id: grant.id,
    expectedGeneration: 3,
    durationSeconds: 86400,
    commandId: "restore",
  });
  expect(f.controller.getSnapshot().selectedAccess?.id).toBe("restored-successor");
});
it("resumes persisted attention from its cursor without requesting a new head", async () => {
  const f = fixture();
  const retained = { ...a, outcome: "unknown" as const };
  const persistence = {
    read: () => ({ cursor: "saved-cursor", results: [retained] }),
    write: vi.fn(),
  };
  f.controller.setAttentionPersistence(persistence);
  const watching = f.controller.watchResults();
  await vi.waitFor(() => expect(f.client.changes).toHaveBeenCalled());
  expect(f.client.head).not.toHaveBeenCalled();
  expect(f.client.changes).toHaveBeenCalledWith("saved-cursor", expect.any(AbortSignal));
  expect(f.controller.getSnapshot().results).toEqual([retained]);
  f.controller.stopResults();
  await watching;
});

it("recovers a pruned saved cursor at head while preserving unknown attention and receiving a new result", async () => {
  const f = fixture();
  const retained = { ...a, outcome: "unknown" as const };
  const created = { ...b, outcome: "created" as const, availability: "available" as const };
  const uncertainAccess = [
    { id: "revoked-grant", action: "restore", generation: 3, restoreCommandId: "original" },
  ];
  const persistence = {
    read: () => ({ cursor: "before-restart", results: [retained], uncertainAccess }),
    write: vi.fn(),
  };
  f.controller.setAttentionPersistence(persistence);
  const pendingChanges = f.client.changes;
  f.client.changes = vi.fn()
    .mockRejectedValueOnce({ code: "history_unavailable" })
    .mockResolvedValueOnce({ cursor: "after-new-result", revision: "new", results: [created] })
    .mockImplementation(pendingChanges);
  const watching = f.controller.watchResults();
  await vi.waitFor(() => expect(f.client.changes).toHaveBeenCalledTimes(3));
  expect(f.client.head).toHaveBeenCalledTimes(1);
  expect(f.client.changes).toHaveBeenNthCalledWith(1, "before-restart", expect.any(AbortSignal));
  expect(f.client.changes).toHaveBeenNthCalledWith(2, "current-head", expect.any(AbortSignal));
  expect(f.client.changes).toHaveBeenNthCalledWith(3, "after-new-result", expect.any(AbortSignal));
  expect(f.controller.getSnapshot().results).toEqual([retained, created]);
  expect(f.controller.getSnapshot().uncertain.has("access:revoked-grant")).toBe(true);
  expect(persistence.write).toHaveBeenLastCalledWith({
    cursor: "after-new-result", results: [retained, created], uncertainAccess,
  });
  expect(f.client.page).toHaveBeenCalled();
  expect(f.client.mutate).not.toHaveBeenCalled();
  f.controller.stopResults();
  await watching;
});

it("bounds repeated history rejection to one head reset without clearing unknown outcomes", async () => {
  const f = fixture();
  const retained = { ...a, outcome: "unknown" as const };
  const write = vi.fn();
  f.controller.setAttentionPersistence({
    read: () => ({ cursor: "expired", results: [retained] }), write,
  });
  f.client.changes = vi.fn().mockRejectedValue({ code: "history_unavailable" });
  await f.controller.watchResults();
  expect(f.client.head).toHaveBeenCalledTimes(1);
  expect(f.client.changes).toHaveBeenCalledTimes(2);
  expect(f.controller.getSnapshot().results).toEqual([retained]);
  expect(f.controller.getSnapshot().error).toContain("updates are unavailable");
  expect(write).toHaveBeenLastCalledWith({
    cursor: "current-head", results: [retained], uncertainAccess: [],
  });
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("does not persist a late reset head after the result subscription stops", async () => {
  const f = fixture();
  const write = vi.fn();
  f.controller.setAttentionPersistence({
    read: () => ({ cursor: "expired", results: [] }), write,
  });
  let finish!: (head: string) => void;
  f.client.head = vi.fn(() => new Promise((resolve) => { finish = resolve; }));
  f.client.changes = vi.fn().mockRejectedValue({ code: "history_unavailable" });
  const watching = f.controller.watchResults();
  await vi.waitFor(() => expect(f.client.head).toHaveBeenCalledTimes(1));
  f.controller.stopResults();
  finish("late-head");
  await watching;
  expect(f.client.changes).toHaveBeenCalledTimes(1);
  expect(write).toHaveBeenLastCalledWith({ cursor: "expired", results: [], uncertainAccess: [] });
});

it("opens the exact managed access lineage for a new blocked message without a newly issued grant", async () => {
  let wire: RemoteAdmissionReviewItem = {
    record: a.record,
    extension: {
      managedAccessGrantId: "revoked-scope-head",
      grantable: true,
      grantUnavailableReason: null,
      grant: null,
      evaluator: null,
      blocked: { admissionId: a.record.id, managedScopeId: "scope", reason: "access_revoked" },
      accessBasis: null,
      attemptId: null,
      result: null,
      recoveredResult: null,
      originNotifiedAt: null,
      retryEligible: false,
      recheckEligible: false,
    },
  };
  const transport: ReviewTransport = {
    capabilities: vi.fn(), review: vi.fn(), grants: vi.fn(), decision: vi.fn(),
    updateGrant: vi.fn(), revokeGrant: vi.fn(), restoreGrant: vi.fn(), retry: vi.fn(),
    recheck: vi.fn(), changes: vi.fn(), item: vi.fn(async () => wire),
  };
  const client = createCanonicalReviewClient(transport);
  expect((await client.lookup(a.record.id)).accessId).toBe("revoked-scope-head");
  const olderExtension = { ...wire.extension };
  delete olderExtension.managedAccessGrantId;
  wire = { ...wire, extension: olderExtension };
  expect((await client.lookup(a.record.id)).accessId).toBeNull();
  expect(transport.decision).not.toHaveBeenCalled();
  expect(transport.restoreGrant).not.toHaveBeenCalled();
});

it("retains a lost Restore fence when a new controller mounts", async () => {
  const f = fixture();
  await f.controller.load();
  const snapshots: import("@t3tools/client-runtime/jones/remoteAdmission/controller").AttentionSnapshot[] =
    [];
  f.controller.setAttentionPersistence({
    read: () => null,
    write: (value) => {
      snapshots.push(value);
    },
  });
  f.client.capabilities = async () => ({
    canDecide: true,
    features: new Set(["reviewV2", "grantRestore"]),
  });
  await f.controller.load();
  f.client.mutate = vi.fn(async () => {
    throw new Error("lost");
  });
  const input = {
    action: "restore" as const,
    id: grant.id,
    expectedGeneration: 3,
    durationSeconds: 86400 as const,
    commandId: "original",
  };
  await f.controller.mutate(input);
  const remounted = createReviewController(f.client);
  remounted.setAttentionPersistence({ read: () => snapshots.at(-1)!, write: () => undefined });
  await remounted.load();
  await remounted.mutate({ ...input, commandId: "new-key" });
  expect(f.client.mutate).toHaveBeenCalledTimes(1);
  expect(remounted.getSnapshot().uncertain.has(`access:${grant.id}`)).toBe(true);
});

it("loads only one page of each Current auxiliary group and pages a group independently", async () => {
  const f = fixture();
  const base = await f.client.page("current");
  f.client.page = vi.fn(async (view, cursor) =>
    view === "access"
      ? {
          ...base,
          records: [],
          access: [{ ...grant, id: cursor ? "second-grant" : "first-grant" }],
          nextCursor: cursor ? null : "access-next",
        }
      : {
          ...base,
          records:
            view === "unresolved"
              ? [{ ...a, outcome: "unknown" as const }]
              : view === "inflight"
                ? [{ ...b, outcome: "awaiting" as const }]
                : base.records,
        },
  );
  await f.controller.load();
  expect(f.client.page).toHaveBeenCalledTimes(4);
  expect(f.controller.getSnapshot().view).toBe("current");
  expect(f.controller.getSnapshot().page?.counts.currentHeldAdmissions).toBe(2);
  expect(f.controller.getSnapshot().groups.unresolved.page?.records[0]?.outcome).toBe("unknown");
  await f.controller.loadGroup("access", true);
  expect(f.client.page).toHaveBeenLastCalledWith("access", "access-next");
  expect(f.controller.getSnapshot().groups.access.page?.access.map((entry) => entry.id)).toEqual([
    "first-grant",
    "second-grant",
  ]);
  expect(f.controller.getSnapshot().groups.inflight.page?.records[0]?.record.id).toBe(b.record.id);
  expect(f.controller.getSnapshot().page?.records).toEqual(base.records);
});
it("preserves auxiliary membership when older pages have a different snapshot revision", async () => {
  const f = fixture();
  await f.controller.load();
  const base = f.controller.getSnapshot().page!;
  f.client.page = vi.fn(async () => ({
    ...base,
    records: [a],
    nextCursor: "next",
    revision: "group-1",
  }));
  await f.controller.loadGroup("unresolved");
  f.client.page = vi.fn(async () => ({
    ...base,
    records: [b],
    nextCursor: null,
    revision: "group-2",
  }));
  await f.controller.loadGroup("unresolved", true);
  expect(f.controller.getSnapshot().groups.unresolved.page?.records).toEqual([a]);
  expect(f.controller.getSnapshot().groups.unresolved.error).toContain("group changed");
  expect(f.controller.getSnapshot().view).toBe("current");
});
it("reads the original lost Restore command, selects its currently revoked successor, and never mutates again", async () => {
  const f = fixture();
  f.client.capabilities = async () => ({
    canDecide: true,
    features: new Set(["reviewV2", "grants", "grantRestore"]),
  });
  await f.controller.load();
  const snapshots: import("@t3tools/client-runtime/jones/remoteAdmission/controller").AttentionSnapshot[] =
    [];
  f.controller.setAttentionPersistence({
    read: () => null,
    write: (snapshot) => {
      snapshots.push(snapshot);
    },
  });
  f.client.mutate = vi.fn(async () => {
    throw new Error("lost");
  });
  await f.controller.mutate({
    action: "restore",
    id: grant.id,
    expectedGeneration: 3,
    durationSeconds: 86400,
    commandId: "original-command",
  });
  expect(snapshots.at(-1)?.uncertainAccess?.[0]?.restoreCommandId).toBe("original-command");
  const currentSuccessor = {
    ...grant,
    id: "original-successor",
    generation: 7,
    state: "revoked" as const,
  };
  f.client.lookupRestore = vi.fn(async () => currentSuccessor);
  const remounted = createReviewController(f.client);
  remounted.setAttentionPersistence({ read: () => snapshots.at(-1)!, write: () => undefined });
  await remounted.load();
  await remounted.lookupAccess(grant.id);
  expect(f.client.lookupRestore).toHaveBeenCalledWith(grant.id, "original-command");
  expect(remounted.getSnapshot().selectedAccess).toEqual(currentSuccessor);
  expect(remounted.getSnapshot().selectedAccessFromId).toBe(grant.id);
  expect(remounted.getSnapshot().uncertain.has(`access:${grant.id}`)).toBe(false);
  expect(f.client.mutate).toHaveBeenCalledTimes(1);
});
it("keeps missing original Restore evidence uncertain without falling back or replaying", async () => {
  const f = fixture();
  f.client.lookupRestore = vi.fn(async () => {
    throw new Error("grant_unavailable");
  });
  f.controller.setAttentionPersistence({
    read: () => ({
      cursor: null,
      results: [],
      uncertainAccess: [
        { id: grant.id, action: "restore", generation: 3, restoreCommandId: "pruned-command" },
      ],
    }),
    write: () => undefined,
  });
  await expect(f.controller.lookupAccess(grant.id)).rejects.toThrow("grant_unavailable");
  expect(f.controller.getSnapshot().uncertain.has(`access:${grant.id}`)).toBe(true);
  expect(f.client.lookupAccess).not.toHaveBeenCalled();
  expect(f.client.mutate).not.toHaveBeenCalled();
});

it("adapts pure Restore readback to the exact original command and preserves current successor state", async () => {
  const successor = {
    grantId: "successor",
    sourceAdmissionId: "source",
    generation: 7,
    state: "revoked" as const,
    binding: {
      receiverEnvironmentId: "receiver",
      sessionId: "synthetic-session",
      originEnvironmentId: "origin",
      originThreadId: "origin-thread",
      destinationThreadId: "destination",
      operation: "t3_thread_send" as const,
      runtimeMode: "full-access",
      interactionMode: "default",
    },
    expiresAt: "2026-11-17T15:00:00Z",
    createdAt: "2026-11-16T15:00:00Z",
    updatedAt: "2026-11-16T16:00:00Z",
    revokedAt: "2026-11-16T16:00:00Z",
    predecessorGrantId: "predecessor",
    serverNow: "2026-11-16T17:00:00Z",
  };
  const transport: ReviewTransport = {
    capabilities: vi.fn(),
    review: vi.fn(),
    item: vi.fn(),
    decision: vi.fn(),
    updateGrant: vi.fn(),
    revokeGrant: vi.fn(),
    restoreGrant: vi.fn(),
    retry: vi.fn(),
    recheck: vi.fn(),
    changes: vi.fn(),
    grants: vi.fn(async () => ({
      snapshotRevision: "snapshot",
      grants: [successor],
      nextCursor: null,
      serverNow: successor.serverNow,
    })),
  };
  const client = createCanonicalReviewClient(transport);
  const current = await client.lookupRestore!("predecessor", "original-command");
  expect(transport.grants).toHaveBeenCalledWith({
    grantId: "predecessor",
    restoreCommandKey: "original-command",
  });
  expect(current.id).toBe("successor");
  expect(current.state).toBe("revoked");
  expect(current.generation).toBe(7);
  expect(transport.restoreGrant).not.toHaveBeenCalled();
  transport.grants = vi.fn(async () => ({
    snapshotRevision: "snapshot",
    grants: [],
    nextCursor: null,
    serverNow: successor.serverNow,
  }));
  await expect(client.lookupRestore!("predecessor", "original-command")).rejects.toThrow(
    "unavailable",
  );
});
