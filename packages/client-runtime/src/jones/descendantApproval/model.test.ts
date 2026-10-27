import {
  EnvironmentId,
  RuntimeRequestId,
  ThreadId,
  type AuthSessionState,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { Atom, AtomRegistry } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  createDescendantApprovalAtoms,
  descendantApprovalChildKey,
  DESCENDANT_APPROVAL_MESSAGES,
  type DescendantApprovalSources,
} from "./model.ts";
import type { ThreadPendingApproval } from "../../state/threadRequests.ts";

const parent = {
  environmentId: EnvironmentId.make("parent-env"),
  threadId: ThreadId.make("parent"),
};
const child = (env = "a"): ScopedThreadRef => ({
  environmentId: EnvironmentId.make(env),
  threadId: ThreadId.make("same-child"),
});
const approval: ThreadPendingApproval = {
  requestId: RuntimeRequestId.make("same-request"),
  requestKind: "command",
  createdAt: "2026-10-26T12:00:00Z",
  detail: "run synthetic command",
  responseCapability: "live",
  options: [{ decision: "accept", label: "Approve" }],
};
const session: AuthSessionState = {
  authenticated: true,
  scopes: ["orchestration:operate"],
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "test",
  },
};
const disposers: Array<() => void> = [];
afterEach(() => {
  for (const dispose of disposers.splice(0)) dispose();
});
function harness(envs = ["a", "b"], offline: ReadonlyArray<string> = []) {
  const registry = AtomRegistry.make();
  const children = Atom.make(
    envs.map((env) => ({
      environmentId: child(env).environmentId,
      id: child(env).threadId,
      title: `Child ${env}`,
      hasPendingApprovals: true,
    })),
  );
  const presentations = Atom.family((_env: EnvironmentId) =>
    Atom.make<Atom.Type<ReturnType<DescendantApprovalSources["presentationAtom"]>>>({
      connection: { phase: "connected", error: null, traceId: null },
    }),
  );
  const shells = Atom.family((_env: EnvironmentId) =>
    Atom.make<Atom.Type<ReturnType<DescendantApprovalSources["shellStateAtom"]>>>({
      status: "live",
      error: Option.none(),
    }),
  );
  const sessions = Atom.family((_env: EnvironmentId) =>
    Atom.make<AuthSessionState | null>(session),
  );
  const statuses = Atom.family((_key: string) =>
    Atom.make<Atom.Type<ReturnType<DescendantApprovalSources["threadStatusAtom"]>>>("live"),
  );
  const errors = Atom.family((_key: string) => Atom.make<string | null>(null));
  const requests = Atom.family((_key: string) =>
    Atom.make<{ approvals: ReadonlyArray<ThreadPendingApproval> } | null>({
      approvals: [approval],
    }),
  );
  const activeDetails = new Map<string, number>();
  const detailRequests = Atom.family((key: string) =>
    Atom.make((get) => {
      activeDetails.set(key, (activeDetails.get(key) ?? 0) + 1);
      get.addFinalizer(() => {
        const remaining = (activeDetails.get(key) ?? 1) - 1;
        if (remaining === 0) activeDetails.delete(key);
        else activeDetails.set(key, remaining);
      });
      return get(requests(key));
    }).pipe(Atom.setIdleTTL(0)),
  );
  const detailRead = vi.fn();
  const sources: DescendantApprovalSources = {
    childThreadInputsAtom: () => children,
    presentationAtom: presentations,
    shellStateAtom: shells,
    sessionStateAtom: sessions,
    threadStatusAtom: (ref) => {
      detailRead(ref);
      return statuses(descendantApprovalChildKey(ref));
    },
    threadErrorAtom: (ref) => {
      detailRead(ref);
      return errors(descendantApprovalChildKey(ref));
    },
    pendingRequestsAtom: (ref) => {
      detailRead(ref);
      return detailRequests(descendantApprovalChildKey(ref));
    },
  };
  const model = createDescendantApprovalAtoms(sources);
  for (const env of offline)
    registry.set(shells(EnvironmentId.make(env)), { status: "cached", error: Option.none() });
  const release = registry.mount(model.stateAtom(parent));
  let mounted = true;
  const unmount = () => {
    if (!mounted) return;
    mounted = false;
    release();
  };
  disposers.push(() => {
    unmount();
    registry.dispose();
  });
  const read = () => registry.get(model.stateAtom(parent));
  const respond = (ref = child(), dispatch = vi.fn(async () => "ok")) =>
    model.respond(registry, { parent, child: ref, requestId: approval.requestId, dispatch });
  return {
    registry,
    model,
    sources,
    children,
    presentations,
    shells,
    sessions,
    statuses,
    errors,
    requests,
    detailRead,
    activeDetails,
    read,
    respond,
    unmount,
  };
}

describe("descendant approval model", () => {
  it("applies each freshness, authorization, pending and capability gate in order while retaining cached detail", () => {
    const h = harness(["a"]);
    const key = descendantApprovalChildKey(child());
    const reason = (expected: keyof typeof DESCENDANT_APPROVAL_MESSAGES) =>
      expect(h.read()?.view).toEqual({
        kind: "unavailable",
        reason: expected,
        message: DESCENDANT_APPROVAL_MESSAGES[expected],
        approval: h.registry.get(h.requests(key))?.approvals[0] ?? null,
      });
    h.registry.set(h.statuses(key), "cached");
    h.registry.set(h.shells(child().environmentId), { status: "cached", error: Option.none() });
    h.registry.set(h.sessions(child().environmentId), null);
    reason("detail");
    h.registry.set(h.statuses(key), "live");
    h.registry.set(h.errors(key), "detail stream failed");
    reason("detail");
    h.registry.set(h.errors(key), null);
    reason("environment");
    h.registry.set(h.shells(child().environmentId), {
      status: "live",
      error: Option.some("shell failed"),
    });
    reason("environment");
    h.registry.set(h.shells(child().environmentId), { status: "live", error: Option.none() });
    h.registry.set(h.presentations(child().environmentId), {
      connection: { phase: "offline", error: null, traceId: null },
    });
    reason("environment");
    h.registry.set(h.presentations(child().environmentId), {
      connection: { phase: "connected", error: null, traceId: null },
    });
    reason("permission");
    h.registry.set(h.sessions(child().environmentId), {
      ...session,
      scopes: ["orchestration:read"],
    });
    reason("permission");
    h.registry.set(h.sessions(child().environmentId), session);
    h.registry.set(h.requests(key), { approvals: [] });
    reason("resolved");
    h.registry.set(h.requests(key), {
      approvals: [{ ...approval, responseCapability: "not_resumable" }],
    });
    reason("capability");
    h.registry.set(h.requests(key), { approvals: [approval] });
    expect(h.read()?.view.kind).toBe("actionable");
  });

  it("keeps missing detail unavailable, refuses dispatch after detail disappears, and recovers when it returns", async () => {
    const h = harness(["a"]);
    const key = descendantApprovalChildKey(child());
    expect(h.read()?.view.kind).toBe("actionable");
    h.registry.set(h.requests(key), null);
    expect(h.read()?.view).toEqual({
      kind: "unavailable",
      reason: "detail",
      message: DESCENDANT_APPROVAL_MESSAGES.detail,
      approval: null,
    });
    const dispatch = vi.fn(async () => "wrong");
    expect(await h.respond(child(), dispatch)).toEqual({ kind: "not-current" });
    expect(dispatch).not.toHaveBeenCalled();
    h.registry.set(h.requests(key), { approvals: [approval] });
    expect(h.read()?.view).toEqual({ kind: "actionable", approval, pendingCount: 1 });
  });

  it("prefers an environment-ready child, preserves explicit selection, and wraps Next without reading unselected detail", () => {
    const h = harness(["a", "b"], ["a"]);
    expect(h.read()?.selected.ref).toEqual(child("b"));
    expect(h.detailRead).toHaveBeenCalled();
    expect(h.detailRead.mock.calls.every(([ref]) => ref.environmentId === "b")).toBe(true);
    h.model.select(h.registry, parent, descendantApprovalChildKey(child()));
    expect(h.read()?.selected.ref).toEqual(child());
    h.registry.set(h.shells(child().environmentId), { status: "live", error: Option.none() });
    expect(h.read()?.selected.ref).toEqual(child());
    h.model.selectNext(h.registry, parent);
    expect(h.read()?.position).toBe(2);
    h.model.selectNext(h.registry, parent);
    expect(h.read()?.position).toBe(1);
  });

  it("deduplicates the exact environment/thread/request across model instances and clears in-flight on failure", async () => {
    const h = harness();
    let reject!: (error: Error) => void;
    const dispatch = vi.fn(
      () =>
        new Promise<string>((_resolve, fail) => {
          reject = fail;
        }),
    );
    const pending = h.respond(child(), dispatch);
    expect(h.read()?.responding).toBe(true);
    const secondView = createDescendantApprovalAtoms(h.sources);
    expect(
      await secondView.respond(h.registry, {
        parent,
        child: child(),
        requestId: approval.requestId,
        dispatch,
      }),
    ).toEqual({ kind: "in-flight" });
    h.model.selectNext(h.registry, parent);
    const otherDispatch = vi.fn(async () => "b response");
    expect(await h.respond(child("b"), otherDispatch)).toEqual({
      kind: "sent",
      result: "b response",
    });
    reject(new Error("synthetic failure"));
    await expect(pending).rejects.toThrow("synthetic failure");
    h.model.selectNext(h.registry, parent);
    expect(h.read()?.responding).toBe(false);
    expect(await h.respond()).toEqual({ kind: "sent", result: "ok" });
    expect(dispatch).toHaveBeenCalledTimes(1);
  });

  it("releases the prior selected detail stream on Next and every detail stream on unmount", async () => {
    const h = harness();
    h.read();
    expect([...h.activeDetails.keys()]).toEqual([descendantApprovalChildKey(child())]);
    h.model.selectNext(h.registry, parent);
    h.read();
    await vi.waitFor(() =>
      expect([...h.activeDetails.keys()]).toEqual([descendantApprovalChildKey(child("b"))]),
    );
    h.unmount();
    await vi.waitFor(() => expect(h.activeDetails.size).toBe(0));
  });

  it("re-reads detail and refuses a vanished or superseded request and stale selection", async () => {
    const h = harness();
    const dispatch = vi.fn(async () => "wrong");
    h.registry.set(h.requests(descendantApprovalChildKey(child())), { approvals: [] });
    expect(await h.respond(child(), dispatch)).toEqual({ kind: "not-current" });
    h.registry.set(h.requests(descendantApprovalChildKey(child())), {
      approvals: [{ ...approval, requestId: RuntimeRequestId.make("replacement") }],
    });
    expect(await h.respond(child(), dispatch)).toEqual({ kind: "not-current" });
    h.model.selectNext(h.registry, parent);
    expect(await h.respond(child(), dispatch)).toEqual({ kind: "not-current" });
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("moves to the remaining sibling after an answer, and removes interrupted or externally answered candidates", () => {
    const h = harness();
    h.registry.set(
      h.children,
      h.registry
        .get(h.children)
        .map((row) => (row.environmentId === "a" ? { ...row, hasPendingApprovals: false } : row)),
    );
    expect(h.read()?.selected.ref).toEqual(child("b"));
    expect(h.read()?.view.kind).toBe("actionable");
    expect(h.read()?.candidates).toHaveLength(1);
    h.registry.set(h.children, []);
    expect(h.read()).toBeNull();
  });

  it("advances to a child's next pending approval and never sends the prior request's response to it", async () => {
    const h = harness(["a"]);
    const key = descendantApprovalChildKey(child());
    const nextApproval = { ...approval, requestId: RuntimeRequestId.make("next-request") };
    h.registry.set(h.requests(key), { approvals: [approval, nextApproval] });
    expect(h.read()?.view).toEqual({ kind: "actionable", approval, pendingCount: 2 });
    h.registry.set(h.requests(key), { approvals: [nextApproval] });
    expect(h.read()?.view).toEqual({ kind: "actionable", approval: nextApproval, pendingCount: 1 });
    const dispatch = vi.fn(async () => "old response");
    expect(await h.respond(child(), dispatch)).toEqual({ kind: "not-current" });
    expect(dispatch).not.toHaveBeenCalled();
  });
});
