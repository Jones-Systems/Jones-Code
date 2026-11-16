import {
  AuthOrchestrationOperateScope,
  sessionGrantsScope,
  type AuthSessionState,
  type EnvironmentId,
  type RuntimeRequestId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { Atom, type AtomRegistry } from "effect/reactivity";
import type { EnvironmentPresentation } from "../../connection/presentation.ts";
import type { EnvironmentThreadShell } from "../../state/models.ts";
import type { EnvironmentShellState } from "../../state/shell.ts";
import type { EnvironmentThreadStatus } from "../../state/threadState.ts";
import type { PendingThreadRequests, ThreadPendingApproval } from "../../state/threadRequests.ts";

export const DESCENDANT_APPROVAL_MESSAGES = {
  detail: "Waiting for current subagent detail.",
  environment: "Waiting for current environment data.",
  permission: "This connection cannot respond to approvals.",
  resolved: "This approval is no longer pending.",
  capability: "This approval can't be answered here. Open the subagent.",
} as const;

export type DescendantApprovalUnavailableReason = keyof typeof DESCENDANT_APPROVAL_MESSAGES;
export type DescendantApprovalView =
  | {
      readonly kind: "actionable";
      readonly approval: ThreadPendingApproval;
      readonly pendingCount: number;
    }
  | {
      readonly kind: "unavailable";
      readonly reason: DescendantApprovalUnavailableReason;
      readonly message: string;
      readonly approval: ThreadPendingApproval | null;
    };
export interface DescendantApprovalCandidate {
  readonly key: string;
  readonly ref: ScopedThreadRef;
  readonly title: string;
  readonly environmentReady: boolean;
}
export interface DescendantApprovalState {
  readonly candidates: ReadonlyArray<DescendantApprovalCandidate>;
  readonly selected: DescendantApprovalCandidate;
  /** One-based position of the selected child. */
  readonly position: number;
  readonly view: DescendantApprovalView;
  readonly responding: boolean;
}
export interface DescendantApprovalSources {
  readonly childThreadInputsAtom: (
    parent: ScopedThreadRef,
  ) => Atom.Atom<
    ReadonlyArray<
      Pick<EnvironmentThreadShell, "environmentId" | "id" | "title" | "hasPendingApprovals">
    >
  >;
  readonly presentationAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<Pick<EnvironmentPresentation, "connection"> | null>;
  readonly shellStateAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<Pick<EnvironmentShellState, "status" | "error">>;
  readonly sessionStateAtom: (environmentId: EnvironmentId) => Atom.Atom<AuthSessionState | null>;
  readonly threadStatusAtom: (ref: ScopedThreadRef) => Atom.Atom<EnvironmentThreadStatus>;
  readonly threadErrorAtom: (ref: ScopedThreadRef) => Atom.Atom<string | null>;
  readonly pendingRequestsAtom: (
    ref: ScopedThreadRef,
  ) => Atom.Atom<Pick<PendingThreadRequests, "approvals"> | null>;
}
export type DescendantApprovalResponse<R> =
  | { readonly kind: "sent"; readonly result: R }
  | { readonly kind: "in-flight" }
  | { readonly kind: "not-current" };

export const descendantApprovalChildKey = (ref: ScopedThreadRef): string =>
  JSON.stringify([ref.environmentId, ref.threadId]);
const responseKey = (ref: ScopedThreadRef, requestId: RuntimeRequestId) =>
  JSON.stringify([ref.environmentId, ref.threadId, requestId]);
// Shared across model instances and views, but isolated by the consuming registry.
const inFlightAtom = Atom.make<ReadonlySet<string>>(new Set<string>()).pipe(Atom.keepAlive);

export function createDescendantApprovalAtoms(sources: DescendantApprovalSources) {
  const selectionAtom = Atom.family((_key: string) =>
    Atom.make<string | null>(null).pipe(Atom.keepAlive),
  );
  const states = Atom.family((key: string) => {
    const [environmentId, threadId] = JSON.parse(key) as [
      ScopedThreadRef["environmentId"],
      ScopedThreadRef["threadId"],
    ];
    const parent = { environmentId, threadId };
    return Atom.make((get): DescendantApprovalState | null => {
      const candidates = get(sources.childThreadInputsAtom(parent))
        .filter((child) => child.hasPendingApprovals)
        .map((child): DescendantApprovalCandidate => {
          const ref = { environmentId: child.environmentId, threadId: child.id };
          const presentation = get(sources.presentationAtom(child.environmentId));
          const shell = get(sources.shellStateAtom(child.environmentId));
          return {
            key: descendantApprovalChildKey(ref),
            ref,
            title: child.title,
            environmentReady:
              presentation?.connection.phase === "connected" &&
              shell.status === "live" &&
              Option.isNone(shell.error),
          };
        });
      const selectedKey = get(selectionAtom(key));
      const selected =
        candidates.find((child) => child.key === selectedKey) ??
        candidates.find((child) => child.environmentReady) ??
        candidates[0];
      if (selected === undefined) return null;
      // Only the selected child mounts a detail stream. Shells provide the candidate list.
      const status = get(sources.threadStatusAtom(selected.ref));
      const error = get(sources.threadErrorAtom(selected.ref));
      const requests = get(sources.pendingRequestsAtom(selected.ref));
      const approvals = requests?.approvals ?? [];
      const approval = approvals[0] ?? null;
      const session = get(sources.sessionStateAtom(selected.ref.environmentId));
      const reason: DescendantApprovalUnavailableReason | null =
        status !== "live" || error !== null || requests === null
          ? "detail"
          : !selected.environmentReady
            ? "environment"
            : session === null || !sessionGrantsScope(session, AuthOrchestrationOperateScope)
              ? "permission"
              : approval === null
                ? "resolved"
                : approval.responseCapability !== "live"
                  ? "capability"
                  : null;
      return {
        candidates,
        selected,
        position: candidates.indexOf(selected) + 1,
        view:
          reason === null && approval !== null
            ? { kind: "actionable", approval, pendingCount: approvals.length }
            : {
                kind: "unavailable",
                reason: reason ?? "resolved",
                message: DESCENDANT_APPROVAL_MESSAGES[reason ?? "resolved"],
                approval,
              },
        responding:
          approval !== null && get(inFlightAtom).has(responseKey(selected.ref, approval.requestId)),
      };
    }).pipe(Atom.setIdleTTL(0));
  });
  const stateAtom = (parent: ScopedThreadRef) => states(descendantApprovalChildKey(parent));
  const select = (
    registry: AtomRegistry.AtomRegistry,
    parent: ScopedThreadRef,
    childKey: string,
  ): void => {
    if (registry.get(stateAtom(parent))?.candidates.some((child) => child.key === childKey))
      registry.set(selectionAtom(descendantApprovalChildKey(parent)), childKey);
  };
  const selectNext = (registry: AtomRegistry.AtomRegistry, parent: ScopedThreadRef): void => {
    const state = registry.get(stateAtom(parent));
    if (state === null) return;
    const next = state.candidates[state.position % state.candidates.length];
    if (next) select(registry, parent, next.key);
  };
  const respond = async <R>(
    registry: AtomRegistry.AtomRegistry,
    input: {
      readonly parent: ScopedThreadRef;
      readonly child: ScopedThreadRef;
      readonly requestId: RuntimeRequestId;
      readonly dispatch: () => Promise<R>;
    },
  ): Promise<DescendantApprovalResponse<R>> => {
    const key = responseKey(input.child, input.requestId);
    if (registry.get(inFlightAtom).has(key)) return { kind: "in-flight" };
    const state = registry.get(stateAtom(input.parent));
    if (
      state === null ||
      state.selected.key !== descendantApprovalChildKey(input.child) ||
      state.view.kind !== "actionable" ||
      state.view.approval.requestId !== input.requestId
    )
      return { kind: "not-current" };
    registry.set(inFlightAtom, new Set([...registry.get(inFlightAtom), key]));
    try {
      return { kind: "sent", result: await input.dispatch() };
    } finally {
      const remaining = new Set(registry.get(inFlightAtom));
      remaining.delete(key);
      registry.set(inFlightAtom, remaining);
    }
  };
  return { stateAtom, select, selectNext, respond };
}

export type DescendantApprovalModel = ReturnType<typeof createDescendantApprovalAtoms>;
