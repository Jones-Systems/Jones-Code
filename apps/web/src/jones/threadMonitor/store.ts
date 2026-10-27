import { randomUUID } from "../../lib/utils";
import type { EnvironmentId, MessageId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import {
  MonitorActionLedger,
  MonitorLifetimeFence,
  clearAcceptedMonitorDraft,
  monitorDraftKey,
  monitorThreadKey,
  sameMonitorLifetime,
} from "@t3tools/client-runtime/jones/thread-monitor";
import { THREAD_MONITOR_LIMITS } from "@t3tools/contracts/jones/desktopThreadMonitor";
import type {
  DesktopThreadMonitorMainBridge,
  ThreadMonitorAction,
  ThreadMonitorActionResult,
  ThreadMonitorDetail,
  ThreadMonitorDraft,
  ThreadMonitorEnvironment,
  ThreadMonitorGroup,
  ThreadMonitorLifetime,
  ThreadMonitorRef,
  ThreadMonitorRow,
  ThreadMonitorCounts,
  ThreadMonitorViewState,
} from "@t3tools/contracts/jones/desktopThreadMonitor";

import { clearedMonitorViewState, emptyMonitorViewState } from "./viewState";

export const GROUPS = ["needsYou", "working", "waiting", "done"] as const;
const INITIAL_CAPS = { needsYou: 3, working: 2, waiting: 1, done: 1 } as const;
export const emptyCounts = (): ThreadMonitorCounts => ({
  needsYou: 0,
  working: 0,
  waiting: 0,
  done: 0,
});
export const refOf = (ref: ThreadMonitorRef): ThreadMonitorRef => ({
  publisherGeneration: ref.publisherGeneration,
  environmentId: ref.environmentId,
  environmentEpoch: ref.environmentEpoch,
  threadId: ref.threadId,
});
const draftKey = (draft: Pick<ThreadMonitorDraft, keyof ThreadMonitorRef | "requestId">) =>
  monitorDraftKey(draft, draft.requestId);
export type MonitorSelection = ThreadMonitorRef & { readonly selectionId: string };
export type MonitorActionIdentity = Pick<
  ThreadMonitorAction,
  keyof ThreadMonitorRef | "type" | "actionId" | "commandId"
> & {
  readonly messageId?: MessageId;
  readonly draftRevision?: number;
  readonly settleCommandId?: ThreadMonitorAction["commandId"];
  readonly requestId?: ThreadMonitorDraft["requestId"];
};

export class MonitorStore {
  readonly fence: MonitorLifetimeFence;
  readonly actionLedger = new MonitorActionLedger();
  readonly drafts = new Map<string, ThreadMonitorDraft>();
  readonly results = new Map<string, ThreadMonitorActionResult>();
  readonly identities = new Map<string, MonitorActionIdentity>();
  readonly caps: Record<ThreadMonitorGroup, number> = { ...INITIAL_CAPS };
  readonly materialized = new Set<string>();
  environments: ReadonlyArray<ThreadMonitorEnvironment> = [];
  rows: ReadonlyArray<ThreadMonitorRow> = [];
  counts = emptyCounts();
  selection: MonitorSelection | null = null;
  detail: ThreadMonitorDetail | null = null;
  viewState: ThreadMonitorViewState;
  private retainedEditorRow: ThreadMonitorRow | null = null;
  private detailRevision = 0;
  snapshotRevision = 0;
  revision = 0;
  private listeners = new Set<() => void>();
  private invalidator: DesktopThreadMonitorMainBridge["invalidateEnvironment"] | null = null;
  private detailRelease: (() => void) | null = null;
  private retiredAwaitingCatalog = new Set<EnvironmentId>();
  private clearedRevisions = new Map<string, number>();
  private pendingDecisionRequests = new Map<string, ReadonlySet<RuntimeRequestId>>();
  private invalidations = new Map<EnvironmentId, Promise<void>>();
  constructor(generation: string) {
    this.fence = new MonitorLifetimeFence(generation);
    this.viewState = emptyMonitorViewState(generation);
  }
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  getRevision = () => this.revision;
  changed() {
    this.revision++;
    for (const listener of this.listeners) listener();
  }
  nextDetailRevision() {
    return ++this.detailRevision;
  }
  editViewState(state: ThreadMonitorViewState): boolean {
    if (
      state.publisherGeneration !== this.fence.publisherGeneration ||
      state.revision <= this.viewState.revision ||
      (state.editor !== null &&
        (!this.fence.accepts(state.editor.ref) || !this.hasThread(state.editor.ref)))
    )
      return false;
    const anchors = (value: ThreadMonitorViewState["list"]) => ({
      ...value,
      anchors: value.anchors.filter((anchor) => {
        if (/^section:(needsYou|working|waiting|done)$/.test(anchor.key)) return true;
        if (anchor.key.startsWith("row:"))
          return this.rows.some(
            (row) => `row:${monitorThreadKey(row)}` === anchor.key && this.fence.accepts(row),
          );
        return this.detail?.entries.some((entry) => `message:${entry.id}` === anchor.key) === true;
      }),
    });
    this.viewState = { ...state, list: anchors(state.list), detail: anchors(state.detail) };
    this.retainedEditorRow =
      state.editor === null
        ? null
        : (this.rows.find((row) => monitorThreadKey(row) === monitorThreadKey(state.editor!.ref)) ??
          (this.retainedEditorRow &&
          monitorThreadKey(this.retainedEditorRow) === monitorThreadKey(state.editor.ref)
            ? this.retainedEditorRow
            : null));
    this.changed();
    return true;
  }
  bindInvalidator(invalidator: DesktopThreadMonitorMainBridge["invalidateEnvironment"] | null) {
    this.invalidator = invalidator;
  }
  observeCatalog(ids: ReadonlySet<EnvironmentId>) {
    for (const id of this.retiredAwaitingCatalog)
      if (!ids.has(id)) this.retiredAwaitingCatalog.delete(id);
  }
  register(environmentId: EnvironmentId): ThreadMonitorLifetime | null {
    if (this.retiredAwaitingCatalog.has(environmentId) || this.invalidations.has(environmentId))
      return null;
    return this.fence.register(environmentId);
  }
  setCohort(
    environments: ReadonlyArray<ThreadMonitorEnvironment>,
    rows: ReadonlyArray<ThreadMonitorRow>,
    counts: ThreadMonitorCounts,
  ) {
    this.environments = environments.filter((env) => this.fence.accepts(env));
    if (this.viewState.editor)
      this.retainedEditorRow =
        this.rows.find(
          (row) => monitorThreadKey(row) === monitorThreadKey(this.viewState.editor!.ref),
        ) ?? this.retainedEditorRow;
    this.rows = rows.filter((row) => this.fence.accepts(row));
    if (
      this.retainedEditorRow &&
      !this.rows.some((row) => monitorThreadKey(row) === monitorThreadKey(this.retainedEditorRow!))
    ) {
      const completed =
        this.retainedEditorRow.completedAt === null
          ? NaN
          : Date.parse(this.retainedEditorRow.completedAt);
      if (
        this.retainedEditorRow.group !== "done" ||
        !Number.isFinite(completed) ||
        Date.now() - completed < THREAD_MONITOR_LIMITS.doneWindowMs
      )
        this.retainedEditorRow = null;
    }
    this.counts = counts;
    this.snapshotRevision++;
    this.changed();
  }
  visibleRows(): ReadonlyArray<ThreadMonitorRow> {
    const chosen = new Set(this.materialized);
    for (const group of GROUPS) {
      for (const row of this.rows
        .filter((entry) => entry.group === group)
        .slice(0, this.caps[group]))
        chosen.add(monitorThreadKey(row));
    }
    for (const key of chosen) this.materialized.add(key);
    const result = this.rows.filter((row) => chosen.has(monitorThreadKey(row)));
    if (
      this.retainedEditorRow &&
      this.viewState.editor &&
      monitorThreadKey(this.retainedEditorRow) === monitorThreadKey(this.viewState.editor.ref) &&
      this.fence.accepts(this.retainedEditorRow) &&
      !result.some((row) => monitorThreadKey(row) === monitorThreadKey(this.retainedEditorRow!))
    )
      result.push(this.retainedEditorRow);
    return result;
  }
  more(group: ThreadMonitorGroup) {
    this.caps[group] += 50;
    this.snapshotRevision++;
    this.changed();
  }
  pendingThreads(environmentId: EnvironmentId): ReadonlySet<ThreadId> {
    return new Set(
      [...this.results.values()]
        .filter(
          (result) =>
            result.environmentId === environmentId &&
            (result.status === "pending" || result.status === "unknown"),
        )
        .map((result) => result.threadId),
    );
  }
  draft(
    ref: ThreadMonitorRef,
    requestId: ThreadMonitorDraft["requestId"] = null,
  ): ThreadMonitorDraft {
    return (
      this.drafts.get(monitorDraftKey(ref, requestId)) ?? {
        ...refOf(ref),
        revision: 0,
        kind: requestId === null ? "reply" : "decision",
        requestId,
        text: "",
        answers: {},
      }
    );
  }
  edit(draft: ThreadMonitorDraft): boolean {
    if (!this.fence.accepts(draft) || !this.hasThread(draft)) return false;
    if ((draft.kind === "reply") !== (draft.requestId === null)) return false;
    const pending = this.pendingDecisionRequests.get(monitorThreadKey(draft));
    if (draft.requestId !== null && pending && !pending.has(draft.requestId)) return false;
    const current = this.drafts.get(draftKey(draft));
    if (draft.revision < (current?.revision ?? 0)) return false;
    if (
      draft.revision === (current?.revision ?? 0) &&
      this.clearedRevisions.get(draftKey(draft)) !== draft.revision - 1
    )
      return false;
    this.clearedRevisions.delete(draftKey(draft));
    this.drafts.set(draftKey(draft), draft);
    this.changed();
    return true;
  }
  hasThread(ref: ThreadMonitorRef) {
    return (
      (this.retainedEditorRow !== null &&
        monitorThreadKey(this.retainedEditorRow) === monitorThreadKey(ref)) ||
      [...this.drafts.values()].some(
        (draft) => monitorThreadKey(draft) === monitorThreadKey(ref),
      ) ||
      this.rows.some((row) => monitorThreadKey(row) === monitorThreadKey(ref)) ||
      (this.selection !== null && monitorThreadKey(this.selection) === monitorThreadKey(ref))
    );
  }
  select(selection: MonitorSelection | null) {
    this.detailRelease?.();
    this.detailRelease = null;
    this.selection = selection;
    this.detail = null;
    this.changed();
  }
  bindDetailRelease(release: () => void) {
    this.detailRelease = release;
    return () => {
      if (this.detailRelease === release) this.detailRelease = null;
      release();
    };
  }
  setDetail(detail: ThreadMonitorDetail, pendingRequestIds?: ReadonlySet<RuntimeRequestId>) {
    if (
      !this.fence.accepts(detail) ||
      this.selection?.selectionId !== detail.selectionId ||
      monitorThreadKey(this.selection) !== monitorThreadKey(detail)
    )
      return;
    this.detail = detail;
    if (pendingRequestIds && detail.unavailableReason === null) {
      const threadKey = monitorThreadKey(detail);
      this.pendingDecisionRequests.set(threadKey, new Set(pendingRequestIds));
      for (const [key, draft] of this.drafts) {
        if (
          draft.requestId !== null &&
          monitorThreadKey(draft) === threadKey &&
          !pendingRequestIds.has(draft.requestId)
        ) {
          this.drafts.delete(key);
          this.clearedRevisions.delete(key);
        }
      }
    }
    this.changed();
  }
  recordIdentity(action: ThreadMonitorAction) {
    this.identities.set(action.actionId, {
      ...refOf(action),
      type: action.type,
      actionId: action.actionId,
      commandId: action.commandId,
      ...("messageId" in action && action.messageId !== undefined
        ? { messageId: action.messageId }
        : {}),
      ...("requestId" in action ? { requestId: action.requestId } : {}),
      ...("draftRevision" in action && action.draftRevision !== undefined
        ? { draftRevision: action.draftRevision }
        : {}),
      ...(action.type === "restore" ? { settleCommandId: action.settleCommandId } : {}),
    });
  }
  result(result: ThreadMonitorActionResult): ThreadMonitorDraft | null {
    if (!this.fence.accepts(result)) return null;
    const known = this.identities.get(result.actionId);
    if (
      known &&
      (known.commandId !== result.commandId || monitorThreadKey(known) !== monitorThreadKey(result))
    )
      return null;
    this.results.set(result.actionId, result);
    let cleared: ThreadMonitorDraft | null = null;
    const identity = this.identities.get(result.actionId);
    if (result.status === "accepted" && result.submittedDraftRevision !== undefined && identity) {
      const key = monitorDraftKey(
        result,
        identity.type === "respondToUserInput" ? (identity.requestId ?? null) : null,
      );
      const current = this.drafts.get(key);
      if (current) {
        const next = clearAcceptedMonitorDraft(current, result.submittedDraftRevision);
        if (next !== current) {
          this.drafts.set(key, next);
          this.clearedRevisions.set(key, result.submittedDraftRevision);
          cleared = next;
        }
      }
    }
    this.changed();
    return cleared;
  }
  retire(environmentId: EnvironmentId): Promise<void> {
    const running = this.invalidations.get(environmentId);
    if (running) return running;
    const lifetime = this.fence.retire(environmentId);
    if (!lifetime) return Promise.resolve();
    this.retiredAwaitingCatalog.add(environmentId);
    this.viewState = clearedMonitorViewState(this.viewState, lifetime);
    if (this.retainedEditorRow?.environmentId === environmentId) this.retainedEditorRow = null;
    this.environments = this.environments.filter((env) => env.environmentId !== environmentId);
    this.rows = this.rows.filter((row) => row.environmentId !== environmentId);
    this.counts = emptyCounts();
    for (const row of this.rows)
      this.counts = { ...this.counts, [row.group]: this.counts[row.group] + 1 };
    for (const [key, draft] of this.drafts)
      if (draft.environmentId === environmentId) {
        this.drafts.delete(key);
        this.clearedRevisions.delete(key);
      }
    for (const [key, result] of this.results)
      if (result.environmentId === environmentId) this.results.delete(key);
    for (const [key, identity] of this.identities)
      if (identity.environmentId === environmentId) this.identities.delete(key);
    for (const key of this.pendingDecisionRequests.keys())
      if ((JSON.parse(key) as string[])[0] === environmentId)
        this.pendingDecisionRequests.delete(key);
    for (const key of this.materialized)
      if ((JSON.parse(key) as string[])[0] === environmentId) this.materialized.delete(key);
    if (this.selection?.environmentId === environmentId) this.select(null);
    this.snapshotRevision++;
    this.changed();
    const frame = { ...lifetime, type: "invalidateEnvironment" as const, requestId: randomUUID() };
    const invalidate = this.invalidator;
    const operation = (async () => {
      if (!invalidate) return;
      const ack = await invalidate(frame);
      if (ack.requestId !== frame.requestId || !sameMonitorLifetime(ack, frame))
        throw new Error("Monitor invalidation was not acknowledged for the retired lifetime");
    })();
    this.invalidations.set(environmentId, operation);
    void operation.then(
      () => {
        this.invalidations.delete(environmentId);
        this.changed();
      },
      () => {
        /* Keep registration fenced after failed native retirement. */
      },
    );
    return operation;
  }
}

export const monitorStore = new MonitorStore(randomUUID());
export const clearMonitorEnvironment = (environmentId: EnvironmentId) =>
  monitorStore.retire(environmentId);
