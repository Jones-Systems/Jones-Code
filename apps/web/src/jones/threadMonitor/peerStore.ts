import { randomUUID } from "../../lib/utils";
import {
  acceptMonitorChunk,
  monitorDraftKey,
  monitorLifetimeKey,
  monitorThreadKey,
  type MonitorChunkState,
} from "@t3tools/client-runtime/jones/thread-monitor";
import {
  DESKTOP_THREAD_MONITOR_PROTOCOL,
  decodeThreadMonitorFrame,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import type {
  ThreadMonitorAction,
  ThreadMonitorActionResult,
  ThreadMonitorDetail,
  ThreadMonitorDraft,
  ThreadMonitorEnvironment,
  ThreadMonitorGroup,
  ThreadMonitorLifetime,
  ThreadMonitorPeerIntent,
  ThreadMonitorPublisherFrame,
  ThreadMonitorRow,
  ThreadMonitorViewState,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { emptyCounts, refOf, type MonitorSelection, type MonitorActionIdentity } from "./store";

import { clearedMonitorViewState } from "./viewState";

export class MonitorPeerStore {
  generation: string | null = null;
  revision = 0;
  viewRevision = -1;
  environments = new Map<string, ThreadMonitorEnvironment>();
  rows = new Map<string, ThreadMonitorRow>();
  readonly retainedRows = new Map<string, ThreadMonitorRow>();
  drafts = new Map<string, ThreadMonitorDraft>();
  results = new Map<string, ThreadMonitorActionResult>();
  actions = new Map<string, MonitorActionIdentity>();
  counts = emptyCounts();
  countsReady = false;
  selection: MonitorSelection | null = null;
  detail: ThreadMonitorDetail | null = null;
  error: string | null = null;
  viewState: ThreadMonitorViewState | null = null;
  viewStateApplyVersion = 0;
  authoritativeViewState: ThreadMonitorViewState | null = null;
  expanded = false;
  collapsed: ThreadMonitorViewState["collapsed"] = {
    needsYou: false,
    working: false,
    waiting: false,
    done: false,
  };
  quick: string | null = null;
  private replayDetail: ThreadMonitorDetail | null = null;
  private retired = new Set<string>();
  private assembly: MonitorChunkState | null = null;
  private requestedRestoreIdentityReplay = false;
  private listeners = new Set<() => void>();
  private snapshot: ReturnType<MonitorPeerStore["readSnapshot"]> | null = null;
  constructor(readonly dispatch: (intent: ThreadMonitorPeerIntent) => Promise<void>) {}
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  getRevision = () => this.revision;
  private readSnapshot() {
    return {
      rows: new Map(this.rows),
      retainedRows: new Map(this.retainedRows),
      environments: new Map(this.environments),
      drafts: new Map(this.drafts),
      results: new Map(this.results),
      actions: new Map(this.actions),
      counts: this.counts,
      countsReady: this.countsReady,
      selection: this.selection,
      detail: this.detail,
      error: this.error,
      expanded: this.expanded,
      collapsed: this.collapsed,
      quick: this.quick,
      authoritativeViewState: this.authoritativeViewState,
    };
  }
  getSnapshot = () => (this.snapshot ??= this.readSnapshot());
  changed() {
    for (const [key, row] of this.rows) this.retainedRows.set(key, row);
    const selectionKey = this.selection ? monitorThreadKey(this.selection) : null;
    const resultKeys = new Set([...this.results.values()].map(monitorThreadKey));
    for (const [key, row] of this.retainedRows)
      if (
        !this.live(row) ||
        (!this.rows.has(key) && key !== this.quick && key !== selectionKey && !resultKeys.has(key))
      )
        this.retainedRows.delete(key);
    if (this.quick !== null && !this.retainedRows.has(this.quick)) this.quick = null;
    this.revision++;
    this.snapshot = this.readSnapshot();
    for (const listener of this.listeners) listener();
  }
  setQuick(key: string | null) {
    if (key === this.quick) return;
    const row = key === null ? null : this.retainedRows.get(key);
    if (key !== null && (!row || !this.live(row))) return;
    this.quick = key;
    this.changed();
  }
  toggleGroup(group: ThreadMonitorGroup) {
    this.collapsed = { ...this.collapsed, [group]: !this.collapsed[group] };
    this.changed();
  }
  private publishExpanded() {
    void this.dispatch({ type: "setExpanded", expanded: this.expanded }).catch(() => {
      this.error = "The native monitor window is unavailable";
      this.changed();
    });
  }
  setExpanded(expanded: boolean) {
    if (expanded === this.expanded) return;
    this.expanded = expanded;
    this.publishExpanded();
    this.changed();
  }
  private applyViewState() {
    const state = this.viewState;
    this.authoritativeViewState = state;
    this.viewStateApplyVersion++;
    this.collapsed = state?.collapsed ?? {
      needsYou: false,
      working: false,
      waiting: false,
      done: false,
    };
    const editor = state?.editor;
    this.quick = editor?.quick && this.live(editor.ref) ? monitorThreadKey(editor.ref) : null;
    if (editor && this.live(editor.ref) && (editor.focused || !editor.quick)) {
      this.expanded = true;
      this.publishExpanded();
    }
  }
  accepts(ref: ThreadMonitorLifetime) {
    return (
      ref.publisherGeneration === this.generation && !this.retired.has(monitorLifetimeKey(ref))
    );
  }
  live(ref: ThreadMonitorLifetime) {
    const env = this.environments.get(ref.environmentId);
    return this.accepts(ref) && env?.environmentEpoch === ref.environmentEpoch;
  }
  receive(frame: ThreadMonitorPublisherFrame) {
    if (frame.type === "reset") {
      if (this.generation === frame.publisherGeneration) return;
      this.generation = frame.publisherGeneration;
      this.environments.clear();
      this.rows.clear();
      this.retainedRows.clear();
      this.drafts.clear();
      this.results.clear();
      this.actions.clear();
      this.retired.clear();
      this.assembly = null;
      this.requestedRestoreIdentityReplay = false;
      this.selection = null;
      this.detail = null;
      this.counts = emptyCounts();
      this.countsReady = false;
      this.viewRevision = -1;
      this.error = null;
      this.viewState = null;
      this.applyViewState();
      this.replayDetail = null;
      this.changed();
      return;
    }
    if (frame.type === "invalidateEnvironment") {
      if (frame.publisherGeneration !== this.generation) return;
      this.retired.add(monitorLifetimeKey(frame));
      this.assembly = null;
      if (this.viewState) {
        this.viewState = clearedMonitorViewState(this.viewState, frame);
        this.applyViewState();
      }
      this.replayDetail = null;
      const environment = this.environments.get(frame.environmentId);
      if (environment?.environmentEpoch === frame.environmentEpoch)
        this.environments.delete(frame.environmentId);
      for (const [key, row] of this.rows)
        if (monitorLifetimeKey(row) === monitorLifetimeKey(frame)) this.rows.delete(key);
      for (const [key, draft] of this.drafts)
        if (monitorLifetimeKey(draft) === monitorLifetimeKey(frame)) this.drafts.delete(key);
      for (const [key, result] of this.results)
        if (monitorLifetimeKey(result) === monitorLifetimeKey(frame)) this.results.delete(key);
      for (const [key, action] of this.actions)
        if (monitorLifetimeKey(action) === monitorLifetimeKey(frame)) this.actions.delete(key);
      if (this.selection && monitorLifetimeKey(this.selection) === monitorLifetimeKey(frame)) {
        this.selection = null;
        this.detail = null;
      }
      this.countsReady = false;
      this.counts = emptyCounts();
      for (const row of this.rows.values())
        this.counts = { ...this.counts, [row.group]: this.counts[row.group] + 1 };
      this.error = null;
      this.changed();
      return;
    }
    if (frame.type === "theme") return;
    if (frame.type === "viewState") {
      const state = frame.state;
      if (
        state.publisherGeneration !== this.generation ||
        (state.editor !== null && !this.live(state.editor.ref)) ||
        (this.viewState !== null && state.revision <= this.viewState.revision)
      )
        return;
      this.viewState = state;
      if (
        state.editor &&
        !state.editor.quick &&
        this.replayDetail &&
        monitorThreadKey(this.replayDetail) === monitorThreadKey(state.editor.ref)
      ) {
        this.selection = {
          ...refOf(this.replayDetail),
          selectionId: this.replayDetail.selectionId,
        };
        this.detail = this.replayDetail;
      }
      this.replayDetail = null;
      this.applyViewState();
      this.changed();
      return;
    }
    if (frame.type === "view") {
      if (frame.publisherGeneration !== this.generation || frame.revision <= this.viewRevision)
        return;
      if (!frame.replace && frame.chunkIndex === 0 && frame.revision !== this.viewRevision + 1) {
        this.assembly = null;
        void this.dispatch({ type: "requestSnapshot" });
        return;
      }
      const next = acceptMonitorChunk(this.assembly, frame, (ref) => this.accepts(ref));
      if (next === null) {
        this.assembly = null;
        void this.dispatch({ type: "requestSnapshot" });
        return;
      }
      this.assembly = next;
      if (!next.complete) return;
      const environments = frame.replace
        ? new Map<string, ThreadMonitorEnvironment>()
        : new Map(this.environments);
      const rows = frame.replace ? new Map<string, ThreadMonitorRow>() : new Map(this.rows);
      for (const chunk of next.chunks)
        for (const env of chunk.environments) environments.set(env.environmentId, env);
      for (const chunk of next.chunks) {
        for (const row of chunk.upserts) {
          if (environments.get(row.environmentId)?.environmentEpoch !== row.environmentEpoch) {
            this.assembly = null;
            void this.dispatch({ type: "requestSnapshot" });
            return;
          }
          rows.set(monitorThreadKey(row), row);
        }
        for (const ref of chunk.removed) rows.delete(monitorThreadKey(ref));
      }
      this.environments = environments;
      this.rows = rows;
      this.counts = frame.counts;
      this.countsReady = true;
      this.viewRevision = frame.revision;
      this.error = null;
      this.assembly = null;
      this.changed();
      if (!this.requestedRestoreIdentityReplay) {
        this.requestedRestoreIdentityReplay = true;
        // Older brokers reject this optional request; their ordinary ready replay remains usable.
        void this.dispatch({
          type: "requestSnapshot",
          restoreIdentityReplay: { publisherGeneration: frame.publisherGeneration },
        }).catch(() => undefined);
      }
      return;
    }
    if (frame.type === "draft") {
      if (!this.live(frame.draft)) return;
      const key = monitorDraftKey(frame.draft, frame.draft.requestId);
      if (frame.draft.revision > (this.drafts.get(key)?.revision ?? -1)) {
        this.drafts.set(key, frame.draft);
        this.changed();
      }
      return;
    }
    if (frame.type === "detail") {
      const selection = this.selection;
      if (this.live(frame.detail) && selection === null) {
        if (
          !this.replayDetail ||
          this.replayDetail.selectionId !== frame.detail.selectionId ||
          frame.detail.revision > this.replayDetail.revision
        )
          this.replayDetail = frame.detail;
        return;
      }
      if (
        !selection ||
        !this.live(frame.detail) ||
        frame.detail.selectionId !== selection.selectionId ||
        selection.environmentId !== frame.detail.environmentId ||
        selection.threadId !== frame.detail.threadId ||
        selection.environmentEpoch !== frame.detail.environmentEpoch ||
        (this.detail && frame.detail.revision <= this.detail.revision)
      )
        return;
      this.detail = frame.detail;
      this.changed();
      return;
    }
    if (frame.type === "actionResult" && this.live(frame)) {
      const prior = this.results.get(frame.actionId);
      if (
        prior &&
        (prior.commandId !== frame.commandId ||
          prior.environmentId !== frame.environmentId ||
          prior.threadId !== frame.threadId ||
          prior.environmentEpoch !== frame.environmentEpoch)
      )
        return;
      const identity = this.actions.get(frame.actionId);
      if (
        identity &&
        (identity.commandId !== frame.commandId ||
          monitorLifetimeKey(identity) !== monitorLifetimeKey(frame) ||
          monitorThreadKey(identity) !== monitorThreadKey(frame) ||
          (frame.restoreIdentity !== undefined &&
            (identity.type !== "restore" ||
              (identity.settleCommandId !== undefined &&
                identity.settleCommandId !== frame.restoreIdentity.settleCommandId))))
      )
        return;
      if (frame.restoreIdentity)
        this.actions.set(frame.actionId, {
          ...refOf(frame),
          type: frame.restoreIdentity.type,
          actionId: frame.actionId,
          commandId: frame.commandId,
          settleCommandId: frame.restoreIdentity.settleCommandId,
        });
      if (
        prior?.status === "accepted" ||
        prior?.status === "rejected" ||
        prior?.status === "unavailable" ||
        (prior?.status === "unknown" && frame.status === "pending")
      ) {
        if (frame.restoreIdentity) this.changed();
        return;
      }
      this.results.set(frame.actionId, frame);
      if (frame.status === "accepted" && frame.submittedDraftRevision !== undefined) {
        const action = this.actions.get(frame.actionId);
        const key = monitorDraftKey(
          frame,
          action?.type === "respondToUserInput" ? action.requestId : null,
        );
        const draft =
          action && (action.type === "send" || action.type === "respondToUserInput")
            ? this.drafts.get(key)
            : undefined;
        if (draft?.revision === frame.submittedDraftRevision)
          this.drafts.set(key, { ...draft, revision: draft.revision + 1, text: "", answers: {} });
      }
      this.changed();
    }
  }
  updateViewState(state: Omit<ThreadMonitorViewState, "revision">) {
    if (
      state.publisherGeneration !== this.generation ||
      (state.editor !== null && !this.live(state.editor.ref))
    )
      return;
    const previous = this.viewState;
    if (
      previous &&
      JSON.stringify({ ...previous, revision: 0 }) === JSON.stringify({ ...state, revision: 0 })
    )
      return;
    this.viewState = { ...state, revision: (previous?.revision ?? 0) + 1 };
    void this.dispatch({ type: "setViewState", state: this.viewState }).catch(() => {
      this.error = "View state synchronization is unavailable";
      this.changed();
    });
    this.changed();
  }
  draft(ref: ThreadMonitorRow, requestId: ThreadMonitorDraft["requestId"] = null) {
    return (
      this.drafts.get(monitorDraftKey(ref, requestId)) ?? {
        ...refOf(ref),
        revision: 0,
        kind: requestId === null ? ("reply" as const) : ("decision" as const),
        requestId,
        text: "",
        answers: {},
      }
    );
  }
  edit(draft: ThreadMonitorDraft) {
    if (!this.live(draft)) return;
    this.drafts.set(monitorDraftKey(draft, draft.requestId), draft);
    this.changed();
    void this.dispatch({ type: "editDraft", draft }).catch(() => {
      this.error = "Draft synchronization is unavailable. Your local text is kept.";
      this.changed();
    });
  }
  select(row: ThreadMonitorRow) {
    if (!this.live(row)) return;
    this.release();
    this.selection = { ...refOf(row), selectionId: randomUUID() };
    this.detail = null;
    this.changed();
    void this.dispatch({ ...this.selection, type: "selectDetail" }).catch(() => {
      this.error = "Thread detail is unavailable";
      this.changed();
    });
  }
  release() {
    if (this.selection)
      void this.dispatch({ ...this.selection, type: "releaseDetail" }).catch(() => undefined);
    this.selection = null;
    this.detail = null;
    this.changed();
  }
  action(action: ThreadMonitorAction) {
    if (!this.live(action)) return;
    try {
      decodeThreadMonitorFrame({ schemaVersion: DESKTOP_THREAD_MONITOR_PROTOCOL, payload: action });
    } catch {
      this.results.set(action.actionId, {
        ...refOf(action),
        type: "actionResult",
        actionId: action.actionId,
        commandId: action.commandId,
        status: "unavailable",
        reason:
          "This action exceeds the monitor’s supported bounds. Your draft is kept; open the thread in Jones Code.",
      });
      this.changed();
      return;
    }
    this.actions.set(action.actionId, {
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
    this.results.set(action.actionId, {
      ...refOf(action),
      type: "actionResult",
      actionId: action.actionId,
      commandId: action.commandId,
      status: "pending",
      reason: "Awaiting command acceptance",
    });
    this.changed();
    void this.dispatch(action).catch(() => {
      if (!this.live(action)) return;
      this.results.set(action.actionId, {
        ...refOf(action),
        type: "actionResult",
        actionId: action.actionId,
        commandId: action.commandId,
        status: "unknown",
        reason: "Delivery is unknown. Reconcile this exact command; do not resend.",
      });
      this.changed();
    });
  }
}
