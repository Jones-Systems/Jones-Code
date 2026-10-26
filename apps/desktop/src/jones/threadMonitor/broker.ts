import {
  THREAD_MONITOR_LIMITS,
  threadMonitorFrameByteLength,
  type ThreadMonitorAction,
  type ThreadMonitorInvalidation,
  type ThreadMonitorInvalidationAck,
  type ThreadMonitorLifetime,
  type ThreadMonitorPeerIntent,
  type ThreadMonitorPublisherFrame,
  type ThreadMonitorRef,
  type ThreadMonitorViewChunk,
} from "@t3tools/contracts/jones/desktopThreadMonitor";

const key = (ref: ThreadMonitorRef) =>
  JSON.stringify([ref.environmentId, ref.environmentEpoch, ref.threadId]);
const sameLifetime = (a: ThreadMonitorLifetime, b: ThreadMonitorLifetime) =>
  a.publisherGeneration === b.publisherGeneration &&
  a.environmentId === b.environmentId &&
  a.environmentEpoch === b.environmentEpoch;
const actionTypes = new Set([
  "send",
  "reconcile",
  "settle",
  "restore",
  "open",
  "respondToApproval",
  "respondToUserInput",
  "usePlan",
]);
export const isMonitorAction = (value: ThreadMonitorPeerIntent): value is ThreadMonitorAction =>
  actionTypes.has(value.type);

export interface MonitorBrokerPorts {
  readonly peer: (frame: ThreadMonitorPublisherFrame) => void;
  readonly publisher: (intent: ThreadMonitorPeerIntent) => void;
  readonly recreatePeer: () => void;
  readonly revealMain: () => void;
  readonly timer: (callback: () => void, milliseconds: number) => () => void;
}

interface Assembly {
  readonly snapshotId: string;
  readonly revision: number;
  readonly chunkCount: number;
  readonly replace: boolean;
  readonly counts: string;
  readonly chunks: ThreadMonitorViewChunk[];
  bytes: number;
  readonly cancel: () => void;
}
interface PendingInvalidation {
  readonly frame: ThreadMonitorInvalidation;
  readonly finish: (ack: ThreadMonitorInvalidationAck) => void;
  readonly cancel: () => void;
}
interface Correlation {
  readonly identity: string;
  readonly ref: ThreadMonitorRef;
  readonly commandId: string;
  readonly type: ThreadMonitorAction["type"];
  readonly draftRevision: number | undefined;
  readonly messageId: string | undefined;
  cancel: (() => void) | undefined;
  terminal: boolean;
  terminalStatus: "accepted" | "rejected" | "unavailable" | null;
  unknown: boolean;
}

/** No text cache survives a peer. Replay comes from the sole authenticated publisher. */
export class ThreadMonitorBroker {
  private generation: string | null = null;
  private revision = -1;
  private synchronized = false;
  private assembly: Assembly | null = null;
  private readonly epochs = new Map<string, number>();
  private readonly retired = new Map<string, number>();
  private readonly members = new Set<string>();
  private readonly actions = new Map<string, Correlation>();
  private readonly drafts = new Map<string, number>();
  private selection: (ThreadMonitorRef & { readonly selectionId: string }) | null = null;
  private detailRevision = -1;
  private readonly invalidations = new Map<string, PendingInvalidation>();
  private peerReady = false;
  private requestingSnapshot = false;
  private replayAllowed = false;
  private viewStateRevision = -1;
  private readonly ports: MonitorBrokerPorts;

  constructor(ports: MonitorBrokerPorts) {
    this.ports = ports;
  }

  accepts(ref: ThreadMonitorLifetime): boolean {
    return (
      ref.publisherGeneration === this.generation &&
      this.epochs.get(ref.environmentId) === ref.environmentEpoch &&
      (this.retired.get(ref.environmentId) ?? -1) < ref.environmentEpoch
    );
  }

  private discardAssembly(): void {
    this.assembly?.cancel();
    this.assembly = null;
  }

  private recover(): void {
    this.discardAssembly();
    this.synchronized = false;
    this.replayAllowed = true;
    this.members.clear();
    if (this.generation && this.peerReady)
      this.ports.peer({ type: "reset", publisherGeneration: this.generation });
    if (!this.requestingSnapshot && this.generation) {
      this.requestingSnapshot = true;
      this.ports.publisher({ type: "requestSnapshot" });
    }
  }

  publisherLost(): void {
    this.discardAssembly();
    this.generation = null;
    this.synchronized = false;
    this.revision = -1;
    this.epochs.clear();
    this.retired.clear();
    this.members.clear();
    this.drafts.clear();
    this.selection = null;
    for (const action of this.actions.values()) action.cancel?.();
    this.actions.clear();
    this.finishInvalidations();
    this.peerReady = false;
    this.requestingSnapshot = false;
    this.replayAllowed = false;
    this.viewStateRevision = -1;
  }

  peerLost(): void {
    this.peerReady = false;
    // Destruction is the fail-closed alternative to acknowledging a purge.
    this.finishInvalidations();
  }

  private finishInvalidations(): void {
    for (const pending of this.invalidations.values()) {
      pending.cancel();
      pending.finish({ ...pending.frame, type: "environmentInvalidated" });
    }
    this.invalidations.clear();
  }

  ready(): void {
    this.peerReady = true;
    this.discardAssembly();
    this.synchronized = false;
    this.replayAllowed = true;
    if (this.generation) this.ports.peer({ type: "reset", publisherGeneration: this.generation });
    this.requestingSnapshot = false;
    this.ports.publisher({ type: "ready" });
  }

  publish(frame: ThreadMonitorPublisherFrame): void {
    if (frame.type === "reset") {
      if (this.generation !== null && this.generation !== frame.publisherGeneration)
        throw new Error("Publisher generation is bound to its document lifecycle");
      this.generation = frame.publisherGeneration;
      this.recover();
      return;
    }
    if (frame.type === "invalidateEnvironment")
      throw new Error("Invalidation requires the acknowledged control channel");
    if (frame.type === "view") {
      this.view(frame);
      return;
    }
    if (frame.type === "viewState") {
      const state = frame.state;
      if (
        state.publisherGeneration !== this.generation ||
        (state.editor && !this.accepts(state.editor.ref))
      )
        throw new Error("Stale monitor view state lifetime");
      if (state.revision < this.viewStateRevision) return;
      this.viewStateRevision = state.revision;
      if (this.peerReady) this.ports.peer(frame);
      return;
    }
    const ref =
      frame.type === "detail" ? frame.detail : frame.type === "draft" ? frame.draft : frame;
    if (frame.type === "theme") {
      if (frame.publisherGeneration !== this.generation)
        throw new Error("Stale publisher generation");
    } else {
      if (!("environmentId" in ref) || !this.accepts(ref))
        throw new Error("Retired monitor lifetime");
      if (frame.type === "detail") {
        if (
          !this.selection ||
          !sameLifetime(this.selection, frame.detail) ||
          this.selection.threadId !== frame.detail.threadId ||
          this.selection.selectionId !== frame.detail.selectionId ||
          frame.detail.revision < this.detailRevision
        )
          return;
        this.detailRevision = frame.detail.revision;
      }
      if (frame.type === "draft") {
        if ((frame.draft.kind === "reply") !== (frame.draft.requestId === null))
          throw new Error("Invalid draft request identity");
        const draftKey = JSON.stringify([
          key(frame.draft),
          frame.draft.kind,
          frame.draft.requestId,
        ]);
        if (frame.draft.revision < (this.drafts.get(draftKey) ?? -1)) return;
        if (!this.drafts.has(draftKey) && this.drafts.size >= THREAD_MONITOR_LIMITS.retiredIds)
          throw new Error("Monitor draft version ledger is full");
        this.drafts.set(draftKey, frame.draft.revision);
      }
      if (frame.type === "actionResult") {
        const correlation = this.actions.get(frame.actionId);
        if (
          !correlation ||
          !sameLifetime(correlation.ref, frame) ||
          correlation.ref.threadId !== frame.threadId ||
          correlation.commandId !== frame.commandId
        )
          return;
        if (correlation.terminal) {
          if (frame.status === correlation.terminalStatus && this.peerReady) this.ports.peer(frame);
          return;
        }
        if (correlation.unknown && frame.status === "pending") return;
        if (frame.status === "unknown") correlation.unknown = true;
        if (
          frame.submittedDraftRevision !== undefined &&
          frame.submittedDraftRevision !== correlation.draftRevision
        )
          throw new Error("Draft result version mismatch");
        if (
          frame.settleCommandId !== undefined &&
          (correlation.type !== "settle" || frame.settleCommandId !== correlation.commandId)
        )
          throw new Error("Settlement result identity mismatch");
        if (
          frame.status === "accepted" ||
          frame.status === "rejected" ||
          frame.status === "unavailable"
        ) {
          correlation.terminal = true;
          correlation.terminalStatus = frame.status;
          correlation.cancel?.();
          correlation.cancel = undefined;
        }
        if (frame.status === "accepted" && correlation.type === "open") this.ports.revealMain();
      }
    }
    if (this.peerReady) this.ports.peer(frame);
  }

  private view(frame: ThreadMonitorViewChunk): void {
    if (frame.publisherGeneration !== this.generation)
      throw new Error("Stale publisher generation");
    if (frame.chunkIndex === 0) {
      if (
        frame.revision < this.revision ||
        (frame.revision === this.revision && !(this.replayAllowed && frame.replace))
      )
        return;
      if (
        (!this.synchronized && !frame.replace) ||
        (this.synchronized && !frame.replace && frame.revision !== this.revision + 1)
      ) {
        this.recover();
        return;
      }
      this.discardAssembly();
      this.assembly = {
        snapshotId: frame.snapshotId,
        revision: frame.revision,
        chunkCount: frame.chunkCount,
        replace: frame.replace,
        counts: JSON.stringify(frame.counts),
        chunks: [],
        bytes: 0,
        cancel: this.ports.timer(() => this.recover(), 5_000),
      };
    }
    const pending = this.assembly;
    if (
      !pending ||
      pending.snapshotId !== frame.snapshotId ||
      pending.revision !== frame.revision ||
      pending.chunkCount !== frame.chunkCount ||
      pending.replace !== frame.replace ||
      pending.counts !== JSON.stringify(frame.counts) ||
      frame.chunkIndex !== pending.chunks.length
    ) {
      this.recover();
      return;
    }
    const bytes = threadMonitorFrameByteLength(frame);
    // Resource refusal requests a fresh bounded snapshot; it never exposes a prefix as a cohort.
    if (pending.chunks.length >= 1_024 || pending.bytes + bytes > 64 * 1_048_576) {
      this.recover();
      throw new Error("Monitor snapshot assembly resource limit");
    }
    if (
      [...frame.environments, ...frame.upserts, ...frame.removed].some(
        (ref) =>
          ref.publisherGeneration !== this.generation ||
          (this.retired.get(ref.environmentId) ?? -1) >= ref.environmentEpoch,
      )
    ) {
      this.recover();
      return;
    }
    pending.chunks.push(frame);
    pending.bytes += bytes;
    if (!frame.complete) return;

    // Only environment declarations from this authorized publisher may establish membership.
    const epochs = new Map(this.epochs);
    for (const chunk of pending.chunks)
      for (const environment of chunk.environments) {
        const current = epochs.get(environment.environmentId);
        if (current !== undefined && current !== environment.environmentEpoch) {
          this.recover();
          throw new Error("Environment epoch changed without retirement");
        }
        epochs.set(environment.environmentId, environment.environmentEpoch);
      }
    for (const chunk of pending.chunks)
      for (const ref of [...chunk.upserts, ...chunk.removed]) {
        if (epochs.get(ref.environmentId) !== ref.environmentEpoch) {
          this.recover();
          throw new Error("Unregistered environment lifetime");
        }
      }
    this.epochs.clear();
    for (const [id, epoch] of epochs) this.epochs.set(id, epoch);
    if (pending.replace) this.members.clear();
    for (const chunk of pending.chunks) {
      for (const row of chunk.upserts) this.members.add(key(row));
      for (const row of chunk.removed) this.members.delete(key(row));
    }
    this.revision = pending.revision;
    this.synchronized = true;
    this.requestingSnapshot = false;
    this.replayAllowed = false;
    this.discardAssembly();
    if (this.peerReady) for (const chunk of pending.chunks) this.ports.peer(chunk);
  }

  invalidate(frame: ThreadMonitorInvalidation): Promise<ThreadMonitorInvalidationAck> {
    if (frame.publisherGeneration !== this.generation)
      throw new Error("Stale invalidation generation");
    const prior = this.invalidations.get(frame.requestId);
    if (prior) throw new Error("Invalidation request is already pending");
    if (
      this.invalidations.size >= THREAD_MONITOR_LIMITS.retiredIds ||
      [...this.invalidations.values()].some((entry) => sameLifetime(entry.frame, frame))
    )
      throw new Error("Monitor retirement is already pending or its ledger is full");
    const current = this.epochs.get(frame.environmentId);
    const retired = this.retired.get(frame.environmentId) ?? -1;
    if (
      (current !== undefined && current !== frame.environmentEpoch) ||
      frame.environmentEpoch < retired
    )
      throw new Error("Invalidation does not name a current lifetime");
    if (
      !this.retired.has(frame.environmentId) &&
      this.retired.size >= THREAD_MONITOR_LIMITS.retiredIds
    )
      throw new Error("Monitor retirement ledger is full");
    // Tombstone and drop every payload before forwarding, including a hidden peer.
    this.retired.set(frame.environmentId, frame.environmentEpoch);
    this.epochs.delete(frame.environmentId);
    this.discardAssembly();
    for (const member of this.members)
      if ((JSON.parse(member) as unknown[])[0] === frame.environmentId) this.members.delete(member);
    for (const draft of this.drafts.keys())
      if ((JSON.parse(JSON.parse(draft)[0] as string) as unknown[])[0] === frame.environmentId)
        this.drafts.delete(draft);
    if (this.selection && sameLifetime(this.selection, frame)) this.selection = null;
    for (const correlation of this.actions.values())
      if (sameLifetime(correlation.ref, frame)) {
        correlation.cancel?.();
        correlation.cancel = undefined;
      }
    if (!this.peerReady) {
      this.ports.recreatePeer();
      return Promise.resolve({ ...frame, type: "environmentInvalidated" });
    }
    return new Promise((finish) => {
      const cancel = this.ports.timer(() => {
        // The host destroys the old webContents synchronously before this completes.
        this.ports.recreatePeer();
        this.peerLost();
      }, THREAD_MONITOR_LIMITS.invalidationTimeoutMs);
      this.invalidations.set(frame.requestId, { frame, finish, cancel });
      this.ports.peer(frame);
    });
  }

  intent(intent: ThreadMonitorPeerIntent): void {
    if (intent.type === "environmentInvalidated") {
      const pending = this.invalidations.get(intent.requestId);
      if (!pending || !sameLifetime(pending.frame, intent))
        throw new Error("Invalid invalidation acknowledgment");
      pending.cancel();
      this.invalidations.delete(intent.requestId);
      pending.finish(intent);
      return;
    }
    if (intent.type === "ready") {
      this.ready();
      return;
    }
    if (intent.type === "setExpanded" || intent.type === "hide")
      throw new Error("Native visibility intent must be handled by the host");
    if (intent.type === "requestSnapshot" || intent.type === "requestMore") {
      this.ports.publisher(intent);
      return;
    }
    if (intent.type === "setViewState") {
      const state = intent.state;
      if (
        state.publisherGeneration !== this.generation ||
        (state.editor && !this.accepts(state.editor.ref))
      )
        throw new Error("Retired monitor view state lifetime");
      // Main owns acceptance and the cache; an unaccepted peer update cannot advance its fence.
      if (state.revision <= this.viewStateRevision)
        throw new Error("Obsolete monitor view state revision");
      this.ports.publisher(intent);
      return;
    }
    const ref = intent.type === "editDraft" ? intent.draft : intent;
    if (!this.accepts(ref)) throw new Error("Retired monitor intent");
    if (!this.synchronized && intent.type !== "releaseDetail")
      throw new Error("Monitor is awaiting a complete snapshot");
    if (intent.type === "releaseDetail") {
      if (!this.selection) return;
      if (
        this.selection.selectionId !== intent.selectionId ||
        !sameLifetime(this.selection, intent) ||
        this.selection.threadId !== intent.threadId
      )
        throw new Error("Obsolete detail release");
      this.selection = null;
      this.detailRevision = -1;
    } else if (intent.type === "selectDetail") {
      if (!this.members.has(key(intent))) throw new Error("Unknown monitor row");
      this.selection = intent;
      this.detailRevision = -1;
    } else if (intent.type === "editDraft") {
      if ((intent.draft.kind === "reply") !== (intent.draft.requestId === null))
        throw new Error("Invalid draft request identity");
      const draftKey = JSON.stringify([
        key(intent.draft),
        intent.draft.kind,
        intent.draft.requestId,
      ]);
      if (intent.draft.revision <= (this.drafts.get(draftKey) ?? -1))
        throw new Error("Obsolete draft version");
      if (
        !this.members.has(key(intent.draft)) &&
        (!this.selection || key(this.selection) !== key(intent.draft))
      )
        throw new Error("Unknown draft target");
      // Main accepts and mirrors the version; a rejected edit cannot advance broker state.
    } else if (isMonitorAction(intent)) {
      const prior = this.actions.get(intent.actionId);
      // Identity deliberately excludes text/answers: neither enters the retained ledger.
      const identity = JSON.stringify([
        intent.type,
        key(intent),
        intent.commandId,
        "messageId" in intent ? intent.messageId : null,
        "requestId" in intent ? intent.requestId : null,
        "planId" in intent ? intent.planId : null,
        "draftRevision" in intent ? intent.draftRevision : null,
        "originalActionId" in intent ? intent.originalActionId : null,
        "originalCommandId" in intent ? intent.originalCommandId : null,
        "settleCommandId" in intent ? intent.settleCommandId : null,
        "decision" in intent ? intent.decision : null,
      ]);
      if (prior) {
        if (prior.identity !== identity) throw new Error("Conflicting monitor action identity");
        return;
      }
      const restoreAcceptedSettlement =
        intent.type === "restore" &&
        [...this.actions.values()].some(
          (entry) =>
            entry.type === "settle" &&
            entry.commandId === intent.settleCommandId &&
            entry.terminalStatus === "accepted" &&
            sameLifetime(entry.ref, intent) &&
            entry.ref.threadId === intent.threadId,
        );
      const reconcilesKnownOperation =
        intent.type === "reconcile" && this.actions.has(intent.originalActionId);
      if (
        !this.members.has(key(intent)) &&
        (!this.selection || key(this.selection) !== key(intent)) &&
        !restoreAcceptedSettlement &&
        !reconcilesKnownOperation
      )
        throw new Error("Unknown action target");
      if (this.actions.size >= THREAD_MONITOR_LIMITS.retiredIds)
        throw new Error("Monitor action ledger is full");
      if (intent.type === "reconcile") {
        const original = this.actions.get(intent.originalActionId);
        if (
          !original ||
          original.commandId !== intent.originalCommandId ||
          !sameLifetime(original.ref, intent) ||
          original.ref.threadId !== intent.threadId ||
          (intent.messageId !== undefined && original.messageId !== intent.messageId)
        )
          throw new Error("Unknown original operation");
      }
      const refOnly = {
        publisherGeneration: intent.publisherGeneration,
        environmentId: intent.environmentId,
        environmentEpoch: intent.environmentEpoch,
        threadId: intent.threadId,
      };
      const actionId = intent.actionId;
      const commandId = intent.commandId;
      const correlation: Correlation = {
        identity,
        ref: refOnly,
        commandId,
        type: intent.type,
        draftRevision: "draftRevision" in intent ? intent.draftRevision : undefined,
        messageId: "messageId" in intent ? intent.messageId : undefined,
        terminal: false,
        terminalStatus: null,
        unknown: false,
        cancel: undefined,
      };
      correlation.cancel = this.ports.timer(() => {
        correlation.cancel = undefined;
        correlation.unknown = true;
        if (this.peerReady && this.accepts(refOnly) && !correlation.terminal)
          this.ports.peer({
            ...refOnly,
            type: "actionResult",
            actionId,
            commandId,
            status: "unknown",
            reason:
              "Command acknowledgment is unavailable. Reconcile the original operation before retrying.",
            ...(correlation.draftRevision === undefined
              ? {}
              : { submittedDraftRevision: correlation.draftRevision }),
          });
      }, THREAD_MONITOR_LIMITS.actionTimeoutMs);
      this.actions.set(intent.actionId, correlation);
    }
    this.ports.publisher(intent);
  }

  dispose(): void {
    this.publisherLost();
  }
}
