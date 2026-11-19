import {
  personalThreadKey,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { threadContextRecord } from "@t3tools/shared/composerContextReferences";
import type { DragEvent } from "react";
import { useComposerDraftStore, type ComposerThreadTarget } from "../../composerDraftStore";
import { formatInlineContextReference } from "../../lib/composerContextReferences";
import { threadContextReference } from "../../lib/composerContextRecords";

export const workstreamDragType = "application/x-jones-workstream";
const overAttribute = "data-workstream-reference-over";

export interface WorkstreamReference {
  readonly name: string;
  readonly threads: ReadonlyArray<ScopedThreadRef & { readonly title: string }>;
}

export function workstreamReferenceMembers(
  state: PersonalWorkstreamsState,
  roster: readonly EnvironmentThreadShell[],
  id: string,
): WorkstreamReference["threads"] {
  return roster.flatMap((thread) =>
    thread.settledOverride !== "settled" &&
    thread.archivedAt == null &&
    state.assignmentsByThreadKey.get(personalThreadKey(thread))?.workstreamId === id
      ? [{ environmentId: thread.environmentId, threadId: thread.id, title: thread.title }]
      : [],
  );
}

let activeDrag: { source: HTMLElement; id: string; reference: WorkstreamReference } | null = null;
let over: HTMLElement | null = null;

function setOver(target: HTMLElement | null) {
  if (target === over) return;
  over?.removeAttribute(overAttribute);
  target?.setAttribute(overAttribute, "");
  over = target;
}

export function finishWorkstreamReferenceDrag() {
  activeDrag = null;
  setOver(null);
}

export function startWorkstreamReferenceDrag(
  event: DragEvent<HTMLDivElement>,
  id: string,
  reference: WorkstreamReference,
) {
  finishWorkstreamReferenceDrag();
  // The browser may expose transfer data outside the window. Keep the roster in this renderer.
  event.dataTransfer.setData(workstreamDragType, id);
  event.dataTransfer.effectAllowed = "copyMove";
  activeDrag = { source: event.currentTarget, id, reference };
}

export function workstreamReferenceDropHandlers(insert: (reference: WorkstreamReference) => void) {
  const accepts = (event: DragEvent<HTMLDivElement>) =>
    activeDrag?.source.isConnected &&
    event.dataTransfer.types.includes(workstreamDragType) &&
    event.target instanceof Element &&
    event.target.closest("[data-chat-column]") === event.currentTarget;

  return {
    onDragOverCapture(event: DragEvent<HTMLDivElement>) {
      if (!accepts(event)) return;
      event.preventDefault();
      event.stopPropagation();
      event.dataTransfer.dropEffect = "copy";
      setOver(event.currentTarget);
    },
    onDragLeaveCapture(event: DragEvent<HTMLDivElement>) {
      if (over !== event.currentTarget) return;
      if (event.relatedTarget instanceof Node && over.contains(event.relatedTarget)) return;
      setOver(null);
    },
    onDropCapture(event: DragEvent<HTMLDivElement>) {
      if (!accepts(event) || !activeDrag) return;
      if (event.dataTransfer.getData(workstreamDragType) !== activeDrag.id) return;
      event.preventDefault();
      event.stopPropagation();
      const { reference } = activeDrag;
      finishWorkstreamReferenceDrag();
      insert(reference);
    },
  };
}

export function insertWorkstreamReference({
  target,
  destination,
  reference,
  hasPrompt,
  insertAtEnd,
}: {
  target: ComposerThreadTarget;
  destination: ScopedThreadRef;
  reference: WorkstreamReference;
  hasPrompt: boolean;
  insertAtEnd: (text: string) => boolean;
}): boolean {
  const records = new Map(
    reference.threads
      .filter(
        (thread) =>
          thread.environmentId !== destination.environmentId ||
          thread.threadId !== destination.threadId,
      )
      .map((thread) => {
        const record = threadContextRecord(thread, thread.title);
        return [record.contextId, record] as const;
      }),
  );
  const mentions = Array.from(records.values(), (record) =>
    formatInlineContextReference(threadContextReference(record)),
  );
  const roster = mentions.length > 0 ? `\nUnsettled threads: ${mentions.join(" ")}` : "";
  if (!insertAtEnd(`${hasPrompt ? "\n\n" : ""}Workstream: ${reference.name}${roster}\n`)) {
    return false;
  }
  useComposerDraftStore.getState().addThreadContexts(target, Array.from(records.values()), {
    appendReference: false,
  });
  return true;
}
