import { useQuietThreadShell } from "../../state/entities";
import { usePendingPreviewRelease } from "./previews";
import { useEffect } from "react";
import { useGT } from "gt-react";
import * as Option from "effect/Option";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { threadEnvironment, useEnvironmentThread } from "../../state/threads";
import { usePendingSends } from "./hooks";
import { pendingSends } from "./store";
import {
  composerDraftHasUserContent,
  DraftId,
  finalizePromotedDraftThreadByRef,
  settleAutoDraftLaunch,
  useComposerDraftStore,
} from "../../composerDraftStore";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { useStableCallback } from "../../hooks/useStableCallback";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { readSavedAutoLaunch } from "../accountRouting/autoLaunchRecovery";
import { acknowledgeAccountRouting } from "../accountRouting/composerAccountRouting";
import { bindRecoveredPendingTurn } from "./send";
import { presentThreadShellFromProjection } from "../../components/ChatView.logic";

function useRecoverAutoLaunches() {
  const gt = useGT();
  const drafts = useComposerDraftStore((state) => state.draftThreadsByThreadKey);
  const dispatchPrepared = useOrchestrationCommand(threadEnvironment.dispatchPreparedTurn, {
    reportFailure: false,
  });
  const dispatch = useStableCallback(dispatchPrepared);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- restore issued launch rows from durable drafts without dispatching any request
  useEffect(() => {
    for (const [key, session] of Object.entries(drafts)) {
      const record = session.pendingAutoLaunch;
      if (!record || record === "unavailable") continue;
      const { environmentId, threadId, messageId, createdAt } = record;
      if (pendingSends.get(environmentId, threadId, messageId)) continue;

      const draftId = DraftId.make(key);
      const threadRef = scopeThreadRef(environmentId, threadId);
      const prepared = readSavedAutoLaunch(record);
      const entryKey = pendingSends.admit(environmentId, threadId, {
        id: messageId,
        role: "user",
        text:
          prepared?.method === "orchestration.launchThread"
            ? prepared.payload.initialMessage.text
            : gt("Auto launch"),
        createdAt,
        updatedAt: createdAt,
        runId: null,
        streaming: false,
      });
      pendingSends.whenAccepted(environmentId, threadId, messageId, () => {
        acknowledgeAccountRouting(
          draftId,
          useComposerDraftStore.getState().getComposerDraft(draftId),
        );
        settleAutoDraftLaunch(draftId, record, true);
        finalizePromotedDraftThreadByRef(threadRef);
      });
      pendingSends.whenRejected(environmentId, threadId, messageId, (entry) => {
        const creationAccepted =
          entry.deliveryEvidence?.kind === "launch" &&
          entry.deliveryEvidence.creation.status === "accepted";
        const store = useComposerDraftStore.getState();
        if (record.draftText && !composerDraftHasUserContent(store.getComposerDraft(draftId)))
          store.setPrompt(draftId, record.draftText);
        if (creationAccepted) acknowledgeAccountRouting(draftId, store.getComposerDraft(draftId));
        settleAutoDraftLaunch(draftId, record, creationAccepted);
        if (creationAccepted) finalizePromotedDraftThreadByRef(threadRef);
      });
      if (prepared) {
        bindRecoveredPendingTurn({
          environmentId,
          threadId,
          messageId,
          key: entryKey,
          prepared,
          dispatch: async (input) => {
            const result = await dispatch({ environmentId, input });
            if (
              result._tag === "Success" &&
              "projection" in result.value &&
              result.value.threadId === threadId
            ) {
              const { projection } = result.value;
              pendingSends.attachThread(
                environmentId,
                threadId,
                presentThreadShellFromProjection(environmentId, threadId, projection, {
                  updatedAt: projection.updatedAt,
                  lastVisitedAt: projection.thread.lastVisitedAt,
                }),
              );
              pendingSends.confirmThreadCreation(environmentId, threadId, messageId);
            }
            return result;
          },
        });
      }
      pendingSends.restoreUncertain(entryKey);
    }
  }, [dispatch, drafts, gt]);
}

function ReconcileThread(props: { environmentId: EnvironmentId; threadId: ThreadId }) {
  const shell = useQuietThreadShell({
    environmentId: props.environmentId,
    threadId: props.threadId,
  });
  const state = useEnvironmentThread(
    shell ? props.environmentId : null,
    shell ? props.threadId : null,
  );
  const entries = usePendingSends();
  const projection = Option.getOrNull(state.data);
  usePendingPreviewRelease(props.environmentId, props.threadId, projection);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Reconcile delivery lifetime with authoritative subscriptions, including inactive routes.
  useEffect(() => {
    if (
      projection &&
      state.status === "live" &&
      entries.some(
        (entry) => entry.environmentId === props.environmentId && entry.threadId === props.threadId,
      )
    )
      pendingSends.reconcile(props.environmentId, props.threadId, projection, shell?.source);
  }, [props.environmentId, props.threadId, projection, state.status, shell, entries]);
  return null;
}
export function PendingSendsCoordinator() {
  useRecoverAutoLaunches();
  const entries = usePendingSends();
  const threads = new Map(
    entries.map((entry) => [JSON.stringify([entry.environmentId, entry.threadId]), entry]),
  );
  return (
    <>
      {[...threads].map(([key, entry]) => (
        <ReconcileThread key={key} environmentId={entry.environmentId} threadId={entry.threadId} />
      ))}
    </>
  );
}
