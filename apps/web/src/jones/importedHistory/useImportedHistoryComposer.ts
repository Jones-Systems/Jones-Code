import { useState } from "react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { resolveImportedHistoryReview } from "@t3tools/client-runtime/jones/imported-history/continuation";
import { importedHistoryCorrelationStorage } from "../../composerDraftStore";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  importedHistoryCommands,
  type PreparedImportedHistoryChoice,
} from "./ContinuationChoiceBanner";

export function useImportedHistoryComposer(
  environmentId: EnvironmentId,
  threadId: ThreadId | null,
) {
  const prepare = useAtomCommand(importedHistoryCommands.prepare, { reportFailure: false });
  const review = useAtomCommand(importedHistoryCommands.review, { reportFailure: false });
  const [choice, setChoice] = useState<{
    environmentId: EnvironmentId;
    threadId: ThreadId;
    prepared: PreparedImportedHistoryChoice | null;
    reason: string | null;
  } | null>(null);
  const refuse = (reason: string) => {
    if (threadId !== null) setChoice({ environmentId, threadId, prepared: null, reason });
    return true;
  };
  const blockSubmission = (multiple: boolean) => {
    if (threadId === null) return true;
    if (multiple) return refuse("Choose one model before reviewing imported history.");
    try {
      if (
        importedHistoryCorrelationStorage(scopeThreadRef(environmentId, threadId)).read() !== null
      )
        return refuse("Observe the saved choice before another submission.");
    } catch {
      return refuse("Imported history correlation storage is unavailable.");
    }
    return false;
  };
  const prepareReview = async (
    input: StartThreadTurnInput,
    draftIdentity: string,
    unchanged: () => boolean,
  ) => {
    try {
      const prepared = await prepare({ environmentId, input });
      if (prepared._tag !== "Success")
        throw new Error("Imported history preparation is unavailable.");
      const delivery = { type: "message" as const, command: prepared.value };
      const reviewed = await review({
        environmentId,
        input: { threadId: input.threadId, delivery },
      });
      const result = resolveImportedHistoryReview(
        reviewed._tag === "Success" ? reviewed.value : null,
      );
      if (unchanged())
        setChoice({
          environmentId,
          threadId: input.threadId,
          reason: result.reason,
          prepared:
            result.status === "available"
              ? { delivery, reviewedBasis: result.reviewedBasis, draftIdentity, unchanged }
              : null,
        });
    } catch (error) {
      if (unchanged())
        refuse(error instanceof Error ? error.message : "Imported history review is unavailable.");
    }
  };
  const current =
    choice?.environmentId === environmentId && choice.threadId === threadId ? choice : null;
  return {
    blockSubmission,
    prepareReview,
    banner: {
      prepared: current?.prepared ?? null,
      reason: current?.reason ?? null,
      onDismiss: () => setChoice(null),
    },
  };
}
