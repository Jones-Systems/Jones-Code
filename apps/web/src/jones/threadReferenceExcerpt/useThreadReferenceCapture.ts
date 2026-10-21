import { useLayoutEffect, useRef } from "react";
import type { ThreadContextRecord } from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { composerTargetKey, type ComposerThreadTarget } from "../../composerDraftStore";
import { readPreparedConnection } from "../../state/session";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useAtomCommand } from "../../state/use-atom-command";
import type { questionAttachmentDraftId } from "../../questionAttachments";
import { addThreadContextsWithCapture } from "./capture";

export function useThreadReferenceCapture(
  target: ComposerThreadTarget,
  questionTarget?: ReturnType<typeof questionAttachmentDraftId> | null,
) {
  const key = composerTargetKey(target);
  const generation = useRef<{ key: string } | null>(null);
  useLayoutEffect(() => {
    generation.current = { key };
    return () => {
      generation.current = null;
    };
  }, [key]);
  const request = useAtomCommand(orchestrationEnvironment.v2.threadReferenceExcerpt, {
    reportFailure: false,
    reportDefect: false,
  });
  return (records: ReadonlyArray<ThreadContextRecord>, options?: { appendReference?: boolean }) => {
    const started = generation.current;
    addThreadContextsWithCapture({
      target,
      records,
      ...options,
      questionTarget,
      isCurrent: () => started !== null && generation.current === started,
      request: async (record) => {
        if (!readPreparedConnection(record.environmentId)) {
          throw Object.assign(new Error("Source environment is disconnected"), {
            _tag: "EnvironmentRpcUnavailableError",
          });
        }
        const result = await request({
          environmentId: record.environmentId,
          input: { threadId: record.threadId },
        });
        if (result._tag !== "Success") throw squashAtomCommandFailure(result);
        return result.value;
      },
    });
  };
}
