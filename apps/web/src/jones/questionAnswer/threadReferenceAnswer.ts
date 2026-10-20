import type { EnvironmentId, ThreadContextRecord } from "@t3tools/contracts";
import { replaceComposerContextReferences } from "@t3tools/shared/composerContextReferences";
import { formatThreadLink } from "@t3tools/shared/threadLinks";

export function projectPendingUserInputAnswer(
  answer: unknown,
  threadContexts: ReadonlyArray<ThreadContextRecord>,
  environmentId: EnvironmentId,
) {
  if (typeof answer !== "string" || threadContexts.length === 0) return answer;
  const recordsById = new Map(threadContexts.map((record) => [record.contextId, record]));
  return replaceComposerContextReferences(answer, (reference) => {
    const record = reference.kind === "thread" ? recordsById.get(reference.contextId) : undefined;
    if (!record) return reference.source;
    return record.environmentId === environmentId
      ? formatThreadLink(record.threadId, reference.label)
      : `${record.title} (threadId: ${record.threadId}; environmentId: ${record.environmentId}; reference only, no history attached; unavailable through this environment's t3_thread_read).`;
  });
}
