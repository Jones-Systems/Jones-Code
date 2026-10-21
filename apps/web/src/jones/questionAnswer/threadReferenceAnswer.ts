import type { EnvironmentId, ThreadContextRecord } from "@t3tools/contracts";
import { replaceComposerContextReferences } from "@t3tools/shared/composerContextReferences";
import { projectThreadReferenceExcerpts } from "@t3tools/shared/jones/threadReferenceExcerpt";
import { formatThreadLink } from "@t3tools/shared/threadLinks";

export function projectPendingUserInputAnswer(
  answer: unknown,
  threadContexts: ReadonlyArray<ThreadContextRecord>,
  environmentId: EnvironmentId,
) {
  if (typeof answer !== "string" || threadContexts.length === 0) return answer;
  const recordsById = new Map(threadContexts.map((record) => [record.contextId, record]));
  const identity = (record: ThreadContextRecord, label: string) =>
    record.environmentId === environmentId
      ? formatThreadLink(record.threadId, label)
      : `${record.title} (threadId: ${record.threadId}; environmentId: ${record.environmentId}; ${record.excerpt?.status === "captured" ? "bounded point-in-time context attached" : "reference only, no history attached"}; unavailable through this environment's t3_thread_read).`;
  const referenced: ThreadContextRecord[] = [];
  const baseline = replaceComposerContextReferences(answer, (reference) => {
    const record = reference.kind === "thread" ? recordsById.get(reference.contextId) : undefined;
    if (!record) return reference.source;
    if (record.excerpt) referenced.push(record);
    return identity(record, reference.label);
  });
  const excerpts = projectThreadReferenceExcerpts(
    referenced.flatMap((record) => (record.excerpt ? [record.excerpt] : [])),
    baseline.length,
  );
  let index = 0;
  return replaceComposerContextReferences(answer, (reference) => {
    const record = reference.kind === "thread" ? recordsById.get(reference.contextId) : undefined;
    if (!record) return reference.source;
    return identity(record, reference.label) + (record.excerpt ? (excerpts[index++] ?? "") : "");
  });
}
