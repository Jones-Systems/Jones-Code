import type { ThreadContextRecord } from "@t3tools/contracts";
import type { ThreadReferenceExcerpt } from "@t3tools/contracts/jones/threadReferenceExcerpt";
import { captureThreadReferenceExcerpt } from "@t3tools/client-runtime/jones/threadReferenceExcerpt";
import {
  composerTargetKey,
  useComposerDraftStore,
  type ComposerThreadTarget,
} from "../../composerDraftStore";
import { pendingDraftWork } from "../../components/chat/pendingDraftWork";
import {
  reserveQuestionExcerptPreparation,
  type questionAttachmentDraftId,
} from "../../questionAttachments";

type QuestionTarget = ReturnType<typeof questionAttachmentDraftId>;

export function addThreadContextsWithCapture(input: {
  target: ComposerThreadTarget;
  records: ReadonlyArray<ThreadContextRecord>;
  appendReference?: boolean;
  questionTarget?: QuestionTarget | null | undefined;
  isCurrent: () => boolean;
  request: (record: ThreadContextRecord) => Promise<unknown>;
}): void {
  const store = useComposerDraftStore.getState();
  const known = new Set(
    store.getComposerDraft(input.target)?.threadContexts.map((r) => r.contextId),
  );
  const added = input.records.filter((record) => {
    if (known.has(record.contextId)) return false;
    known.add(record.contextId);
    return true;
  });
  store.addThreadContexts(
    input.target,
    added,
    input.appendReference === undefined ? {} : { appendReference: input.appendReference },
  );
  const key = composerTargetKey(input.target);
  for (const record of added) {
    if (record.excerpt) continue;
    let present = true;
    const unsubscribe = useComposerDraftStore.subscribe((state) => {
      present &&=
        state.draftsByThreadKey[key]?.threadContexts.some(
          (current) =>
            current.contextId === record.contextId &&
            current.environmentId === record.environmentId &&
            current.threadId === record.threadId,
        ) === true;
    });
    pendingDraftWork.begin(key);
    const releaseQuestion = input.questionTarget
      ? reserveQuestionExcerptPreparation(input.questionTarget)
      : undefined;
    void captureThreadReferenceExcerpt({
      threadId: record.threadId,
      request: () => input.request(record),
    })
      .then((excerpt: ThreadReferenceExcerpt) => {
        if (!present || !input.isCurrent()) return;
        useComposerDraftStore.setState((state) => {
          const draft = state.draftsByThreadKey[key];
          // The subscription invalidates removal even if the same id is added again before capture ends.
          if (!draft || !present) return state;
          return {
            draftsByThreadKey: {
              ...state.draftsByThreadKey,
              [key]: {
                ...draft,
                threadContexts: draft.threadContexts.map((current) =>
                  current.contextId === record.contextId && !current.excerpt
                    ? { ...current, excerpt }
                    : current,
                ),
              },
            },
          };
        });
      })
      .finally(() => {
        unsubscribe();
        pendingDraftWork.end(key);
        releaseQuestion?.();
      });
  }
}
