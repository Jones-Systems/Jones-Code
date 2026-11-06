import {
  OrchestratorMcpFailure,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestratorMcpThreadReadResult,
  type OrchestratorMcpThreadTimelineItem,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { lastReply } from "./FinalText.ts";

const threadId = ThreadId.make("remote-thread");
const runId = RunId.make("task-run");
const item = (
  changes: Partial<OrchestratorMcpThreadTimelineItem> = {},
): OrchestratorMcpThreadTimelineItem => ({
  position: 0,
  visibility: "local",
  sourceThreadId: threadId,
  itemId: TurnItemId.make("reply"),
  runId,
  messageId: null,
  createdBy: null,
  creationSource: null,
  type: "assistant_message",
  status: "completed",
  title: null,
  text: "prefix",
  textTruncated: true,
  nextTextOffset: 6,
  updatedAt: "2026-10-22T00:00:00.000Z",
  ...changes,
});
const page = (
  items: ReadonlyArray<OrchestratorMcpThreadTimelineItem>,
  nextPosition: number | null = null,
  envelopeThreadId = threadId,
): OrchestratorMcpThreadReadResult => ({
  thread: {
    threadId: envelopeThreadId,
    projectId: ProjectId.make("remote-project"),
    title: "Remote task",
    createdBy: "user",
    creationSource: "web",
    status: "completed",
    latestRunId: runId,
    activeRunId: null,
    providerInstanceId: ProviderInstanceId.make("codex"),
    model: "synthetic-model",
    runtimeMode: "auto",
    interactionMode: "default",
    linkedPullRequest: null,
    titleRegeneration: null,
    branch: null,
    worktreePath: null,
    parentThreadId: null,
    relationshipToParent: null,
    runCount: 1,
    itemCount: items.length,
    pendingRequestCount: 0,
    archived: false,
    settled: false,
    settledAt: null,
    snoozed: false,
    snoozedUntil: null,
    createdAt: "2026-10-22T00:00:00.000Z",
    updatedAt: "2026-10-22T00:00:00.000Z",
  },
  recentRuns: [],
  items,
  nextPosition,
  hasMore: nextPosition !== null,
});
const readPages = (pages: ReadonlyArray<OrchestratorMcpThreadReadResult>) => {
  let index = 0;
  return () =>
    index < pages.length
      ? Effect.succeed(pages[index++]!)
      : Effect.fail(
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Fixture pages exhausted",
          }),
        );
};

it.effect.each([
  { name: "first empty page", pages: [page([], null, ThreadId.make("other-thread"))] },
  {
    name: "later timeline page",
    pages: [
      page([item({ textTruncated: false })], 0),
      page([], null, ThreadId.make("other-thread")),
    ],
  },
  {
    name: "text continuation",
    pages: [
      page([item()]),
      page([item({ text: "suffix", textTruncated: false })], null, ThreadId.make("other-thread")),
    ],
  },
])("rejects a wrong-thread envelope on the $name", ({ pages }) =>
  Effect.gen(function* () {
    const result = yield* lastReply(readPages(pages), threadId, runId).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(result.failure.code).toBe("orchestration_error");
  }),
);

it.effect("accepts inherited source items inside the requested thread envelope", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("ancestor-thread");
    const result = yield* lastReply(
      readPages([
        page([item({ visibility: "inherited", sourceThreadId })]),
        page([
          item({ visibility: "inherited", sourceThreadId, text: "suffix", textTruncated: false }),
        ]),
      ]),
      threadId,
      runId,
    );
    expect(result).toBe("prefixsuffix");
  }),
);

it.effect.each([
  { name: "missing continuation offset", pages: [page([item({ nextTextOffset: undefined })])] },
  { name: "null continuation offset", pages: [page([item({ nextTextOffset: null })])] },
  { name: "empty continuation page", pages: [page([item()]), page([])] },
  {
    name: "wrong continuation item",
    pages: [
      page([item()]),
      page([item({ itemId: TurnItemId.make("other"), text: "suffix", textTruncated: false })]),
    ],
  },
  {
    name: "wrong continuation run",
    pages: [
      page([item()]),
      page([item({ runId: RunId.make("later-run"), text: "suffix", textTruncated: false })]),
    ],
  },
  {
    name: "nonadvancing continuation",
    pages: [
      page([item()]),
      page([item({ text: "again", nextTextOffset: 6 })]),
      page([item({ text: "suffix", textTruncated: false })]),
    ],
  },
])("refuses partial final text after $name", ({ pages }) =>
  Effect.gen(function* () {
    const result = yield* lastReply(readPages(pages), threadId, runId).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(result.failure.code).toBe("orchestration_error");
  }),
);

it.effect("retrieves every continuation of the exact run's last reply", () =>
  Effect.gen(function* () {
    const requests: Array<Parameters<Parameters<typeof lastReply>[0]>[0]> = [];
    const read = readPages([
      page([item({ text: "earlier", textTruncated: false })], 0),
      page([
        item({ position: 1 }),
        item({
          itemId: TurnItemId.make("later"),
          runId: RunId.make("later-run"),
          text: "unrelated",
          textTruncated: false,
        }),
      ]),
      page([item({ position: 1, text: "-middle", nextTextOffset: 13 })]),
      page([item({ position: 1, text: "-end", textTruncated: false, nextTextOffset: null })]),
    ]);
    const result = yield* lastReply(
      (input) => {
        requests.push(input);
        return read();
      },
      threadId,
      runId,
    );
    expect(result).toBe("prefix-middle-end");
    expect(requests.map((request) => request.textOffset)).toEqual([undefined, undefined, 6, 13]);
    expect(requests.slice(2).every((request) => request.itemId === "reply")).toBe(true);
  }),
);

it.effect.each([true, false])("preserves exact-run error precedence when failed is %s", (failed) =>
  Effect.gen(function* () {
    const result = yield* lastReply(
      readPages([
        page([
          item({ text: "answer", textTruncated: false }),
          item({
            itemId: TurnItemId.make("error"),
            type: "error",
            text: "failure",
            textTruncated: false,
          }),
          item({
            itemId: TurnItemId.make("later-error"),
            runId: RunId.make("later-run"),
            type: "error",
            text: "unrelated",
            textTruncated: false,
          }),
        ]),
      ]),
      threadId,
      runId,
      failed,
    );
    expect(result).toBe(failed ? "failure" : "answer");
  }),
);

it.effect.each([
  { name: "missing timeline cursor", pages: [{ ...page([item()]), hasMore: true }] },
  {
    name: "nonadvancing timeline cursor",
    pages: [page([item()], 0), page([item()], 0), page([item({ textTruncated: false })])],
  },
  {
    name: "skipped text offset",
    pages: [
      page([item({ nextTextOffset: 9 })]),
      page([item({ text: "end", textTruncated: false })]),
    ],
  },
  {
    name: "empty final chunk",
    pages: [page([item()]), page([item({ text: "", textTruncated: false })])],
  },
  {
    name: "missing final text",
    pages: [page([item()]), page([item({ text: null, textTruncated: false })])],
  },
])("rejects $name without publishing a partial result", ({ pages }) =>
  Effect.gen(function* () {
    const result = yield* lastReply(readPages(pages), threadId, runId).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
  }),
);

it.effect("uses UTF-16 offsets when a reply contains a supplementary character", () =>
  Effect.gen(function* () {
    const result = yield* lastReply(
      readPages([
        page([item({ text: "🦊", nextTextOffset: 2 })]),
        page([item({ text: " done", textTruncated: false })]),
      ]),
      threadId,
      runId,
    );
    expect(result).toBe("🦊 done");
  }),
);
