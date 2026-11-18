import { threadContextRecord } from "@t3tools/shared/composerContextReferences";
import { useEffect } from "react";
import {
  type ComposerContextRecord,
  ComposerContextId,
  EnvironmentId,
  ThreadId,
} from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

import { act, render, stubGlobal } from "../../../test/render";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { formatInlineContextReference } from "../../lib/composerContextReferences";
import { rememberCopiedComposerAttachments } from "../../lib/copiedComposerAttachments";
import { pendingDraftWork } from "./pendingDraftWork";
import { useComposerContextImport } from "./useComposerContextImport";

vi.mock("~/state/use-atom-query-runner", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/state/use-atom-query-runner")>()),
  useAtomQueryRunner: () => async () => ({ _tag: "Success", value: { relativeUrl: "/file" } }),
}));
const capture = vi.hoisted(() =>
  vi.fn(async ({ input }: { input: { threadId: string } }) => ({
    _tag: "Success" as const,
    value: {
      threadId: input.threadId,
      excerpt: {
        status: "captured" as const,
        capturedAt: "2026-10-21T00:00:00.000Z",
        sourceSnapshotSequence: 12,
        earlierHistory: "omitted" as const,
        text: "Bounded source context",
      },
    },
  })),
);
vi.mock("../../state/use-atom-command", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/use-atom-command")>()),
  useAtomCommand: () => capture,
}));
vi.mock("~/state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("~/state/session")>()),
  readPreparedConnection: () => ({ httpBaseUrl: "http://localhost" }),
}));

type ImportInput = Parameters<typeof useComposerContextImport>[0];
type Importer = ReturnType<typeof useComposerContextImport>;

function Harness({ input, ready }: { input: ImportInput; ready: (value: Importer) => void }) {
  const importer = useComposerContextImport(input);
  useEffect(() => ready(importer));
  return null;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((fulfill) => {
    resolve = fulfill;
  });
  return { promise, resolve };
}

it.each([
  { kind: "file", edit: "delete" },
  { kind: "image", edit: "delete" },
  { kind: "file", edit: "undo" },
  { kind: "file", edit: "retain" },
  { kind: "file", edit: "switch" },
  { kind: "file", edit: "cached" },
  { kind: "file", edit: "question" },
  { kind: "image", edit: "question" },
  { kind: "image", edit: "annotation" },
  { kind: "image", edit: "delete-annotation" },
] as const)("settles a delayed $kind import after $edit", async ({ kind, edit }) => {
  const target = DraftId.make(`clipboard-${kind}-${edit}`);
  const store = useComposerDraftStore.getState();
  using _draft = { [Symbol.dispose]: () => store.clearComposerContent(target) };
  const sourceEnvironmentId = EnvironmentId.make("clipboard-source");
  rememberCopiedComposerAttachments(
    sourceEnvironmentId,
    edit === "cached"
      ? new Map([["source-attachment", new File(["notes"], "notes.txt")]])
      : new Map(),
  );
  using _copy = {
    [Symbol.dispose]: () => rememberCopiedComposerAttachments(sourceEnvironmentId, new Map()),
  };
  const promptRef = { current: "" };
  const downloaded = deferred<Response>();
  const requested = deferred<void>();
  const settled = deferred<void>();
  const input = {
    environmentId: EnvironmentId.make("clipboard-target"),
    linkedEnvironments: new Map(),
    activeThread: undefined,
    activeThreadId: null,
    composerDraftTarget: target,
    attachmentDraftTarget: target,
    attachmentTargetKey: target,
    attachmentTargetKeyRef: { current: target },
    questionAttachmentTarget: edit === "question" ? target : null,
    pendingUserInputs: [],
    composerContextRecords: new Map(),
    composerImages: [],
    composerFilesRef: { current: [] },

    promptRef,
    addComposerFilesToDraft: (files: Parameters<typeof store.addFiles>[1]) =>
      store.addFiles(target, files, { appendReference: edit !== "question" }),
  } satisfies ImportInput & { promptRef: { current: string } };
  let importer!: Importer;
  await using _screen = await render(
    <Harness
      input={input}
      ready={(value) => {
        importer = value;
      }}
    />,
  );
  const end = pendingDraftWork.end.bind(pendingDraftWork);
  using _settled = vi.spyOn(pendingDraftWork, "end").mockImplementation((key) => {
    end(key);
    settled.resolve();
  });
  using _fetch = stubGlobal("fetch", () => {
    requested.resolve();
    return downloaded.promise;
  });
  const contextId = ComposerContextId.make(`${kind}_source`);
  const attachmentFields = {
    version: 1 as const,
    contextId,
    label: "notes.txt",
    attachmentId: "source-attachment",
    name: "notes.txt",
    mimeType: kind === "file" ? "text/plain" : "image/png",
    sizeBytes: 5,
  };
  const attachment = (
    kind === "file"
      ? { ...attachmentFields, kind: "file" as const }
      : { ...attachmentFields, kind: "image" as const }
  ) satisfies ComposerContextRecord;
  const annotation = {
    version: 1,
    kind: "preview-annotation",
    contextId: ComposerContextId.make("preview-annotation_source"),
    label: "Page annotation",
    annotationId: "source",
    pageUrl: "http://localhost/page",
    pageTitle: "Page",
    comment: "Check this",
    targetSummary: "Page",
    styleChanges: [],
    screenshotContextId: contextId,
  } satisfies ComposerContextRecord;
  const annotated = edit === "annotation" || edit === "delete-annotation";
  let pastedPrompt = "";
  await act(async () => {
    const remapped = importer.importContextRecords(
      annotated ? [attachment, annotation] : [attachment],
      sourceEnvironmentId,
    );
    pastedPrompt = formatInlineContextReference({
      kind: annotated ? "preview-annotation" : kind,
      contextId: remapped.get(annotated ? annotation.contextId : contextId)!,
      label: annotated ? "Page annotation" : "notes.txt",
    });
    if (edit === "question") pastedPrompt = "Question answer";
    promptRef.current = pastedPrompt;
    store.setPrompt(target, pastedPrompt);
    if (edit !== "cached") await requested.promise;
  });
  if (edit !== "cached") expect(pendingDraftWork.has(target)).toBe(true);
  await act(() => {
    if (
      edit !== "annotation" &&
      edit !== "retain" &&
      edit !== "cached" &&
      edit !== "question" &&
      edit !== "switch"
    ) {
      promptRef.current = "Keep this edit";
      store.setPrompt(target, promptRef.current);
    }
    if (edit === "undo") {
      promptRef.current = pastedPrompt;
      store.setPrompt(target, pastedPrompt);
    }
    if (edit === "switch") input.attachmentTargetKeyRef.current = DraftId.make("other-draft");
  });
  await act(async () => {
    downloaded.resolve(new Response("notes", { headers: { "Content-Type": attachment.mimeType } }));
    await settled.promise;
  });
  expect(pendingDraftWork.has(target)).toBe(false);
  const draft = store.getComposerDraft(target)!;
  expect(draft.prompt).toBe(promptRef.current);
  const attachments = kind === "file" ? draft.files : draft.images;
  expect(attachments.map((file) => file.name)).toEqual(
    edit === "undo" ||
      edit === "retain" ||
      edit === "annotation" ||
      edit === "cached" ||
      edit === "question"
      ? ["notes.txt"]
      : [],
  );
});

it.each(["same", "other-environment"] as const)(
  "preserves thread identity across repeated pastes: %s",
  async (variant) => {
    const otherEnvironment = variant === "other-environment";
    const target = DraftId.make(`thread-collision-${variant}`);
    const store = useComposerDraftStore.getState();
    using _draft = { [Symbol.dispose]: () => store.clearComposerContent(target) };
    const environmentId = EnvironmentId.make("thread-source");
    const otherEnvironmentId = EnvironmentId.make("other-thread-source");
    const first = threadContextRecord(
      { environmentId, threadId: ThreadId.make("same-id") },
      "First",
    );
    const pasted = otherEnvironment
      ? threadContextRecord(
          { environmentId: otherEnvironmentId, threadId: first.threadId },
          "Second",
        )
      : first;
    store.addThreadContexts(target, [first], { appendReference: false });
    let importer!: Importer;
    await using _screen = await render(
      <Harness
        input={{
          environmentId,
          linkedEnvironments: new Map([[otherEnvironmentId, "Other machine"]]),
          activeThread: undefined,
          activeThreadId: null,
          composerDraftTarget: target,
          attachmentDraftTarget: target,
          attachmentTargetKey: target,
          attachmentTargetKeyRef: { current: target },
          questionAttachmentTarget: null,
          pendingUserInputs: [],
          composerContextRecords: new Map([[first.contextId, { kind: "thread", record: first }]]),
          composerImages: [],
          composerFilesRef: { current: [] },

          promptRef: { current: "" },
          addComposerFilesToDraft: () => [],
        }}
        ready={(value) => {
          importer = value;
        }}
      />,
    );
    let importedId: string | undefined;
    await act(() => {
      importedId = importer
        .importContextRecords([pasted], pasted.environmentId, target)
        .get(pasted.contextId);
    });
    const records = store.getComposerDraft(target)!.threadContexts;
    expect(records.find((record) => record.contextId === importedId)).toMatchObject({
      environmentId: pasted.environmentId,
      threadId: pasted.threadId,
    });
    expect(records).toHaveLength(otherEnvironment ? 2 : 1);
    expect(records[0]).toEqual(first);
    await act(() => {
      const repeated = importer.importContextRecords([pasted], pasted.environmentId, target);
      expect(repeated.get(pasted.contextId)).toBe(importedId);
    });
    expect(store.getComposerDraft(target)!.threadContexts).toEqual(records);
  },
);

function referenceInput(target: DraftId): ImportInput {
  return {
    environmentId: EnvironmentId.make("destination"),
    linkedEnvironments: new Map(),
    activeThread: undefined,
    activeThreadId: null,
    composerDraftTarget: target,
    attachmentDraftTarget: target,
    attachmentTargetKey: target,
    attachmentTargetKeyRef: { current: target },
    questionAttachmentTarget: null,
    pendingUserInputs: [],
    composerContextRecords: new Map(),
    composerImages: [],
    composerFilesRef: { current: [] },
    promptRef: { current: "ordinary draft" },
    addComposerFilesToDraft: () => {
      throw new Error("A thread reference must not become a file");
    },
  };
}

it("captures unlinked ordinary thread references from their source without transcript files", async () => {
  const target = DraftId.make("ordinary-foreign-reference");
  const store = useComposerDraftStore.getState();
  using _draft = { [Symbol.dispose]: () => store.clearComposerContent(target) };
  store.setPrompt(target, "ordinary draft");
  let importer!: Importer;
  await using _screen = await render(
    <Harness
      input={referenceInput(target)}
      ready={(value) => {
        importer = value;
      }}
    />,
  );
  const record = threadContextRecord(
    {
      environmentId: EnvironmentId.make("unlinked-source"),
      threadId: ThreadId.make("source-thread"),
    },
    "Remote work",
  );
  const callsBefore = capture.mock.calls.length;
  const fetch = vi.fn(() => {
    throw new Error("No transcript download");
  });
  using _fetch = stubGlobal("fetch", fetch);
  await act(async () => {
    importer.importContextRecords([record], record.environmentId);
  });
  expect(capture.mock.calls.slice(callsBefore)).toEqual(
    [{ environmentId: record.environmentId, input: { threadId: record.threadId } }].map((call) => [
      call,
    ]),
  );
  expect(store.getComposerDraft(target)?.threadContexts[0]).toMatchObject({
    environmentId: record.environmentId,
    threadId: record.threadId,
    excerpt: { status: "captured", text: "Bounded source context" },
  });
  expect(store.getComposerDraft(target)?.prompt).toBe("ordinary draft");
  expect(store.getComposerDraft(target)?.files).toEqual([]);
  expect(fetch).not.toHaveBeenCalled();
});

it("does not apply an ordinary reference capture after changing composer routes", async () => {
  const target = DraftId.make("ordinary-capture-old-route");
  const replacement = DraftId.make("ordinary-capture-new-route");
  const store = useComposerDraftStore.getState();
  using _drafts = {
    [Symbol.dispose]: () => {
      store.clearComposerContent(target);
      store.clearComposerContent(replacement);
    },
  };
  const result = deferred<Awaited<ReturnType<typeof capture>>>();
  capture.mockImplementationOnce(() => result.promise);
  let importer!: Importer;
  const ready = (value: Importer) => {
    importer = value;
  };
  await using screen = await render(<Harness input={referenceInput(target)} ready={ready} />);
  const record = threadContextRecord(
    { environmentId: EnvironmentId.make("source"), threadId: ThreadId.make("route-bound-thread") },
    "Remote work",
  );
  await act(() => {
    importer.importContextRecords([record], record.environmentId);
  });
  expect(pendingDraftWork.has(target)).toBe(true);
  await screen.rerender(<Harness input={referenceInput(replacement)} ready={ready} />);
  await act(async () =>
    result.resolve({
      _tag: "Success",
      value: {
        threadId: record.threadId,
        excerpt: {
          status: "captured",
          capturedAt: "2026-10-21T00:00:00.000Z",
          sourceSnapshotSequence: 12,
          earlierHistory: "omitted",
          text: "Late context",
        },
      },
    }),
  );
  expect(store.getComposerDraft(target)?.threadContexts[0]?.excerpt).toBeUndefined();
  expect(store.getComposerDraft(replacement)?.threadContexts ?? []).toEqual([]);
  expect(pendingDraftWork.has(target)).toBe(false);
});
