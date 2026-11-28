import {
  captureExplicitComposerModelPick,
  consumeSubmittedModelPick,
} from "../jones/threadConfiguration/modelSelection";
import {
  decodeImportedHistoryCorrelation,
  encodeImportedHistoryCorrelation,
  type ImportedHistoryCorrelation,
  type ImportedHistoryCorrelationStorage,
} from "@t3tools/client-runtime/jones/imported-history/continuation";
import { useAtomValue } from "@effect/atom-react";
import { msg } from "gt-react-native";
import { m } from "../i18n/messages";
import {
  EnvironmentId as EnvironmentIdSchema,
  ModelSelection as ModelSelectionSchema,
  ComposerContextId,
  ComposerContextRecord,
  COMPOSER_CONTEXT_MAX_RECORDS,
  ForwardCompatibleArray,
  OrchestrationMessageContext,
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  ProjectId as ProjectIdSchema,
  ProviderInteractionMode as ProviderInteractionModeSchema,
  ProviderOptionSelection as ProviderOptionSelectionSchema,
  RuntimeMode as RuntimeModeSchema,
  type EnvironmentId,
  type ModelSelection,
  type ProjectId,
  type ProviderInteractionMode,
  type ProviderOptionSelection,
  type RuntimeMode,
  type ScopedThreadRef,
  type ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { useEffect, useState } from "react";
import { AsyncResult, Atom, type AtomRegistry } from "effect/reactivity";
import { parseScopedThreadKey, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type {
  EnvironmentShellState,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import {
  captureComposerModelSelectionSnapshot,
  composerModelSelectionSnapshotIsCurrent,
  recordComposerModelSelectionInteraction,
} from "../jones/threadConfiguration/outboxSelection";

import { writeFileAtomically } from "../lib/atomic-file";
import { createComposerContextHistory, referencedComposerContext } from "../lib/composerContext";
import { isQueuedEditDraftKey } from "./queued-edit-draft-key";
import {
  collectComposerContextReferences,
  formatComposerContextReference,
  sanitizeComposerContextLabel,
  replaceComposerContextReferences,
} from "@t3tools/shared/composerContextReferences";
import { imageMimeType } from "@t3tools/shared/image";
import { videoMimeType } from "@t3tools/shared/video";
import { DraftComposerAttachmentSchema } from "../lib/composer-image-schema";
import {
  composerAttachmentFileReferenceKey,
  isComposerAttachmentFileRetained,
} from "../lib/composerAttachmentFiles";
import {
  registerComposerAttachmentUnusedHandler,
  retainComposerAttachmentFileForPreview,
} from "../lib/composerAttachmentPreviewRetention";
import type { DraftComposerAttachment, FileBackedComposerAttachment } from "../lib/composerImages";
import { SerializedAsyncQueue } from "../lib/serialized-async-queue";
import { appAtomRegistry } from "./atom-registry";
import {
  abortComposerDraftOperations,
  abortComposerDraftOperationsWhere,
} from "./composerDraftOperations";
import {
  isNewTaskDraftKey,
  newTaskDraftKey,
  parseLegacyNewTaskDraftKey,
} from "./new-task-draft-key";
import {
  decodeQueuedThreadMessage,
  encodeQueuedThreadMessage,
  QueuedThreadMessageSchema,
  type QueuedThreadMessage,
} from "./thread-outbox-model";
import { flushThreadOutbox, threadOutboxManager } from "./thread-outbox";
import { composerDraftEnvironmentId } from "../lib/composerAttachmentUploadQueue";
import {
  cloudOutboxArchiveFencesAtom,
  type ArchivedOutboxRow,
} from "../jones/accountRouting/cloudOutboxFence";

export { cloudOutboxArchiveFencesAtom } from "../jones/accountRouting/cloudOutboxFence";

const COMPOSER_DRAFTS_SCHEMA_VERSION = 1;
const COMPOSER_DRAFTS_DIRECTORY = "composer-drafts";
const COMPOSER_DRAFTS_FILE = "drafts.json";
const AUTO_ACCOUNT_ARCHIVE_FILE = "signed-out-auto-account-outbox.json";
const AUTO_ACCOUNT_ARCHIVE_VERSION = 1;
const PERSIST_DEBOUNCE_MS = 200;

export const composerContextImportsAtom = Atom.make<Record<string, boolean>>({}).pipe(
  Atom.keepAlive,
);

export function setComposerContextImporting(draftKey: string, importing: boolean): void {
  const next = { ...appAtomRegistry.get(composerContextImportsAtom) };
  if (importing) next[draftKey] = true;
  else delete next[draftKey];
  appAtomRegistry.set(composerContextImportsAtom, next);
}

interface ComposerDraftSelection {
  readonly text: string;
  readonly start: number;
  readonly end: number;
}

// Keyed by draft so a caret move in one composer neither replaces another draft's caret
// nor notifies its readers. Kept alive so the caret survives the composer unmounting.
const composerDraftSelectionAtom = Atom.family((_draftKey: string) =>
  Atom.make<ComposerDraftSelection | null>(null).pipe(
    Atom.keepAlive,
    Atom.withLabel("mobile:composer-draft-selection"),
  ),
);
const NO_COMPOSER_DRAFT_SELECTION_ATOM = Atom.make<ComposerDraftSelection | null>(null);

// Never persisted. One counter across drafts, so an editor moved between drafts never
// sees a later reset numbered below one it already applied.
let lastComposerDraftReset = 0;
const composerDraftResetAtom = Atom.family((_draftKey: string) =>
  Atom.make(0).pipe(Atom.withLabel("mobile:composer-draft-reset")),
);
const NO_COMPOSER_DRAFT_RESET_ATOM = Atom.make(0);

function nextComposerDraftReset(): number {
  lastComposerDraftReset += 1;
  return lastComposerDraftReset;
}

function markComposerDraftReset(draftKey: string): void {
  appAtomRegistry.set(composerDraftResetAtom(draftKey), nextComposerDraftReset());
}

/**
 * Raised when a draft's text is replaced outright (sent, discarded), or when the editor switches
 * drafts. iOS editor events read it synchronously, so an edit the editor made before applying the
 * reset cannot restore the text or land in the next draft. Android ignores it.
 */
export function useComposerDraftResetGeneration(draftKey: string | null): () => number {
  const atom = draftKey === null ? NO_COMPOSER_DRAFT_RESET_ATOM : composerDraftResetAtom(draftKey);
  // Subscribing re-renders the editor so it sends the reset even when the text did not change.
  useAtomValue(atom);
  // The editor stays mounted across drafts (remounting drops the keyboard), so a switch takes a
  // fresh generation; the selected draft's own, possibly older, stamp would move it backwards.
  const [selected, setSelected] = useState({ draftKey, generation: 0 });
  if (selected.draftKey !== draftKey) {
    setSelected({ draftKey, generation: nextComposerDraftReset() });
  }
  return () => Math.max(selected.generation, appAtomRegistry.get(atom));
}

export function rememberComposerDraftSelection(
  draftKey: string,
  text: string,
  selection: { start: number; end: number },
): void {
  appAtomRegistry.set(composerDraftSelectionAtom(draftKey), { text, ...selection });
}

function matchingComposerDraftSelection(
  selection: ComposerDraftSelection | null,
  text: string,
): { start: number; end: number } | null {
  if (selection?.text !== text) return null;
  return { start: selection.start, end: selection.end };
}

/** Read the post-chip-insertion selection or the editor restores the pre-insert caret before the chip. */
function readComposerDraftSelection(
  draftKey: string,
  text: string,
): { start: number; end: number } | null {
  return matchingComposerDraftSelection(
    appAtomRegistry.get(composerDraftSelectionAtom(draftKey)),
    text,
  );
}

export function useComposerDraftSelection(
  draftKey: string | null,
  text: string,
): { start: number; end: number } | null {
  const selection = useAtomValue(
    draftKey === null ? NO_COMPOSER_DRAFT_SELECTION_ATOM : composerDraftSelectionAtom(draftKey),
  );
  return draftKey === null ? null : matchingComposerDraftSelection(selection, text);
}

export interface ComposerDraftInsertion {
  readonly text: string;
  readonly start: number;
  readonly end: number;
}

export function captureComposerDraftInsertion(
  draftKey: string,
  selection?: { start: number; end: number },
): ComposerDraftInsertion {
  const { text } = getComposerDraftSnapshot(draftKey);
  return {
    text,
    ...(selection ??
      readComposerDraftSelection(draftKey, text) ?? { start: text.length, end: text.length }),
  };
}

function mapCapturedRange(
  captured: ComposerDraftInsertion,
  text: string,
): { start: number; end: number } | null {
  if (captured.text === text) return captured;
  // A replaced selection is not safe to delete after edits; only a caret follows them.
  if (captured.start !== captured.end) return null;
  const previous = captured.text;
  const shared = Math.min(previous.length, text.length);
  let prefix = 0;
  while (prefix < shared && previous[prefix] === text[prefix]) prefix += 1;
  let suffix = 0;
  while (
    suffix < shared - prefix &&
    previous[previous.length - 1 - suffix] === text[text.length - 1 - suffix]
  ) {
    suffix += 1;
  }
  // Text typed at the caret lands after it, so the insertion stays before that text.
  if (captured.start <= prefix) return captured;
  if (captured.start < previous.length - suffix) return null;
  const caret = captured.start + text.length - previous.length;
  return { start: caret, end: caret };
}

/** Keep a caret after the insertion where it was, or the next keystroke lands before typed text. */
function caretAfterInsertion(
  draftKey: string,
  previousText: string,
  range: { start: number; end: number },
  inserted: number,
): { start: number; end: number } {
  const caret = readComposerDraftSelection(draftKey, previousText);
  if (caret && caret.start > range.end) {
    const delta = range.start + inserted - range.end;
    return { start: caret.start + delta, end: caret.end + delta };
  }
  return { start: range.start + inserted, end: range.start + inserted };
}

function contextInsertionRange(
  draftKey: string,
  draft: ComposerDraft,
  target?: ComposerDraftInsertion,
) {
  const captured = target ?? appAtomRegistry.get(composerDraftSelectionAtom(draftKey));
  // Follow edits made during writes; append when an edit touched the captured range.
  const selection = captured ? mapCapturedRange(captured, draft.text) : null;
  const start = Math.max(0, Math.min(selection?.start ?? draft.text.length, draft.text.length));
  return { start, end: Math.max(start, Math.min(selection?.end ?? start, draft.text.length)) };
}

function draftWithoutInsertionSelection(
  draftKey: string,
  draft: ComposerDraft,
  target?: ComposerDraftInsertion,
) {
  const { start, end } = contextInsertionRange(draftKey, draft, target);
  const text = `${draft.text.slice(0, start)} ${draft.text.slice(end)}`;
  return withReferencedContextFiles(draft, text, referencedComposerContext(text, draft.context));
}

export function countComposerDraftAttachmentsAfterSelection(
  draftKey: string,
  target: ComposerDraftInsertion,
): number {
  return getComposerDraftAfterSelection(draftKey, target).attachments.length;
}

export function getComposerDraftAfterSelection(
  draftKey: string,
  target: ComposerDraftInsertion,
): ComposerDraft {
  return draftWithoutInsertionSelection(draftKey, getComposerDraftSnapshot(draftKey), target);
}

function withReferencedContextFiles(
  draft: ComposerDraft,
  text: string,
  context: OrchestrationMessageContext | undefined,
): ComposerDraft {
  const previousIds = new Set(
    draft.context?.records.flatMap((record) =>
      "attachmentId" in record ? [record.attachmentId] : [],
    ),
  );
  const retainedIds = new Set(
    context?.records.flatMap((record) => ("attachmentId" in record ? [record.attachmentId] : [])),
  );
  const attachments = draft.attachments.filter(
    (attachment) =>
      attachment.type === "image" ||
      !previousIds.has(attachment.id) ||
      retainedIds.has(attachment.id),
  );
  // Keep the array identity on plain text edits so attachment readers see no change.
  return {
    ...draft,
    text,
    context,
    attachments: attachments.length === draft.attachments.length ? draft.attachments : attachments,
  };
}

/** Retains file bytes while native text undo can restore their references. */
export function createComposerDraftContextHistory() {
  const restoreContext = createComposerContextHistory();
  const files = new Map<
    string,
    { attachment: FileBackedComposerAttachment; release: () => void }
  >();
  return {
    restore(text: string, draft: ComposerDraft) {
      for (const attachment of draft.attachments) {
        if (attachment.type !== "file") continue;
        const previous = files.get(attachment.id);
        files.delete(attachment.id);
        if (previous?.attachment.fileUri === attachment.fileUri) {
          previous.attachment = attachment;
          files.set(attachment.id, previous);
        } else {
          previous?.release();
          files.set(attachment.id, {
            attachment,
            release: retainComposerAttachmentFileForPreview(attachment),
          });
        }
      }
      const limit = Math.max(COMPOSER_CONTEXT_MAX_RECORDS, draft.attachments.length);
      while (files.size > limit) {
        const oldest = files.keys().next().value!;
        files.get(oldest)!.release();
        files.delete(oldest);
      }
      const context = restoreContext(text, draft.context);
      const liveIds = new Set(draft.attachments.map((attachment) => attachment.id));
      const attachments = (context?.records ?? []).flatMap((record) => {
        if (
          record.kind !== "file" ||
          !("attachmentId" in record) ||
          liveIds.has(record.attachmentId)
        )
          return [];
        const saved = files.get(record.attachmentId)?.attachment;
        liveIds.add(record.attachmentId);
        return saved
          ? [{ ...saved, uploadedAttachmentId: undefined, uploadEnvironmentId: undefined }]
          : [];
      });
      return { context, attachments };
    },
    dispose() {
      for (const file of files.values()) file.release();
      files.clear();
    },
  };
}

export function setComposerDraftContext(
  draftKey: string,
  context: OrchestrationMessageContext | undefined,
): void {
  updateComposerDrafts((current) => ({
    ...current,
    [draftKey]: { ...normalizeDraft(current[draftKey]), context },
  }));
}

export function insertComposerDraftContext(
  draftKey: string,
  content: {
    text: string;
    context: OrchestrationMessageContext;
    attachments?: ReadonlyArray<DraftComposerAttachment>;
  },
  target?: ComposerDraftInsertion,
): boolean {
  let inserted = false;
  let removed: ReadonlyArray<DraftComposerAttachment> = [];
  updateComposerDrafts((current) => {
    const draft = normalizeDraft(current[draftKey]);
    const attachments = content.attachments ?? [];
    const retained = draftWithoutInsertionSelection(draftKey, draft, target);
    if (
      attachments.length > 0 &&
      retained.attachments.length + attachments.length > PROVIDER_SEND_TURN_MAX_ATTACHMENTS
    )
      return current;
    const nextDraft = draftWithInsertedContext(
      draftKey,
      { ...draft, attachments: [...draft.attachments, ...attachments] },
      content,
      target,
    );
    if (!nextDraft) return current;
    inserted = true;
    removed = draft.attachments.filter((attachment) => !nextDraft.attachments.includes(attachment));
    return { ...current, [draftKey]: nextDraft };
  });
  scheduleUnusedComposerAttachmentCleanup(inserted ? removed : (content.attachments ?? []));
  return inserted;
}

function draftWithInsertedContext(
  draftKey: string,
  draft: ComposerDraft,
  content: { text: string; context: OrchestrationMessageContext },
  target?: ComposerDraftInsertion,
  maxContextRecords = COMPOSER_CONTEXT_MAX_RECORDS,
): ComposerDraft | null {
  const { start, end } = contextInsertionRange(draftKey, draft, target);
  const before = draft.text.slice(0, start);
  const after = draft.text.slice(end);
  const insertion = `${before.length > 0 && !/\s$/.test(before) && !/^\s/.test(content.text) ? " " : ""}${content.text}${!/\s$/.test(content.text) && (after.length === 0 || !/^\s/.test(after)) ? " " : ""}`;
  const text = before + insertion + after;
  const records = new Map(draft.context?.records.map((record) => [record.contextId, record]));
  for (const record of content.context.records) records.set(record.contextId, record);
  const context = referencedComposerContext(text, { version: 1, records: [...records.values()] });
  if ((context?.records.length ?? 0) > maxContextRecords) return null;
  rememberComposerDraftSelection(
    draftKey,
    text,
    caretAfterInsertion(draftKey, draft.text, { start, end }, insertion.length),
  );
  return withReferencedContextFiles(draft, text, context);
}

export class ComposerDraftPersistenceError extends Schema.TaggedError<ComposerDraftPersistenceError>()(
  "ComposerDraftPersistenceError",
  {
    operation: Schema.Literals(["open", "read", "decode", "encode", "write", "hydrate"]),
    directory: Schema.String,
    fileName: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Composer draft persistence operation ${this.operation} failed for ${this.directory}/${this.fileName}.`;
  }
}

const isComposerDraftPersistenceError = Schema.is(ComposerDraftPersistenceError);

export interface ComposerDraft {
  readonly importedHistoryCorrelation?: string;
  readonly text: string;
  readonly context?: OrchestrationMessageContext;
  readonly attachments: ReadonlyArray<DraftComposerAttachment>;
  readonly importedShareIds?: ReadonlyArray<string>;
  readonly modelSelection?: ModelSelection;
  /** Model and options for an explicitly selected fresh-launch automatic account. */
  readonly automaticAccountModel?: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
  /** `/clear` armed this draft: its next message starts a fresh provider session. */
  readonly freshContext?: boolean;
  readonly workspaceSelection?: ComposerDraftWorkspaceSelection;
  readonly project?: ComposerDraftProject;
}

export interface ComposerDraftProject {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId;
  readonly createdAt: string;
}

export interface ComposerDraftContent {
  readonly text: string;
  readonly context?: OrchestrationMessageContext;
  readonly attachments: ReadonlyArray<DraftComposerAttachment>;
  readonly sourceShareId?: string;
}

export interface ComposerDraftWorkspaceSelection {
  readonly mode: "local" | "worktree";
  readonly branch: string | null;
  readonly worktreePath: string | null;
  readonly startFromOrigin?: boolean;
  readonly pullRequestReference?: string;
  /**
   * Local mode chosen before refs named the current branch. It is never cleared when refs load:
   * a local task runs on the checkout, so the draft keeps following it (a later checkout change
   * included) until the user picks a branch, rather than pinning the first name refs reported.
   */
  readonly followsCurrentCheckout?: boolean;
  /**
   * Tracked repositories a new worktree takes, by path. Absent until the user chooses, so the
   * server's default applies.
   */
  readonly repositories?: ReadonlyArray<string>;
}

export type ComposerDraftSettingsUpdate = Pick<
  ComposerDraft,
  | "modelSelection"
  | "automaticAccountModel"
  | "runtimeMode"
  | "interactionMode"
  | "freshContext"
  | "workspaceSelection"
  | "project"
>;

const ComposerDraftWorkspaceSelectionSchema = Schema.Struct({
  mode: Schema.Literals(["local", "worktree"]),
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  startFromOrigin: Schema.optional(Schema.Boolean),
  pullRequestReference: Schema.optional(Schema.String),
  followsCurrentCheckout: Schema.optional(Schema.Boolean),
  repositories: Schema.optional(Schema.Array(Schema.String)),
});

const ComposerDraftProjectSchema = Schema.Struct({
  environmentId: EnvironmentIdSchema,
  projectId: ProjectIdSchema,
  createdAt: Schema.String,
});

// Recovery can exceed send limits; keep payloads reloadable and let send guards request trimming.
const PersistedComposerContextSchema = Schema.Struct({
  version: Schema.Literal(1),
  records: ForwardCompatibleArray(ComposerContextRecord),
});

const ComposerDraftSchema = Schema.Struct({
  importedHistoryCorrelation: Schema.optional(Schema.String),
  text: Schema.String,
  context: Schema.optional(PersistedComposerContextSchema),
  attachments: Schema.Array(DraftComposerAttachmentSchema),
  importedShareIds: Schema.optional(Schema.Array(Schema.String)),
  modelSelection: Schema.optional(ModelSelectionSchema),
  automaticAccountModel: Schema.optional(ModelSelectionSchema),
  runtimeMode: Schema.optional(RuntimeModeSchema),
  interactionMode: Schema.optional(ProviderInteractionModeSchema),
  freshContext: Schema.optional(Schema.Boolean),
  workspaceSelection: Schema.optional(ComposerDraftWorkspaceSelectionSchema),
  project: Schema.optional(ComposerDraftProjectSchema),
});

const PersistedComposerDraftsSchema = Schema.Struct({
  schemaVersion: Schema.Literal(COMPOSER_DRAFTS_SCHEMA_VERSION),
  drafts: Schema.Record(Schema.String, ComposerDraftSchema),
  stickyModelSelection: Schema.optional(ModelSelectionSchema),
  modelOptionMemory: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Record(Schema.String, Schema.Array(ProviderOptionSelectionSchema)),
    ),
  ),
  cloudAccountId: Schema.optional(Schema.String),
  restoredCloudEnvironmentIds: Schema.optional(Schema.Array(EnvironmentIdSchema)),
  signedOutDrafts: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        drafts: Schema.Record(Schema.String, ComposerDraftSchema),
        queuedMessages: Schema.Array(QueuedThreadMessageSchema),
        draftEnvironmentIds: Schema.optional(Schema.Array(EnvironmentIdSchema)),
      }),
    ),
  ),
});

const decodePersistedComposerDraftsDocument = Schema.decodeUnknownSync(
  PersistedComposerDraftsSchema,
);

const decodeAutoAccountArchive = Schema.decodeUnknownSync(
  Schema.Struct({
    schemaVersion: Schema.Literal(AUTO_ACCOUNT_ARCHIVE_VERSION),
    accounts: Schema.Record(Schema.String, Schema.Array(QueuedThreadMessageSchema)),
  }),
);

const EMPTY_DRAFT: ComposerDraft = {
  text: "",
  attachments: [],
};

export const composerDraftsAtom = Atom.make<Record<string, ComposerDraft>>({}).pipe(
  Atom.keepAlive,
  Atom.withLabel("mobile:composer-drafts"),
);

/**
 * Whether persisted drafts have merged in. Hydration starts with the app's
 * attachment upload worker; a failed load stays false until the next
 * `ensureComposerDraftsLoaded` caller retries it.
 */
export const composerDraftsLoadedAtom = Atom.make(false).pipe(
  Atom.keepAlive,
  Atom.withLabel("mobile:composer-drafts-loaded"),
);

export const stickyComposerModelSelectionAtom = Atom.make<ModelSelection | null>(null).pipe(
  Atom.keepAlive,
  Atom.withLabel("mobile:sticky-composer-model-selection"),
);

export type ModelOptionMemoryState = Readonly<
  Record<string, Readonly<Record<string, ReadonlyArray<ProviderOptionSelection>>>>
>;

export const modelOptionMemoryAtom = Atom.make<ModelOptionMemoryState>({}).pipe(
  Atom.keepAlive,
  Atom.withLabel("mobile:model-option-memory"),
);

interface SignedOutDrafts {
  readonly drafts: Record<string, ComposerDraft>;
  readonly queuedMessages: ReadonlyArray<QueuedThreadMessage>;
  readonly draftEnvironmentIds?: ReadonlyArray<EnvironmentId> | undefined;
}

interface ComposerCloudDraftState {
  readonly accountId: string | null;
  readonly restoredEnvironmentIds?: ReadonlyArray<EnvironmentId> | undefined;
  readonly signedOut: Record<string, SignedOutDrafts>;
}

export const composerCloudDraftsAtom = Atom.make<ComposerCloudDraftState>({
  accountId: null,
  signedOut: {},
}).pipe(Atom.keepAlive);

let loadPromise: Promise<void> | null = null;
let persistTimer: ReturnType<typeof setTimeout> | null = null;
let persistRetryNeeded = false;
const persistenceQueue = new SerializedAsyncQueue();
let persistedAutoAccountArchive: string | null = null;

export function resetComposerDraftsLoadState(): void {
  loadPromise = null;
  appAtomRegistry.set(composerDraftsLoadedAtom, false);
  persistRetryNeeded = false;
}

function attachmentContextRecord(
  attachment: DraftComposerAttachment,
  contextId = ComposerContextId.make(attachment.id),
) {
  const common = {
    version: 1 as const,
    contextId,
    label: sanitizeComposerContextLabel(attachment.name, attachment.type),
    attachmentId: attachment.id,
    name: attachment.name,
    mimeType: attachment.mimeType,
    sizeBytes: attachment.sizeBytes,
  };
  // Document pickers label images as files; promote the record so every client can open them as images.
  return attachment.type === "image" || imageMimeType(attachment) !== null
    ? { ...common, kind: "image" as const }
    : { ...common, kind: "file" as const };
}

function restoreMissingComposerFileReferences(
  draft: ComposerDraft,
  attachments = draft.attachments,
): ComposerDraft {
  const records = [...(draft.context?.records ?? [])];
  const usedIds = new Set<string>(records.map((record) => record.contextId));
  const referenced = new Set(
    collectComposerContextReferences(draft.text).map(
      (reference) => `${reference.kind}:${reference.contextId}`,
    ),
  );
  let text = draft.text;
  let changed = false;
  for (const attachment of attachments) {
    if (
      attachment.type === "image" ||
      imageMimeType(attachment) !== null ||
      videoMimeType(attachment) !== null
    )
      continue;
    let record = records.find(
      (candidate) =>
        candidate.kind === "file" &&
        "attachmentId" in candidate &&
        candidate.attachmentId === attachment.id,
    );
    if (!record) {
      const baseId = attachment.id.replace(/[^a-z0-9_-]/gi, "_").slice(0, 110) || "file";
      let contextId = baseId;
      for (let suffix = 2; usedIds.has(contextId); suffix += 1) contextId = `${baseId}_${suffix}`;
      usedIds.add(contextId);
      record = attachmentContextRecord(attachment, ComposerContextId.make(contextId));
      records.push(record);
      changed = true;
    }
    const key = `${record.kind}:${record.contextId}`;
    if (referenced.has(key)) continue;
    text += `${text.length > 0 && !/\s$/.test(text) ? " " : ""}${formatComposerContextReference(record)} `;
    referenced.add(key);
    changed = true;
  }
  return changed ? { ...draft, text, context: { version: 1, records } } : draft;
}

function normalizeDraft(draft: ComposerDraft | undefined): ComposerDraft {
  if (!draft) {
    return EMPTY_DRAFT;
  }
  return {
    ...draft,
    text: draft.text,
    attachments: draft.attachments,
  };
}

export function getComposerDraftSnapshot(draftKey: string): ComposerDraft {
  return captureComposerModelSelectionSnapshot(draftKey, {
    ...normalizeDraft(appAtomRegistry.get(composerDraftsAtom)[draftKey]),
  });
}

export function isComposerDraftEmpty(draft: ComposerDraft): boolean {
  return isEmptyDraft(draft);
}

function isEmptyDraft(draft: ComposerDraft): boolean {
  return (
    draft.text.length === 0 &&
    draft.attachments.length === 0 &&
    draft.modelSelection === undefined &&
    draft.automaticAccountModel === undefined &&
    draft.runtimeMode === undefined &&
    draft.interactionMode === undefined &&
    draft.freshContext !== true &&
    draft.workspaceSelection === undefined &&
    draft.importedHistoryCorrelation === undefined
  );
}

function withComposerDraft(
  current: Record<string, ComposerDraft>,
  draftKey: string,
  draft: ComposerDraft,
): Record<string, ComposerDraft> {
  if (isEmptyDraft(draft) && draft.project === undefined) {
    const next = { ...current };
    delete next[draftKey];
    return next;
  }
  return { ...current, [draftKey]: draft };
}

export { isNewTaskDraftKey, newTaskDraftKey } from "./new-task-draft-key";

function newDraftId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export function migrateLegacyNewTaskDraft(
  key: string,
  draft: ComposerDraft,
  now: string,
): readonly [key: string, draft: ComposerDraft] {
  const restored = restoreMissingComposerFileReferences(draft);
  const legacy = draft.project === undefined ? parseLegacyNewTaskDraftKey(key) : null;
  if (legacy === null) {
    return [key, restored];
  }
  return [
    newTaskDraftKey(newDraftId()),
    {
      ...restored,
      project: {
        environmentId: EnvironmentIdSchema.make(legacy.environmentId),
        projectId: ProjectIdSchema.make(legacy.projectId),
        createdAt: now,
      },
    },
  ];
}

export function decodePersistedComposerState(value: unknown): {
  readonly drafts: Record<string, ComposerDraft>;
  readonly stickyModelSelection: ModelSelection | null;
  readonly modelOptionMemory: ModelOptionMemoryState;
  readonly cloudDrafts: ComposerCloudDraftState;
} {
  const parsed = decodePersistedComposerDraftsDocument(value);
  const now = new Date().toISOString();
  return {
    drafts: Object.fromEntries(
      Object.entries(parsed.drafts)
        .map(([key, draft]) =>
          migrateLegacyNewTaskDraft(
            key,
            isNewTaskDraftKey(key) &&
              draft.modelSelection &&
              draft.text.length === 0 &&
              draft.attachments.length === 0 &&
              draft.runtimeMode === undefined &&
              draft.interactionMode === undefined &&
              draft.workspaceSelection === undefined
              ? { ...draft, modelSelection: undefined }
              : draft,
            now,
          ),
        )
        // Retain contentless drafts with import receipts to prevent re-importing native shares after restart.
        .filter(([, draft]) => !isEmptyDraft(draft) || (draft.importedShareIds?.length ?? 0) > 0)
        // Queued-edit sessions are memory-only; persisted drafts would outlive their run association and become unreachable.
        .filter(([key]) => !isQueuedEditDraftKey(key)),
    ),
    stickyModelSelection: parsed.stickyModelSelection ?? null,
    modelOptionMemory: parsed.modelOptionMemory ?? {},
    cloudDrafts: {
      accountId: parsed.cloudAccountId ?? null,
      restoredEnvironmentIds: parsed.restoredCloudEnvironmentIds,
      signedOut: Object.fromEntries(
        Object.entries(parsed.signedOutDrafts ?? {}).map(([id, saved]) => [
          id,
          {
            drafts: Object.fromEntries(
              Object.entries(saved.drafts).map(([key, draft]) =>
                migrateLegacyNewTaskDraft(key, draft, now),
              ),
            ),
            queuedMessages: saved.queuedMessages.map(decodeQueuedThreadMessage),
            draftEnvironmentIds: saved.draftEnvironmentIds,
          },
        ]),
      ),
    },
  };
}

async function getComposerDraftsFile(fileName = COMPOSER_DRAFTS_FILE) {
  const { Directory, File, Paths } = await import("expo-file-system");
  const directory = new Directory(Paths.document, COMPOSER_DRAFTS_DIRECTORY);
  directory.create({ idempotent: true, intermediates: true });
  return new File(directory, fileName);
}

function mergeAutoAccountArchive(
  cloudDrafts: ComposerCloudDraftState,
  document: string,
): ComposerCloudDraftState {
  const archive = decodeAutoAccountArchive(JSON.parse(document));
  const signedOut = { ...cloudDrafts.signedOut };
  for (const [accountId, rows] of Object.entries(archive.accounts)) {
    const saved = signedOut[accountId];
    const messages = new Map(saved?.queuedMessages.map((message) => [message.messageId, message]));
    for (const row of rows) {
      const message = decodeQueuedThreadMessage(row);
      if (!message.accountRouting) throw new Error("Expected an automatic account outbox row.");
      messages.set(message.messageId, message);
    }
    signedOut[accountId] = {
      ...saved,
      drafts: saved?.drafts ?? {},
      queuedMessages: [...messages.values()],
    };
  }
  return { ...cloudDrafts, signedOut };
}

async function loadPersistedComposerState(): Promise<
  ReturnType<typeof decodePersistedComposerState>
> {
  let operation: ComposerDraftPersistenceError["operation"] = "open";
  let fileName = COMPOSER_DRAFTS_FILE;
  try {
    const file = await getComposerDraftsFile();
    operation = "read";
    const document = file.exists ? await file.text() : undefined;
    operation = "decode";
    const persisted = decodePersistedComposerState(
      document === undefined
        ? { schemaVersion: COMPOSER_DRAFTS_SCHEMA_VERSION, drafts: {} }
        : JSON.parse(document),
    );
    fileName = AUTO_ACCOUNT_ARCHIVE_FILE;
    operation = "open";
    const archiveFile = await getComposerDraftsFile(fileName);
    if (!archiveFile.exists) {
      persistedAutoAccountArchive = null;
      return persisted;
    }
    operation = "read";
    const archive = await archiveFile.text();
    operation = "decode";
    const cloudDrafts = mergeAutoAccountArchive(persisted.cloudDrafts, archive);
    persistedAutoAccountArchive = archive;
    return { ...persisted, cloudDrafts };
  } catch (cause) {
    throw new ComposerDraftPersistenceError({
      operation,
      directory: COMPOSER_DRAFTS_DIRECTORY,
      fileName,
      cause,
    });
  }
}

async function writePersistedComposerState(
  drafts: Record<string, ComposerDraft>,
  stickyModelSelection: ModelSelection | null,
  cloudDrafts = appAtomRegistry.get(composerCloudDraftsAtom),
): Promise<void> {
  let operation: ComposerDraftPersistenceError["operation"] = "encode";
  let fileName = COMPOSER_DRAFTS_FILE;
  try {
    const accounts = new Map<string, ReadonlyArray<unknown>>();
    const signedOutDrafts = Object.fromEntries(
      Object.entries(cloudDrafts.signedOut).map(([id, saved]) => {
        const queuedMessages: Array<unknown> = [];
        const automaticMessages: Array<unknown> = [];
        for (const message of saved.queuedMessages) {
          (message.accountRouting === undefined ? queuedMessages : automaticMessages).push(
            encodeQueuedThreadMessage(message),
          );
        }
        if (automaticMessages.length > 0) accounts.set(id, automaticMessages);
        return [
          id,
          { drafts: saved.drafts, queuedMessages, draftEnvironmentIds: saved.draftEnvironmentIds },
        ];
      }),
    );
    const nonEmptyDrafts = Object.fromEntries(
      Object.entries(drafts).filter(([, draft]) => !isEmptyDraft(draft)),
    );
    const document = {
      schemaVersion: COMPOSER_DRAFTS_SCHEMA_VERSION,
      drafts: nonEmptyDrafts,
      ...(stickyModelSelection ? { stickyModelSelection } : {}),
      ...(Object.keys(appAtomRegistry.get(modelOptionMemoryAtom)).length > 0
        ? { modelOptionMemory: appAtomRegistry.get(modelOptionMemoryAtom) }
        : {}),
      ...(cloudDrafts.accountId ? { cloudAccountId: cloudDrafts.accountId } : {}),
      ...(cloudDrafts.restoredEnvironmentIds?.length
        ? { restoredCloudEnvironmentIds: cloudDrafts.restoredEnvironmentIds }
        : {}),
      ...(Object.keys(signedOutDrafts).length > 0 ? { signedOutDrafts } : {}),
    } as const;
    const encoded = JSON.stringify(document);
    // Persist the protected rows before replacing a document that may still contain them.
    if (persistedAutoAccountArchive !== null || accounts.size > 0) {
      fileName = AUTO_ACCOUNT_ARCHIVE_FILE;
      const archive = JSON.stringify({
        schemaVersion: AUTO_ACCOUNT_ARCHIVE_VERSION,
        accounts: Object.fromEntries(accounts),
      });
      if (archive !== persistedAutoAccountArchive) {
        operation = "open";
        const archiveFile = await getComposerDraftsFile(fileName);
        operation = "write";
        await writeFileAtomically(archiveFile, archive);
        persistedAutoAccountArchive = archive;
      }
    }
    fileName = COMPOSER_DRAFTS_FILE;
    operation = "open";
    const file = await getComposerDraftsFile();
    operation = "write";
    await writeFileAtomically(file, encoded);
  } catch (cause) {
    throw new ComposerDraftPersistenceError({
      operation,
      directory: COMPOSER_DRAFTS_DIRECTORY,
      fileName,
      cause,
    });
  }
}

/** Propagates write failures so callers can decide whether to restart. */
export async function flushComposerDrafts(): Promise<void> {
  // Hydrate before writing or partial atom state would clobber persisted drafts.
  ensureComposerDraftsLoaded();
  if (loadPromise !== null) {
    await loadPromise;
  }
  // Edits during writes schedule more debounces; drain until none remain.
  do {
    while (persistTimer !== null || persistRetryNeeded) {
      if (persistTimer !== null) clearTimeout(persistTimer);
      persistTimer = null;
      persistRetryNeeded = false;
      try {
        await persistenceQueue.run(() =>
          writePersistedComposerState(
            appAtomRegistry.get(composerDraftsAtom),
            appAtomRegistry.get(stickyComposerModelSelectionAtom),
          ),
        );
      } catch (error) {
        persistRetryNeeded = true;
        throw error;
      }
    }
    // Draining includes fired debounce writes still awaiting hydration inside the queue.
    await persistenceQueue.run(() => Promise.resolve());
  } while (persistTimer !== null || persistRetryNeeded);
}

function signedOutAttachmentOwners() {
  return Object.values(appAtomRegistry.get(composerCloudDraftsAtom).signedOut).flatMap((saved) => [
    ...Object.values(saved.drafts),
    ...saved.queuedMessages,
  ]);
}

export function findLocalComposerClipboardAttachment(
  knownEnvironmentIds: Iterable<EnvironmentId>,
  environmentId: EnvironmentId,
  id: string,
): DraftComposerAttachment | undefined {
  const queuedMessages = Object.values(
    appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom),
  ).flat();
  for (const [key, draft] of Object.entries(appAtomRegistry.get(composerDraftsAtom))) {
    if (
      composerDraftEnvironmentId(key, knownEnvironmentIds, queuedMessages, draft) !== environmentId
    )
      continue;
    const attachment = draft.attachments.find((entry) => entry.id === id);
    if (attachment) return attachment;
  }
  return queuedMessages
    .filter((message) => message.environmentId === environmentId)
    .flatMap((message) => message.attachments)
    .find((attachment) => attachment.id === id);
}

function isComposerAttachmentFileReferenced(fileUri: string): boolean {
  if (isComposerAttachmentFileRetained(fileUri)) {
    return true;
  }
  const referenceKey = composerAttachmentFileReferenceKey(fileUri);
  const drafts = Object.values(appAtomRegistry.get(composerDraftsAtom));
  const queuedMessages = Object.values(
    appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom),
  ).flat();
  return [...drafts, ...queuedMessages, ...signedOutAttachmentOwners()].some((owner) =>
    owner.attachments.some(
      (attachment) =>
        attachment.fileUri !== undefined &&
        composerAttachmentFileReferenceKey(attachment.fileUri) === referenceKey,
    ),
  );
}

function isComposerAttachmentUploadReferenced(
  environmentId: EnvironmentId,
  attachmentId: string,
): boolean {
  const drafts = Object.values(appAtomRegistry.get(composerDraftsAtom));
  const queuedMessages = Object.values(
    appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom),
  ).flat();
  return [...drafts, ...queuedMessages, ...signedOutAttachmentOwners()].some((owner) =>
    owner.attachments.some(
      (attachment) =>
        attachment.uploadEnvironmentId === environmentId &&
        attachment.uploadedAttachmentId === attachmentId,
    ),
  );
}

export async function releaseUnusedComposerAttachmentFiles(
  attachments: ReadonlyArray<DraftComposerAttachment>,
): Promise<void> {
  const candidates = new Set(
    attachments.flatMap((attachment) =>
      attachment.fileUri !== undefined ? [attachment.fileUri] : [],
    ),
  );
  const uploadCandidates = new Map<EnvironmentId, Set<string>>();
  for (const attachment of attachments) {
    if (
      attachment.uploadEnvironmentId === undefined ||
      attachment.uploadedAttachmentId === undefined
    ) {
      continue;
    }
    const ids = uploadCandidates.get(attachment.uploadEnvironmentId) ?? new Set<string>();
    ids.add(attachment.uploadedAttachmentId);
    uploadCandidates.set(attachment.uploadEnvironmentId, ids);
  }
  if (candidates.size === 0 && uploadCandidates.size === 0) {
    return;
  }

  // Hydrate before scanning references; cold-start atoms would otherwise mark owned files unused.
  await waitForComposerDraftsLoaded();
  await flushComposerDrafts();
  if (!(await threadOutboxManager.load())) {
    // Unreadable outbox stores may still own files; do not treat them as empty.
    return;
  }
  await flushThreadOutbox();

  const allFilesReferenced = [...candidates].every(isComposerAttachmentFileReferenced);
  const allUploadsReferenced = [...uploadCandidates].every(([environmentId, attachmentIds]) =>
    [...attachmentIds].every((attachmentId) =>
      isComposerAttachmentUploadReferenced(environmentId, attachmentId),
    ),
  );
  if (allFilesReferenced && allUploadsReferenced) {
    return;
  }

  let incomingShareFileUris: ReadonlySet<string>;
  try {
    const { loadIncomingShareDrafts } = await import("../features/sharing/incoming-share-storage");
    const incomingShares = await loadIncomingShareDrafts({ strict: true });
    incomingShareFileUris = new Set(
      incomingShares.flatMap((share) =>
        share.attachments.flatMap((attachment) =>
          attachment.fileUri !== undefined
            ? [composerAttachmentFileReferenceKey(attachment.fileUri)]
            : [],
        ),
      ),
    );
  } catch (error) {
    console.warn("[composer-attachments] could not verify incoming share ownership", error);
    return;
  }

  const { removePersistedComposerAttachmentFile } = await import("../lib/composerImages");
  for (const fileUri of candidates) {
    // Re-check immediately before deletion; restores or edits can acquire ownership after scanning.
    if (
      isComposerAttachmentFileReferenced(fileUri) ||
      incomingShareFileUris.has(composerAttachmentFileReferenceKey(fileUri))
    ) {
      continue;
    }
    await removePersistedComposerAttachmentFile(fileUri);
  }

  if (uploadCandidates.size > 0) {
    const { releasePendingAttachmentUploads } = await import("../lib/attachmentUpload");
    for (const [environmentId, attachmentIds] of uploadCandidates) {
      for (const attachmentId of attachmentIds) {
        if (isComposerAttachmentUploadReferenced(environmentId, attachmentId)) {
          continue;
        }
        try {
          await releasePendingAttachmentUploads(environmentId, [attachmentId]);
        } catch (error) {
          // The server expires pending uploads; local discard must survive disconnection or deletion failure.
          console.warn("[composer-attachments] could not remove pending upload", {
            environmentId,
            attachmentId,
            error,
          });
        }
      }
    }
  }
}

export function scheduleUnusedComposerAttachmentCleanup(
  attachments: ReadonlyArray<DraftComposerAttachment>,
): void {
  if (
    !attachments.some(
      (attachment) =>
        attachment.fileUri !== undefined || attachment.uploadedAttachmentId !== undefined,
    )
  ) {
    return;
  }
  void releaseUnusedComposerAttachmentFiles(attachments).catch((error) => {
    console.warn("[composer-attachments] could not remove unused files", error);
  });
}

registerComposerAttachmentUnusedHandler((attachment) => {
  scheduleUnusedComposerAttachmentCleanup([attachment]);
});

export function schedulePersistComposerState(): void {
  if (persistTimer !== null) {
    clearTimeout(persistTimer);
  }
  persistTimer = setTimeout(() => {
    persistTimer = null;
    // Enqueue before awaiting hydration so flushComposerDrafts cannot drain ahead of this write.
    void persistenceQueue.run(async () => {
      try {
        await waitForComposerDraftsLoaded();
        await writePersistedComposerState(
          appAtomRegistry.get(composerDraftsAtom),
          appAtomRegistry.get(stickyComposerModelSelectionAtom),
        );
        persistRetryNeeded = false;
      } catch (error) {
        // Failed debounces leave no timer; final flush must retry after reading persisted ownership.
        persistRetryNeeded = true;
        console.warn("[composer-drafts] failed to persist drafts", error);
      }
    });
  }, PERSIST_DEBOUNCE_MS);
}

export function ensureComposerDraftsLoaded(): void {
  if (loadPromise !== null) {
    return;
  }
  const loading = loadPersistedComposerState().then((persisted) => {
    appAtomRegistry.set(composerCloudDraftsAtom, persisted.cloudDrafts);
    if (Object.keys(persisted.drafts).length > 0) {
      const current = appAtomRegistry.get(composerDraftsAtom);
      appAtomRegistry.set(composerDraftsAtom, {
        ...persisted.drafts,
        ...current,
      });
    }
    if (
      persisted.stickyModelSelection !== null &&
      appAtomRegistry.get(stickyComposerModelSelectionAtom) === null
    ) {
      appAtomRegistry.set(stickyComposerModelSelectionAtom, persisted.stickyModelSelection);
    }
    if (Object.keys(persisted.modelOptionMemory).length > 0) {
      const current = appAtomRegistry.get(modelOptionMemoryAtom);
      appAtomRegistry.set(modelOptionMemoryAtom, {
        ...persisted.modelOptionMemory,
        ...Object.fromEntries(
          Object.entries(current).map(([instanceId, models]) => [
            instanceId,
            { ...persisted.modelOptionMemory[instanceId], ...models },
          ]),
        ),
      });
    }
    appAtomRegistry.set(composerDraftsLoadedAtom, true);
  });
  loadPromise = loading;
  void loading.catch((cause) => {
    if (loadPromise === loading) loadPromise = null;
    console.warn(
      "[composer-drafts] failed to hydrate drafts",
      isComposerDraftPersistenceError(cause)
        ? cause
        : new ComposerDraftPersistenceError({
            operation: "hydrate",
            directory: COMPOSER_DRAFTS_DIRECTORY,
            fileName: COMPOSER_DRAFTS_FILE,
            cause,
          }),
    );
  });
}

export async function waitForComposerDraftsLoaded(): Promise<void> {
  ensureComposerDraftsLoaded();
  if (loadPromise !== null) {
    await loadPromise;
  }
}

export async function getComposerCloudAccountId(): Promise<string | null> {
  await waitForComposerDraftsLoaded();
  return appAtomRegistry.get(composerCloudDraftsAtom).accountId;
}

export async function getRestoredCloudEnvironmentIds(): Promise<ReadonlyArray<EnvironmentId>> {
  await waitForComposerDraftsLoaded();
  return appAtomRegistry.get(composerCloudDraftsAtom).restoredEnvironmentIds ?? [];
}

/** Release ownership only after sign-out has preserved the data and removed the relay routes. */
export async function finishCloudOutboxArchive(): Promise<void> {
  const cloud = appAtomRegistry.get(composerCloudDraftsAtom);
  const restoredEnvironmentIds = cloud.restoredEnvironmentIds;
  if (!restoredEnvironmentIds?.length) return;
  appAtomRegistry.set(composerCloudDraftsAtom, { ...cloud, restoredEnvironmentIds: undefined });
  schedulePersistComposerState();
  try {
    await flushComposerDrafts();
  } catch (error) {
    appAtomRegistry.set(composerCloudDraftsAtom, {
      ...appAtomRegistry.get(composerCloudDraftsAtom),
      restoredEnvironmentIds,
    });
    throw error;
  }
}

function archivedDraftEnvironmentIds(saved: SignedOutDrafts): Set<EnvironmentId> {
  if (Object.keys(saved.drafts).length === 0) return new Set();
  if (saved.draftEnvironmentIds !== undefined) return new Set(saved.draftEnvironmentIds);
  const environmentIds = new Set<EnvironmentId>();
  // Archives written before environment IDs were recorded can still identify scoped drafts.
  for (const [key, draft] of Object.entries(saved.drafts)) {
    const scoped = parseScopedThreadKey(key);
    const environmentId = composerDraftEnvironmentId(
      key,
      scoped === null ? [] : [scoped.environmentId],
      saved.queuedMessages,
      draft,
    );
    if (environmentId !== null) environmentIds.add(environmentId);
  }
  return environmentIds;
}

/** Hold delivery from before the archive snapshot until relay removal and its cleanup finish. */
export function beginCloudOutboxArchive(environmentIds: ReadonlySet<EnvironmentId>): () => void {
  const next = new Map(appAtomRegistry.get(cloudOutboxArchiveFencesAtom));
  for (const environmentId of environmentIds) {
    if (next.has(environmentId)) throw new Error(m(msg("Could not preserve queued messages.")));
    next.set(environmentId, []);
  }
  appAtomRegistry.set(cloudOutboxArchiveFencesAtom, next);
  return () => {
    const remaining = new Map(appAtomRegistry.get(cloudOutboxArchiveFencesAtom));
    for (const environmentId of environmentIds) remaining.delete(environmentId);
    appAtomRegistry.set(cloudOutboxArchiveFencesAtom, remaining);
  };
}

export async function archiveCloudComposerDrafts(
  accountId: string | null,
  environmentIds: ReadonlySet<EnvironmentId>,
  knownEnvironmentIds: Iterable<EnvironmentId>,
): Promise<void> {
  await waitForComposerDraftsLoaded();
  if (!(await threadOutboxManager.load()))
    throw new Error(m(msg("Could not preserve queued messages.")));
  await flushThreadOutbox();
  const cloud = appAtomRegistry.get(composerCloudDraftsAtom);
  // A failed or superseded activation can pass a different ID from the persisted live owner.
  const owner = cloud.restoredEnvironmentIds?.length
    ? cloud.accountId
    : (accountId ?? cloud.accountId);
  if (cloud.restoredEnvironmentIds?.length && owner === null)
    throw new Error(m(msg("Could not preserve queued messages.")));
  if (owner === null) return;
  const queued = Object.values(
    appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom),
  ).flat();
  const current = appAtomRegistry.get(composerDraftsAtom);
  const remaining = { ...current };
  const saved = cloud.signedOut[owner];
  const savedDrafts = { ...saved?.drafts };
  const savedDraftEnvironmentIds = saved
    ? archivedDraftEnvironmentIds(saved)
    : new Set<EnvironmentId>();
  for (const [key, draft] of Object.entries(current)) {
    const environmentId = composerDraftEnvironmentId(key, knownEnvironmentIds, queued, draft);
    if (environmentId !== null && environmentIds.has(environmentId)) {
      savedDraftEnvironmentIds.add(environmentId);
      savedDrafts[key] = draft;
      delete remaining[key];
    }
  }
  const savedMessages = new Map(
    saved?.queuedMessages.map((message) => [message.messageId, message]),
  );
  const archivedRows = new Map<EnvironmentId, ArchivedOutboxRow[]>();
  for (const message of queued) {
    if (!environmentIds.has(message.environmentId)) continue;
    savedMessages.set(message.messageId, message);
    const row = { message, revision: threadOutboxManager.revisionOf(message.messageId) };
    const rows = archivedRows.get(message.environmentId);
    if (rows) rows.push(row);
    else archivedRows.set(message.environmentId, [row]);
  }
  appAtomRegistry.set(composerDraftsAtom, remaining);
  appAtomRegistry.set(composerCloudDraftsAtom, {
    ...cloud,
    // Retain the owner through removal so cold-start cleanup can retry before another account activates.
    accountId: owner,
    signedOut: {
      ...cloud.signedOut,
      [owner]: {
        drafts: savedDrafts,
        queuedMessages: [...savedMessages.values()],
        draftEnvironmentIds: [...savedDraftEnvironmentIds],
      },
    },
  });
  // A disconnected environment has no registry removal to abort its open draft operations.
  abortComposerDraftOperationsWhere((key) => {
    const environmentId = composerDraftEnvironmentId(
      key,
      knownEnvironmentIds,
      queued,
      current[key],
    );
    return environmentId !== null && environmentIds.has(environmentId);
  });
  schedulePersistComposerState();
  await flushComposerDrafts();
  const fences = appAtomRegistry.get(cloudOutboxArchiveFencesAtom);
  if (fences.size > 0) {
    const durable = new Set(
      appAtomRegistry.get(composerCloudDraftsAtom).signedOut[owner]?.queuedMessages,
    );
    const next = new Map(fences);
    for (const [environmentId, rows] of archivedRows) {
      if (next.has(environmentId)) {
        next.set(
          environmentId,
          rows.filter(({ message }) => durable.has(message)),
        );
      }
    }
    appAtomRegistry.set(cloudOutboxArchiveFencesAtom, next);
  }
}

function sameDraftAttachmentIds(
  left: ReadonlyArray<DraftComposerAttachment>,
  right: ReadonlyArray<DraftComposerAttachment>,
): boolean {
  return (
    left.length === right.length &&
    left.every((attachment, index) => attachment.id === right[index]?.id)
  );
}

/** An in-flight delivery can finish after sign-out took its snapshot. */
export async function removeDeliveredCloudQueuedMessage(
  message: QueuedThreadMessage,
): Promise<void> {
  await waitForComposerDraftsLoaded();
  const cloud = appAtomRegistry.get(composerCloudDraftsAtom);
  const signedOut = { ...cloud.signedOut };
  let changed = false;
  for (const [accountId, saved] of Object.entries(signedOut)) {
    const archived = saved.queuedMessages.find(
      (candidate) =>
        candidate.environmentId === message.environmentId &&
        candidate.messageId === message.messageId,
    );
    if (
      !archived ||
      archived.commandId !== message.commandId ||
      archived.threadId !== message.threadId ||
      archived.text !== message.text ||
      !sameDraftAttachmentIds(archived.attachments, message.attachments)
    )
      continue;
    if (
      JSON.stringify([
        archived.modelSelection,
        archived.accountRouting,
        archived.modelSelectionExplicit,
        archived.runtimeMode,
        archived.interactionMode,
        archived.creation,
      ]) !==
      JSON.stringify([
        message.modelSelection,
        message.accountRouting,
        message.modelSelectionExplicit,
        message.runtimeMode,
        message.interactionMode,
        message.creation,
      ])
    )
      continue;
    const editorKey = `pending-task:${message.messageId}`;
    const editor = saved.drafts[editorKey];
    if (
      editor &&
      (editor.text !== message.text ||
        !sameDraftAttachmentIds(editor.attachments, message.attachments) ||
        (editor.modelSelection !== undefined &&
          JSON.stringify(editor.modelSelection) !== JSON.stringify(message.modelSelection)) ||
        (editor.runtimeMode !== undefined && editor.runtimeMode !== message.runtimeMode) ||
        (editor.interactionMode !== undefined &&
          editor.interactionMode !== message.interactionMode) ||
        (editor.workspaceSelection !== undefined &&
          (editor.workspaceSelection.mode !== message.creation?.workspaceMode ||
            editor.workspaceSelection.branch !== message.creation?.branch ||
            editor.workspaceSelection.worktreePath !== message.creation?.worktreePath ||
            editor.workspaceSelection.pullRequestReference !==
              message.creation?.pullRequestReference ||
            (editor.workspaceSelection.startFromOrigin ?? false) !==
              (message.creation?.startFromOrigin ?? false) ||
            JSON.stringify(editor.workspaceSelection.repositories) !==
              JSON.stringify(message.creation?.repositories))))
    )
      continue;
    const drafts = { ...saved.drafts };
    delete drafts[editorKey];
    signedOut[accountId] = {
      ...saved,
      drafts,
      queuedMessages: saved.queuedMessages.filter((candidate) => candidate !== archived),
    };
    changed = true;
  }
  if (!changed) return;
  appAtomRegistry.set(composerCloudDraftsAtom, { ...cloud, signedOut });
  schedulePersistComposerState();
  try {
    await flushComposerDrafts();
  } catch (error) {
    // Keep archive updates pending after failure; live outbox removal can still require a later successful flush.
    schedulePersistComposerState();
    throw error;
  }
}

export async function restoreCloudComposerDrafts(accountId: string): Promise<void> {
  await waitForComposerDraftsLoaded();
  const before = appAtomRegistry.get(composerCloudDraftsAtom);
  if (before.accountId !== accountId && before.restoredEnvironmentIds?.length)
    throw new Error(m(msg("Could not restore queued messages.")));
  const archived = before.signedOut[accountId];
  if (archived) {
    const environmentIds = archivedDraftEnvironmentIds(archived);
    for (const message of archived.queuedMessages) environmentIds.add(message.environmentId);
    if (environmentIds.size > 0) {
      // Record the owner before the first live write. A crash can leave both copies on disk.
      const restoredEnvironmentIds = [
        ...new Set([...(before.restoredEnvironmentIds ?? []), ...environmentIds]),
      ];
      appAtomRegistry.set(composerCloudDraftsAtom, {
        ...before,
        accountId,
        restoredEnvironmentIds,
      });
      schedulePersistComposerState();
      await flushComposerDrafts();
    }
    if (!(await threadOutboxManager.load()))
      throw new Error(m(msg("Could not restore queued messages.")));
    for (const message of archived.queuedMessages) {
      // Delivery can retire a snapshot while an outbox write settles. Recheck before enqueueing.
      while (true) {
        const stillArchived = appAtomRegistry
          .get(composerCloudDraftsAtom)
          .signedOut[accountId]?.queuedMessages.includes(message);
        if (!stillArchived) break;
        const queued = Object.values(
          appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom),
        )
          .flat()
          .find((current) => current.messageId === message.messageId);
        if (queued === undefined) {
          await threadOutboxManager.enqueue(message);
          break;
        }
        if (await threadOutboxManager.confirmQueued(queued)) break;
      }
    }
  }
  const cloud = appAtomRegistry.get(composerCloudDraftsAtom);
  const saved = cloud.signedOut[accountId];
  if (saved) {
    updateComposerDrafts((current) => {
      const restored = { ...current };
      for (const [key, draft] of Object.entries(saved.drafts)) {
        const existing = current[key];
        const attachmentIds = new Set(existing?.attachments.map((attachment) => attachment.id));
        const selection =
          existing?.modelSelection !== undefined || existing?.automaticAccountModel !== undefined
            ? existing
            : draft;
        restored[key] = existing
          ? {
              ...draft,
              ...existing,
              modelSelection: selection.modelSelection,
              automaticAccountModel:
                selection.modelSelection === undefined
                  ? selection.automaticAccountModel
                  : undefined,
              text: mergeComposerDraftText(existing.text, draft.text),
              context: mergeReferencedComposerContext(
                mergeComposerDraftText(existing.text, draft.text),
                draft.context,
                existing.context,
              ),
              // A concurrent import must not lose files, even above the send limit.
              attachments: [
                ...existing.attachments,
                ...draft.attachments.filter((attachment) => !attachmentIds.has(attachment.id)),
              ],
              importedShareIds: [
                ...new Set([
                  ...(existing.importedShareIds ?? []),
                  ...(draft.importedShareIds ?? []),
                ]),
              ],
            }
          : draft;
      }
      return restored;
    });
  }
  const signedOut = { ...cloud.signedOut };
  delete signedOut[accountId];
  appAtomRegistry.set(composerCloudDraftsAtom, { ...cloud, accountId, signedOut });
  schedulePersistComposerState();
  await flushComposerDrafts();
}

function updateComposerDrafts(
  update: (current: Record<string, ComposerDraft>) => Record<string, ComposerDraft>,
): void {
  const current = appAtomRegistry.get(composerDraftsAtom);
  const next = update(current);
  if (next === current) {
    return;
  }
  appAtomRegistry.set(composerDraftsAtom, next);
  schedulePersistComposerState();
}

export function setStickyComposerModelSelection(modelSelection: ModelSelection): void {
  appAtomRegistry.set(stickyComposerModelSelectionAtom, modelSelection);
  schedulePersistComposerState();
}

export function setComposerDraftText(draftKey: string, value: string): void {
  let removed: ReadonlyArray<DraftComposerAttachment> = [];
  updateComposerDrafts((current) => {
    const existing = normalizeDraft(current[draftKey]);
    const context = referencedComposerContext(value, existing.context);
    const draft = withReferencedContextFiles(existing, value, context);
    removed = existing.attachments.filter((attachment) => !draft.attachments.includes(attachment));
    return withComposerDraft(current, draftKey, draft);
  });
  scheduleUnusedComposerAttachmentCleanup(removed);
}

export function insertComposerDraftText(
  draftKey: string,
  value: string,
  target: ComposerDraftInsertion,
): void {
  const draft = getComposerDraftSnapshot(draftKey);
  const { start, end } = contextInsertionRange(draftKey, draft, target);
  const text = draft.text.slice(0, start) + value + draft.text.slice(end);
  const caret = caretAfterInsertion(draftKey, draft.text, { start, end }, value.length);
  setComposerDraftText(draftKey, text);
  rememberComposerDraftSelection(draftKey, text, caret);
}

export function appendComposerDraftText(draftKey: string, value: string): void {
  updateComposerDrafts((current) => {
    const existing = normalizeDraft(current[draftKey]);
    return {
      ...current,
      [draftKey]: {
        ...existing,
        text: `${existing.text}${value}`,
      },
    };
  });
}

// Pasted images all share one name, so an image's bytes decide whether it is a repeat.
function isSameAttachment(a: DraftComposerAttachment, b: DraftComposerAttachment): boolean {
  if (a.mimeType !== b.mimeType || a.sizeBytes !== b.sizeBytes || a.name !== b.name) return false;
  if (a.type !== "image" || b.type !== "image") return true;
  if (a.contentDigest !== undefined && b.contentDigest !== undefined) {
    return a.contentDigest === b.contentDigest;
  }
  return a.dataUrl === b.dataUrl && a.fileUri === b.fileUri;
}

/**
 * Restore puts back attachments after failed sends, so it skips the duplicate check and the caps.
 * Returns how many attachments the caps rejected.
 */
export function appendComposerDraftAttachments(
  draftKey: string,
  incoming: ReadonlyArray<DraftComposerAttachment>,
  options?: {
    readonly restore?: boolean;
    readonly appendReference?: boolean;
    readonly insertion?: ComposerDraftInsertion;
    readonly maxAttachments?: number;
  },
): number {
  if (incoming.length === 0) {
    return 0;
  }
  let rejected: ReadonlyArray<DraftComposerAttachment> = [];
  let removed: ReadonlyArray<DraftComposerAttachment> = [];
  let duplicates: ReadonlyArray<DraftComposerAttachment> = [];
  updateComposerDrafts((current) => {
    const existing = normalizeDraft(current[draftKey]);
    const retained = options?.appendReference
      ? draftWithoutInsertionSelection(draftKey, existing, options.insertion)
      : existing;
    const seen = [...retained.attachments];
    const attachments = options?.restore
      ? incoming
      : incoming.filter((attachment) => {
          if (seen.some((other) => isSameAttachment(other, attachment))) return false;
          seen.push(attachment);
          return true;
        });
    duplicates = incoming.filter((attachment) => !attachments.includes(attachment));
    const remaining = options?.restore
      ? attachments.length
      : Math.max(
          0,
          Math.min(
            PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
            options?.maxAttachments ?? PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
          ) - retained.attachments.length,
        );
    const contextCapacity =
      options?.appendReference && !options.restore
        ? Math.max(0, COMPOSER_CONTEXT_MAX_RECORDS - (retained.context?.records.length ?? 0))
        : attachments.length;
    const accepted = attachments.slice(0, Math.min(remaining, contextCapacity));
    rejected = attachments.slice(accepted.length);
    if (accepted.length === 0) {
      return current;
    }
    let draft = { ...existing, attachments: [...existing.attachments, ...accepted] };
    if (options?.appendReference) {
      const records = accepted.map((attachment) => attachmentContextRecord(attachment));
      const inserted = draftWithInsertedContext(
        draftKey,
        draft,
        {
          text: records.map(formatComposerContextReference).join(" "),
          context: { version: 1, records },
        },
        options.insertion,
        options.restore ? Infinity : COMPOSER_CONTEXT_MAX_RECORDS,
      );
      if (!inserted) {
        rejected = attachments;
        return current;
      }
      draft = { ...inserted, attachments: [...inserted.attachments] };
      removed = existing.attachments.filter(
        (attachment) => !draft.attachments.includes(attachment),
      );
    }
    return {
      ...current,
      [draftKey]: draft,
    };
  });
  scheduleUnusedComposerAttachmentCleanup([...rejected, ...removed, ...duplicates]);
  return rejected.length;
}

export function replaceComposerDraftAttachments(
  draftKey: string,
  attachments: ReadonlyArray<DraftComposerAttachment>,
): void {
  const previousAttachments = getComposerDraftSnapshot(draftKey).attachments;
  const retainedIds = new Set(attachments.map((attachment) => attachment.id));
  updateComposerDrafts((current) => {
    const existing = normalizeDraft(current[draftKey]);
    const droppedContextIds = new Set(
      existing.context?.records
        .filter((record) => "attachmentId" in record && !retainedIds.has(record.attachmentId))
        .map((record) => record.contextId),
    );
    const text = replaceComposerContextReferences(existing.text, (ref) =>
      droppedContextIds.has(ref.contextId) ? "" : ref.source,
    );
    const draft = {
      ...existing,
      text,
      context: referencedComposerContext(text, existing.context),
      attachments,
    };
    return withComposerDraft(current, draftKey, draft);
  });
  scheduleUnusedComposerAttachmentCleanup(
    previousAttachments.filter((attachment) => !retainedIds.has(attachment.id)),
  );
}

export function removeComposerDraftAttachment(draftKey: string, imageId: string): void {
  const previousAttachments = getComposerDraftSnapshot(draftKey).attachments;
  updateComposerDrafts((current) => {
    const existing = normalizeDraft(current[draftKey]);
    const removedIds = new Set(
      existing.context?.records
        .filter((record) => "attachmentId" in record && record.attachmentId === imageId)
        .map((record) => record.contextId),
    );
    const text = replaceComposerContextReferences(existing.text, (ref) =>
      removedIds.has(ref.contextId) ? "" : ref.source,
    );
    const draft = {
      ...existing,
      text,
      context: referencedComposerContext(text, existing.context),
      attachments: existing.attachments.filter((image) => image.id !== imageId),
    };
    return withComposerDraft(current, draftKey, draft);
  });
  scheduleUnusedComposerAttachmentCleanup(
    previousAttachments.filter((attachment) => attachment.id === imageId),
  );
}

export function setComposerDraftAttachmentUpload(
  draftKey: string,
  attachment: DraftComposerAttachment,
): boolean {
  let previous: DraftComposerAttachment | undefined;
  updateComposerDrafts((current) => {
    const draft = current[draftKey];
    previous = draft?.attachments.find((candidate) => candidate.id === attachment.id);
    if (!draft || !previous) return current;
    if (
      previous.uploadedAttachmentId === attachment.uploadedAttachmentId &&
      previous.uploadEnvironmentId === attachment.uploadEnvironmentId
    )
      return current;
    return {
      ...current,
      [draftKey]: {
        ...draft,
        attachments: draft.attachments.map((candidate) =>
          candidate.id === attachment.id
            ? {
                ...candidate,
                uploadedAttachmentId: attachment.uploadedAttachmentId,
                uploadEnvironmentId: attachment.uploadEnvironmentId,
              }
            : candidate,
        ),
      },
    };
  });
  if (previous) scheduleUnusedComposerAttachmentCleanup([previous]);
  return previous !== undefined;
}

export function updateComposerDraftSettings(
  draftKey: string,
  settings: Partial<ComposerDraftSettingsUpdate>,
): ComposerDraft {
  if (
    Object.hasOwn(settings, "modelSelection") ||
    Object.hasOwn(settings, "automaticAccountModel")
  ) {
    recordComposerModelSelectionInteraction(draftKey);
  }
  // Bind before publishing: subscribers may synchronously clear or reselect a model.
  const draft = captureComposerModelSelectionSnapshot(draftKey, {
    ...normalizeDraft(appAtomRegistry.get(composerDraftsAtom)[draftKey]),
    ...settings,
    ...(settings.modelSelection === undefined
      ? {}
      : {
          modelSelection: captureExplicitComposerModelPick(settings.modelSelection),
        }),
  });
  updateComposerDrafts((current) => withComposerDraft(current, draftKey, draft));
  return draft;
}

export function consumeSubmittedComposerModelPick(
  draftKey: string,
  submittedPick: ModelSelection | undefined,
): void {
  updateComposerDrafts((current) => {
    const draft = current[draftKey];
    if (!draft) return current;
    const next = consumeSubmittedModelPick(draft, submittedPick);
    if (next === draft) return current;
    recordComposerModelSelectionInteraction(draftKey);
    return withComposerDraft(current, draftKey, next);
  });
}

/** Lets the thread's model show again once the server accepted this exact pick. */
export function releaseComposerDraftModelSelection(
  draftKey: string,
  modelSelection: ModelSelection,
): void {
  updateComposerDrafts((current) => {
    const existing = current[draftKey];
    // A newer pick, even of the same model, is still waiting for its own reply.
    if (existing?.modelSelection !== modelSelection) return current;
    const { modelSelection: _modelSelection, ...draft } = existing;
    return withComposerDraft(current, draftKey, draft);
  });
}

export function setFreshSessionPending(draftKey: string, pending: boolean): void {
  if ((getComposerDraftSnapshot(draftKey).freshContext === true) === pending) return;
  updateComposerDraftSettings(draftKey, { freshContext: pending ? true : undefined });
}

export function isFreshSessionPending(draftKey: string): boolean {
  return getComposerDraftSnapshot(draftKey).freshContext === true;
}

export function subscribeComposerDraftModelReconciliation(_input: {
  readonly registry: AtomRegistry.AtomRegistry;
  readonly threadRefsAtom: Atom.Atom<ReadonlyArray<ScopedThreadRef>>;
  readonly threadShellAtom: (ref: ScopedThreadRef) => Atom.Atom<EnvironmentThreadShell | null>;
  readonly shellStateAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<AsyncResult.AsyncResult<EnvironmentShellState, unknown>>;
  readonly pendingThreadIdsAtom: (environmentId: EnvironmentId) => Atom.Atom<ReadonlySet<ThreadId>>;
}): () => void {
  // A live shell matching a picker value does not acknowledge the user's send.
  // Keep the existing subscriber API; durable enqueue consumes only its captured pick.
  return () => {};
}

export function clearComposerDraftContentState(
  current: Record<string, ComposerDraft>,
  draftKey: string,
  options?: {
    readonly clearModelSelection?: boolean;
    readonly clearWorkspaceSelection?: boolean;
  },
): Record<string, ComposerDraft> {
  const existing = current[draftKey];
  if (!existing) {
    return current;
  }
  const {
    importedShareIds: _importedShareIds,
    context: _context,
    modelSelection,
    automaticAccountModel,
    workspaceSelection,
    project: _project,
    ...retained
  } = existing;
  const draft = {
    ...retained,
    ...(options?.clearModelSelection || modelSelection === undefined ? {} : { modelSelection }),
    ...(options?.clearModelSelection || automaticAccountModel === undefined
      ? {}
      : { automaticAccountModel }),
    ...(options?.clearWorkspaceSelection || workspaceSelection === undefined
      ? {}
      : { workspaceSelection }),
    text: "",
    attachments: [],
  };
  if (isEmptyDraft(draft)) {
    const next = { ...current };
    delete next[draftKey];
    return next;
  }
  return {
    ...current,
    [draftKey]: draft,
  };
}

export function restoreComposerDraftSnapshotState(
  current: Record<string, ComposerDraft>,
  draftKey: string,
  snapshot: ComposerDraft,
): Record<string, ComposerDraft> {
  const next = { ...current };
  if (isEmptyDraft(snapshot)) {
    delete next[draftKey];
  } else {
    next[draftKey] = snapshot;
  }
  return next;
}

function stripAttachmentUploadReference(
  attachment: DraftComposerAttachment,
): DraftComposerAttachment {
  const { uploadedAttachmentId: _id, uploadEnvironmentId: _environmentId, ...rest } = attachment;
  return rest;
}

function mergeComposerDraftText(existing: string, incoming: string): string {
  if (incoming.length === 0) {
    return existing;
  }
  if (existing.length === 0) {
    return incoming;
  }
  if (existing === incoming || existing.endsWith(`\n\n${incoming}`)) {
    return existing;
  }
  return `${existing}\n\n${incoming}`;
}

function mergeReferencedComposerContext(
  text: string,
  first?: OrchestrationMessageContext,
  second?: OrchestrationMessageContext,
) {
  const records = new Map((first?.records ?? []).map((record) => [record.contextId, record]));
  for (const record of second?.records ?? []) records.set(record.contextId, record);
  if (records.size === 0) return undefined;
  return referencedComposerContext(text, { version: 1, records: [...records.values()] });
}

export function mergeComposerDraftContentState(
  current: Record<string, ComposerDraft>,
  draftKey: string,
  content: ComposerDraftContent,
  restoreAttachments = false,
): Record<string, ComposerDraft> {
  const existing = normalizeDraft(current[draftKey]);
  if (content.sourceShareId && existing.importedShareIds?.includes(content.sourceShareId)) {
    return current;
  }
  const attachmentIds = new Set(existing.attachments.map((attachment) => attachment.id));
  const incomingAttachments = content.attachments.filter((attachment) => {
    if (attachmentIds.has(attachment.id)) {
      return false;
    }
    attachmentIds.add(attachment.id);
    return true;
  });
  const combinedAttachments = [...existing.attachments, ...incomingAttachments];
  const attachments = restoreAttachments
    ? combinedAttachments
    : combinedAttachments.slice(0, PROVIDER_SEND_TURN_MAX_ATTACHMENTS);
  const text = mergeComposerDraftText(existing.text, content.text);
  const context = mergeReferencedComposerContext(text, existing.context, content.context);
  const importedShareIds = content.sourceShareId
    ? [...(existing.importedShareIds ?? []), content.sourceShareId]
    : existing.importedShareIds;
  if (
    text === existing.text &&
    attachments.length === existing.attachments.length &&
    content.context === undefined &&
    importedShareIds === existing.importedShareIds
  ) {
    return current;
  }
  return {
    ...current,
    // The composer shows non-image files only as inline chips, so a shared file
    // merged without one would be attached but invisible until sent. Other
    // merges carry their own text, and appending chips to it would defeat
    // mergeComposerDraftText's duplicate check on a retried merge.
    [draftKey]: restoreMissingComposerFileReferences(
      {
        ...existing,
        text,
        attachments,
        context,
        ...(importedShareIds ? { importedShareIds } : {}),
      },
      content.sourceShareId ? attachments.slice(existing.attachments.length) : [],
    ),
  };
}

/** Persists before the share inbox item can be acknowledged. */
export async function mergeComposerDraftContent(
  draftKey: string,
  content: ComposerDraftContent,
  options?: {
    readonly onMerged?: (draft: ComposerDraft) => void;
    /** Recovery preserves every file and leaves trimming to the editor. */
    readonly restoreAttachments?: boolean;
  },
): Promise<{ readonly skippedAttachmentCount: number }> {
  ensureComposerDraftsLoaded();
  if (loadPromise !== null) {
    await loadPromise;
  }
  if (persistTimer !== null) {
    clearTimeout(persistTimer);
    persistTimer = null;
  }
  const current = appAtomRegistry.get(composerDraftsAtom);
  const next = mergeComposerDraftContentState(
    current,
    draftKey,
    content,
    options?.restoreAttachments,
  );
  const currentAttachmentIds = new Set(
    normalizeDraft(current[draftKey]).attachments.map((attachment) => attachment.id),
  );
  const nextAttachmentIds = new Set(
    normalizeDraft(next[draftKey]).attachments.map((attachment) => attachment.id),
  );
  const skippedAttachmentCount = content.attachments.filter(
    (attachment) =>
      !currentAttachmentIds.has(attachment.id) && !nextAttachmentIds.has(attachment.id),
  ).length;
  // Publish content and receipt before awaiting disk; concurrent typing then preserves the receipt in later serialized writes.
  if (next !== current) {
    appAtomRegistry.set(composerDraftsAtom, next);
  }
  options?.onMerged?.(normalizeDraft(next[draftKey]));
  await persistenceQueue.run(() =>
    writePersistedComposerState(next, appAtomRegistry.get(stickyComposerModelSelectionAtom)),
  );
  return { skippedAttachmentCount };
}

export function sameComposerDraftState(a: ComposerDraft, b: ComposerDraft): boolean {
  return (
    a.text === b.text &&
    a.attachments === b.attachments &&
    a.context === b.context &&
    a.importedShareIds === b.importedShareIds &&
    a.modelSelection === b.modelSelection &&
    a.automaticAccountModel === b.automaticAccountModel &&
    a.runtimeMode === b.runtimeMode &&
    a.interactionMode === b.interactionMode &&
    a.freshContext === b.freshContext &&
    a.workspaceSelection === b.workspaceSelection
  );
}

/** Restore untouched drafts exactly; after concurrent edits, remove only merged text and attachments. */
export function undoComposerDraftMergeState(
  current: Record<string, ComposerDraft>,
  draftKey: string,
  snapshot: ComposerDraft,
  merged: ComposerDraft,
): Record<string, ComposerDraft> {
  const existing = normalizeDraft(current[draftKey]);
  const modelSelectionUntouched = composerModelSelectionSnapshotIsCurrent(draftKey, merged);
  if (modelSelectionUntouched && sameComposerDraftState(existing, merged)) {
    return restoreComposerDraftSnapshotState(current, draftKey, snapshot);
  }
  const insertedText = merged.text.startsWith(snapshot.text)
    ? merged.text.slice(snapshot.text.length)
    : "";
  const snapshotAttachmentIds = new Set(snapshot.attachments.map((attachment) => attachment.id));
  const insertedAttachmentIds = new Set(
    merged.attachments
      .filter((attachment) => !snapshotAttachmentIds.has(attachment.id))
      .map((attachment) => attachment.id),
  );
  const undoSetting = <
    K extends
      | "modelSelection"
      | "automaticAccountModel"
      | "runtimeMode"
      | "interactionMode"
      | "freshContext"
      | "workspaceSelection",
  >(
    key: K,
  ): ComposerDraft[K] => (existing[key] === merged[key] ? snapshot[key] : existing[key]);
  const text =
    insertedText.length > 0 && existing.text.startsWith(merged.text)
      ? snapshot.text + existing.text.slice(merged.text.length)
      : insertedText.length > 0 && existing.text.endsWith(insertedText)
        ? existing.text.slice(0, existing.text.length - insertedText.length)
        : existing.text;
  const draft = {
    ...existing,
    text,
    context: referencedComposerContext(text, existing.context),
    attachments: existing.attachments.filter(
      (attachment) => !insertedAttachmentIds.has(attachment.id),
    ),
    modelSelection: modelSelectionUntouched
      ? undoSetting("modelSelection")
      : existing.modelSelection,
    automaticAccountModel: modelSelectionUntouched
      ? undoSetting("automaticAccountModel")
      : existing.automaticAccountModel,
    runtimeMode: undoSetting("runtimeMode"),
    interactionMode: undoSetting("interactionMode"),
    freshContext: undoSetting("freshContext"),
    workspaceSelection: undoSetting("workspaceSelection"),
  };
  return withComposerDraft(current, draftKey, draft);
}

/** Withdraw a merge and any following recovery merge in one draft write. */
export async function undoComposerDraftMerge(
  draftKey: string,
  snapshot: ComposerDraft,
  merged: ComposerDraft,
  followingMerge?: { readonly before: ComposerDraft; readonly after: ComposerDraft },
): Promise<void> {
  ensureComposerDraftsLoaded();
  if (loadPromise !== null) {
    await loadPromise;
  }
  if (persistTimer !== null) {
    clearTimeout(persistTimer);
    persistTimer = null;
  }
  const current = appAtomRegistry.get(composerDraftsAtom);
  const beforeFollowing = followingMerge
    ? undoComposerDraftMergeState(current, draftKey, followingMerge.before, followingMerge.after)
    : current;
  const next = undoComposerDraftMergeState(beforeFollowing, draftKey, snapshot, merged);
  appAtomRegistry.set(composerDraftsAtom, next);
  await persistenceQueue.run(() =>
    writePersistedComposerState(next, appAtomRegistry.get(stickyComposerModelSelectionAtom)),
  );
}

export function clearComposerDraftContent(
  draftKey: string,
  options?: {
    readonly clearModelSelection?: boolean;
    readonly clearWorkspaceSelection?: boolean;
    // Defer sweeping during enqueue; rollback can briefly remove ownership before restoring attachments.
    readonly deferAttachmentCleanup?: boolean;
  },
): void {
  if (options?.clearModelSelection) recordComposerModelSelectionInteraction(draftKey);
  const previousAttachments = getComposerDraftSnapshot(draftKey).attachments;
  markComposerDraftReset(draftKey);
  updateComposerDrafts((current) => clearComposerDraftContentState(current, draftKey, options));
  if (!options?.deferAttachmentCleanup) {
    scheduleUnusedComposerAttachmentCleanup(previousAttachments);
  }
}

export function clearComposerDraft(
  draftKey: string,
  options?: { readonly deferAttachmentCleanup?: boolean },
): void {
  recordComposerModelSelectionInteraction(draftKey);
  abortComposerDraftOperations(draftKey);
  markComposerDraftReset(draftKey);
  const previousAttachments = getComposerDraftSnapshot(draftKey).attachments;
  updateComposerDrafts((current) => {
    if (!current[draftKey]) {
      return current;
    }
    const next = { ...current };
    const correlation = current[draftKey]?.importedHistoryCorrelation;
    if (correlation === undefined) delete next[draftKey];
    else next[draftKey] = { ...EMPTY_DRAFT, importedHistoryCorrelation: correlation };
    return next;
  });
  if (!options?.deferAttachmentCleanup) {
    scheduleUnusedComposerAttachmentCleanup(previousAttachments);
  }
}

export function removeComposerDraftsForEnvironment(
  drafts: Record<string, ComposerDraft>,
  environmentId: EnvironmentId,
): Record<string, ComposerDraft> {
  const environmentPrefix = `${environmentId}:`;
  return Object.fromEntries(
    Object.entries(drafts).filter(
      ([draftKey, draft]) =>
        !draftKey.startsWith(environmentPrefix) && draft.project?.environmentId !== environmentId,
    ),
  );
}

export function createNewTaskDraft(project: {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId;
}): string {
  const draftKey = newTaskDraftKey(newDraftId());
  const stamp: ComposerDraftProject = {
    environmentId: project.environmentId,
    projectId: project.projectId,
    createdAt: new Date().toISOString(),
  };
  updateComposerDrafts((current) => ({
    ...current,
    [draftKey]: { ...EMPTY_DRAFT, project: stamp },
  }));
  return draftKey;
}

export function retargetNewTaskDraft(
  draftKey: string,
  project: { readonly environmentId: EnvironmentId; readonly projectId: ProjectId },
  options?: { readonly keepPullRequestWorkspace?: boolean },
): void {
  updateComposerDrafts((current) => {
    const existing = current[draftKey];
    const stamp = existing?.project;
    if (
      stamp !== undefined &&
      stamp.environmentId === project.environmentId &&
      stamp.projectId === project.projectId
    ) {
      return current;
    }
    const { workspaceSelection, ...retained } = normalizeDraft(existing);
    // The PR checkout is recreated on the target machine, so only the old local path is dropped.
    const keptWorkspaceSelection =
      options?.keepPullRequestWorkspace && workspaceSelection?.pullRequestReference !== undefined
        ? { ...workspaceSelection, worktreePath: null }
        : undefined;
    // Keep bytes across environment moves but drop server-specific upload stamps.
    const attachments = retained.attachments.map((attachment) =>
      attachment.uploadEnvironmentId !== undefined &&
      attachment.uploadEnvironmentId !== project.environmentId
        ? stripAttachmentUploadReference(attachment)
        : attachment,
    );
    return {
      ...current,
      [draftKey]: {
        ...retained,
        ...(keptWorkspaceSelection ? { workspaceSelection: keptWorkspaceSelection } : {}),
        attachments,
        project: {
          environmentId: project.environmentId,
          projectId: project.projectId,
          createdAt: stamp?.createdAt ?? new Date().toISOString(),
        },
      },
    };
  });
}

/** Retargets a saved draft after loading. Returns false if absent or cancelled before the write. */
export async function retargetSavedNewTaskDraft(
  draftKey: string,
  project: { readonly environmentId: EnvironmentId; readonly projectId: ProjectId },
  signal?: AbortSignal,
): Promise<boolean> {
  await waitForComposerDraftsLoaded();
  if (signal?.aborted || appAtomRegistry.get(composerDraftsAtom)[draftKey] === undefined)
    return false;
  retargetNewTaskDraft(draftKey, project);
  return true;
}

export function findNewTaskDraftKeys(
  drafts: Readonly<Record<string, ComposerDraft>>,
  project: { readonly environmentId: EnvironmentId; readonly projectId: ProjectId },
): ReadonlyArray<string> {
  return Object.entries(drafts)
    .filter(
      ([key, draft]) =>
        isNewTaskDraftKey(key) &&
        draft.project?.environmentId === project.environmentId &&
        draft.project.projectId === project.projectId,
    )
    .sort(([, left], [, right]) =>
      (right.project?.createdAt ?? "").localeCompare(left.project?.createdAt ?? ""),
    )
    .map(([key]) => key);
}

export async function clearComposerDraftsEnvironment(environmentId: EnvironmentId): Promise<void> {
  ensureComposerDraftsLoaded();
  if (loadPromise !== null) {
    await loadPromise;
  }

  const current = appAtomRegistry.get(composerDraftsAtom);
  const next = removeComposerDraftsForEnvironment(current, environmentId);
  const removedAttachments = Object.entries(current)
    .filter(([draftKey]) => next[draftKey] === undefined)
    .flatMap(([, draft]) => draft.attachments);
  // Keys with no stored draft yet (an empty thread composer) can still have a pick running.
  const environmentPrefix = `${environmentId}:`;
  abortComposerDraftOperationsWhere(
    (draftKey) =>
      draftKey.startsWith(environmentPrefix) ||
      (current[draftKey] !== undefined && next[draftKey] === undefined),
  );

  if (persistTimer !== null) {
    clearTimeout(persistTimer);
    persistTimer = null;
  }
  appAtomRegistry.set(composerDraftsAtom, next);
  await persistenceQueue.run(() =>
    writePersistedComposerState(next, appAtomRegistry.get(stickyComposerModelSelectionAtom)),
  );
  await releaseUnusedComposerAttachmentFiles(removedAttachments);
}

const composerDraftAtom = Atom.family((draftKey: string) =>
  Atom.make((get) => get(composerDraftsAtom)[draftKey] ?? EMPTY_DRAFT),
);
const EMPTY_DRAFT_ATOM = Atom.make(EMPTY_DRAFT);

type ComposerDraftsByKey = Readonly<Record<string, ComposerDraft>>;
const EMPTY_DRAFTS: ComposerDraftsByKey = {};
function sameDrafts(a: ComposerDraftsByKey, b: ComposerDraftsByKey): boolean {
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((key) => a[key] === b[key]);
}
export const questionAttachmentDraftsAtom = Atom.family((prefix: string) =>
  Atom.make((get): ComposerDraftsByKey => {
    if (prefix === "") return EMPTY_DRAFTS;
    const matching = Object.entries(get(composerDraftsAtom)).filter(([key]) =>
      key.startsWith(prefix),
    );
    return matching.length === 0 ? EMPTY_DRAFTS : Object.fromEntries(matching);
  }).pipe(Atom.withEquality(sameDrafts)),
);

export const newTaskComposerDraftsAtom = Atom.make((get): ComposerDraftsByKey => {
  const matching = Object.entries(get(composerDraftsAtom)).filter(([key]) =>
    isNewTaskDraftKey(key),
  );
  return matching.length === 0 ? EMPTY_DRAFTS : Object.fromEntries(matching);
}).pipe(Atom.withEquality(sameDrafts));

type ComposerDraftUploadSource = Pick<ComposerDraft, "attachments" | "project">;
type ComposerDraftUploadSources = Readonly<Record<string, ComposerDraftUploadSource>>;
export const composerDraftUploadSourcesAtom = Atom.make((get): ComposerDraftUploadSources =>
  Object.fromEntries(
    Object.entries(get(composerDraftsAtom)).map(([key, draft]) => [
      key,
      { attachments: draft.attachments, project: draft.project },
    ]),
  ),
).pipe(
  Atom.withEquality((a: ComposerDraftUploadSources, b: ComposerDraftUploadSources) => {
    const keys = Object.keys(a);
    return (
      keys.length === Object.keys(b).length &&
      keys.every(
        (key) => a[key]?.attachments === b[key]?.attachments && a[key]?.project === b[key]?.project,
      )
    );
  }),
);

type ComposerDraftSettings = Pick<
  ComposerDraft,
  "modelSelection" | "runtimeMode" | "interactionMode" | "freshContext"
>;
const EMPTY_DRAFT_SETTINGS: ComposerDraftSettings = {};
const composerDraftSettingsAtom = Atom.family((draftKey: string) =>
  Atom.make((get): ComposerDraftSettings => {
    const draft = get(composerDraftAtom(draftKey));
    return {
      modelSelection: draft.modelSelection,
      runtimeMode: draft.runtimeMode,
      interactionMode: draft.interactionMode,
      freshContext: draft.freshContext,
    };
  }).pipe(
    Atom.withEquality(
      (a: ComposerDraftSettings, b: ComposerDraftSettings) =>
        a.modelSelection === b.modelSelection &&
        a.runtimeMode === b.runtimeMode &&
        a.interactionMode === b.interactionMode &&
        a.freshContext === b.freshContext,
    ),
  ),
);
const EMPTY_DRAFT_SETTINGS_ATOM = Atom.make(EMPTY_DRAFT_SETTINGS);

export function useComposerDraftSettings(draftKey: string | null): ComposerDraftSettings {
  const settings = useAtomValue(
    draftKey ? composerDraftSettingsAtom(draftKey) : EMPTY_DRAFT_SETTINGS_ATOM,
  );
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- starts loading persisted composer drafts from native storage on first use
  useEffect(() => {
    ensureComposerDraftsLoaded();
  }, []);
  return settings;
}

export function useComposerDraft(draftKey: string | null): ComposerDraft {
  const draft = useAtomValue(draftKey ? composerDraftAtom(draftKey) : EMPTY_DRAFT_ATOM);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- starts loading persisted composer drafts from native storage on first use
  useEffect(() => {
    ensureComposerDraftsLoaded();
  }, []);
  return draft;
}

export function useStickyComposerModelSelection(): ModelSelection | null {
  const selection = useAtomValue(stickyComposerModelSelectionAtom);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- starts loading persisted composer drafts from native storage on first use
  useEffect(() => {
    ensureComposerDraftsLoaded();
  }, []);
  return selection;
}

const importedHistoryLocks = new Map<string, SerializedAsyncQueue>();
export function mobileImportedHistoryStorage(
  environmentId: EnvironmentId,
  threadId: ThreadId,
): ImportedHistoryCorrelationStorage {
  const key = scopedThreadKey({ environmentId, threadId });
  let lock = importedHistoryLocks.get(key);
  if (lock === undefined) {
    lock = new SerializedAsyncQueue();
    importedHistoryLocks.set(key, lock);
  }
  const queue = lock;
  const read = () => {
    const raw = getComposerDraftSnapshot(key).importedHistoryCorrelation;
    if (raw === undefined) return null;
    const value = decodeImportedHistoryCorrelation(raw);
    if (value.environmentId !== environmentId || value.command.threadId !== threadId)
      throw new Error("Saved imported history choice belongs to another environment or thread.");
    return value;
  };
  return {
    withLock: (operation) =>
      queue.run(async () => {
        ensureComposerDraftsLoaded();
        if (loadPromise !== null) await loadPromise;
        return operation();
      }),
    read,
    reserve(value) {
      if (
        read() !== null ||
        value.environmentId !== environmentId ||
        value.command.threadId !== threadId
      )
        throw new Error("Imported history correlation cannot be replaced.");
      const encoded = encodeImportedHistoryCorrelation(value);
      updateComposerDrafts((current) => ({
        ...current,
        [key]: { ...normalizeDraft(current[key]), importedHistoryCorrelation: encoded },
      }));
    },
    remove(value) {
      if (
        getComposerDraftSnapshot(key).importedHistoryCorrelation !==
        encodeImportedHistoryCorrelation(value)
      )
        return;
      updateComposerDrafts((current) => {
        const { importedHistoryCorrelation: _correlation, ...draft } = normalizeDraft(current[key]);
        return withComposerDraft(current, key, draft);
      });
    },
  };
}
export async function flushMobileImportedHistoryCorrelation(
  value: ImportedHistoryCorrelation,
): Promise<void> {
  const key = scopedThreadKey({
    environmentId: value.environmentId,
    threadId: value.command.threadId,
  });
  const encoded = encodeImportedHistoryCorrelation(value);
  await flushComposerDrafts();
  const persisted = await loadPersistedComposerState();
  if (persisted.drafts[key]?.importedHistoryCorrelation !== encoded)
    throw new Error("Imported history correlation was not persisted exactly.");
}
