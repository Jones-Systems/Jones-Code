import { useThreadReferenceCapture } from "../threadReferenceExcerpt/useThreadReferenceCapture";
import { useEffect, useState } from "react";
import {
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  type ComposerContextClipboardFragment,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { fileAttachmentTooLargeMessage } from "@t3tools/client-runtime/state/attachments";
import { replaceComposerContextReferences } from "@t3tools/shared/composerContextReferences";
import {
  useComposerDraftStore,
  composerFileNeedsReattach,
  type ComposerFileAttachment,
  type ComposerImageAttachment,
} from "../../composerDraftStore";
import {
  changeQuestionAttachmentPreparation,
  clearQuestionAttachmentDraft,
  countQuestionAttachments,
  questionAttachmentDraftId,
  useQuestionAttachmentPreparation,
} from "../../questionAttachments";
import {
  classifyComposerAttachmentFile,
  fileAttachmentStagingLimit,
  normalizeComposerImageFileMimeType,
} from "../../components/chat/composerAttachmentFiles";
import { prepareImageForAttachment } from "../../lib/imageCompression";
import {
  releaseDraftAttachment,
  retryAttachmentUpload,
  startAttachmentUpload,
  useAttachmentUploadStore,
} from "../../lib/attachmentUploadQueue";
import { videoMimeType } from "../../types";
import { randomUUID } from "../../lib/utils";
import {
  asKnownContextRecord,
  threadContextRecord,
  threadContextReference,
} from "../../lib/composerContextRecords";
import { formatInlineContextReference } from "../../lib/composerContextReferences";
import { readThreadShell } from "../../state/entities";
import { useAtomQueryRunner } from "../../state/use-atom-query-runner";
import { assetEnvironment } from "../../state/assets";
import { readPreparedConnection } from "../../state/session";
import { resolveAssetUrl } from "../../assets/assetUrls";
import type { PendingUserInput } from "../../session-logic";
import type { QuestionAnswerAttachmentCapabilities } from "./questionAnswerCard";

// A collapsed card may unmount while preparing files. Tokens keep Remove all
// from letting an older completion enter a newly populated draft for that question.
const preparations = new Map<string, Set<symbol>>();

export function useQuestionAnswerAttachments(input: {
  threadRef: ScopedThreadRef;
  prompt: PendingUserInput;
  questionId: string;
  disabled: boolean;
  allowAdding: boolean;
  capabilities: QuestionAnswerAttachmentCapabilities;
}) {
  const { threadRef, prompt, questionId, disabled, capabilities } = input;
  const { environmentId, threadId } = threadRef;
  const target = questionAttachmentDraftId(environmentId, threadId, prompt.requestId, questionId);
  const keys = prompt.questions.map((q) =>
    questionAttachmentDraftId(environmentId, threadId, prompt.requestId, q.id),
  );
  const addCapturedThreadContexts = useThreadReferenceCapture(target, target);
  const draft = useComposerDraftStore((state) => state.draftsByThreadKey[target]);
  const uploads = useAttachmentUploadStore((state) => state.uploadsByImageId);
  const preparationCount = useQuestionAttachmentPreparation((state) => state.counts[target] ?? 0);
  const [error, setError] = useState<{ target: string; message: string } | null>(null);
  const createAssetUrl = useAtomQueryRunner(assetEnvironment.createUrl, { reportFailure: false });
  const fileLimit = fileAttachmentStagingLimit(capabilities);
  const attachments = [...(draft?.images ?? []), ...(draft?.files ?? [])];
  const canAttach = !disabled && input.allowAdding && capabilities.supportsQuestionAttachments;

  useEffect(() => {
    if (
      disabled ||
      !capabilities.supportsQuestionAttachments ||
      !capabilities.supportsAttachmentUploads
    )
      return;
    for (const attachment of [...(draft?.images ?? []), ...(draft?.files ?? [])]) {
      if (
        attachment.type === "file" &&
        (composerFileNeedsReattach(attachment) ||
          fileLimit === null ||
          attachment.sizeBytes > fileLimit)
      )
        continue;
      startAttachmentUpload({ environmentId, image: attachment, draftTarget: target });
    }
  }, [
    disabled,
    capabilities.supportsQuestionAttachments,
    capabilities.supportsAttachmentUploads,
    draft?.images,
    draft?.files,
    environmentId,
    fileLimit,
    target,
  ]);

  const report = (message: string) => setError({ target, message });
  const reserve = () => {
    if (!canAttach) {
      report("This question cannot accept attachments right now.");
      return null;
    }
    if (countQuestionAttachments(keys) >= PROVIDER_SEND_TURN_MAX_ATTACHMENTS) {
      report(
        `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per answer request.`,
      );
      return null;
    }
    const token = Symbol();
    const current = preparations.get(target) ?? new Set<symbol>();
    current.add(token);
    preparations.set(target, current);
    changeQuestionAttachmentPreparation(target, 1);
    return {
      active: () =>
        preparations.get(target)?.has(token) === true &&
        (useQuestionAttachmentPreparation.getState().counts[target] ?? 0) > 0,
      release: () => {
        const pending = preparations.get(target);
        if (!pending?.delete(token)) return;
        if (pending.size === 0) preparations.delete(target);
        changeQuestionAttachmentPreparation(target, -1);
      },
    };
  };
  const storeFile = async (source: File, isActive: () => boolean) => {
    const kind = classifyComposerAttachmentFile(source);
    if (kind === "unsupported-image") {
      report(`'${source.name}' is not a supported image type.`);
      return;
    }
    let attachment: ComposerImageAttachment | ComposerFileAttachment;
    if (kind === "image") {
      const prepared = await prepareImageForAttachment(
        normalizeComposerImageFileMimeType(source),
        PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
      );
      if (!prepared.ok) {
        report(`'${source.name}' could not be prepared as an image.`);
        return;
      }
      if (!isActive()) return;
      const file = prepared.file;
      attachment = {
        type: "image",
        id: randomUUID(),
        name: file.name || "image",
        mimeType: file.type,
        sizeBytes: file.size,
        file,
        previewUrl: URL.createObjectURL(file),
      };
      const stored = useComposerDraftStore.getState().addImages(target, [attachment]);
      if (!stored.includes(attachment.id)) URL.revokeObjectURL(attachment.previewUrl);
    } else {
      if (fileLimit === null) {
        report("This server does not accept file attachments.");
        return;
      }
      if (source.size <= 0) {
        report(`'${source.name}' is empty or could not be read.`);
        return;
      }
      if (source.size > fileLimit) {
        report(fileAttachmentTooLargeMessage(source.name, fileLimit));
        return;
      }
      if (!isActive()) return;
      const mimeType =
        videoMimeType({ name: source.name, mimeType: source.type }) ??
        (source.type || "application/octet-stream");
      const file =
        source.type === mimeType ? source : new File([source], source.name, { type: mimeType });
      attachment = {
        type: "file",
        id: randomUUID(),
        name: file.name || "file",
        mimeType,
        sizeBytes: file.size,
        file,
      };
      useComposerDraftStore.getState().addFiles(target, [attachment], { appendReference: false });
    }
  };
  const addFiles = async (files: ReadonlyArray<File>) => {
    // Reserve before awaiting so concurrent pastes share the whole request's limit.
    const reservations = files.flatMap((file) => {
      const reservation = reserve();
      return reservation ? [{ file, reservation }] : [];
    });
    await Promise.all(
      reservations.map(({ file, reservation }) =>
        storeFile(file, reservation.active)
          .catch(() => report(`'${file.name}' could not be attached.`))
          .finally(reservation.release),
      ),
    );
  };
  const importFile = async (load: () => Promise<File>, isCurrent: () => boolean = () => true) => {
    const reservation = reserve();
    if (!reservation) return;
    const loadAndStore = async () =>
      storeFile(await load(), () => reservation.active() && isCurrent());
    await loadAndStore()
      .catch(() =>
        report("The attached context could not be loaded. Try again or attach the file directly."),
      )
      .finally(reservation.release);
  };
  const addThreads = (
    refs: ReadonlyArray<ScopedThreadRef>,
    insert: (text: string) => void,
    isCurrent: () => boolean,
  ) => {
    if (disabled || !input.allowAdding || !isCurrent()) return;
    const records = refs.flatMap((ref) => {
      const shell = readThreadShell(ref);
      return shell ? [threadContextRecord(ref, shell.title)] : [];
    });
    if (records.length) {
      addCapturedThreadContexts(records, { appendReference: false });
      insert(
        records
          .map((record) => formatInlineContextReference(threadContextReference(record)))
          .join(" ") + " ",
      );
    }
  };
  const importClipboard = (text: string, fragment: ComposerContextClipboardFragment) => {
    const knownRecords = fragment.records.flatMap((value) => {
      const record = asKnownContextRecord(value);
      return record ? [record] : [];
    });
    const records = new Map(knownRecords.map((record) => [record.contextId, record]));
    for (const record of knownRecords) {
      if (record.kind === "thread") {
        addCapturedThreadContexts([record], { appendReference: false });
      }
      if (record.kind !== "image" && record.kind !== "file") continue;
      const sourceEnvironmentId = fragment.source.environmentId;
      void importFile(async () => {
        if (!sourceEnvironmentId) throw new Error("Missing attachment environment");
        const connection = readPreparedConnection(sourceEnvironmentId);
        if (!connection) throw new Error("Disconnected attachment environment");
        const result = await createAssetUrl({
          environmentId: sourceEnvironmentId,
          input: { resource: { _tag: "attachment", attachmentId: record.attachmentId } },
        });
        if (result._tag !== "Success") throw squashAtomCommandFailure(result);
        const url = resolveAssetUrl(connection.httpBaseUrl, result.value.relativeUrl);
        if (!url) throw new Error("Attachment URL unavailable");
        const response = await fetch(url, { signal: AbortSignal.timeout(60_000) });
        if (!response.ok) throw new Error("Attachment unavailable");
        return new File([await response.blob()], record.name, { type: record.mimeType });
      });
    }
    return replaceComposerContextReferences(text, (occurrence) => {
      const record = records.get(occurrence.contextId);
      if (!record) return occurrence.source;
      if (record.kind === "thread") return occurrence.source;
      if (record.kind === "image" || record.kind === "file") return record.name;
      return occurrence.source;
    });
  };
  const remove = (attachment: ComposerFileAttachment | ComposerImageAttachment) => {
    if (disabled) return;
    releaseDraftAttachment(attachment);
    if (attachment.type === "image") {
      useComposerDraftStore.getState().removeImage(target, attachment.id);
    } else useComposerDraftStore.getState().removeFile(target, attachment.id);
  };
  const clear = () => {
    if (disabled) return;
    preparations.delete(target);
    clearQuestionAttachmentDraft(target);
  };
  return {
    target,
    attachments,
    uploads,
    preparationCount,
    addFiles,
    addThreads,
    importClipboard,
    remove,
    clear,
    canAttach,
    error: error?.target === target ? error.message : null,
    retry: (image: ComposerImageAttachment | ComposerFileAttachment) => {
      if (canAttach) retryAttachmentUpload({ environmentId, image, draftTarget: target });
    },
  };
}
