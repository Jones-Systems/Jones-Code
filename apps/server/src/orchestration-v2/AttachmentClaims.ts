import { createHash } from "node:crypto";
import * as FileSystem from "effect/FileSystem";
import {
  ChatAttachmentId,
  getProviderAttachmentLimitError,
  type ChatAttachment,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  planAttachmentClaim,
  resolveAttachmentPath,
  resolveAttachmentPathById,
} from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import type { NormalizationAttachmentV1 } from "./NormalizationWitness.ts";

export class AttachmentClaimError extends Schema.TaggedError<AttachmentClaimError>()(
  "AttachmentClaimError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export const validateAttachmentLimits = Effect.fn("AttachmentClaims.validateAttachmentLimits")(
  function* (attachments: ReadonlyArray<ChatAttachment>) {
    const error = getProviderAttachmentLimitError(attachments);
    if (error) return yield* new AttachmentClaimError({ message: error });
  },
);

export interface ClaimedAttachments {
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly claimedPaths: ReadonlyArray<string>;
  readonly witnessAttachments: ReadonlyArray<NormalizationAttachmentV1>;
}

export function attachmentIsPendingUpload(attachment: ChatAttachment): boolean {
  return parseThreadSegmentFromAttachmentId(attachment.id) === PENDING_ATTACHMENT_THREAD_SEGMENT;
}

const hashFile = Effect.fnUntraced(function* (path: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const hash = createHash("sha256");
  let sizeBytes = 0;
  yield* fileSystem.stream(path).pipe(
    Stream.runForEach((bytes) =>
      Effect.sync(() => {
        hash.update(bytes);
        sizeBytes += bytes.byteLength;
      }),
    ),
  );
  return { contentSha256: hash.digest("hex"), sizeBytes };
});

export const probePendingAttachment = Effect.fn("AttachmentClaims.probePendingAttachment")(
  function* (pendingId: string) {
    if (parseThreadSegmentFromAttachmentId(pendingId) !== PENDING_ATTACHMENT_THREAD_SEGMENT)
      return yield* new AttachmentClaimError({
        message: "Normalization witness does not identify a pending upload.",
      });
    const config = yield* ServerConfig.ServerConfig;
    const path = resolveAttachmentPathById({
      attachmentsDir: config.attachmentsDir,
      attachmentId: pendingId,
    });
    if (path === null) return null;
    return yield* hashFile(path).pipe(
      Effect.catchTag("PlatformError", (cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.succeed(null)
          : Effect.fail(
              new AttachmentClaimError({
                message: "Pending upload could not be checked for replay.",
                cause,
              }),
            ),
      ),
    );
  },
);

/** Remove partial claims only before dispatch, or after proving they were not accepted. */
export const releaseClaimedAttachments = Effect.fn("AttachmentClaims.releaseClaimedAttachments")(
  function* (claimedPaths: ReadonlyArray<string>) {
    if (claimedPaths.length === 0) return;
    const fileSystem = yield* FileSystem.FileSystem;
    yield* Effect.forEach(claimedPaths, (path) => fileSystem.remove(path).pipe(Effect.ignore), {
      concurrency: 1,
      discard: true,
    }).pipe(Effect.uninterruptible);
  },
);

/**
 * Claims pending uploads into the target thread's attachment store before the
 * command enters the orchestrator: verifies the staged file, copies it under a
 * thread-scoped id, and rewrites the attachment ref. A copy, not a move — the
 * pending file stays behind as the retry source for a failed bootstrap, and
 * the periodic pending sweep reclaims it later. Already-claimed attachments
 * pass through untouched.
 */
export const claimPendingAttachments = Effect.fn("AttachmentClaims.claimPendingAttachments")(
  function* (input: {
    readonly threadId: string;
    readonly attachments: ReadonlyArray<ChatAttachment>;
  }) {
    yield* validateAttachmentLimits(input.attachments);
    if (
      new Set(input.attachments.map((attachment) => attachment.id)).size !==
      input.attachments.length
    ) {
      return yield* new AttachmentClaimError({
        message: "Duplicate attachment ids are not allowed.",
      });
    }
    if (!input.attachments.some(attachmentIsPendingUpload)) {
      return {
        attachments: input.attachments,
        claimedPaths: [],
        witnessAttachments: [],
      } satisfies ClaimedAttachments;
    }
    const serverConfig = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const claimedPaths: string[] = [];
    const witnessAttachments: NormalizationAttachmentV1[] = [];
    const attachments = yield* Effect.forEach(
      input.attachments,
      (attachment) =>
        Effect.gen(function* () {
          if (!attachmentIsPendingUpload(attachment)) {
            return attachment;
          }
          const claim = planAttachmentClaim({
            attachmentsDir: serverConfig.attachmentsDir,
            threadId: input.threadId,
            attachmentId: attachment.id,
          });
          if (!claim.ok) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: ${claim.reason}.`,
            });
          }
          const info = yield* fileSystem.stat(claim.currentPath).pipe(
            Effect.mapError(
              (cause) =>
                new AttachmentClaimError({
                  message: `Attachment '${attachment.name}' cannot be sent: attachment not found.`,
                  cause,
                }),
            ),
          );
          if (Number(info.size) !== attachment.sizeBytes) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: stored size does not match.`,
            });
          }
          const normalized: ChatAttachment = {
            ...attachment,
            id: ChatAttachmentId.make(claim.finalId),
            mimeType: attachment.mimeType.toLowerCase(),
          };
          const expectedPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment: normalized,
          });
          if (expectedPath !== claim.finalPath) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: attachment type does not match the upload.`,
            });
          }
          // A copy, not a hard link: an agent editing the delivered file in
          // place must not mutate the retry source. fs.copyFile cannot be
          // cancelled, so the copy and its rollback registration stay in one
          // uninterruptible region: an interrupt landing mid-copy still waits
          // for the write to settle and records the path before cleanup runs.
          yield* fileSystem.copyFile(claim.currentPath, claim.finalPath).pipe(
            Effect.mapError(
              (cause) =>
                new AttachmentClaimError({
                  message: `Failed to claim attachment '${attachment.name}' for this thread.`,
                  cause,
                }),
            ),
            Effect.andThen(Effect.sync(() => claimedPaths.push(claim.finalPath))),
            Effect.uninterruptible,
          );
          const copied = yield* hashFile(claim.finalPath).pipe(
            Effect.mapError(
              (cause) =>
                new AttachmentClaimError({
                  message: `Failed to witness attachment '${attachment.name}'.`,
                  cause,
                }),
            ),
          );
          if (copied.sizeBytes !== attachment.sizeBytes)
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' changed size while being copied.`,
            });
          witnessAttachments.push({ pendingId: attachment.id, finalId: claim.finalId, ...copied });
          return normalized;
        }),
      { concurrency: 1 },
    ).pipe(Effect.onError(() => releaseClaimedAttachments(claimedPaths)));
    return { attachments, claimedPaths, witnessAttachments } satisfies ClaimedAttachments;
  },
);
