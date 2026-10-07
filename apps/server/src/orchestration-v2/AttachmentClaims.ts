import * as FileSystem from "effect/FileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import * as Path from "effect/Path";
import {
  ChatAttachmentId,
  getProviderAttachmentLimitError,
  type ChatAttachment,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";

import {
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  planAttachmentClaim,
  planCorrelatedAttachmentClaim,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { canonicalJson, sha256 } from "./CanonicalJson.ts";

export class AttachmentClaimError extends Schema.TaggedError<AttachmentClaimError>()(
  "AttachmentClaimError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const isAttachmentClaimError = Schema.is(AttachmentClaimError);

export const validateAttachmentLimits = Effect.fn("AttachmentClaims.validateAttachmentLimits")(
  function* (attachments: ReadonlyArray<ChatAttachment>) {
    const error = getProviderAttachmentLimitError(attachments);
    if (error) return yield* new AttachmentClaimError({ message: error });
  },
);

export interface ClaimedAttachments {
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly claimedPaths: ReadonlyArray<string>;
}

export function attachmentIsPendingUpload(attachment: ChatAttachment): boolean {
  return parseThreadSegmentFromAttachmentId(attachment.id) === PENDING_ATTACHMENT_THREAD_SEGMENT;
}

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
      return { attachments: input.attachments, claimedPaths: [] } satisfies ClaimedAttachments;
    }
    const serverConfig = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const claimedPaths: string[] = [];
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
          return normalized;
        }),
      { concurrency: 1 },
    ).pipe(Effect.onError(() => releaseClaimedAttachments(claimedPaths)));
    return { attachments, claimedPaths } satisfies ClaimedAttachments;
  },
);

export interface CorrelatedAttachments extends ClaimedAttachments {
  readonly requestDigest: string;
  readonly recoveredMessageId?: string;
}

export type CorrelatedClaimDisposition =
  | { readonly type: "unhandled" }
  | { readonly type: "rejected" }
  | { readonly type: "accepted"; readonly recoveredMessageId?: string };

export interface CorrelatedClaimPorts {
  readonly storeIdentity: object;
  readonly commandId: string;
  readonly requestData: unknown;
  readonly read: (
    claim: CorrelatedAttachments,
  ) => Effect.Effect<CorrelatedClaimDisposition, AttachmentClaimError>;
  readonly withRollbackReadback: (
    claim: CorrelatedAttachments,
    removeOwned: Effect.Effect<void>,
  ) => Effect.Effect<void, AttachmentClaimError>;
  readonly knownNotAccepted: (error: unknown) => boolean;
}

const commandClaims = new WeakMap<
  object,
  Map<
    string,
    {
      readonly digest: string;
      readonly lock: Semaphore.Semaphore;
    }
  >
>();

// Request data retains optional-field presence. Runtime readers/locks are separate
// ports; they must never enter a command digest or be serialized as request data.
function claimData(value: unknown): unknown {
  if (value === undefined) return ["absent"];
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return ["value", value];
  if (typeof value === "number" && Number.isFinite(value)) return ["number", value];
  if (Array.isArray(value)) return ["array", value.map(claimData)];
  if (
    typeof value === "object" &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  )
    return [
      "object",
      Object.keys(value)
        .sort()
        .map((key) => [key, claimData(Reflect.get(value, key))]),
    ];
  throw new AttachmentClaimError({
    message:
      "Attachment request contains an unsupported execution handle; its provenance needs reconciliation.",
  });
}

/** Keep file ownership and the real dispatch in one command-scoped lifetime. */
export const withCorrelatedClaims = <A, E, R>(
  input: {
    readonly threadId: string;
    readonly attachments: ReadonlyArray<ChatAttachment>;
    readonly ports: CorrelatedClaimPorts;
  },
  use: (claim: CorrelatedAttachments) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    yield* validateAttachmentLimits(input.attachments);
    if (new Set(input.attachments.map((value) => value.id)).size !== input.attachments.length)
      return yield* new AttachmentClaimError({
        message: "Duplicate attachment ids are not allowed.",
      });
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const pathService = yield* Path.Path.pipe(Effect.provide(NodePath.layer));
    const root = yield* fs
      .realPath(config.attachmentsDir)
      .pipe(
        Effect.mapError(
          (cause) =>
            new AttachmentClaimError({ message: "Attachment store is unavailable.", cause }),
        ),
      );
    const snapshots = yield* Effect.forEach(
      input.attachments,
      (attachment) =>
        Effect.gen(function* () {
          const path = resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment });
          if (path === null)
            return yield* new AttachmentClaimError({ message: "Invalid attachment path." });
          const physicalPath = yield* fs.realPath(path);
          if (physicalPath !== pathService.join(root, pathService.basename(path)))
            return yield* new AttachmentClaimError({
              message: "An attachment file was replaced by a link.",
            });
          const info = yield* fs.stat(path);
          const bytes = yield* fs.readFile(path);
          if (
            info.type !== "File" ||
            Option.isNone(info.ino) ||
            Number(info.size) !== attachment.sizeBytes ||
            bytes.length !== attachment.sizeBytes
          )
            return yield* new AttachmentClaimError({
              message: "Stored attachment size or type does not match.",
            });
          return { attachment, path, bytes, digest: sha256(bytes), info };
        }).pipe(
          Effect.mapError((cause) =>
            isAttachmentClaimError(cause)
              ? cause
              : new AttachmentClaimError({ message: "Cannot read attachment bytes.", cause }),
          ),
        ),
      { concurrency: 1 },
    );
    const data = yield* Effect.try({
      try: () => claimData(input.ports.requestData),
      catch: (cause) =>
        isAttachmentClaimError(cause)
          ? cause
          : new AttachmentClaimError({
              message: "Cannot correlate attachment request data.",
              cause,
            }),
    });
    const requestDigest = sha256(
      canonicalJson({
        data,
        threadId: input.threadId,
        uploads: snapshots.map(({ attachment, digest }) => ({ attachment, digest })),
      }),
    );
    const plans = yield* Effect.try({
      try: () =>
        snapshots.map((snapshot, ordinal) => {
          if (!attachmentIsPendingUpload(snapshot.attachment))
            return { ...snapshot, claim: undefined, normalized: snapshot.attachment };
          const claim = planCorrelatedAttachmentClaim({
            attachmentsDir: config.attachmentsDir,
            threadId: input.threadId,
            attachmentId: snapshot.attachment.id,
            requestDigest,
            bytesDigest: snapshot.digest,
            ordinal,
          });
          if (!claim.ok)
            throw new AttachmentClaimError({
              message: `Cannot claim attachment: ${claim.reason}.`,
            });
          const normalized: ChatAttachment = {
            ...snapshot.attachment,
            id: ChatAttachmentId.make(claim.finalId),
            mimeType: snapshot.attachment.mimeType.toLowerCase(),
          };
          if (
            resolveAttachmentPath({
              attachmentsDir: config.attachmentsDir,
              attachment: normalized,
            }) !== claim.finalPath
          )
            throw new AttachmentClaimError({
              message: "Attachment type does not match the original upload.",
            });
          return { ...snapshot, claim, normalized };
        }),
      catch: (cause) =>
        isAttachmentClaimError(cause)
          ? cause
          : new AttachmentClaimError({
              message: "Cannot plan correlated attachment claims.",
              cause,
            }),
    });
    let entries = commandClaims.get(input.ports.storeIdentity);
    if (entries === undefined) {
      entries = new Map();
      commandClaims.set(input.ports.storeIdentity, entries);
    }
    let entry = entries.get(input.ports.commandId);
    if (entry !== undefined && entry.digest !== requestDigest)
      return yield* new AttachmentClaimError({
        message: "Another attachment request already owns this command.",
      });
    if (entry === undefined) {
      entry = { digest: requestDigest, lock: Semaphore.makeUnsafe(1) };
      entries.set(input.ports.commandId, entry);
    }
    return yield* entry.lock.withPermit(
      Effect.scoped(
        Effect.gen(function* () {
          const proposed: CorrelatedAttachments = {
            attachments: plans.map((value) => value.normalized),
            claimedPaths: [],
            requestDigest,
          };
          const disposition = yield* input.ports.read(proposed);
          if (disposition.type === "rejected") return yield* use(proposed);
          const owned: Array<{
            readonly path: string;
            readonly dev: number;
            readonly ino: number;
            readonly digest: string;
          }> = [];
          let entered = false;
          const verifySource = Effect.forEach(
            snapshots,
            ({ path, digest, info }) =>
              Effect.gen(function* () {
                const current = yield* fs.stat(path);
                if (
                  (yield* fs.realPath(path)) !==
                    pathService.join(root, pathService.basename(path)) ||
                  current.type !== "File" ||
                  current.dev !== info.dev ||
                  Option.getOrNull(current.ino) !== Option.getOrNull(info.ino) ||
                  sha256(yield* fs.readFile(path)) !== digest
                )
                  return yield* new AttachmentClaimError({
                    message: "Attachment source changed during intake.",
                  });
              }),
            { concurrency: 1, discard: true },
          );
          const rollback = Effect.gen(function* () {
            for (const file of owned) {
              const info = yield* fs.stat(file.path);
              if (
                (yield* fs.realPath(file.path)) !==
                  pathService.join(root, pathService.basename(file.path)) ||
                info.type !== "File" ||
                Option.getOrNull(info.nlink) !== 1 ||
                info.dev !== file.dev ||
                Option.getOrNull(info.ino) !== file.ino ||
                sha256(yield* fs.readFile(file.path)) !== file.digest
              )
                continue;
              yield* fs.remove(file.path);
            }
          }).pipe(Effect.ignore, Effect.uninterruptible);
          const prepare = Effect.gen(function* () {
            for (const plan of plans) {
              if (plan.claim === undefined) continue;
              const path = plan.claim.finalPath;
              if (disposition.type === "accepted") {
                const info = yield* fs.stat(path);
                if (
                  info.type !== "File" ||
                  Option.getOrNull(info.nlink) !== 1 ||
                  Number(info.size) !== plan.bytes.length ||
                  (yield* fs.realPath(path)) !==
                    pathService.join(root, pathService.basename(path)) ||
                  sha256(yield* fs.readFile(path)) !== plan.digest
                )
                  return yield* new AttachmentClaimError({
                    message: "Accepted attachment bytes are missing or changed.",
                  });
              } else {
                yield* Effect.gen(function* () {
                  const handle = yield* fs.open(path, { flag: "wx+", mode: 0o600 });
                  const identity = yield* handle.stat;
                  const ino = Option.getOrNull(identity.ino);
                  if (
                    identity.type !== "File" ||
                    ino === null ||
                    Option.getOrNull(identity.nlink) !== 1
                  )
                    return yield* new AttachmentClaimError({
                      message: "Exclusive attachment ownership is unavailable.",
                    });
                  yield* handle.writeAll(plan.bytes);
                  yield* handle.sync;
                  if (sha256(yield* fs.readFile(path)) !== plan.digest)
                    return yield* new AttachmentClaimError({
                      message: "Attachment claim readback is unknown.",
                    });
                  owned.push({ path, dev: identity.dev, ino, digest: plan.digest });
                }).pipe(Effect.uninterruptible);
              }
            }
            yield* verifySource;
            for (const file of owned) {
              const info = yield* fs.stat(file.path);
              if (
                (yield* fs.realPath(file.path)) !==
                  pathService.join(root, pathService.basename(file.path)) ||
                info.type !== "File" ||
                Option.getOrNull(info.nlink) !== 1 ||
                info.dev !== file.dev ||
                Option.getOrNull(info.ino) !== file.ino ||
                sha256(yield* fs.readFile(file.path)) !== file.digest
              )
                return yield* new AttachmentClaimError({
                  message: "Fresh attachment ownership changed before dispatch.",
                });
            }
          }).pipe(
            Effect.mapError((cause) =>
              isAttachmentClaimError(cause)
                ? cause
                : new AttachmentClaimError({
                    message: "Attachment claim ownership or readback is unavailable.",
                    cause,
                  }),
            ),
          );
          const result = Effect.gen(function* () {
            yield* prepare;
            entered = true;
            return yield* use({
              ...proposed,
              ...(disposition.type === "accepted" && disposition.recoveredMessageId !== undefined
                ? { recoveredMessageId: disposition.recoveredMessageId }
                : {}),
              claimedPaths: owned.map((value) => value.path),
            });
          });
          return yield* result.pipe(
            Effect.onError((cause) =>
              Effect.gen(function* () {
                if (owned.length === 0) return;
                const fresh = { ...proposed, claimedPaths: owned.map((file) => file.path) };
                if (!entered) {
                  yield* input.ports.withRollbackReadback(fresh, rollback).pipe(Effect.ignore);
                  return;
                }
                if (cause.reasons.length !== 1) return;
                const error = cause.reasons[0];
                if (error?._tag !== "Fail" || !input.ports.knownNotAccepted(error.error)) return;
                yield* input.ports.withRollbackReadback(fresh, rollback).pipe(Effect.ignore);
              }),
            ),
          );
        }),
      ),
    );
  });
