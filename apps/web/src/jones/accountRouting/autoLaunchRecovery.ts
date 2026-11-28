import {
  CommandId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import type { PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import {
  decodePreparedThreadTurn,
  encodePreparedThreadTurn,
} from "@t3tools/client-runtime/jones/preparedThreadTurn";
import { sha256 } from "@noble/hashes/sha2";
import { bytesToHex } from "@noble/hashes/utils";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Schema from "effect/Schema";
import type { DraftId } from "../../draftId";

const correlationFields = {
  environmentId: EnvironmentId,
  projectId: ProjectId,
  threadId: ThreadId,
  commandId: CommandId,
  messageId: MessageId,
  createdAt: Schema.String,
};
const LegacyAutoLaunchRecord = Schema.Struct({
  version: Schema.Literal(1),
  ...correlationFields,
  wire: Schema.optionalKey(Schema.String),
  draftText: Schema.optionalKey(Schema.String),
});
const StoredAutoLaunchRecord = Schema.Struct({
  version: Schema.Literal(2),
  ...correlationFields,
  digest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});
const AutoLaunchRecoveryRecord = Schema.Union([LegacyAutoLaunchRecord, StoredAutoLaunchRecord]);
export const PendingAutoLaunch = Schema.Union([
  AutoLaunchRecoveryRecord,
  Schema.Literal("unavailable"),
]);
export type PendingAutoLaunch = typeof PendingAutoLaunch.Type;
export type AutoLaunchRecoveryRecord = typeof AutoLaunchRecoveryRecord.Type;
type StoredAutoLaunchRecord = typeof StoredAutoLaunchRecord.Type;
type DraftScope = Pick<AutoLaunchRecoveryRecord, "environmentId" | "projectId" | "threadId">;
const legacyInlineAsset = /\b(?:blob:|data:[^\s]*;base64,)/i;
const maxLegacySavedTextLength = 256 * 1024;

function containsTransientReference(value: string): boolean {
  // Rich context can include page URLs and even URLs embedded in legacy message text.
  return (
    /\bblob:/i.test(value) ||
    /\/api\/(?:assets|attachments\/upload)\/[\w-]+\.[\w-]+/i.test(value) ||
    /\b(?:https?|wss?):\/\/(?:[^/\s"@]+:[^/\s"@]+@|[^\s"\\]*[?&#](?:wsTicket|dpop|access_token|token)=)/i.test(
      value,
    )
  );
}

export interface CapturedAutoLaunch {
  readonly record: AutoLaunchRecoveryRecord;
  readonly wire: string;
}

function matchesDraft(record: DraftScope, draft: DraftScope) {
  return (
    record.environmentId === draft.environmentId &&
    record.projectId === draft.projectId &&
    record.threadId === draft.threadId
  );
}

function freshAutoLaunch(prepared: PreparedThreadTurn) {
  if (
    prepared.method !== ORCHESTRATION_V2_WS_METHODS.launchThread ||
    prepared.payload.accountRouting?.mode !== "auto" ||
    prepared.payload.reuseExistingThread ||
    prepared.payload.creationSource !== "web" ||
    (prepared.payload.workspaceStrategy.type !== "root" &&
      prepared.payload.workspaceStrategy.type !== "existing_worktree")
  )
    return null;
  return prepared.payload;
}

function wireDigest(wire: string) {
  return bytesToHex(sha256(new TextEncoder().encode(wire)));
}

export function captureAutoLaunch(
  prepared: PreparedThreadTurn,
  scope: DraftScope & { readonly messageId: MessageId },
  createdAt: string,
  plainDraftText: string | undefined,
): CapturedAutoLaunch {
  const launch = freshAutoLaunch(prepared);
  if (
    !launch ||
    launch.threadId !== scope.threadId ||
    launch.projectId !== scope.projectId ||
    launch.initialMessage.messageId !== scope.messageId
  )
    throw new Error("The prepared Auto launch does not match this draft.");

  let wire: string;
  try {
    wire = JSON.stringify(encodePreparedThreadTurn(prepared));
  } catch {
    throw new Error("Could not save the exact Auto launch request. The message was not sent.");
  }
  const correlation = {
    ...scope,
    commandId: launch.commandId,
    createdAt,
  };
  // The codec can drop unsupported records. Refuse to issue if the saved request would differ.
  if (stableStringify(decodeWire(correlation, wire)?.prepared) !== stableStringify(prepared))
    throw new Error("Could not save the exact Auto launch request. The message was not sent.");
  if (
    plainDraftText !== undefined &&
    launch.initialMessage.attachments.length === 0 &&
    launch.initialMessage.context === undefined &&
    legacyTextFits(wire, plainDraftText) &&
    !containsTransientReference(plainDraftText)
  ) {
    return { record: { ...correlation, version: 1, wire, draftText: plainDraftText }, wire };
  }
  return { record: { ...correlation, version: 2, digest: wireDigest(wire) }, wire };
}

function decodeWire(
  record: Pick<AutoLaunchRecoveryRecord, "commandId" | "threadId" | "projectId" | "messageId">,
  wire: string,
) {
  if (containsTransientReference(wire)) return null;
  try {
    const prepared = decodePreparedThreadTurn(JSON.parse(wire));
    const launch = freshAutoLaunch(prepared);
    return launch &&
      launch.commandId === record.commandId &&
      launch.threadId === record.threadId &&
      launch.projectId === record.projectId &&
      launch.initialMessage.messageId === record.messageId &&
      JSON.stringify(encodePreparedThreadTurn(prepared)) === wire
      ? { prepared, message: launch.initialMessage }
      : null;
  } catch {
    return null;
  }
}

function legacyTextFits(wire: string, draftText: string): boolean {
  return (
    wire.length + draftText.length <= maxLegacySavedTextLength &&
    !legacyInlineAsset.test(wire) &&
    !legacyInlineAsset.test(draftText)
  );
}

function readLegacyAutoLaunch(record: typeof LegacyAutoLaunchRecord.Type) {
  const { wire, draftText } = record;
  if (!wire || !legacyTextFits(wire, draftText ?? "")) return null;
  const decoded = decodeWire(record, wire);
  return decoded?.message.attachments.length === 0 && decoded.message.context === undefined
    ? { ...decoded, draftText }
    : null;
}

const decodeAutoLaunchRecoveryRecord = Schema.decodeUnknownSync(AutoLaunchRecoveryRecord);

export function normalizePendingAutoLaunch(
  value: unknown,
  scope: DraftScope,
): PendingAutoLaunch | undefined {
  if (value === undefined) return undefined;
  try {
    const record = decodeAutoLaunchRecoveryRecord(value);
    if (!matchesDraft(record, scope)) return "unavailable";
    if (record.version === 2 || readLegacyAutoLaunch(record)) return record;
    const { wire: _wire, draftText: _draftText, ...correlation } = record;
    return correlation;
  } catch {
    return "unavailable";
  }
}

const databaseName = "jones:auto-launch-recovery";
const storeName = "launches";

function withLaunchStore<A>(
  mode: IDBTransactionMode,
  use: (store: IDBObjectStore) => IDBRequest<A>,
): Promise<A> {
  return new Promise((resolve, reject) => {
    const opening = indexedDB.open(databaseName, 1);
    let blocked = false;
    opening.onblocked = () => {
      blocked = true;
      reject(new Error("Auto launch recovery storage is blocked."));
    };
    opening.onerror = () => reject(opening.error);
    opening.onupgradeneeded = () => {
      if (blocked) opening.transaction?.abort();
      else opening.result.createObjectStore(storeName);
    };
    opening.onsuccess = () => {
      const database = opening.result;
      try {
        if (blocked) return;
        const transaction = database.transaction(storeName, mode, { durability: "strict" });
        transaction.onabort = () =>
          reject(transaction.error ?? new Error("Auto launch recovery transaction aborted."));
        const request = use(transaction.objectStore(storeName));
        transaction.oncomplete = () => {
          if (request.error) reject(request.error);
          else resolve(request.result);
        };
      } catch (error) {
        reject(error);
      } finally {
        // Pending transactions finish before the browser closes the connection.
        database.close();
      }
    };
  });
}

function launchStorageKey(draftId: DraftId, record: StoredAutoLaunchRecord) {
  return [
    record.environmentId,
    record.projectId,
    record.threadId,
    draftId,
    record.commandId,
    record.messageId,
  ];
}

const SavedAutoLaunchValue = Schema.Struct({
  wire: Schema.String,
  editableDraft: Schema.optionalKey(Schema.String),
  draftText: Schema.optionalKey(Schema.String),
});
const decodeSavedAutoLaunchValue = Schema.decodeUnknownSync(SavedAutoLaunchValue);

function safeDraftText(value: string | undefined) {
  return value !== undefined && !containsTransientReference(value) ? value : undefined;
}

export async function saveAutoLaunch(
  draftId: DraftId,
  { record, wire }: CapturedAutoLaunch,
  draft: { readonly editableDraft: string | undefined; readonly draftText: string | undefined },
): Promise<void> {
  if (record.version === 1) return;
  // Keep the text if the editable snapshot contains a transient reference outside the wire.
  const editableDraft = safeDraftText(draft.editableDraft);
  const draftText = editableDraft === undefined ? safeDraftText(draft.draftText) : undefined;
  const value = editableDraft
    ? { wire, editableDraft }
    : draftText
      ? { wire, draftText }
      : { wire };
  await withLaunchStore("readwrite", (store) =>
    store.add(value, launchStorageKey(draftId, record)),
  );
}

export async function readSavedAutoLaunch(draftId: DraftId, record: AutoLaunchRecoveryRecord) {
  if (record.version === 1) {
    const legacy = readLegacyAutoLaunch(record);
    return legacy
      ? { ...legacy, editableDraft: undefined, draftText: safeDraftText(legacy.draftText) }
      : null;
  }
  try {
    const saved = decodeSavedAutoLaunchValue(
      await withLaunchStore("readonly", (store) => store.get(launchStorageKey(draftId, record))),
    );
    if (wireDigest(saved.wire) !== record.digest) return null;
    const decoded = decodeWire(record, saved.wire);
    return decoded
      ? {
          ...decoded,
          editableDraft: safeDraftText(saved.editableDraft),
          draftText: safeDraftText(saved.draftText),
        }
      : null;
  } catch {
    // Missing or damaged bytes leave correlation evidence and an uncertain outcome.
    return null;
  }
}

export function forgetSavedAutoLaunch(draftId: DraftId, record: AutoLaunchRecoveryRecord) {
  if (record.version === 1) return;
  void withLaunchStore("readwrite", (store) =>
    store.delete(launchStorageKey(draftId, record)),
  ).catch(() => undefined);
}
