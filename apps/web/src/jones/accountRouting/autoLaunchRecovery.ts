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
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Schema from "effect/Schema";

const AutoLaunchRecoveryRecord = Schema.Struct({
  version: Schema.Literal(1),
  environmentId: EnvironmentId,
  projectId: ProjectId,
  threadId: ThreadId,
  commandId: CommandId,
  messageId: MessageId,
  createdAt: Schema.String,
  wire: Schema.optionalKey(Schema.String),
  draftText: Schema.optionalKey(Schema.String),
});
export const PendingAutoLaunch = Schema.Union([
  AutoLaunchRecoveryRecord,
  Schema.Literal("unavailable"),
]);
export type PendingAutoLaunch = typeof PendingAutoLaunch.Type;
export type AutoLaunchRecoveryRecord = typeof AutoLaunchRecoveryRecord.Type;
type DraftScope = Pick<AutoLaunchRecoveryRecord, "environmentId" | "projectId" | "threadId">;
const inlineAsset = /\b(?:blob:|data:[^\s]*;base64,)/i;
const maxSavedTextLength = 256 * 1024;

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
    prepared.payload.creationSource !== "web"
  )
    return null;
  return prepared.payload;
}

function canSaveWire(prepared: PreparedThreadTurn) {
  const launch = freshAutoLaunch(prepared);
  // Rich context can also carry runtime references through the legacy text conversion.
  // Keep its exact request in memory and save only the correlation IDs across reloads.
  return (
    launch !== null &&
    (launch.workspaceStrategy.type === "root" ||
      launch.workspaceStrategy.type === "existing_worktree") &&
    launch.initialMessage.attachments.length === 0 &&
    launch.initialMessage.context === undefined
  );
}

export function captureAutoLaunch(
  prepared: PreparedThreadTurn,
  scope: DraftScope,
  createdAt: string,
  draftText: string | undefined,
): AutoLaunchRecoveryRecord {
  const launch = freshAutoLaunch(prepared);
  if (!launch || launch.threadId !== scope.threadId || launch.projectId !== scope.projectId)
    throw new Error("The prepared Auto launch does not match this draft.");
  const record: AutoLaunchRecoveryRecord = {
    environmentId: scope.environmentId,
    projectId: scope.projectId,
    threadId: scope.threadId,
    version: 1,
    commandId: launch.commandId,
    messageId: launch.initialMessage.messageId,
    createdAt,
  };
  if (draftText === undefined || !canSaveWire(prepared)) return record;
  const wire = JSON.stringify(encodePreparedThreadTurn(prepared));
  const saved = { ...record, wire, draftText };
  return stableStringify(readSavedAutoLaunch(saved)) === stableStringify(prepared) ? saved : record;
}

export function readSavedAutoLaunch(record: AutoLaunchRecoveryRecord): PreparedThreadTurn | null {
  if (
    !record.wire ||
    record.wire.length + (record.draftText?.length ?? 0) > maxSavedTextLength ||
    inlineAsset.test(record.wire) ||
    inlineAsset.test(record.draftText ?? "")
  )
    return null;
  try {
    const prepared = decodePreparedThreadTurn(JSON.parse(record.wire));
    const launch = freshAutoLaunch(prepared);
    return launch &&
      launch.commandId === record.commandId &&
      launch.threadId === record.threadId &&
      launch.projectId === record.projectId &&
      launch.initialMessage.messageId === record.messageId &&
      canSaveWire(prepared) &&
      JSON.stringify(encodePreparedThreadTurn(prepared)) === record.wire
      ? prepared
      : null;
  } catch {
    return null;
  }
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
    if (record.wire && readSavedAutoLaunch(record)) return record;
    const { wire: _wire, draftText: _draftText, ...correlation } = record;
    return correlation;
  } catch {
    return "unavailable";
  }
}
