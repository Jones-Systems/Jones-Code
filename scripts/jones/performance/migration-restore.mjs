import {
  assertCurrentDatabaseSource,
  qualificationDatabaseSource,
  assertQualificationDatabaseSource,
} from "./sources.mjs";

export const migrationRestoreSchema = "jones-performance-migration-restore/v2";
export const nativeBackupSchema = "jones-performance-native-backup/v1";
export const migrationRestoreOptIn = "JONES_MIGRATION_RESTORE_REQUEST";
export const originalJonesNames = Object.freeze([
  "WorktreeOwnershipLeases",
  "ProjectionThreadRuntimeIdentity",
  "NativeCreationIntents",
  "NativeCreationCommandIdentities",
  "WorkstreamsNativeAttempts",
  "WorkstreamsProviderEnrollments",
]);
export const foreignV2Names = Object.freeze([
  "V2NativeAcceptance",
  "DeletionWorktreeAdmission",
  "OrdinaryCheckoutOwnership",
  "AttachmentCleanup",
  "OrdinaryCheckoutExecutionLifetime",
  "ImportedApplicationAttachments",
  "CommandNormalizationWitness",
]);
export const historicalQualificationRequirements = Object.freeze(
  [
    {
      directory: "baseline",
      sourceRevision: "e5a31aceec91484b64315c63dcce80f6e7581604",
      forkCount: 4,
    },
    {
      directory: "live-baseline",
      sourceRevision: "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4",
      forkCount: 2,
    },
    {
      directory: "history",
      sourceRevision: "c4c68bb0b33eafb72545e6e23b0b7258e49bd613",
      forkCount: 6,
    },
    {
      directory: "lease",
      sourceRevision: "da5f4aee0035beec471b38598eaa2857d1e5155c",
      foreign: "lookup007",
    },
    {
      directory: "lease-current",
      sourceRevision: "7c86493f6eff9ba9b30d3ff20ae33e10cdfb4607",
      foreign: "lookup007",
    },
    {
      directory: "v2-aggregate-77",
      sourceRevision: "ae25e5d04bec70c2c0af51af70329a9e9086d15e",
      foreignPrefix: 7,
    },
    {
      directory: "v2-aggregate-91",
      sourceRevision: "09ead6ea565ffce428e9cda1bd5696c69ce9e616",
      foreignPrefix: 5,
    },
  ].map(Object.freeze),
);
export const qualificationCases = Object.freeze(
  [
    { id: "open-e5", seed: "baseline", kind: "open" },
    { id: "open-414", seed: "live-baseline", kind: "open" },
    { id: "open-six", seed: "history", kind: "open" },
    { id: "foreign-lease-007", seed: "lease", kind: "foreign" },
    { id: "foreign-87", seed: "lease-current", kind: "foreign" },
    { id: "foreign-77", seed: "v2-aggregate-77", kind: "foreign" },
    { id: "foreign-91", seed: "v2-aggregate-91", kind: "foreign" },
    { id: "rollback-injected", seed: "current-v2", kind: "rollback" },
    { id: "restore-pre", seed: "live-baseline", kind: "restore" },
    { id: "restore-post", seed: "live-baseline", kind: "restore" },
    {
      id: "receiving-creation-lookup",
      seed: "lease",
      kind: "successor",
      prerequisite: "055_OrchestrationV2/RecoveryIndexes",
    },
  ].map(Object.freeze),
);

function refuse(code, message) {
  throw Object.assign(new Error(message), { code });
}

export function migrationRestoreCase(caseId) {
  const specification = qualificationCases.find(({ id }) => id === caseId);
  if (!specification) refuse("invalid_case", "migration restore case is not in the V2 case table");
  return specification;
}

export function validateMigrationRestoreRequest(caseId, input) {
  const specification = migrationRestoreCase(caseId);
  if (input?.explicitSyntheticRequest !== true)
    refuse("unavailable", "migration restore qualification requires explicit synthetic opt-in");
  if (input.signal !== undefined && !(input.signal instanceof AbortSignal))
    refuse("invalid_options", "qualification signal must be an AbortSignal");
  if (
    input.timeoutMs !== undefined &&
    (!Number.isSafeInteger(input.timeoutMs) || input.timeoutMs < 1 || input.timeoutMs > 120000)
  )
    refuse("invalid_options", "qualification deadline must be between 1 and 120000 milliseconds");
  input.signal?.throwIfAborted();
  const candidate = assertCurrentDatabaseSource(input.candidate);
  if (
    input.binding?.repository !== candidate.repository ||
    input.binding?.sourceRevision !== candidate.sourceRevision
  )
    refuse("invalid_source", "qualification binding must name the exact receiving candidate");
  let historical;
  if (specification.seed !== "current-v2") {
    const requirement = historicalQualificationRequirements.find(
      ({ directory }) => directory === specification.seed,
    );
    historical = assertQualificationDatabaseSource(
      qualificationDatabaseSource(requirement.sourceRevision),
    );
  }
  return Object.freeze({ specification, candidate, historical });
}

export async function runMigrationRestoreCase(caseId, input) {
  const request = validateMigrationRestoreRequest(caseId, input);
  return (await import("./migration-restore-worker.mjs")).runBoundMigrationRestoreCase(
    request,
    input,
  );
}
