import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as SchemaAST from "effect/SchemaAST";
import { EnvironmentWorkstreamOperationConflictError } from "./environmentHttpGroups.ts";
import { EnvironmentId, TrimmedNonEmptyString } from "../baseSchemas.ts";
import { JonesBuildStartedAt } from "./fleetUpdates/source.ts";

import { EnvironmentAuthenticatedAuth, EnvironmentHttpApi } from "../environmentHttp.ts";
import v2Conformance from "../../contracts/workstreams-registration-context/v2/fixtures/conformance.json" with { type: "json" };
import {
  JonesNativePrepareInput,
  JonesQuitPreparation,
  JonesUpdateCapability,
  JonesUpdateExactInput,
  JonesUpdateInstallInput,
  JonesUpdateState,
  JonesUpdateReconcileInput,
  JonesUpdateRecordRetiredInput,
} from "./jonesUpdates.ts";

// The public API metadata is also consumed by independently upgraded clients.
// Keep the Jones routes and authentication identity stable through relocation.
describe("Jones environment HTTP registration", () => {
  it("preserves the environment group order", () => {
    expect(Object.keys(EnvironmentHttpApi.groups)).toEqual([
      "providerQueue",
      "queueDispatch",
      "voiceReview",
      "hostStatus",
      "workQueueMetadata",
      "settlementOrigins",
      "settlementRestore",
      "metadata",
      "auth",
      "mcpOAuth",
      "orchestration",
      "pullRequests",
      "workstreams",
      "workstreamsNative",
      "projects",
      "connect",
      "conversationLibrary",
      "jonesUpdates",
      "jonesFleetUpdates",
      "remoteAdmission",
      "jonesImportedHistory",
      "previewCompanion",
      "webhooks",
    ]);
  });

  it("preserves Jones route methods, paths, and authentication on every endpoint", () => {
    const groups = EnvironmentHttpApi.groups;
    const routes = [
      ...Object.values(groups.voiceReview.endpoints),
      ...Object.values(groups.hostStatus.endpoints),
      ...Object.values(groups.workQueueMetadata.endpoints),
      ...Object.values(groups.settlementOrigins.endpoints),
      ...Object.values(groups.settlementRestore.endpoints),
      ...Object.values(groups.conversationLibrary.endpoints),
      ...Object.values(groups.workstreams.endpoints),
      ...Object.values(groups.jonesUpdates.endpoints),
      ...Object.values(groups.workstreamsNative.endpoints),
    ];
    expect(routes.map(({ identifier, method, path }) => [identifier, method, path])).toEqual([
      ["recent", "GET", "/api/voice-review/recent"],
      ["registrySnapshot", "GET", "/api/voice-review/registry/snapshot"],
      ["registryWorkstreams", "GET", "/api/voice-review/registry/workstreams"],
      ["registryEvents", "GET", "/api/voice-review/registry/events"],
      ["correctAssociation", "POST", "/api/voice-review/registry/associations"],
      ["correctLabel", "POST", "/api/voice-review/registry/labels"],
      ["diagnostics", "GET", "/api/voice-review/drafts/:id/diagnostics"],
      ["list", "GET", "/api/voice-review/drafts"],
      ["get", "GET", "/api/voice-review/drafts/:id"],
      ["pause", "POST", "/api/voice-review/drafts/:id/pause"],
      ["play", "POST", "/api/voice-review/drafts/:id/play"],
      ["editBegin", "POST", "/api/voice-review/drafts/:id/edit-begin"],
      ["editSave", "POST", "/api/voice-review/drafts/:id/edit-save"],
      ["editCancel", "POST", "/api/voice-review/drafts/:id/edit-cancel"],
      ["sendNow", "POST", "/api/voice-review/drafts/:id/send-now"],
      ["delete", "POST", "/api/voice-review/drafts/:id/delete"],
      ["snapshot", "GET", "/api/host-status"],
      ["snapshot", "GET", "/api/work-queue/metadata"],
      ["resolve", "POST", "/api/jones/settlement-origins"],
      ["inspect", "POST", "/api/jones/settlement-restores/inspect"],
      ["conversationLibrary", "POST", "/api/conversation-library"],
      ["appearanceRead", "POST", "/api/workstreams/appearance/read"],
      ["appearanceSave", "POST", "/api/workstreams/appearance/write"],
      ["registrationContext", "GET", "/api/workstreams/registration-context"],
      ["operationSubmit", "POST", "/api/workstreams/operations/submit"],
      ["operationRead", "POST", "/api/workstreams/operations/read"],
      ["setupStatus", "GET", "/api/workstreams/setup-status"],
      ["threadPlacements", "POST", "/api/workstreams/thread-placements"],
      ["list", "GET", "/api/workstreams"],
      ["references", "GET", "/api/workstreams/references"],
      ["reference", "GET", "/api/workstreams/references/:nativeReferenceId"],
      ["detail", "GET", "/api/workstreams/:workstreamId"],
      ["memberships", "GET", "/api/workstreams/:workstreamId/memberships"],
      ["declarations", "GET", "/api/workstreams/:workstreamId/declarations"],
      ["edges", "GET", "/api/workstreams/:workstreamId/edges"],
      ["history", "GET", "/api/workstreams/:workstreamId/history"],
      ["command", "GET", "/api/workstreams/commands/:commandId"],
      ["submit", "POST", "/api/workstreams/commands"],
      ["reconcile", "POST", "/api/jones-updates/reconcile"],
      ["recordRetired", "POST", "/api/jones-updates/record-retired"],
      ["prepareNative", "POST", "/api/jones-updates/prepare-native"],
      ["prepareQuit", "POST", "/api/jones-updates/prepare-quit"],
      ["state", "GET", "/api/jones-updates"],
      ["sourceRelation", "GET", "/api/jones-updates/source-relation"],
      ["check", "POST", "/api/jones-updates/check"],
      ["download", "POST", "/api/jones-updates/download"],
      ["downloadExact", "POST", "/api/jones-updates/download-exact"],
      ["install", "POST", "/api/jones-updates/install"],
      ["ownerOperations", "POST", "/api/workstreams/native/owner/v1/operations"],
      ["context", "GET", "/api/workstreams/native/v1/context"],
      ["attestations", "POST", "/api/workstreams/native/v1/attestations"],
      ["settlements", "POST", "/api/workstreams/native/v1/settlements"],
      ["settlementLookup", "POST", "/api/workstreams/native/v1/settlements/lookup"],
      ["contextV2", "GET", "/api/workstreams/native/v2/context"],
      ["attestationsV2", "POST", "/api/workstreams/native/v2/attestations"],
      ["settlementsV2", "POST", "/api/workstreams/native/v2/settlements"],
      ["settlementLookupV2", "POST", "/api/workstreams/native/v2/settlements/lookup"],
    ]);
    const bearerHeaders = Schema.Struct({
      authorization: Schema.optionalKey(Schema.String),
      dpop: Schema.optionalKey(Schema.String),
    });
    for (const endpoint of routes) {
      expect(endpoint.middlewares.has(EnvironmentAuthenticatedAuth)).toBe(true);
      expect(endpoint.headers?.ast).toEqual(bearerHeaders.ast);
    }
  });
});

it("requires a nonnegative integer count in quit preparation responses", () => {
  const decode = Schema.decodeUnknownSync(JonesQuitPreparation);
  expect(decode({ prepared: false, threadCount: 0 })).toEqual({ prepared: false, threadCount: 0 });
  expect(decode({ prepared: true, threadCount: 2 })).toEqual({ prepared: true, threadCount: 2 });
  for (const threadCount of [-1, 0.5, Infinity, NaN]) {
    expect(() => decode({ prepared: true, threadCount })).toThrow();
  }
});

it("requires an explicit native attempt without changing ordinary install requests", () => {
  const base = { stagedHandle: "a".repeat(64), environmentId: "fixture", currentVersion: "1.0.0" };
  const decode = Schema.decodeUnknownSync(JonesNativePrepareInput);
  expect(() => decode(base)).toThrow();
  expect(() => decode({ ...base, transactionId: "invalid" })).toThrow();
  expect(decode({ ...base, transactionId: "b".repeat(64) })).toEqual({
    ...base,
    transactionId: "b".repeat(64),
  });
  expect(Schema.decodeSync(JonesUpdateInstallInput)(base)).toEqual(base);
});

it("selects an exact source by full commit only and keeps older capability shapes decodable", () => {
  const decode = Schema.decodeUnknownSync(JonesUpdateExactInput);
  const targetSource = "a".repeat(40);
  expect(decode({ targetSource })).toEqual({ targetSource });
  for (const invalid of ["a".repeat(39), "A".repeat(40), "a".repeat(41), "0.1.0-preview.1", ""])
    expect(() => decode({ targetSource: invalid })).toThrow();
  expect(() => decode({})).toThrow();
  const legacy = { check: true, download: true, install: false };
  expect(Schema.decodeSync(JonesUpdateCapability)(legacy)).toEqual(legacy);
  expect(Schema.decodeSync(JonesUpdateCapability)({ ...legacy, exactSource: true })).toEqual({
    ...legacy,
    exactSource: true,
  });
});

it("requires the complete immutable binding for an optional staged replacement", () => {
  const decode = Schema.decodeUnknownSync(JonesUpdateExactInput);
  const replace = {
    stagedHandle: "retained-stage",
    sourceSha: "b".repeat(40),
    operationId: "11111111-1111-4111-8111-111111111111",
    environmentId: "synthetic-host",
    currentVersion: "0.1.0-preview.1",
    expectedInstalledSource: "c".repeat(40),
  };
  const input = { targetSource: "a".repeat(40), replace };
  expect(decode(input)).toEqual(input);
  for (const field of Object.keys(replace)) {
    const incomplete: Record<string, unknown> = { ...replace };
    delete incomplete[field];
    expect(() => decode({ ...input, replace: incomplete })).toThrow();
  }
  for (const patch of [
    { sourceSha: "b".repeat(39) },
    { expectedInstalledSource: "C".repeat(40) },
    { operationId: "11111111-1111-5111-8111-111111111111" },
    { stagedHandle: "" },
    { environmentId: "" },
    { currentVersion: "" },
  ])
    expect(() => decode({ ...input, replace: { ...replace, ...patch } })).toThrow();
  const capability = {
    check: true,
    download: true,
    install: false,
    exactSource: true,
    replaceStaged: true,
  };
  expect(Schema.decodeSync(JonesUpdateCapability)(capability)).toEqual(capability);
});

it("accepts both explicitly versioned registration responses on the local HTTP boundary", () => {
  const responseSchema = [
    ...EnvironmentHttpApi.groups.workstreams.endpoints.registrationContext.success,
  ][0];
  if (responseSchema === undefined) throw new Error("Missing registration response schema");
  const accepts = Schema.is(responseSchema);
  const v2 = v2Conformance.cases.find(
    (fixture) => fixture.name === "legacy singleton shape with v2 protocol",
  )?.value;
  if (v2 === undefined) throw new Error("Missing singleton fixture");
  expect(accepts(v2)).toBe(true);
  expect(accepts({ ...v2, protocol: "workstreams-registration-context/1.0.0" })).toBe(true);
  expect(accepts({ ...v2, protocol: "workstreams-registration-context/3.0.0" })).toBe(false);
});

it("keeps operation conflicts on HTTP 409 with the closed operation error shape", () => {
  const endpoint = EnvironmentHttpApi.groups.workstreams.endpoints.operationSubmit;
  const conflict = EnvironmentWorkstreamOperationConflictError;
  expect(
    [...endpoint.error].some(
      (schema) => SchemaAST.resolveAt<number>("httpApiStatus")(schema.ast) === 409,
    ),
  ).toBe(true);
  expect(SchemaAST.resolveAt<number>("httpApiStatus")(conflict.ast)).toBe(409);
  const decode = Schema.decodeUnknownSync(conflict);
  for (const value of [
    { code: "operation_id_conflict" },
    { code: "thread_operation_in_progress", open_operation_id: "operation-00000001" },
  ]) {
    expect(decode(value)).toEqual(value);
  }
  for (const value of [
    { code: "conflict" },
    { code: "operation_id_conflict", message: "unexpected envelope" },
    { code: "thread_operation_in_progress", open_operation_id: "" },
  ]) {
    expect(() => decode(value)).toThrow();
  }
});

// Frozen wire decoder from 25b131a0af87b4ef868b6336fcf759a4c5a2acd8.
// Supplemental fields must remain acceptable to clients built before settlement support.
const BaseJonesUpdatePhase = Schema.Literals([
  "checking",
  "building",
  "no-new",
  "available",
  "downloading",
  "verifying",
  "staged",
  "preparing",
  "installing",
  "committed",
  "rolled-back",
  "blocked",
  "error",
]);

const BaseJonesUpdateProvenance = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sourceSha: TrimmedNonEmptyString,
  sourceTree: TrimmedNonEmptyString,
  workflow: TrimmedNonEmptyString,
  runId: Schema.Number,
  runAttempt: Schema.Number,
  artifactId: Schema.Number,
  artifactDigest: TrimmedNonEmptyString,
  platform: Schema.Literals(["linux", "darwin"]),
  architecture: Schema.Literals(["x64", "arm64"]),
  version: Schema.optionalKey(TrimmedNonEmptyString),
  buildStartedAt: Schema.optionalKey(JonesBuildStartedAt),
  payloadSha256: Schema.optionalKey(TrimmedNonEmptyString),
});

const BaseJonesUpdateCapability = Schema.Struct({
  check: Schema.Boolean,
  download: Schema.Boolean,
  install: Schema.Boolean,
  /**
   * The server accepts the exact-source download action. Absent means unsupported, which is how
   * older servers read; it is never advertised without `download`.
   */
  exactSource: Schema.optionalKey(Schema.Boolean),
  reason: Schema.optionalKey(
    Schema.Literals([
      "bootstrap-required",
      "source-unqualified",
      "unsupported-platform",
      "native-consent-required",
      "blocked",
    ]),
  ),
});

/**
 * Present only on a host enrolled to follow a laptop. Such a host offers, stages and installs
 * exactly `leaderSource`, the laptop's committed source, and only a build whose commit has
 * `leaderTree`; null means the laptop has not published one yet, so the host holds. Hosts without
 * a policy select their own builds.
 */
const BaseJonesUpdatePolicy = Schema.Struct({
  role: Schema.Literal("follower"),
  leaderSource: Schema.NullOr(Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/))),
  /** Sent with every published `leaderSource`. */
  leaderTree: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/))),
});

/** The staged handle is immutable even when a later check discovers another build. */
const BaseJonesUpdateState = Schema.Struct({
  source: Schema.Literal("jones-actions"),
  channel: Schema.Literal("jones-main"),
  phase: BaseJonesUpdatePhase,
  revision: Schema.optionalKey(Schema.Number),
  capability: BaseJonesUpdateCapability,
  provenance: Schema.optionalKey(BaseJonesUpdateProvenance),
  stagedHandle: Schema.optionalKey(TrimmedNonEmptyString),
  message: Schema.optionalKey(Schema.String),
  checkedAt: Schema.optionalKey(Schema.String),
  environmentId: Schema.optionalKey(EnvironmentId),
  currentVersion: Schema.optionalKey(Schema.String),
  currentBuildStartedAt: Schema.optionalKey(JonesBuildStartedAt),
  installedSource: Schema.optionalKey(
    Schema.Struct({
      repository: Schema.Literal("Jones-Systems/Jones-Code"),
      sha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
      tree: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    }),
  ),
  updateId: Schema.optionalKey(TrimmedNonEmptyString),
  outcome: Schema.optionalKey(
    Schema.Struct({
      status: Schema.Literals(["committed", "rolled-back", "blocked"]),
      reason: Schema.optionalKey(Schema.String),
      fromVersion: TrimmedNonEmptyString,
      targetVersion: TrimmedNonEmptyString,
    }),
  ),
  recovery: Schema.optionalKey(
    Schema.Struct({
      method: Schema.Literals(["clone", "copy"]),
      bytes: Schema.Number,
      completedAt: Schema.String,
      durationMs: Schema.optionalKey(Schema.Number),
    }),
  ),
  migrationPlan: Schema.optionalKey(
    Schema.Struct({
      pendingUpstream: Schema.Array(Schema.Number),
      pendingJones: Schema.Array(Schema.Number),
    }),
  ),
  policy: Schema.optionalKey(BaseJonesUpdatePolicy),
});

const decodeBaseJonesUpdateState = Schema.decodeUnknownSync(BaseJonesUpdateState);
const decodeJonesUpdateState = Schema.decodeUnknownSync(JonesUpdateState);
const decodeJonesUpdateReconcileInput = Schema.decodeUnknownSync(JonesUpdateReconcileInput);
const decodeJonesUpdateRecordRetiredInput = Schema.decodeUnknownSync(JonesUpdateRecordRetiredInput);

it("preserves old and new decoder compatibility without changing failed outcomes", () => {
  const oldState = {
    source: "jones-actions",
    channel: "jones-main",
    phase: "error",
    capability: { check: true, download: true, install: false },
    updateId: "11111111-1111-4111-8111-111111111111",
    outcome: {
      status: "blocked",
      fromVersion: "0.1.0",
      targetVersion: "0.2.0",
      reason: "launcher-restarted",
    },
  };
  const supplemental = {
    settlement: "legacy-reconciled",
    settlementRefusal: "restore-pending",
    cause: "backup refused",
    launcherVersion: "0.1.0",
    childVersion: "0.2.0",
    launcherSettlement: "unsupported",
    launcherRefresh: "recommended",
    launcherRefreshReason: "settlement-unsupported",
  };
  expect(decodeBaseJonesUpdateState({ ...oldState, ...supplemental })).toEqual(oldState);
  expect(decodeJonesUpdateState(oldState)).toEqual(oldState);
  expect(decodeJonesUpdateState({ ...oldState, ...supplemental })).toEqual({
    ...oldState,
    ...supplemental,
  });
  for (const refusal of [
    "not-failed",
    "not-found",
    "insufficient-evidence",
    "restore-pending",
    "active-changed",
    "trial-evidence-present",
    "retired-evidence-mismatch",
    "settlement-write-failed",
    "capability-unsupported",
  ]) {
    expect(
      decodeJonesUpdateState({ ...oldState, settlementRefusal: refusal }).settlementRefusal,
    ).toBe(refusal);
  }
});

it("requires both reconciliation identities and bounds retired evidence location", () => {
  const input = {
    operationId: "11111111-1111-4111-8111-111111111111",
    requestId: "22222222-2222-4222-8222-222222222222",
  };
  expect(decodeJonesUpdateReconcileInput(input)).toEqual(input);
  expect(() => decodeJonesUpdateReconcileInput({ operationId: input.operationId })).toThrow();
  expect(() => decodeJonesUpdateRecordRetiredInput(input)).toThrow();
  expect(
    decodeJonesUpdateRecordRetiredInput({
      ...input,
      retiredLocation: "synthetic-retired",
    }),
  ).toEqual({ ...input, retiredLocation: "synthetic-retired" });
});
