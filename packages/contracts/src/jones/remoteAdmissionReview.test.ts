import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  RemoteAdmissionRecord,
  RemoteAdmissionStatus,
  RemoteAdmissionEvaluation,
} from "./remoteAdmission.ts";
import {
  EnvironmentAuthenticatedAuth,
  EnvironmentInternalError,
  EnvironmentHttpConflictError,
} from "../environmentHttp.ts";
import { RemoteAdmissionDecision } from "./remoteAdmissionHttp.ts";
import {
  REMOTE_ADMISSION_REVIEW_VERSION,
  makeRemoteAdmissionReviewHttpGroup,
  RemoteAdmissionReviewCapabilities,
  RemoteAdmissionReviewFeatures,
  RemoteAdmissionReviewCounts,
  RemoteAdmissionReviewQuery,
  RemoteAdmissionReviewDecision,
  RemoteAdmissionReviewDecisionResult,
  RemoteAdmissionReviewConflict,
  RemoteAdmissionReviewError,
  RemoteAdmissionReviewUnauthorized,
  RemoteAdmissionReviewUnavailable,
  RemoteAdmissionReviewHttpQuery,
  RemoteAdmissionGrantHttpQuery,
  RemoteAdmissionChangesHttpQuery,
  RemoteAdmissionBlockedAuditReason,
  RemoteAdmissionBlockedMark,
} from "./remoteAdmissionReview.ts";

const persistedRecordJson = Schema.fromJsonString(RemoteAdmissionRecord);
const decodeBlockedAuditReason = Schema.decodeSync(RemoteAdmissionBlockedAuditReason);
const decodeUnknownBlockedMark = Schema.decodeUnknownSync(RemoteAdmissionBlockedMark);
const decodePersistedRecordJson = Schema.decodeSync(persistedRecordJson);
const encodePersistedRecordJson = Schema.encodeSync(persistedRecordJson);
const decodeUnknownStatus = Schema.decodeUnknownSync(RemoteAdmissionStatus);
const decodeUnknownEvaluation = Schema.decodeUnknownSync(RemoteAdmissionEvaluation);
const decodeUnknownReviewDecision = Schema.decodeUnknownSync(RemoteAdmissionReviewDecision);
const decodeUnknownDecision = Schema.decodeUnknownSync(RemoteAdmissionDecision);
const decodeUnknownReviewDecisionResult = Schema.decodeUnknownSync(RemoteAdmissionReviewDecisionResult);
const decodeRecord = Schema.decodeSync(RemoteAdmissionRecord);
const decodeUnknownReviewCapabilities = Schema.decodeUnknownSync(RemoteAdmissionReviewCapabilities);
const decodeReviewCounts = Schema.decodeSync(RemoteAdmissionReviewCounts);
const decodeUnknownReviewQuery = Schema.decodeUnknownSync(RemoteAdmissionReviewQuery);
const decodeReviewHttpQuery = Schema.decodeSync(RemoteAdmissionReviewHttpQuery);
const decodeGrantHttpQuery = Schema.decodeSync(RemoteAdmissionGrantHttpQuery);
const decodeChangesHttpQuery = Schema.decodeSync(RemoteAdmissionChangesHttpQuery);
const decodeUnknownReviewConflict = Schema.decodeUnknownSync(RemoteAdmissionReviewConflict);
const decodeUnknownReviewUnavailable = Schema.decodeUnknownSync(RemoteAdmissionReviewUnavailable);
const decodeUnknownReviewUnauthorized = Schema.decodeUnknownSync(RemoteAdmissionReviewUnauthorized);
const decodeReviewError = Schema.decodeSync(RemoteAdmissionReviewError);
const decodeUnknownEnvironmentHttpConflictError = Schema.decodeUnknownSync(
  EnvironmentHttpConflictError,
);
const decodeUnknownReviewError = Schema.decodeUnknownSync(RemoteAdmissionReviewError);

it("keeps malicious audit facts separate from access-blocked admission marks", () => {
  expect(decodeBlockedAuditReason("malicious")).toBe("malicious");
  expect(() =>
    decodeUnknownBlockedMark({
      admissionId: "request",
      managedScopeId: "scope",
      reason: "malicious",
    }),
  ).toThrow();
});

const digest = "a".repeat(64);
const policy = { enabled: true, revision: "policy", modelSelection: null };
const record = {
  id: "admission",
  bindingDigest: digest,
  status: "held",
  commandId: null,
  createdAt: "2026-11-16T12:00:00.000Z",
  updatedAt: "2026-11-16T12:00:00.000Z",
  idempotencyKey: "request-key",
  requestDigest: digest,
  envelope: {
    identity: { clientId: "session", linkId: "link" },
    operation: "t3_thread_send",
    payload: { message: "synthetic" },
    target: {
      projectId: "project",
      threadId: "thread",
      runtimeMode: "full-access",
      interactionMode: "default",
    },
    receiverPolicy: policy,
  },
  reason: "Access revoked",
  category: "out_of_scope",
  resolvedBy: null,
  resolvedAt: null,
};
const counts = {
  currentHeldAdmissions: 449,
  blockedHeldAdmissions: 1,
  heldAdmissions: 450,
  inFlightAdmissions: 2,
  failedAdmissions: 3,
  unresolvedAdmissions: 4,
  activeAccessScopes: 5,
  activeThreads: 2,
};

describe("remote admission review compatibility", () => {
  it("declares additive authenticated routes without replacing V1 paths", () => {
    const headers = Schema.Struct({
      authorization: Schema.optionalKey(Schema.String),
      dpop: Schema.optionalKey(Schema.String),
    });
    const group = makeRemoteAdmissionReviewHttpGroup({
      OptionalBearerHeaders: headers,
      EnvironmentAuthenticatedAuth,
      EnvironmentInternalError,
    });
    const routes = Object.values(group.endpoints);
    expect(routes.map(({ identifier, method, path }) => [identifier, method, path])).toEqual([
      ["capabilities", "GET", "/api/jones/remote-admission/capabilities"],
      ["review", "GET", "/api/jones/remote-admission/review"],
      ["item", "GET", "/api/jones/remote-admission/review/item"],
      ["decision", "POST", "/api/jones/remote-admission/review/decision"],
      ["grants", "GET", "/api/jones/remote-admission/grants"],
      ["updateGrant", "POST", "/api/jones/remote-admission/grants/update"],
      ["revokeGrant", "POST", "/api/jones/remote-admission/grants/revoke"],
      ["restoreGrant", "POST", "/api/jones/remote-admission/grants/restore"],
      ["retry", "POST", "/api/jones/remote-admission/review/retry"],
      ["recheck", "POST", "/api/jones/remote-admission/review/recheck"],
      ["changes", "GET", "/api/jones/remote-admission/changes"],
    ]);
    for (const endpoint of routes) {
      expect(endpoint.middlewares.has(EnvironmentAuthenticatedAuth)).toBe(true);
      expect(endpoint.headers?.ast).toEqual(headers.ast);
    }
  });
  it("keeps the complete persisted V1 bytes and managed hold literals decodable", () => {
    const persisted = JSON.stringify(record);
    const decoded = decodePersistedRecordJson(persisted);
    expect(decoded).toEqual(record);
    expect(encodePersistedRecordJson(decoded)).toBe(persisted);
    for (const status of [
      "evaluating",
      "passed",
      "held",
      "approved",
      "denied",
      "cancelled",
      "dispatching",
      "dispatched",
    ])
      expect(decodeUnknownStatus(status)).toBe(status);
    for (const status of ["blocked", "done", "unknown", "failed"])
      expect(() => decodeUnknownStatus(status)).toThrow();
    expect(() =>
      decodeUnknownEvaluation({
        verdict: "hold",
        category: "access_revoked",
        reason: "revoked",
      }),
    ).toThrow();
  });

  it("retains an old client's once decision and strips additive grant intent for V1", () => {
    const once = { id: "admission", bindingDigest: digest, decision: "approve" };
    const extended = { ...once, grant: { durationSeconds: 86400 } };
    expect(decodeUnknownReviewDecision(once)).toEqual(once);
    expect(decodeUnknownDecision(extended)).toEqual(once);
    expect(decodeUnknownReviewDecision(extended)).toEqual(extended);
    const committed = decodeUnknownReviewDecisionResult({
      committed: true,
      record,
      grant: null,
    });
    expect(decodeRecord(committed.record)).toEqual(record);
    expect(() =>
      decodeUnknownReviewDecisionResult({
        ...committed,
        committed: false,
      }),
    ).toThrow();
  });

  it("separates independent support flags from authenticated owner permission", () => {
    const features = Object.fromEntries(
      Object.keys(RemoteAdmissionReviewFeatures.fields).map((key) => [key, false]),
    );
    const decode = decodeUnknownReviewCapabilities;
    for (const key of Object.keys(features)) {
      const value = {
        version: REMOTE_ADMISSION_REVIEW_VERSION,
        features: { ...features, [key]: true },
        principal: { canDecide: false },
      };
      expect(decode(value)).toEqual(value);
    }
    expect(() =>
      decode({ version: REMOTE_ADMISSION_REVIEW_VERSION, features, principal: {} }),
    ).toThrow();
    const originalV2 = {
      version: REMOTE_ADMISSION_REVIEW_VERSION,
      features: { reviewV2: true, grants: true, conflicts: true, changeFeed: true },
      principal: { canDecide: true },
    };
    expect(decode(originalV2)).toEqual(originalV2);
    expect(decode(originalV2).features.managedAccess).toBeUndefined();
  });

  it("uses typed disjoint held counts and bounded opaque per-view paging", () => {
    expect(decodeReviewCounts(counts)).toEqual(counts);
    for (const delta of [
      { heldAdmissions: 451 },
      { blockedHeldAdmissions: -1 },
      { activeThreads: 1.5 },
    ])
      expect(() =>
        decodeReviewCounts({ ...counts, ...delta }),
      ).toThrow();
    const decode = decodeUnknownReviewQuery;
    for (const view of ["current", "blocked", "done", "unresolved", "inflight", "access"])
      expect(decode({ view, cursor: "opaque-token", limit: 100 })).toEqual({
        view,
        cursor: "opaque-token",
        limit: 100,
      });
    expect(decode({ view: "current" })).toEqual({ view: "current" });
    for (const limit of [0, 101, 1.5]) expect(() => decode({ view: "current", limit })).toThrow();
    expect(() => decode({ view: "all" })).toThrow();
  });

  it("decodes numeric HTTP query strings without changing local numeric DTOs", () => {
    expect(
      decodeReviewHttpQuery({ view: "current", limit: "50" }),
    ).toEqual({ view: "current", limit: 50 });
    expect(decodeGrantHttpQuery({ limit: "100" })).toEqual({
      limit: 100,
    });
    expect(decodeChangesHttpQuery({ timeoutMs: "50000" })).toEqual({
      timeoutMs: 50000,
    });
    for (const limit of ["0", "101", "1.5", "NaN"])
      expect(() =>
        decodeReviewHttpQuery({ view: "current", limit }),
      ).toThrow();
  });

  it("returns known current state on conflicts and preserves unavailable versus neutral 401", () => {
    const current = {
      admission: {
        id: record.id,
        bindingDigest: digest,
        status: "held",
        commandId: null,
        createdAt: record.createdAt,
        updatedAt: record.updatedAt,
      },
      grant: null,
      chainHead: null,
      attempt: null,
    };
    const conflict = {
      code: "conflict",
      reason: "binding_changed",
      message: "Request changed",
      current,
    };
    expect(decodeUnknownReviewConflict(conflict)).toEqual(conflict);
    expect(() =>
      decodeUnknownReviewConflict({ ...conflict, current: undefined }),
    ).toThrow();
    const unavailable = { code: "history_unavailable", message: "Refresh required" };
    expect(decodeUnknownReviewUnavailable(unavailable)).toEqual(
      unavailable,
    );
    const unauthorized = {
      code: "review_unavailable",
      message: "Request review is unavailable for this session",
    };
    expect(decodeUnknownReviewUnauthorized(unauthorized)).toEqual(
      unauthorized,
    );
    expect(
      decodeReviewError({
        code: "owner_required",
        message: "Owner required",
      }).code,
    ).toBe("owner_required");
    const legacyError = { _tag: "EnvironmentHttpConflictError", message: "Legacy conflict" };
    const oldDecode = decodeUnknownEnvironmentHttpConflictError;
    expect(oldDecode(legacyError).message).toBe(legacyError.message);
    expect(oldDecode({ ...legacyError, current }).message).toBe(legacyError.message);
    expect(() => oldDecode(conflict)).toThrow();
    expect(() => decodeUnknownReviewError(legacyError)).toThrow();
  });
});
