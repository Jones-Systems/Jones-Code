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

it("keeps malicious audit facts separate from access-blocked admission marks", () => {
  expect(Schema.decodeSync(RemoteAdmissionBlockedAuditReason)("malicious")).toBe("malicious");
  expect(() =>
    Schema.decodeUnknownSync(RemoteAdmissionBlockedMark)({
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
    const schema = Schema.fromJsonString(RemoteAdmissionRecord);
    const decoded = Schema.decodeSync(schema)(persisted);
    expect(decoded).toEqual(record);
    expect(Schema.encodeSync(schema)(decoded)).toBe(persisted);
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
      expect(Schema.decodeUnknownSync(RemoteAdmissionStatus)(status)).toBe(status);
    for (const status of ["blocked", "done", "unknown", "failed"])
      expect(() => Schema.decodeUnknownSync(RemoteAdmissionStatus)(status)).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionEvaluation)({
        verdict: "hold",
        category: "access_revoked",
        reason: "revoked",
      }),
    ).toThrow();
  });

  it("retains an old client's once decision and strips additive grant intent for V1", () => {
    const once = { id: "admission", bindingDigest: digest, decision: "approve" };
    const extended = { ...once, grant: { durationSeconds: 86400 } };
    expect(Schema.decodeUnknownSync(RemoteAdmissionReviewDecision)(once)).toEqual(once);
    expect(Schema.decodeUnknownSync(RemoteAdmissionDecision)(extended)).toEqual(once);
    expect(Schema.decodeUnknownSync(RemoteAdmissionReviewDecision)(extended)).toEqual(extended);
    const committed = Schema.decodeUnknownSync(RemoteAdmissionReviewDecisionResult)({
      committed: true,
      record,
      grant: null,
    });
    expect(Schema.decodeSync(RemoteAdmissionRecord)(committed.record)).toEqual(record);
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionReviewDecisionResult)({
        ...committed,
        committed: false,
      }),
    ).toThrow();
  });

  it("separates independent support flags from authenticated owner permission", () => {
    const features = Object.fromEntries(
      Object.keys(RemoteAdmissionReviewFeatures.fields).map((key) => [key, false]),
    );
    const decode = Schema.decodeUnknownSync(RemoteAdmissionReviewCapabilities);
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
    expect(Schema.decodeSync(RemoteAdmissionReviewCounts)(counts)).toEqual(counts);
    for (const delta of [
      { heldAdmissions: 451 },
      { blockedHeldAdmissions: -1 },
      { activeThreads: 1.5 },
    ])
      expect(() =>
        Schema.decodeSync(RemoteAdmissionReviewCounts)({ ...counts, ...delta }),
      ).toThrow();
    const decode = Schema.decodeUnknownSync(RemoteAdmissionReviewQuery);
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
      Schema.decodeSync(RemoteAdmissionReviewHttpQuery)({ view: "current", limit: "50" }),
    ).toEqual({ view: "current", limit: 50 });
    expect(Schema.decodeSync(RemoteAdmissionGrantHttpQuery)({ limit: "100" })).toEqual({
      limit: 100,
    });
    expect(Schema.decodeSync(RemoteAdmissionChangesHttpQuery)({ timeoutMs: "50000" })).toEqual({
      timeoutMs: 50000,
    });
    for (const limit of ["0", "101", "1.5", "NaN"])
      expect(() =>
        Schema.decodeSync(RemoteAdmissionReviewHttpQuery)({ view: "current", limit }),
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
    expect(Schema.decodeUnknownSync(RemoteAdmissionReviewConflict)(conflict)).toEqual(conflict);
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionReviewConflict)({ ...conflict, current: undefined }),
    ).toThrow();
    const unavailable = { code: "history_unavailable", message: "Refresh required" };
    expect(Schema.decodeUnknownSync(RemoteAdmissionReviewUnavailable)(unavailable)).toEqual(
      unavailable,
    );
    const unauthorized = {
      code: "review_unavailable",
      message: "Request review is unavailable for this session",
    };
    expect(Schema.decodeUnknownSync(RemoteAdmissionReviewUnauthorized)(unauthorized)).toEqual(
      unauthorized,
    );
    expect(
      Schema.decodeSync(RemoteAdmissionReviewError)({
        code: "owner_required",
        message: "Owner required",
      }).code,
    ).toBe("owner_required");
    const legacyError = { _tag: "EnvironmentHttpConflictError", message: "Legacy conflict" };
    const oldDecode = Schema.decodeUnknownSync(EnvironmentHttpConflictError);
    expect(oldDecode(legacyError).message).toBe(legacyError.message);
    expect(oldDecode({ ...legacyError, current }).message).toBe(legacyError.message);
    expect(() => oldDecode(conflict)).toThrow();
    expect(() => Schema.decodeUnknownSync(RemoteAdmissionReviewError)(legacyError)).toThrow();
  });
});
