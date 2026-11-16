import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  REMOTE_ADMISSION_PEER_STATUS_VERSION,
  REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION,
  REMOTE_ADMISSION_RECOVERED_RESULT_VERSION,
  REMOTE_ADMISSION_RECOVERY_STATUS_VERSION,
  RemoteAdmissionPeerStatusInput,
  RemoteAdmissionPeerStatus,
  RemoteAdmissionCapabilitiesRegisterInput,
  RemoteAdmissionRecoveryStatusInput,
  RemoteAdmissionRecoveryStatus,
  RemoteAdmissionRetryInput,
  RemoteAdmissionNoticeRegisterInput,
  RemoteAdmissionNoticeAckInput,
  RemoteAdmissionRecoveredResult,
  RemoteAdmissionObservationInput,
  RemoteAdmissionAttemptRegisterInput,
} from "./remoteAdmissionReview.ts";

const stages = {
  creation: "confirmed",
  availability: "unknown",
  initialMessage: "unconfirmed",
  preparation: "unknown",
  providerProgress: "unknown",
};
const result = {
  outcome: "created",
  stages,
  threadId: "confirmed-thread",
  recordedAt: "2026-11-16T12:00:00Z",
  reasonCode: "projection_missing",
};
const identity = {
  receiverEnvironmentId: "receiver",
  admissionId: "admission",
  recoveredRevision: 1,
};

describe("remote admission recovery wire separation", () => {
  it("keeps pure peer status free of capability registration and owner counts", () => {
    const decode = Schema.decodeUnknownSync(RemoteAdmissionPeerStatusInput);
    expect(
      decode({ after: "session-revision", timeoutMs: 50000, supports: { retryObservation: true } }),
    ).toEqual({ after: "session-revision", timeoutMs: 50000 });
    expect(decode({})).toEqual({});
    expect(() => decode({ timeoutMs: 50001 })).toThrow();
    expect(() => decode({ timeoutMs: -1 })).toThrow();
    const response = {
      after: "session-revision",
      receiverEnvironmentId: "receiver",
      sessionId: "authenticated-receiver-session",
      peerLongPoll: false,
      serverNow: "2026-11-16T12:00:00Z",
      grants: [],
      attempts: [],
      notices: [],
    };
    expect(
      Schema.decodeUnknownSync(RemoteAdmissionPeerStatus)({
        ...response,
        heldAdmissions: 450,
        ownerSequence: "secret",
      }),
    ).toEqual(response);
  });

  it("registers each feature independently with exact versions and a coordinator generation", () => {
    const decode = Schema.decodeUnknownSync(RemoteAdmissionCapabilitiesRegisterInput);
    const features = {
      peerProjection: REMOTE_ADMISSION_PEER_STATUS_VERSION,
      peerLongPoll: REMOTE_ADMISSION_PEER_STATUS_VERSION,
      retryObservation: REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION,
      recoveredResultObservation: REMOTE_ADMISSION_RECOVERED_RESULT_VERSION,
      recoveryInspection: REMOTE_ADMISSION_RECOVERY_STATUS_VERSION,
    };
    for (const [key, value] of Object.entries(features)) {
      const input = { coordinatorGeneration: "link-generation", versions: { [key]: value } };
      expect(decode(input)).toEqual(input);
    }
    expect(() => decode({ versions: features })).toThrow();
    expect(() =>
      decode({ coordinatorGeneration: "generation", versions: { retryObservation: true } }),
    ).toThrow();
    expect(() =>
      decode({
        coordinatorGeneration: "generation",
        versions: { peerProjection: "future-version" },
      }),
    ).toThrow();
  });

  it("does not turn missing availability into deletion or provider completion", () => {
    const recovered = {
      admissionId: "admission",
      revision: 1,
      evidence: {
        evidenceDigest: "a".repeat(64),
        observedAt: result.recordedAt,
        identityPath: "hashed",
        creationCommandId: "mcp:launch:hash",
        initialMessageCommandId: "mcp:launch:hash:initial-message",
        abandonedClaim: true,
        readsComplete: true,
        contradictions: [],
      },
      result,
    };
    expect(Schema.decodeUnknownSync(RemoteAdmissionRecoveredResult)(recovered)).toEqual(recovered);
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionRecoveredResult)({ ...recovered, revision: 0 }),
    ).toThrow();
    const status = {
      receiverEnvironmentId: "receiver",
      state: "created",
      reasonCode: "projection_missing",
      stages,
      confirmedThreadId: "confirmed-thread",
      requestsRoute: "/requests",
    };
    expect(Schema.decodeUnknownSync(RemoteAdmissionRecoveryStatus)(status)).toEqual(status);
    expect(
      Schema.decodeUnknownSync(RemoteAdmissionRecoveryStatus)({
        ...status,
        payload: "never disclose",
      }),
    ).not.toHaveProperty("payload");
  });

  it("keeps local locator and peer request inspection separate from owner mutation", () => {
    const decode = Schema.decodeUnknownSync(RemoteAdmissionRecoveryStatusInput);
    expect(decode({ locator: "opaque-locator" })).toEqual({ locator: "opaque-locator" });
    expect(decode({ clientRequestId: "original-key", originThreadId: "origin" })).toEqual({
      clientRequestId: "original-key",
      originThreadId: "origin",
    });
    expect(() => decode({ clientRequestId: "original-key" })).toThrow();
    expect(() => decode({ decision: "approve" })).toThrow();
    expect(
      Schema.decodeUnknownSync(RemoteAdmissionObservationInput)({
        clientRequestId: "original-key",
        decision: "approve",
      }),
    ).toEqual({ clientRequestId: "original-key" });
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionRetryInput)({
        id: "admission",
        bindingDigest: "a".repeat(64),
      }),
    ).toThrow();
  });

  it("uses distinct immutable attempt and recovered notice registration identities", () => {
    const attempt = { attemptId: "attempt", observationKey: "new-observation" };
    expect(Schema.decodeSync(RemoteAdmissionAttemptRegisterInput)(attempt)).toEqual(attempt);
    const notice = { ...identity, observationKey: "notice-observation" };
    expect(Schema.decodeSync(RemoteAdmissionNoticeRegisterInput)(notice)).toEqual(notice);
    expect(() => Schema.decodeUnknownSync(RemoteAdmissionNoticeRegisterInput)(attempt)).toThrow();
    for (const disposition of ["delivered", "suppressed"]) {
      const ack = { ...identity, disposition };
      expect(Schema.decodeUnknownSync(RemoteAdmissionNoticeAckInput)(ack)).toEqual(ack);
    }
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionNoticeAckInput)({
        ...identity,
        disposition: "dismissed",
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RemoteAdmissionNoticeAckInput)({
        admissionId: "admission",
        disposition: "delivered",
      }),
    ).toThrow();
  });
});
