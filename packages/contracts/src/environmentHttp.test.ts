import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import { QualifiedQuota } from "./providerQueue.ts";
import {
  NativeCommandObservationV2,
  OrchestrationCommandObservation,
  ThreadTurnStartCommand,
} from "./orchestrationNative.ts";

import {
  EnvironmentAuthInvalidError,
  EnvironmentAuthenticatedAuth,
  EnvironmentInternalError,
  EnvironmentOperationForbiddenError,
  EnvironmentOrchestrationHttpApi,
  EnvironmentRequestInvalidError,
  EnvironmentResourceNotFoundError,
  EnvironmentScopeRequiredError,
  ProviderGoalStateObservation,
} from "./environmentHttp.ts";

const traceId = "trace-1";

const descriptorCodec = <S extends Schema.ConstraintCodec<unknown, unknown>>(
  descriptor: Schema.Top,
  known: S,
): S => {
  expect(descriptor.ast).toEqual(known.ast);
  return known;
};

describe("historical HTTP dispatch rejection contract", () => {
  const endpoint = EnvironmentOrchestrationHttpApi.endpoints.dispatch;

  it("declares only the authenticated POST compatibility endpoint with optional bearer headers", () => {
    expect(endpoint.method).toBe("POST");
    expect(endpoint.path).toBe("/api/orchestration/dispatch");
    expect(endpoint.middlewares.has(EnvironmentAuthenticatedAuth)).toBe(true);
    if (endpoint.headers === undefined) throw new Error("dispatch must declare optional bearer headers");
    const headers = descriptorCodec(endpoint.headers, Schema.toCodecStringTree(Schema.Struct({
      authorization: Schema.optionalKey(Schema.String),
      dpop: Schema.optionalKey(Schema.String),
    })));
    const decode = Schema.decodeUnknownSync(headers);
    expect(decode({})).toEqual({});
    expect(decode({ authorization: "Bearer credential", dpop: "proof" })).toEqual({
      authorization: "Bearer credential", dpop: "proof",
    });
    expect(() => decode({ authorization: 4 })).toThrow();
  });

  it("preserves the narrow historical turn-start payload for rejection without accepting a retired command union", () => {
    const descriptor = endpoint.payload.get("application/json")?.schemas[0];
    if (descriptor === undefined) throw new Error("dispatch must declare the historical JSON payload");
    const payloadSchema = descriptorCodec(descriptor, Schema.toCodecJson(ThreadTurnStartCommand));
    const wire = {
      type: "thread.turn.start",
      commandId: "historical-command-1",
      threadId: "thread-1",
      message: { messageId: "message-1", role: "user", text: "historical prompt", attachments: [] },
      runtimeMode: "full-access",
      interactionMode: "default",
      bootstrap: { runSetupScript: false },
      dispatchGuard: {
        observedSnapshotSequence: 12,
        expectedModelSelection: { instanceId: "codex_work", model: "model-1" },
        expectedSessionStatus: null,
        expectedActiveTurnId: null,
        expectedLatestTurnId: null,
        requireIdle: true,
      },
      createdAt: "2026-10-03T12:00:00Z",
    };
    const decoded = Schema.decodeUnknownSync(payloadSchema)(wire);
    expect(Schema.encodeSync(payloadSchema)(decoded)).toEqual(wire);
    for (const type of ["thread.create", "thread.delete", "message.dispatch", "thread.imported-history.start"]) {
      expect(() => Schema.decodeUnknownSync(payloadSchema)({ ...wire, type })).toThrow();
    }
  });

  it("declares no successful response and retains the exact rejection and authorization error families", () => {
    const success = Array.from(endpoint.success);
    expect(success).toHaveLength(1);
    for (const descriptor of success) {
      const schema = descriptorCodec(descriptor, Schema.toCodecJson(Schema.Never));
      for (const wire of [undefined, null, {}, { sequence: 1 }, { accepted: true }]) {
        expect(() => Schema.decodeUnknownSync(schema)(wire)).toThrow();
      }
    }
    const errorSchemas = Array.from(endpoint.error);
    const knownErrorSchemas = [
      Schema.toCodecJson(EnvironmentRequestInvalidError),
      Schema.toCodecJson(EnvironmentScopeRequiredError),
      Schema.toCodecJson(EnvironmentInternalError),
      Schema.toCodecJson(EnvironmentAuthInvalidError),
    ] as const;
    expect(errorSchemas.map((schema) => schema.ast)).toEqual(knownErrorSchemas.map((schema) => schema.ast));
    for (const error of [
      new EnvironmentRequestInvalidError({ code: "invalid_request", reason: "dispatch_guard_bootstrap_unsupported", traceId }),
      new EnvironmentRequestInvalidError({ code: "invalid_request", reason: "invalid_command", traceId }),
      new EnvironmentScopeRequiredError({ code: "insufficient_scope", requiredScope: "orchestration:operate", traceId }),
      new EnvironmentAuthInvalidError({ code: "auth_invalid", reason: "missing_credential", traceId }),
      new EnvironmentInternalError({ code: "internal_error", reason: "internal_error", traceId }),
    ]) {
      expect(knownErrorSchemas.some((schema) => Exit.isSuccess(Schema.decodeUnknownExit(schema)(error)))).toBe(true);
    }
  });
});

describe("environment HTTP errors", () => {
  // A client squashes the cause and shows `message`; an empty one becomes a generic
  // "The environment request failed." that names nothing the reader can act on.
  it("each carries a message that names its reason", () => {
    const errors = [
      new EnvironmentRequestInvalidError({
        code: "invalid_request",
        reason: "invalid_command",
        traceId,
      }),
      new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId,
      }),
      new EnvironmentScopeRequiredError({
        code: "insufficient_scope",
        requiredScope: "orchestration:read",
        traceId,
      }),
      new EnvironmentOperationForbiddenError({
        code: "operation_forbidden",
        reason: "current_session_revoke_not_allowed",
        traceId,
      }),
      new EnvironmentResourceNotFoundError({
        code: "not_found",
        reason: "thread_not_found",
        traceId,
      }),
      new EnvironmentInternalError({
        code: "internal_error",
        reason: "orchestration_snapshot_failed",
        traceId,
      }),
    ] as const;
    const details = [
      "invalid_command",
      "missing_credential",
      "orchestration:read",
      "current_session_revoke_not_allowed",
      "thread_not_found",
      "orchestration_snapshot_failed",
    ];
    errors.forEach((error, index) => {
      expect(error.message).toContain(details[index]);
    });
  });
});

describe("command observation HTTP response codecs", () => {
  it("preserves the V1 response and requires a separate strict V2 response", () => {
    const v1Endpoint = EnvironmentOrchestrationHttpApi.endpoints.commandObservation;
    const v2Endpoint = EnvironmentOrchestrationHttpApi.endpoints.commandObservationV2;
    const v1Descriptor = Array.from(v1Endpoint.success)[0];
    const v2Descriptor = Array.from(v2Endpoint.success)[0];
    if (v1Descriptor === undefined || v2Descriptor === undefined) {
      throw new Error("Both command observation endpoints must declare a success codec");
    }
    const v1Schema = descriptorCodec(v1Descriptor, Schema.toCodecJson(OrchestrationCommandObservation));
    const v2Schema = descriptorCodec(v2Descriptor, Schema.toCodecJson(NativeCommandObservationV2));
    const decodeV1 = Schema.decodeUnknownSync(v1Schema);
    const decodeV2 = Schema.decodeUnknownSync(v2Schema);
    const v1 = {
      threadId: "thread-1",
      commandId: "command-1",
      messageId: "message-1",
      snapshotSequence: 1,
      commandStatus: "not_found",
      acceptedSequence: null,
      correlation: "missing",
      turn: null,
      target: null,
    };
    const v2 = {
      version: 2,
      threadId: "thread-1",
      commandId: "command-1",
      messageId: "message-1",
      commandStatus: "not_found",
      identity: null,
      identityVerification: "missing",
      correlation: "missing",
      snapshot: { snapshotSequence: 1, targetEventSequence: 0, complete: true },
      correlatedMessageId: null,
      run: null,
      target: null,
      receipt: null,
    };
    expect(Schema.encodeSync(v1Schema)(decodeV1(v1))).toEqual(v1);
    expect(Schema.encodeSync(v2Schema)(decodeV2(v2))).toEqual(v2);
    expect(() => decodeV1(v2)).toThrow();
    expect(() => decodeV2(v1)).toThrow();
    expect(() => decodeV2({ ...v2, version: 1 })).toThrow();
    expect(() => decodeV2({ ...v2, acceptedSequence: 1 })).toThrow();
    expect(v1Endpoint.path).toBe("/api/orchestration/threads/:threadId/commands/:commandId");
    expect(v2Endpoint.path).toBe("/api/orchestration/v2/threads/:threadId/commands/:commandId");
  });
});

describe("native provider observation codecs", () => {
  it("preserves existing live goal states, reasons, native cursor and millisecond evidence", () => {
    const observation = {
      schema: "t3.provider-goal-state/v1", threadId: "thread-1", providerInstanceId: "codex_work",
      nativeThreadId: null, observedAtMs: 0, state: "unknown", reasonCode: "unsupported",
    };
    const decode = Schema.decodeUnknownSync(ProviderGoalStateObservation);
    for (const state of ["active", "inactive", "unknown"]) {
      const wire = { ...observation, state };
      expect(Schema.encodeSync(ProviderGoalStateObservation)(decode(wire))).toEqual(wire);
    }
    expect(() => decode({ ...observation, state: "unsupported" })).toThrow();
    expect(() => decode({ ...observation, observedAtMs: -1 })).toThrow();
  });

  it("preserves qualified quota nulls, omissions and separate unsupported evidence", () => {
    const quota = {
      schemaVersion: "codex.t3-qualified-quota/v1", instanceId: "codex_work",
      probeId: "123e4567-e89b-42d3-a456-426614174000", status: "unsupported",
      attemptedAt: "2026-10-02T12:00:00Z", quotaReceivedAt: null, probeCompletedAt: "2026-10-02T12:00:01Z",
      complete: false, rateLimitsByLimitId: null, windowProvenance: [], failureCode: "unsupported_account", capabilityRefs: [],
    };
    const decode = Schema.decodeUnknownSync(QualifiedQuota);
    expect(Schema.encodeSync(QualifiedQuota)(decode(quota))).toEqual(quota);
    const windows = {
      ...quota, rateLimitsByLimitId: {
        main: { primary: { usedPercent: 12, resetsAt: null }, secondary: null, spendControlReached: false },
      },
    };
    expect(Schema.encodeSync(QualifiedQuota)(decode(windows))).toEqual(windows);
    expect(() => decode({ ...quota, rateLimitsByLimitId: { main: { primary: { usedPercent: Infinity } } } })).toThrow();
  });
});
