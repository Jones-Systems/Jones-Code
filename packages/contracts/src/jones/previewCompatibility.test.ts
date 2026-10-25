import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";
import {
  PreviewAutomationHost,
  PREVIEW_AUTOMATION_OPERATIONS,
  PreviewAutomationResponse,
  PreviewAutomationHostIdentity,
  PreviewAutomationRuntimeIdentity,
} from "../previewAutomation.ts";
const decodeAutomationHost = Schema.decodeUnknownSync(PreviewAutomationHost);

describe("preview resilience mixed-version wire compatibility", () => {
  it("keeps ping outside the baseline operations array accepted by legacy servers", () => {
    const LegacyHost = Schema.Struct({
      ...PreviewAutomationHostIdentity.fields,
      supportedOperations: Schema.optional(
        Schema.Array(Schema.Literals(PREVIEW_AUTOMATION_OPERATIONS)),
      ),
      runtimeIdentity: Schema.optional(PreviewAutomationRuntimeIdentity),
    });
    const host = {
      clientId: "desktop-1",
      environmentId: "environment-1",
      supportedOperations: [...PREVIEW_AUTOMATION_OPERATIONS],
      supportsPing: true,
      supportsSnapshotBarrier: true,
    };
    expect(Schema.decodeUnknownSync(LegacyHost)(host)).toEqual({
      clientId: host.clientId,
      environmentId: host.environmentId,
      supportedOperations: host.supportedOperations,
    });
    expect(decodeAutomationHost(host)).toMatchObject({
      supportsPing: true,
      supportsSnapshotBarrier: true,
    });
  });

  it("accepts legacy response errors and optional authoritative outcome", () => {
    const decode = Schema.decodeUnknownSync(PreviewAutomationResponse);
    const response = {
      clientId: "desktop-1",
      connectionId: "connection-1",
      requestId: "request-1",
      ok: false,
      error: { _tag: "PreviewAutomationTimeoutError", message: "deadline" },
    };
    expect(decode(response)).toEqual(response);
    expect(
      decode({ ...response, error: { ...response.error, outcome: "not_started" } }).error?.outcome,
    ).toBe("not_started");
  });
});
