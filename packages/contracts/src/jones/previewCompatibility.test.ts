import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";
import {
  PreviewAutomationHost,
  PREVIEW_AUTOMATION_OPERATIONS,
  PreviewAutomationResponse,
  PreviewAutomationHostIdentity,
  PreviewAutomationRuntimeIdentity,
} from "../previewAutomation.ts";
import { PreviewSessionSnapshot } from "../preview.ts";
const decodeAutomationHost = Schema.decodeUnknownSync(PreviewAutomationHost);
const { headless: _headless, ...legacySnapshotFields } = PreviewSessionSnapshot.fields;
const decodeLegacySnapshot = Schema.decodeUnknownSync(Schema.Struct(legacySnapshotFields));
const decodeSnapshot = Schema.decodeUnknownSync(PreviewSessionSnapshot);

describe("preview resilience mixed-version wire compatibility", () => {
  it("lets an old snapshot reader ignore the additive headless flag", () => {
    const legacy = {
      threadId: "thread",
      tabId: "tab",
      runtime: "server",
      navStatus: { _tag: "Idle" },
      canGoBack: false,
      canGoForward: false,
      updatedAt: "2026-11-03T00:00:00.000Z",
    };
    expect(decodeLegacySnapshot({ ...legacy, headless: true })).toEqual(legacy);
    expect(decodeSnapshot(legacy)).toEqual(legacy);
  });
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
    expect(Schema.decodeSync(LegacyHost)(host)).toEqual({
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
