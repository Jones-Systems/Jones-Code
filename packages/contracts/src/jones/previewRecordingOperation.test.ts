import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  PreviewRecordingOperationInput,
  PreviewRecordingOperationResult,
  PreviewRecordingReportedStart,
} from "./previewRecordingOperation.ts";
import {
  PREVIEW_AUTOMATION_V1_OPERATIONS,
  PreviewAutomationResponse,
} from "../previewAutomation.ts";

const acceptsResponse = Schema.is(PreviewAutomationResponse);

describe("recording operation evidence contract", () => {
  const handle = { requestId: "preview-1", clientId: "client", connectionId: "connection" };
  const acceptsInput = Schema.is(PreviewRecordingOperationInput);
  it("requires an explicit tab and a complete bounded optional handle", () => {
    expect(acceptsInput({ tabId: "tab" })).toBe(true);
    expect(acceptsInput({ tabId: "tab", handle })).toBe(true);
    const decodeJson = Schema.decodeUnknownSync(Schema.toCodecJson(PreviewRecordingOperationInput));
    expect(() => decodeJson({ tabId: "tab", handle: null })).toThrow();
    for (const value of [
      {},
      { tabId: "" },
      { tabId: "tab", handle: null },
      { tabId: "tab", handle: {} },
      ...Object.keys(handle).map((key) => ({ tabId: "tab", handle: { ...handle, [key]: "" } })),
      { tabId: "tab", handle: { ...handle, requestId: "x".repeat(129) } },
      { tabId: "tab", handle: { ...handle, clientId: "x".repeat(129) } },
      { tabId: "tab", handle: { ...handle, connectionId: "x".repeat(65) } },
    ])
      expect(acceptsInput(value)).toBe(false);
  });

  it("validates bounded reported start metadata without rewriting the legacy wire", () => {
    const valid = Schema.is(PreviewRecordingReportedStart);
    for (const startedAt of [null, "2026-11-02T12:00:00.000Z", "2026-11-02T07:00:00-05:00"]) {
      expect(valid({ tabId: "tab", recording: true, startedAt })).toBe(true);
    }
    for (const startedAt of [
      "",
      "not-a-date",
      "2026-02-30T12:00:00Z",
      "2026-13-01T12:00:00Z",
      "x".repeat(129),
      1,
    ]) {
      expect(valid({ tabId: "tab", recording: true, startedAt })).toBe(false);
    }
    expect(valid({ tabId: "tab", recording: "yes", startedAt: null })).toBe(false);
    expect(acceptsResponse({ ...handle, ok: true })).toBe(true);
    expect(
      acceptsResponse({
        ...handle,
        ok: true,
        error: { _tag: "Legacy", message: "legacy" },
      }),
    ).toBe(true);
    expect(PREVIEW_AUTOMATION_V1_OPERATIONS).not.toContain("recordingOperation");
  });

  it("bounds ambiguity and prevents pre-dispatch evidence", () => {
    const valid = Schema.is(PreviewRecordingOperationResult);
    const candidate = { handle, operation: "recordingStart", status: "unknown" };
    const wrap = (result: unknown) => ({ schemaVersion: 1, tabId: "tab", result });
    expect(
      valid(wrap({ status: "ambiguous", candidates: [candidate, candidate], truncated: false })),
    ).toBe(true);
    expect(
      valid(
        wrap({
          status: "ambiguous",
          candidates: Array.from({ length: 9 }, () => candidate),
          truncated: true,
        }),
      ),
    ).toBe(false);
    expect(
      valid(wrap({ status: "pending", handle, operation: "recordingStart", dispatched: true })),
    ).toBe(false);
    expect(
      valid(wrap({ status: "pending", handle, operation: "recordingStart", dispatched: false })),
    ).toBe(true);
    expect(valid(wrap({ status: "unavailable", reason: "not_retained" }))).toBe(true);
    expect(valid(wrap({ status: "unavailable", reason: "foreign" }))).toBe(false);
  });
});
