import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { EnvironmentId, ThreadId } from "./baseSchemas.ts";
import {
  PreviewAutomationHost,
  PreviewAutomationInvalidSelectorError,
  PreviewAutomationRuntimeIdentity,
  PreviewAutomationStatus,
} from "./previewAutomation.ts";
import { PreviewSessionSnapshot, PreviewEvent } from "./preview.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
const decodeSessionSnapshot = Schema.decodeUnknownSync(PreviewSessionSnapshot);
const decodePreviewEvent = Schema.decodeUnknownSync(PreviewEvent);

describe("PreviewSessionSnapshot headless compatibility", () => {
  const legacy = {
    threadId: "thread",
    tabId: "tab",
    runtime: "server",
    navStatus: { _tag: "Idle" },
    canGoBack: false,
    canGoForward: false,
    updatedAt: "2026-11-03T00:00:00.000Z",
  };
  it.each([undefined, false, true])("decodes headless=%s in snapshots and events", (headless) => {
    const snapshot = { ...legacy, ...(headless === undefined ? {} : { headless }) };
    expect(decodeSessionSnapshot(snapshot)).toEqual(snapshot);
    const event = {
      type: "resized",
      threadId: "thread",
      tabId: "tab",
      createdAt: legacy.updatedAt,
      serverEpoch: "server-epoch",
      revision: 1,
      snapshot,
    };
    expect(decodePreviewEvent(event)).toEqual(event);
  });
});

const decodeAutomationHost = Schema.decodeUnknownSync(PreviewAutomationHost);
const decodeAutomationStatus = Schema.decodeUnknownSync(PreviewAutomationStatus);
const decodeRuntimeIdentity = Schema.decodeUnknownSync(PreviewAutomationRuntimeIdentity);

describe("PreviewAutomationHost", () => {
  it("accepts legacy hosts and current operation advertisements", () => {
    expect(decodeAutomationHost({ clientId: "legacy", environmentId: "environment-1" })).toEqual({
      clientId: "legacy",
      environmentId: "environment-1",
    });
    expect(
      decodeAutomationHost({
        clientId: "current",
        environmentId: "environment-1",
        supportedOperations: ["status", "resize"],
      }).supportedOperations,
    ).toEqual(["status", "resize"]);
  });
  it("accepts a versioned runtime descriptor and rejects incompatible versions", () => {
    const runtimeIdentity = {
      schemaVersion: 1,
      runtimeKind: "electron",
      runtimeInstanceId: "runtime-1",
      appVersion: "0.1.0",
      buildCommit: "a".repeat(40),
    };
    expect(decodeRuntimeIdentity(runtimeIdentity)).toEqual(runtimeIdentity);
    expect(
      decodeAutomationHost({ clientId: "current", environmentId: "environment-1", runtimeIdentity })
        .runtimeIdentity,
    ).toEqual(runtimeIdentity);
    expect(() => decodeRuntimeIdentity({ ...runtimeIdentity, schemaVersion: 2 })).toThrow();
    expect(() => decodeRuntimeIdentity({ ...runtimeIdentity, runtimeKind: "browser" })).toThrow();
    expect(() =>
      decodeRuntimeIdentity({ ...runtimeIdentity, buildCommit: "a".repeat(39) }),
    ).toThrow();
    expect(decodeRuntimeIdentity({ ...runtimeIdentity, buildCommit: null }).buildCommit).toBeNull();
  });
});

describe("PreviewAutomationStatus", () => {
  it("accepts an optional broker-selected client receipt with a legacy null descriptor", () => {
    const status = {
      available: true,
      visible: false,
      tabId: null,
      url: null,
      title: null,
      loading: false,
      selectedClient: {
        clientId: "legacy",
        connectionId: "connection-1",
        requestId: "preview-0",
        completedAt: "2026-09-27T00:00:00.000Z",
        runtimeIdentity: null,
      },
    };
    expect(decodeAutomationStatus(status)).toEqual(status);
  });
  it("accepts old hosts without viewport data and exposes it from current hosts", () => {
    const base = {
      available: true,
      visible: false,
      tabId: "preview-t",
      url: "https://example.com",
      title: "Example",
      loading: false,
    };
    expect(decodeAutomationStatus(base)).toEqual(base);
    expect(
      decodeAutomationStatus({
        ...base,
        viewportSetting: { _tag: "preset", presetId: "pixel-8", width: 412, height: 915 },
        viewport: { width: 412, height: 915 },
      }).viewport,
    ).toEqual({ width: 412, height: 915 });
  });
});

describe("PreviewAutomationInvalidSelectorError", () => {
  const invalidSelector = (selectorKind: "locator" | "selector") =>
    new PreviewAutomationInvalidSelectorError({
      operation: "click",
      environmentId: EnvironmentId.make("environment-1"),
      threadId: ThreadId.make("thread-1"),
      providerSessionId: "session-1",
      providerInstanceId: ProviderInstanceId.make("claude"),
      clientId: "desktop",
      connectionId: "connection-1",
      requestId: "request-1",
      timeoutMs: 15_000,
      remoteTag: "Error",
      remoteMessageLength: 0,
      cause: null,
      selectorKind,
      selectorLength: 42,
    });

  it("tells agents which locator forms are accepted and where CSS goes", () => {
    const { message } = invalidSelector("locator");
    expect(message).toContain("aria-ref=");
    expect(message).toMatch(/role/);
    expect(message).toMatch(/text/);
    expect(message).toMatch(/CSS[^.]*\bselector\b/);
  });

  it("points an invalid CSS selector at the locator alternative", () => {
    const { message } = invalidSelector("selector");
    expect(message).toMatch(/CSS/);
    expect(message).toMatch(/\blocator\b/);
    expect(message).toContain("aria-ref=");
  });
});
