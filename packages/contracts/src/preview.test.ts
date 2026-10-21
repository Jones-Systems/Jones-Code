import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  PreviewAutomationRuntimeIdentity,
  PreviewAutomationHost,
  PreviewAutomationOpenInput,
  PreviewAutomationStatus,
} from "./previewAutomation.ts";

const decodeOpenInput = Schema.decodeUnknownSync(PreviewAutomationOpenInput);
const decodeAutomationHost = Schema.decodeUnknownSync(PreviewAutomationHost);
const decodeAutomationStatus = Schema.decodeUnknownSync(PreviewAutomationStatus);
const decodeRuntimeIdentity = Schema.decodeUnknownSync(PreviewAutomationRuntimeIdentity);

describe("PreviewAutomationOpenInput", () => {
  it("retains the legacy show visibility alias", () => {
    expect(decodeOpenInput({ show: false })).toEqual({ show: false });
  });
});

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
