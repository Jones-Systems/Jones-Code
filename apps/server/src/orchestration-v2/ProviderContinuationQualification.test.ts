import { describe, expect, it } from "vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

import type { ProviderSessionRuntime } from "../persistence/ProviderSessionRuntime.ts";
import {
  qualifyProviderContinuation,
  type LegacyStoppedRuntimeProofV1,
  type ProviderContinuationAccessibility,
  type ProviderContinuationSource,
  type ProviderContinuationTarget,
} from "./ProviderContinuationQualification.ts";

const driver = ProviderDriverKind.make("codex");
const targetId = ProviderInstanceId.make("codex-target");
const storedRow = {
  threadId: ThreadId.make("legacy-thread"),
  providerName: "codex",
  providerInstanceId: null,
  adapterKey: "codex",
  runtimeMode: "full-access",
  status: "stopped",
  lastSeenAt: "2026-10-01T00:00:00.000Z",
  resumeCursor: { threadId: "native-codex-history" },
  runtimePayload: { cwd: "/fixture/project" },
} satisfies ProviderSessionRuntime;
// This fixture models a transcript read from a historical source home.
// The persisted row itself has no continuation key or promoted instance id.
const historicalStoreKey = "codex:home:/fixture/historical-shared-home";
const source: ProviderContinuationSource = {
  provenance: "legacy_row",
  threadId: storedRow.threadId,
  providerInstanceId: storedRow.providerInstanceId,
  driver: ProviderDriverKind.make(storedRow.adapterKey),
  nativeThreadId: storedRow.resumeCursor.threadId,
  continuationKey: historicalStoreKey,
  status: storedRow.status,
};
const stoppedProof: LegacyStoppedRuntimeProofV1 = {
  schema: "t3.legacy-stopped-runtime-proof/v1",
  source: "persisted_runtime_row",
  threadId: storedRow.threadId,
  providerInstanceId: storedRow.providerInstanceId,
  driver: ProviderDriverKind.make(storedRow.adapterKey),
  nativeThreadId: storedRow.resumeCursor.threadId,
  status: storedRow.status,
};
const target: ProviderContinuationTarget = {
  providerInstanceId: targetId,
  driver,
  continuationKey: historicalStoreKey,
  supportsNativeResume: true,
};
const accessibility: ProviderContinuationAccessibility = {
  providerInstanceId: targetId,
  driver,
  nativeThreadId: storedRow.resumeCursor.threadId,
  continuationKey: historicalStoreKey,
  source: "historical_store",
};

const qualified = {
  type: "qualified",
  nativeThreadId: storedRow.resumeCursor.threadId,
  continuationKey: historicalStoreKey,
};

describe("legacy continuation qualification", () => {
  it.each(["historical_store", "native_read"] as const)(
    "qualifies a genuine stopped Codex row using separately demonstrated %s accessibility",
    (proofSource) => {
      expect(
        qualifyProviderContinuation({
          source,
          target,
          stoppedProof,
          accessibility: { ...accessibility, source: proofSource },
        }),
      ).toEqual(qualified);
      expect(storedRow.providerInstanceId).toBeNull();
      expect(storedRow).not.toHaveProperty("continuationKey");
    },
  );

  it("holds a raw legacy cursor even when current instance and driver match", () => {
    expect(qualifyProviderContinuation({ source, target })).toEqual({
      type: "unknown",
      reason: "stopped_proof_missing",
    });
    expect(qualifyProviderContinuation({ source, target, stoppedProof })).toEqual({
      type: "unknown",
      reason: "accessibility_missing",
    });
  });

  it("does not accept importer-assigned stopped status as proof", () => {
    expect(
      qualifyProviderContinuation({
        source: { ...source, provenance: "native_import" },
        target,
        accessibility,
      }),
    ).toEqual({ type: "unknown", reason: "stopped_proof_missing" });
  });

  it.each([undefined, "starting", "running", "error"])(
    "holds a source with unproved stopped status %s",
    (status) => {
      expect(
        qualifyProviderContinuation({
          source: { ...source, status },
          target,
          accessibility,
          stoppedProof,
        }),
      ).toEqual({ type: "unknown", reason: "source_not_stopped" });
    },
  );

  it.each([
    { threadId: ThreadId.make("another-thread") },
    { providerInstanceId: targetId },
    { driver: ProviderDriverKind.make("claudeAgent") },
    { nativeThreadId: "another-native-thread" },
  ])("holds stopped proof for a different source tuple %j", (mismatch) => {
    expect(
      qualifyProviderContinuation({
        source,
        target,
        accessibility,
        stoppedProof: { ...stoppedProof, ...mismatch },
      }),
    ).toEqual({ type: "unknown", reason: "stopped_proof_mismatch" });
  });

  it.each([
    { providerInstanceId: ProviderInstanceId.make("another-account") },
    { driver: ProviderDriverKind.make("claudeAgent") },
    { nativeThreadId: "another-native-thread" },
    { continuationKey: "codex:home:/fixture/another-home" },
    { continuationKey: "" },
  ])("holds inaccessible or mismatched target evidence %j", (mismatch) => {
    expect(
      qualifyProviderContinuation({
        source,
        target,
        stoppedProof,
        accessibility: { ...accessibility, ...mismatch },
      }),
    ).toEqual({ type: "unknown", reason: "accessibility_mismatch" });
  });

  it("uses exact native accessibility when the legacy row has no historical key", () => {
    expect(
      qualifyProviderContinuation({
        source: { ...source, continuationKey: undefined },
        target,
        stoppedProof,
        accessibility: { ...accessibility, source: "native_read" },
      }),
    ).toEqual(qualified);
  });

  it("reports a genuinely unsupported target only after source proof is established", () => {
    const incompatible = { ...target, driver: ProviderDriverKind.make("claudeAgent") };
    expect(qualifyProviderContinuation({ source, target: incompatible })).toEqual({
      type: "unknown",
      reason: "stopped_proof_missing",
    });
    expect(
      qualifyProviderContinuation({ source, target: incompatible, stoppedProof }),
    ).toEqual({ type: "unsupported", reason: "driver_incompatible" });
    expect(
      qualifyProviderContinuation({
        source,
        target: { ...target, supportsNativeResume: false },
        stoppedProof,
      }),
    ).toEqual({ type: "unsupported", reason: "native_resume_unsupported" });
  });

  it("reports an incompatible proved historical store as unsupported", () => {
    expect(
      qualifyProviderContinuation({
        source: { ...source, continuationKey: "codex:home:/fixture/old-home" },
        target,
        stoppedProof,
        accessibility,
      }),
    ).toEqual({ type: "unsupported", reason: "store_incompatible" });
  });
});

describe("V2 continuation qualification", () => {
  const v2Source: ProviderContinuationSource = {
    ...source,
    provenance: "v2_binding",
    providerInstanceId: targetId,
    status: "error",
  };

  it("keeps stopped/error native history distinct from live-process adoption", () => {
    expect(qualifyProviderContinuation({ source: v2Source, target })).toEqual(qualified);
  });

  it("requires historical identity rather than inferring a current setting", () => {
    expect(
      qualifyProviderContinuation({ source: { ...v2Source, continuationKey: null }, target }),
    ).toEqual({ type: "unknown", reason: "historical_identity_unproved" });
  });

  it("reports provider-native resume being genuinely unsupported", () => {
    expect(
      qualifyProviderContinuation({
        source: v2Source,
        target: { ...target, supportsNativeResume: false },
      }),
    ).toEqual({ type: "unsupported", reason: "native_resume_unsupported" });
  });

  it("requires explicit portable handoff for a different driver", () => {
    expect(
      qualifyProviderContinuation({
        source: v2Source,
        target: { ...target, driver: ProviderDriverKind.make("claudeAgent") },
      }),
    ).toEqual({ type: "unsupported", reason: "driver_incompatible" });
  });
});
