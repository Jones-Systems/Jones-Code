import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import capabilityFixture from "../test-fixtures/queue-dispatch-capability.json" with { type: "json" };
import { ExecutionEnvironmentDescriptor, ORCHESTRATION_PROTOCOL_VERSION } from "./environment.ts";
import { OrchestrationCommandObservation, ThreadTurnDispatchGuard } from "./orchestration.ts";
import { QUEUE_DISPATCH_CAPABILITY, QueueDispatchCapability } from "./queueProtocol.ts";

const decodeCapability = Schema.decodeUnknownSync(QueueDispatchCapability);
const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-preview.changed-build",
  orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
  capabilities: {
    repositoryIdentity: true,
    queueDispatch: capabilityFixture,
  },
};

describe("queue dispatch capability", () => {
  it("round-trips the cross-language fixture without depending on a build version", () => {
    const decoded = decodeCapability(capabilityFixture);
    expect(Schema.encodeSync(QueueDispatchCapability)(decoded)).toEqual(capabilityFixture);
    expect(QUEUE_DISPATCH_CAPABILITY).toEqual(capabilityFixture);
    expect(decoded.orchestrationProtocolVersion).toBe(ORCHESTRATION_PROTOCOL_VERSION);
    for (const serverVersion of ["0.0.44-preview.old-build", "0.0.0-preview.changed-build"]) {
      expect(decodeDescriptor({ ...descriptor, serverVersion }).capabilities.queueDispatch).toEqual(
        capabilityFixture,
      );
    }
  });

  it("rejects missing or contradictory claims and unknown capability revisions", () => {
    for (const field of Object.keys(capabilityFixture)) {
      const missing: Record<string, unknown> = { ...capabilityFixture };
      delete missing[field];
      expect(() => decodeCapability(missing)).toThrow();
      expect(() => decodeCapability({ ...capabilityFixture, [field]: "unsupported/v2" })).toThrow();
      expect(() =>
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, queueDispatch: missing },
        }),
      ).toThrow();
    }
  });

  it("rejects unknown capability keys rather than silently stripping their claims", () => {
    expect(() => decodeCapability({ ...capabilityFixture, nativeCreation: true })).toThrow();
    expect(() =>
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          queueDispatch: { ...capabilityFixture, extra: true },
        },
      }),
    ).toThrow();
  });

  it("keeps legacy descriptors decodable and creation advertisement independent", () => {
    const legacy = decodeDescriptor({
      ...descriptor,
      capabilities: { repositoryIdentity: true },
    });
    expect(legacy.capabilities.queueDispatch).toBeUndefined();
    const current = decodeDescriptor(descriptor);
    expect(current.capabilities.queueDispatch).toEqual(capabilityFixture);
    expect(current.capabilities.repositoryIdentity).toBe(true);
    expect(current.capabilities.nativeBootstrapCreation).toBeUndefined();
  });

  it("preserves every native idle guard field and rejects disabled idle enforcement", () => {
    const guard = {
      observedSnapshotSequence: 3,
      expectedModelSelection: { instanceId: "codex", model: "test-model" },
      expectedSessionStatus: null,
      expectedActiveTurnId: null,
      expectedLatestTurnId: null,
      requireIdle: true,
    };
    const decodeGuard = Schema.decodeUnknownSync(ThreadTurnDispatchGuard);
    expect(Schema.encodeSync(ThreadTurnDispatchGuard)(decodeGuard(guard))).toEqual(guard);
    expect(() => decodeGuard({ ...guard, requireIdle: false })).toThrow();
    const { requireIdle: _requireIdle, ...unguarded } = guard;
    expect(() => decodeGuard(unguarded)).toThrow();
  });

  it("preserves rejected and unknown command observation without inventing acceptance", () => {
    const decodeObservation = Schema.decodeUnknownSync(OrchestrationCommandObservation);
    for (const commandStatus of ["rejected", "not_found"]) {
      const observation = {
        threadId: "thread-1",
        commandId: "command-1",
        messageId: "message-1",
        snapshotSequence: 3,
        commandStatus,
        acceptedSequence: null,
        correlation: "missing",
        turn: null,
        target: null,
      };
      expect(decodeObservation(observation)).toEqual(observation);
    }
  });
});
