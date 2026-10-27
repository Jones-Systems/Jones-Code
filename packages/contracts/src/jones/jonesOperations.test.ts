import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  JonesOperationAction,
  JonesOperationCode,
  JonesOperationFenceInput,
  JonesOperationId,
  JonesOperationReadInput,
  JonesOperationReadResult,
  JonesOperationReceipt,
  JonesOperationStartInput,
  JonesOperationStopInput,
  JonesRecoveryPreflight,
  JonesRecoveryPreflightInput,
  JonesSourceSha,
} from "./jonesOperations.ts";

const operationId = "11111111-1111-4111-8111-111111111111";
const stopId = "22222222-2222-4222-8222-222222222222";
const planId = "33333333-3333-4333-8333-333333333333";
const source = "a".repeat(40);
const tree = "b".repeat(40);
const environmentId = "synthetic-host";
const at = "2026-10-27T04:00:00Z";
const start = {
  operationId,
  environmentId,
  expectedInstalledSource: "c".repeat(40),
  action: { kind: "download-exact", targetSource: source },
  correlation: { planId, deviceKey: environmentId },
} as const;
const receipt = {
  ...start,
  protocol: 1,
  phase: "accepted",
  acceptedAt: at,
  updatedAt: at,
} as const;

describe("durable Jones operation contracts", () => {
  it("requires UUID v4 identities and full lowercase source commits", () => {
    const decodeId = Schema.decodeUnknownSync(JonesOperationId);
    expect(decodeId(operationId)).toBe(operationId);
    for (const id of ["", "operation-1", "AAAAAAAA-1111-4111-8111-111111111111",
      "11111111-1111-5111-8111-111111111111", "11111111-1111-4111-7111-111111111111"])
      expect(() => decodeId(id)).toThrow();
    const decodeSource = Schema.decodeUnknownSync(JonesSourceSha);
    expect(decodeSource(source)).toBe(source);
    for (const sha of ["", source.slice(1), source + "a", source.toUpperCase(), "1.0.0"])
      expect(() => decodeSource(sha)).toThrow();
  });

  it("keeps action, installed source, environment and optional plan correlation", () => {
    const decode = Schema.decodeUnknownSync(JonesOperationStartInput);
    expect(decode(start)).toEqual(start);
    const { correlation: _correlation, ...standalone } = start;
    expect(decode(standalone)).toEqual(standalone);
    for (const field of ["operationId", "environmentId", "expectedInstalledSource", "action"]) {
      const incomplete: Record<string, unknown> = { ...start };
      delete incomplete[field];
      expect(() => decode(incomplete)).toThrow();
    }
    for (const correlation of [{ planId }, { deviceKey: environmentId },
      { planId, deviceKey: "x".repeat(129) }])
      expect(() => decode({ ...start, correlation })).toThrow();
    expect(decode({ ...start, correlation: { planId, deviceKey: "x".repeat(128) } })
      .correlation?.deviceKey).toHaveLength(128);
    expect(() => decode({ ...start, environmentId: "" })).toThrow();
  });

  it("keeps exact offered artifacts and reviewed policy-bearing installs distinct", () => {
    const decode = Schema.decodeUnknownSync(JonesOperationAction);
    const download = { kind: "download", artifactId: 42, sourceSha: source };
    const install = {
      kind: "install",
      stagedHandle: "retained-stage",
      currentVersion: "1.0.0",
      targetSource: source,
      continueRunningThreads: false,
      recovery: { policy: "clone-required", preflightId: planId },
    };
    expect(decode(download)).toEqual(download);
    expect(decode(install)).toEqual(install);
    expect(decode({ ...install, recovery: { policy: "clone-or-copy", preflightId: planId } }))
      .toEqual({ ...install, recovery: { policy: "clone-or-copy", preflightId: planId } });
    for (const field of ["stagedHandle", "currentVersion", "targetSource"]) {
      const incomplete: Record<string, unknown> = { ...install };
      delete incomplete[field];
      expect(() => decode(incomplete)).toThrow();
    }
    for (const recovery of [{ policy: "clone-required" }, { preflightId: planId },
      { policy: "copy-required", preflightId: planId }])
      expect(() => decode({ ...install, recovery })).toThrow();
    expect(() => decode({ kind: "download", artifactId: 42 })).toThrow();
    expect(() => decode({ kind: "install", version: "1.0.0" })).toThrow();
  });

  it("binds repeatable Stop and fence requests to an exact operation and environment", () => {
    const stop = { operationId, stopId, environmentId };
    const fence = start;
    expect(Schema.decodeUnknownSync(JonesOperationStopInput)(stop)).toEqual(stop);
    expect(Schema.decodeUnknownSync(JonesOperationFenceInput)(fence)).toEqual(fence);
    expect(() => Schema.decodeUnknownSync(JonesOperationFenceInput)({ operationId, environmentId })).toThrow();
    expect(() => Schema.decodeUnknownSync(JonesOperationStopInput)(fence)).toThrow();
    for (const input of [{ operationId }, { environmentId }]) {
      expect(() => Schema.decodeUnknownSync(JonesOperationStopInput)(input)).toThrow();
      expect(() => Schema.decodeUnknownSync(JonesOperationFenceInput)(input)).toThrow();
    }
  });

  it("round-trips unresolved and terminal phases without conflating Stop request and outcome", () => {
    const decode = Schema.decodeUnknownSync(JonesOperationReceipt);
    for (const phase of ["accepted", "running", "stop-requested", "adopting", "handed-off", "held",
      "completed", "rolled-back", "stopped", "refused", "failed", "blocked", "fenced", "interrupted"])
      expect(decode({ ...receipt, phase }).phase).toBe(phase);
    const stopped = {
      ...receipt,
      phase: "stopped",
      jobId: stopId,
      stop: { stopId, requestedAt: at, result: "stopped" },
      code: "future-reap-proof",
      message: "Owned child reaped; no stage adopted.",
    };
    expect(decode(stopped)).toEqual(stopped);
    const stopping = { ...receipt, phase: "stop-requested", stop: { stopId, requestedAt: at } };
    expect(decode(stopping)).toEqual(stopping);
    const { acceptedAt: _acceptedAt, ...neverAccepted } = receipt;
    expect(decode({ ...neverAccepted, phase: "fenced" })).toEqual({ ...neverAccepted, phase: "fenced" });
    expect(() => decode({ ...receipt, protocol: 2 })).toThrow();
    expect(() => decode({ ...receipt, phase: "unknown" })).toThrow();
    expect(() => decode({ ...receipt, stop: { stopId, requestedAt: at, result: "cancelled" } })).toThrow();
  });

  it("preserves host-specific stage provenance and requested versus observed recovery", () => {
    const completed = {
      ...receipt,
      phase: "completed",
      staged: { stagedHandle: "host-stage", sourceSha: source, sourceTree: tree, artifactId: 91,
        version: "2.0.0" },
      updateId: operationId,
      outcome: { status: "committed", fromVersion: "1.0.0", targetVersion: "2.0.0" },
      recovery: { requested: "clone-or-copy", observed: "copy" },
    };
    const decode = Schema.decodeUnknownSync(JonesOperationReceipt);
    expect(decode(completed)).toEqual(completed);
    const codec = Schema.toCodecJson(JonesOperationReceipt);
    expect(Schema.decodeUnknownSync(codec)(Schema.encodeUnknownSync(codec)(completed)))
      .toEqual(completed);
    expect(decode({ ...completed, recovery: { requested: "clone-required" } }).recovery)
      .toEqual({ requested: "clone-required" });
    for (const staged of [{ stagedHandle: "host-stage" },
      { ...completed.staged, sourceTree: "invalid" }])
      expect(() => decode({ ...completed, staged })).toThrow();
    expect(() => decode({ ...completed, recovery: { observed: "copy" } })).toThrow();
  });

  it("bounds read batches and preserves absence as a null observation", () => {
    const decodeRead = Schema.decodeUnknownSync(JonesOperationReadInput);
    const decodeResult = Schema.decodeUnknownSync(JonesOperationReadResult);
    const reads = { environmentId, operationIds: Array.from({ length: 32 }, () => operationId) };
    expect(decodeRead(reads)).toEqual(reads);
    expect(() => decodeRead({ ...reads, operationIds: [...reads.operationIds, stopId] })).toThrow();
    const missing = { operationId: stopId, receipt: null };
    const result = { receipts: [{ operationId, receipt }, missing] };
    expect(decodeResult(result)).toEqual(result);
    expect(decodeResult({ receipts: Array.from({ length: 32 }, () => missing) }).receipts).toHaveLength(32);
    expect(() => decodeResult({ receipts: Array.from({ length: 33 }, () => missing) })).toThrow();
  });

  it("accepts new reason codes while enforcing code and message bounds", () => {
    const decode = Schema.decodeUnknownSync(JonesOperationCode);
    expect(decode("future-reason-9")).toBe("future-reason-9");
    expect(decode("x".repeat(48))).toHaveLength(48);
    for (const code of ["", "Uppercase", "reason_with_underscore", "9reason", "x".repeat(49)])
      expect(() => decode(code)).toThrow();
    const decodeReceipt = Schema.decodeUnknownSync(JonesOperationReceipt);
    expect(decodeReceipt({ ...receipt, message: "x".repeat(1024) }).message).toHaveLength(1024);
    expect(() => decodeReceipt({ ...receipt, message: "x".repeat(1025) })).toThrow();
  });
});

describe("target-owned recovery preflight contracts", () => {
  const binding = { environmentId, stagedHandle: "host-stage", currentVersion: "1.0.0",
    expectedInstalledSource: "c".repeat(40) };
  const preflight = {
    protocol: 1,
    preflightId: planId,
    measuredAt: at,
    expiresAt: "2026-10-27T04:05:00Z",
    binding,
    firstInstall: false,
    stateBytes: 1024,
    candidateBytes: 2048,
    volumes: [{ role: "state", availableBytes: 4096, copyRequiredBytes: 8192, cloneRequiredBytes: 2048 }],
    clone: { status: "proved" },
    policies: { cloneOrCopy: { admissible: false, reason: "Low copy space" },
      cloneRequired: { admissible: true } },
  };

  it("carries target binding, capacity and independently admissible recovery policies", () => {
    expect(Schema.decodeUnknownSync(JonesRecoveryPreflightInput)(binding)).toEqual(binding);
    const decode = Schema.decodeUnknownSync(JonesRecoveryPreflight);
    expect(decode(preflight)).toEqual(preflight);
    for (const field of Object.keys(binding)) {
      const incomplete: Record<string, unknown> = { ...binding };
      delete incomplete[field];
      expect(() => Schema.decodeUnknownSync(JonesRecoveryPreflightInput)(incomplete)).toThrow();
    }
    const firstInstall = { ...preflight, firstInstall: true, stateBytes: 0,
      clone: { status: "unsupported", reason: "No existing state" } };
    expect(decode(firstInstall)).toEqual(firstInstall);
    expect(decode({ ...preflight, clone: { status: "unknown" } }).clone.status).toBe("unknown");
    expect(() => decode({ ...preflight, protocol: 2 })).toThrow();
  });

  it("bounds volume observations and policy explanations", () => {
    const decode = Schema.decodeUnknownSync(JonesRecoveryPreflight);
    const volume = preflight.volumes[0];
    expect(decode({ ...preflight, volumes: Array.from({ length: 4 }, () => volume) }).volumes)
      .toHaveLength(4);
    expect(() => decode({ ...preflight, volumes: Array.from({ length: 5 }, () => volume) })).toThrow();
    expect(() => decode({ ...preflight, clone: { status: "proved", reason: "x".repeat(513) } }))
      .toThrow();
    expect(() => decode({ ...preflight, policies: { ...preflight.policies,
      cloneRequired: { admissible: false, reason: "x".repeat(513) } } })).toThrow();
  });
});
