import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { JonesUpdateState } from "./jonesUpdates.ts";

// Independent old-client decoder: no projection from the current schema under test.
const oldClientState = Schema.Struct({
  source: Schema.Literal("jones-actions"),
  channel: Schema.Literal("jones-main"),
  phase: Schema.Literals([
    "checking",
    "building",
    "no-new",
    "available",
    "downloading",
    "verifying",
    "staged",
    "preparing",
    "installing",
    "committed",
    "rolled-back",
    "blocked",
    "error",
  ]),
  capability: Schema.Struct({
    check: Schema.Boolean,
    download: Schema.Boolean,
    install: Schema.Boolean,
  }),
  stagedHandle: Schema.optionalKey(Schema.String),
  provenance: Schema.optionalKey(
    Schema.Struct({ sourceSha: Schema.String, artifactId: Schema.Number }),
  ),
  migrationPlan: Schema.optionalKey(
    Schema.Struct({
      pendingUpstream: Schema.Array(Schema.Number),
      pendingJones: Schema.Array(Schema.Number),
    }),
  ),
});
const decodeState = Schema.decodeUnknownSync(JonesUpdateState);
const decodeOldState = Schema.decodeSync(oldClientState);

describe("alternative offer wire compatibility", () => {
  it("keeps old phases and retained fields decodable independently; old servers omit offers", () => {
    const old = {
      source: "jones-actions",
      channel: "jones-main",
      phase: "staged",
      capability: { check: true, download: true, install: true },
      stagedHandle: "retained",
      migrationPlan: { pendingUpstream: [42], pendingJones: [7] },
    };
    expect(decodeState(old).alternativeCandidate).toBeUndefined();
    const provenance = {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: "b".repeat(40),
      sourceTree: "c".repeat(40),
      workflow: "linux",
      runId: 1,
      runAttempt: 1,
      artifactId: 2,
      artifactDigest: "d".repeat(64),
      platform: "linux",
      architecture: "x64",
    };
    const wire = {
      ...old,
      provenance,
      alternativeCandidate: {
        checkedAt: "2026-10-29T00:00:00Z",
        expiresAt: "2099-01-01T00:00:00Z",
        provenance: { ...provenance, sourceSha: "f".repeat(40), artifactId: 3 },
      },
    };
    const modern = decodeState(wire);
    expect(modern.alternativeCandidate?.provenance.artifactId).toBe(3);
    expect(decodeOldState(modern)).toEqual({
      ...old,
      provenance: { sourceSha: provenance.sourceSha, artifactId: 2 },
    });
  });
});

describe("progress and check record wire compatibility", () => {
  const progress = {
    schema: 1,
    kind: "install",
    operationId: "11111111-1111-4111-8111-111111111111",
    phase: "prep",
    stepIndex: 2,
    stepCount: 6,
    stepKey: "startup-readiness",
    stepLabel: "Checking startup readiness",
    stepStartedAt: "2026-11-10T00:00:05.000Z",
    startedAt: "2026-11-10T00:00:00.000Z",
    updatedAt: "2026-11-10T00:00:05.000Z",
    reportedAt: "2026-11-10T00:00:07.000Z",
    steps: [
      {
        key: "verify-runtime",
        label: "Verifying the selected runtime",
        startedAt: "2026-11-10T00:00:00.000Z",
        endedAt: "2026-11-10T00:00:05.000Z",
      },
      { key: "startup-readiness", label: "Checking startup readiness" },
    ],
  };
  const old = {
    source: "jones-actions",
    channel: "jones-main",
    phase: "preparing",
    capability: { check: true, download: true, install: true },
    stagedHandle: "retained",
  };

  it("old hosts omit the new keys and old clients ignore them", () => {
    expect(decodeState(old)).toEqual(old);
    const modern = decodeState({
      ...old,
      progress,
      lastCheck: { at: "2026-11-10T00:00:00.000Z", outcome: "failed", reason: "offline" },
      stagedAlreadyRunning: true,
    });
    expect(modern.progress?.stepKey).toBe("startup-readiness");
    expect(modern.lastCheck?.outcome).toBe("failed");
    expect(decodeOldState(modern)).toEqual(old);
  });

  it("rejects records outside their bounds", () => {
    for (const patch of [
      { stepLabel: "l".repeat(81) },
      { stepCount: 13 },
      { stepIndex: 0 },
      { steps: Array.from({ length: 13 }, () => progress.steps[1]) },
      { error: { code: "bad code", message: "x" } },
      { error: { code: "failed", message: "x".repeat(241) } },
    ])
      expect(() => decodeState({ ...old, progress: { ...progress, ...patch } })).toThrow();
  });
});
