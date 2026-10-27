import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import type { DesktopBridge } from "../ipc.ts";
import {
  JonesInstallPlan,
  JonesInstallPlanDevice,
  JonesInstallPlanGrant,
  JonesInstallPlanHostOperation,
  JonesInstallPlanLaptop,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "./installPlan.ts";

const planId = "11111111-1111-4111-8111-111111111111";
const operationId = "22222222-2222-4222-8222-222222222222";
const stopId = "33333333-3333-4333-8333-333333333333";
const enrollmentId = "44444444-4444-4444-8444-444444444444";
const source = "a".repeat(40);
const tree = "b".repeat(40);
const installed = "c".repeat(40);
const at = "2026-10-27T04:00:00Z";
const environmentId = "synthetic-remote";
const target = {
  repository: "Jones-Systems/Jones-Code",
  sourceSha: source,
  sourceTree: tree,
  version: "2.0.0",
} as const;
const select = {
  environmentId,
  label: "Synthetic remote",
  route: "follower-operations",
  enrollmentId,
} as const;
const review = {
  reviewedAt: at,
  stagedHandle: "host-stage",
  targetSource: source,
  currentVersion: "1.0.0",
  expectedInstalledSource: installed,
  continueRunningThreads: false,
  recovery: { policy: "clone-required", preflightId: stopId },
} as const;
const operationStart = {
  kind: "operation-start",
  input: {
    operationId,
    environmentId,
    expectedInstalledSource: installed,
    action: { kind: "download-exact", targetSource: source },
    correlation: { planId, deviceKey: environmentId },
  },
} as const;
const directiveInput = {
  directiveId: operationId,
  enrollmentId,
  environmentId,
  repository: "Jones-Systems/Jones-Code",
  targetSource: source,
  targetTree: tree,
  expectedDirectiveId: null,
} as const;
const directive = {
  directiveId: operationId,
  targetSource: source,
  targetTree: tree,
  mode: "manual-review",
  issuedAt: at,
} as const;
const lease = { claimant: "synthetic-window", reservedAt: at, leaseUntil: "2026-10-27T04:01:00Z" };
const operation = { operationId, step: "download", request: operationStart, lease } as const;
const device = { ...select, membership: "selected", addedAt: at, operations: [operation] } as const;
const laptop = { environmentId: "synthetic-laptop", operations: [] } as const;
const plan = {
  schema: 1,
  planId,
  revision: 1,
  createdAt: at,
  origin: "update-button",
  target,
  from: { version: "1.0.0", sourceSha: installed },
  laptop,
  devices: [device],
} as const;
const state = {
  schema: 1,
  current: plan,
  last: null,
  laptop: { observedAt: at, activationBlocked: false },
  view: { complete: false },
} as const;
const receipt = {
  ...operationStart.input,
  protocol: 1,
  phase: "running",
  acceptedAt: at,
  updatedAt: at,
  jobId: stopId,
} as const;

describe("install-plan renderer authority", () => {
  const decode = Schema.decodeUnknownSync(JonesInstallPlanRequest);

  it("accepts the bounded request vocabulary and retains CAS and review bindings", () => {
    const requests = [
      { type: "read" },
      { type: "publication" },
      {
        type: "start",
        origin: "update-everything",
        target,
        select: [select],
        laptop: { action: "install", review },
      },
      { type: "edit", planId, expectedRevision: 1, select: [], deselect: [environmentId] },
      { type: "laptop", planId, action: "preflight", review },
      {
        type: "claim",
        planId,
        environmentId,
        claimant: "synthetic-window",
        step: "install",
        explicit: true,
        action: {
          kind: "install",
          stagedHandle: review.stagedHandle,
          currentVersion: review.currentVersion,
          targetSource: source,
          recovery: review.recovery,
        },
        expectedInstalledSource: installed,
        review,
      },
      {
        type: "claim",
        planId,
        environmentId,
        claimant: "synthetic-window",
        step: "directive",
        explicit: false,
        enrollmentId,
        expectedDirectiveId: null,
      },
      {
        type: "report",
        planId,
        environmentId,
        operationId,
        evidence: { kind: "receipt", receipt },
      },
      { type: "end", planId, environmentId, expectedRevision: 2 },
      { type: "dismiss", planId, expectedRevision: 3 },
    ];
    for (const request of requests) expect(decode(request)).toEqual(request);
    expect(() => decode({ type: "dismiss", planId })).toThrow();
    expect(() => decode({ type: "end", planId, expectedRevision: 1 })).toThrow();
    expect(() =>
      decode({ type: "claim", planId, environmentId, claimant: "window", step: "install" }),
    ).toThrow();
    expect(() =>
      decode({
        type: "start",
        origin: "update-button",
        target: { ...target, repository: "another/repository" },
        select: [],
      }),
    ).toThrow();
  });

  it("rejects renderer verification, arbitrary recording and native laptop evidence reports", () => {
    for (const type of ["verify", "record", "bindLaptopInstall", "recordLaptopOutcome"])
      expect(() => decode({ type, planId, evidence: { activeSource: source } })).toThrow();
    for (const evidence of [
      { kind: "laptop", activeSource: source, committedTransactionId: "fabricated" },
      { kind: "receipt", receipt: { operationId, phase: "completed" } },
      { kind: "outcome", status: "committed" },
    ])
      expect(() =>
        decode({ type: "report", planId, environmentId, operationId, evidence }),
      ).toThrow();
    expect(() => decode({ type: "laptop", planId, action: "verify" })).toThrow();
  });

  it("drops unrecognized native evidence fields from otherwise valid renderer requests", () => {
    const native = {
      binding: { transactionId: "fabricated" },
      gate: { activeSource: source },
      gateClosed: { reason: "changed" },
      activationBlocked: false,
    };
    expect(decode({ type: "read", ...native })).toEqual({ type: "read" });
    expect(
      decode({
        type: "start",
        origin: "update-button",
        target,
        select: [],
        laptop: { action: "download", ...native },
        ...native,
      }),
    ).toEqual({
      type: "start",
      origin: "update-button",
      target,
      select: [],
      laptop: { action: "download" },
    });
    expect(decode({ type: "laptop", planId, action: "install", review, ...native })).toEqual({
      type: "laptop",
      planId,
      action: "install",
      review,
    });
  });

  it("accepts only the target receipt or fleet directive report envelopes", () => {
    const status = {
      operationProtocol: 1,
      planDirectiveProtocol: 1,
      environmentId,
      enrollment: null,
      operation: null,
      update: null,
      follower: {
        protocol: 1,
        leaderSource: null,
        state: "waiting-for-leader",
        planDirective: directive,
      },
    };
    for (const evidence of [
      { kind: "directive", status },
      { kind: "directive-refused", reason: "conflict", message: "Enrollment changed" },
    ]) {
      const request = { type: "report", planId, environmentId, operationId, evidence };
      expect(decode(request)).toEqual(request);
    }
    expect(() =>
      decode({
        type: "report",
        planId,
        environmentId,
        operationId,
        evidence: { kind: "directive-refused", reason: "unknown-reason", message: "x" },
      }),
    ).toThrow();
  });

  it("bounds edit/start selections, deselections and renderer claimant labels", () => {
    const selections = Array.from({ length: 16 }, () => select);
    expect(decode({ type: "start", origin: "settings", target, select: selections })).toMatchObject(
      { select: selections },
    );
    expect(() =>
      decode({ type: "start", origin: "settings", target, select: [...selections, select] }),
    ).toThrow();
    expect(() =>
      decode({
        type: "edit",
        planId,
        expectedRevision: 1,
        select: [...selections, select],
        deselect: [],
      }),
    ).toThrow();
    expect(() =>
      decode({
        type: "edit",
        planId,
        expectedRevision: 1,
        select: [],
        deselect: Array.from({ length: 17 }, () => environmentId),
      }),
    ).toThrow();
    expect(() =>
      decode({
        type: "start",
        origin: "settings",
        target,
        select: [{ ...select, label: "x".repeat(65) }],
      }),
    ).toThrow();
    expect(() =>
      decode({
        type: "claim",
        planId,
        environmentId,
        claimant: "x".repeat(65),
        step: "download",
        explicit: true,
      }),
    ).toThrow();
  });
});

describe("install-plan persisted and native output contracts", () => {
  it("keeps native transaction/generation/source evidence on output, distinct from requests", () => {
    const bound = {
      ...laptop,
      binding: {
        operationId,
        stagedHandle: "laptop-stage",
        targetSource: source,
        targetTree: tree,
        transactionId: "native-transaction",
        fromGeneration: "old-generation",
        boundAt: at,
      },
      gate: {
        openedAt: at,
        basis: "plan-install",
        transactionId: "native-transaction",
        activeGeneration: "new-generation",
        activeSource: source,
        activeTree: tree,
      },
    };
    expect(Schema.decodeUnknownSync(JonesInstallPlanLaptop)(bound)).toEqual(bound);
    const observed = {
      ...state,
      current: { ...plan, laptop: bound },
      laptop: {
        observedAt: at,
        environmentId: laptop.environmentId,
        activeVersion: "2.0.0",
        activeGeneration: "new-generation",
        activeSource: source,
        activeTree: tree,
        committedTransactionId: "native-transaction",
        activationBlocked: false,
      },
    };
    expect(Schema.decodeUnknownSync(JonesInstallPlanState)(observed)).toEqual(observed);
    const codec = Schema.toCodecJson(JonesInstallPlanState);
    expect(Schema.decodeSync(codec)(Schema.encodeUnknownSync(codec)(observed))).toEqual(observed);
    expect(() =>
      Schema.decodeUnknownSync(JonesInstallPlanLaptop)({
        ...bound,
        gate: { openedAt: at, basis: "plan-install", activeSource: source, activeTree: tree },
      }),
    ).toThrow();
    const closed = { ...bound, gateClosed: { at, reason: "rolled-back" } };
    expect(Schema.decodeUnknownSync(JonesInstallPlanLaptop)(closed)).toEqual(closed);
  });

  it("retains host operation leases, Stop/fence correlation, and directive observations", () => {
    const held = { ...operation, stop: { stopId, lease }, fence: { lease }, receipt };
    expect(Schema.decodeSync(JonesInstallPlanHostOperation)(held)).toEqual(held);
    const applied = {
      ...operation,
      step: "directive",
      request: { kind: "plan-directive", input: directiveInput },
      directive: { kind: "applied", observedAt: at, directive },
    };
    expect(Schema.decodeUnknownSync(JonesInstallPlanHostOperation)(applied)).toEqual(applied);
    expect(() =>
      Schema.decodeUnknownSync(JonesInstallPlanHostOperation)({
        ...operation,
        request: { kind: "operation-stop", input: { operationId, stopId, environmentId } },
      }),
    ).toThrow();
  });

  it("round-trips all four dispatch envelopes with their target-bound operation IDs", () => {
    const decode = Schema.decodeUnknownSync(JonesInstallPlanGrant);
    const grant = { planId, environmentId, operationId, leaseUntil: lease.leaseUntil };
    for (const [step, request] of [
      ["download", operationStart],
      ["stop", { kind: "operation-stop", input: { operationId, stopId, environmentId } }],
      ["fence", { kind: "operation-fence", input: operationStart.input }],
      ["directive", { kind: "plan-directive", input: directiveInput }],
    ] as const)
      expect(decode({ ...grant, step, request })).toEqual({ ...grant, step, request });
    expect(() =>
      decode({
        ...grant,
        step: "stop",
        request: { kind: "operation-stop", input: { operationId, environmentId } },
      }),
    ).toThrow();
  });

  it("bounds persisted devices and host/Laptop operations without discarding unresolved work", () => {
    expect(Schema.decodeSync(JonesInstallPlan)(plan)).toEqual(plan);
    expect(
      Schema.decodeSync(JonesInstallPlan)({
        ...plan,
        devices: Array.from({ length: 16 }, () => device),
      }).devices,
    ).toHaveLength(16);
    expect(() =>
      Schema.decodeSync(JonesInstallPlan)({
        ...plan,
        devices: Array.from({ length: 17 }, () => device),
      }),
    ).toThrow();
    expect(
      Schema.decodeSync(JonesInstallPlanDevice)({
        ...device,
        operations: Array.from({ length: 16 }, () => operation),
      }).operations,
    ).toHaveLength(16);
    expect(() =>
      Schema.decodeSync(JonesInstallPlanDevice)({
        ...device,
        operations: Array.from({ length: 17 }, () => operation),
      }),
    ).toThrow();
    const laptopOperation = {
      operationId,
      step: "download",
      requestedAt: at,
      worker: { pid: 123, processGroup: 123 },
      result: { status: "held", at, code: "exit-unproved" },
    };
    expect(
      Schema.decodeUnknownSync(JonesInstallPlanLaptop)({
        ...laptop,
        operations: Array.from({ length: 16 }, () => laptopOperation),
      }).operations,
    ).toHaveLength(16);
    expect(() =>
      Schema.decodeUnknownSync(JonesInstallPlanLaptop)({
        ...laptop,
        operations: Array.from({ length: 17 }, () => laptopOperation),
      }),
    ).toThrow();
    expect(() => Schema.decodeUnknownSync(JonesInstallPlan)({ ...plan, schema: 2 })).toThrow();
  });

  it("carries read-only refusals, publication withholding and bounded edit outcomes", () => {
    const result = {
      state: { ...state, readOnly: { code: "newer-schema", message: "Retained read-only" } },
      publication: { kind: "withheld", code: "plan-managed", message: "Plan owns this target" },
      refused: { code: "gate-closed", message: "Laptop not committed" },
      applied: [{ environmentId, result: "locked-started", reason: "Already dispatched" }],
    };
    expect(Schema.decodeUnknownSync(JonesInstallPlanResult)(result)).toEqual(result);
    const outcome = { environmentId, result: "unchanged" };
    expect(() =>
      Schema.decodeUnknownSync(JonesInstallPlanResult)({
        state,
        applied: Array.from({ length: 17 }, () => outcome),
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeSync(JonesInstallPlanResult)({
        state,
        refused: { code: "gate-closed", message: "x".repeat(513) },
      }),
    ).toThrow();
  });

  it("allows older desktop bridges to omit both plan hooks", () => {
    const legacy: Pick<DesktopBridge, "installPlan" | "onInstallPlan"> = {};
    expect(legacy.installPlan).toBeUndefined();
    expect(legacy.onInstallPlan).toBeUndefined();
  });
});

it("decodes an independent exact-bound native preflight without granting any plan or policy authority", () => {
  const decode = Schema.decodeUnknownSync(JonesInstallPlanRequest);
  const input = {
    environmentId: "synthetic-laptop",
    stagedHandle: "stage",
    currentVersion: "old",
    expectedInstalledSource: installed,
  };
  expect(decode({ type: "recovery-preflight", input })).toEqual({
    type: "recovery-preflight",
    input,
  });
  for (const key of Object.keys(input)) {
    const partial = Object.fromEntries(Object.entries(input).filter(([name]) => name !== key));
    expect(() => decode({ type: "recovery-preflight", input: partial })).toThrow();
  }
  expect(
    decode({
      type: "recovery-preflight",
      input,
      planId,
      preflightId: operationId,
      policy: "clone-required",
      activeGeneration: "fabricated",
    }),
  ).toEqual({ type: "recovery-preflight", input });
});
