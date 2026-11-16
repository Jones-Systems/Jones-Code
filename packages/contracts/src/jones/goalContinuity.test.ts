import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  ContinuityCounter,
  ContinuityDeliverySettlement,
  ContinuityOwner,
  ContinuityPatch,
  ContinuityRecoveryCapability,
  ContinuityRequest,
  ContinuityResult,
  ContinuityWitness,
  ContinuityDeliveryReceipt,
  ContinuityDispatchPermit,
  ContinuityPendingTicket,
  ContinuityPreparedTicket,
  ContinuityRecoveryAuthorization,
  ContinuityRecoveryTransitionResult,
  DeliveryGate,
  DeliveryPhase,
  ContinuityProducerIdentity,
  ReconcileDeliveryRequest,
  ReconcileDeliveryResult,
} from "./goalContinuity.ts";

const owner = {
  environmentId: "env",
  threadId: "thread",
  providerInstanceId: "codex",
  providerThreadId: "pt",
  driver: "codex",
  nativeThreadId: "native",
};
const target = { goalId: "goal", revision: "1", generation: "9007199254740993", owner };
const isContinuityRequest = Schema.is(ContinuityRequest);
const isContinuityResult = Schema.is(ContinuityResult);

const compiledIs1 = Schema.is(ContinuityOwner);
const compiledIs2 = Schema.is(ContinuityWitness);
const compiledIs3 = Schema.is(ContinuityPatch);
const compiledIs4 = Schema.is(ContinuityRequest);
const compiledIs5 = Schema.is(ContinuityProducerIdentity);
const compiledIs6 = Schema.is(ContinuityResult);
const compiledIs7 = Schema.is(ContinuityRecoveryCapability);
const compiledDecodeSync8 = Schema.decodeSync(ContinuityRequest);

describe("goal continuity wire vocabulary", () => {
  it("adds bounded delivery discovery without producer authority or request-key mutation", () => {
    const request = {
      action: "discover_deliveries",
      expected: { ...target, snapshotDigest: "a".repeat(64) },
    };
    expect(isContinuityRequest(request)).toBe(true);
    expect(isContinuityRequest({ ...request, cursor: "opaque.cursor" })).toBe(true);
    for (const extra of [
      { cursor: "" },
      { cursor: "x".repeat(1025) },
      { deliveryId: "guess" },
      { clientRequestId: "mutation" },
      { producerOrigin: "authority" },
    ])
      expect(isContinuityRequest({ ...request, ...extra })).toBe(false);
    expect(
      isContinuityResult({
        outcome: "deliveries",
        expected: request.expected,
        fenceRevision: "0",
        deliveries: [],
        nextCursor: null,
      }),
    ).toBe(true);
    expect(
      isContinuityResult({
        outcome: "deliveries",
        expected: request.expected,
        fenceRevision: "0",
        deliveries: [],
        nextCursor: "x".repeat(1025),
      }),
    ).toBe(false);
  });
  it("accepts canonical signed-64 decimal strings without number coercion", () => {
    const decode = Schema.decodeUnknownSync(ContinuityCounter);
    expect(decode("9007199254740993")).toBe("9007199254740993");
    expect(decode("9223372036854775807")).toBe("9223372036854775807");
    for (const value of [
      0,
      1,
      "",
      "00",
      "01",
      "+1",
      "-1",
      "1.0",
      "1e2",
      " 1",
      "1\n",
      "9223372036854775808",
    ])
      expect(() => decode(value)).toThrow();
  });

  it("keeps durable ownership separate from required execution witness fields", () => {
    expect(compiledIs1(owner)).toBe(true);
    expect(compiledIs2(owner)).toBe(false);
    expect(
      compiledIs2({
        ...owner,
        providerSessionId: "runtime-session",
        runtimeGeneration: "incarnation",
        runId: "run",
        attemptId: "attempt",
      }),
    ).toBe(true);
    expect(compiledIs1({ ...owner, nativeThreadId: " " })).toBe(false);
  });

  it("models explicit collection operations and disallows ordinary patch-to-complete", () => {
    expect(compiledIs3({ status: "complete" })).toBe(false);
    expect(compiledIs3({ blockers: [] })).toBe(false);
    expect(compiledIs3({ boundaries: { remove: ["boundary"] } })).toBe(false);
    expect(
      compiledIs3({
        boundaries: {
          append: [
            { id: "boundary", text: "Completed", finishIds: ["finish"], evidenceIds: ["proof"] },
          ],
        },
      }),
    ).toBe(true);
    expect(
      compiledIs3({
        blockers: { add: [{ id: "gate", text: "Owner proof" }], remove: [] },
        focus: null,
      }),
    ).toBe(true);
    expect(
      compiledIs4({
        action: "checkpoint",
        clientRequestId: "req",
        expected: target,
        patch: { status: "active" },
      }),
    ).toBe(true);
    expect(
      compiledIs4({
        action: "close",
        clientRequestId: "req",
        expected: target,
        completionEvidenceIds: ["evidence"],
      }),
    ).toBe(true);
    expect(
      compiledIs4({
        action: "checkpoint",
        expected: target,
        patch: { focus: "new" },
      }),
    ).toBe(false);
    expect(
      compiledIs4({
        action: "checkpoint",
        clientRequestId: " req ",
        expected: target,
        patch: { focus: "new" },
      }),
    ).toBe(false);
  });

  it("accepts full native runtime generation identities without the request-key length limit", () => {
    const runtimeGeneration =
      "event:thread:" + "nested-delegation:".repeat(32) + "provider-session:incarnation";
    expect(runtimeGeneration.length).toBeGreaterThan(256);
    expect(
      compiledIs2({
        ...owner,
        providerSessionId: "runtime-session",
        runtimeGeneration,
        runId: "run",
        attemptId: "attempt",
      }),
    ).toBe(true);
    expect(
      compiledIs5({
        producerId: "producer",
        driver: "codex",
        providerInstanceId: "codex",
        providerSessionId: "runtime-session",
        runtimeGeneration,
      }),
    ).toBe(true);
  });

  it("requires proposal and persisted confirmation references instead of an agent confirmation boolean", () => {
    const intent = {
      goalId: "goal",
      revision: "1",
      northStar: "Outcome",
      preservedQualities: [],
      finishItems: [{ id: "finish", text: "Finish" }],
      source: { type: "owner_request", reference: "ref", identity: "owner" },
      derivation: null,
      initializationMode: "confirm_first",
      startEvidence: [{ id: "start", reference: "ref", assertion: "Requested" }],
      predecessor: null,
    };
    const request = {
      action: "initialize",
      clientRequestId: "req",
      expectedOwner: owner,
      expectedCurrent: null,
      intent,
    };
    expect(
      compiledIs4({
        ...request,
        initialization: { type: "confirm", proposalId: "proposal", confirmationId: "response" },
      }),
    ).toBe(true);
    expect(
      compiledIs4({
        ...request,
        initialization: { type: "confirm", confirmed: true },
      }),
    ).toBe(false);
    expect(
      compiledIs4({
        ...request,
        initialization: { type: "propose", proposalId: "proposal" },
      }),
    ).toBe(true);
  });

  it("keeps request uncertainty and unsupported strict restoration explicit", () => {
    expect(
      compiledIs6({
        outcome: "unknown",
        clientRequestId: "req",
        requestDigest: "a".repeat(64),
        previousGeneration: "9007199254740993",
        attemptedGeneration: "9007199254740994",
      }),
    ).toBe(true);
    expect(compiledIs6({ outcome: "unknown", clientRequestId: "req" })).toBe(false);
    expect(
      compiledIs7({
        strongIdentity: true,
        turnStart: "fresh_context",
        compaction: "fresh_context_injection",
        strictPreResumptionBarrier: "supported",
      }),
    ).toBe(false);
  });

  it("rejects boundary rewrites rather than silently stripping excess fields", () => {
    const decode = Schema.decodeUnknownSync(ContinuityPatch);
    for (const operations of [
      { append: [], remove: ["boundary"] },
      { append: [], replace: [] },
      { append: [], update: [{ id: "boundary", text: "replacement" }] },
    ])
      expect(() => decode({ boundaries: operations })).toThrow();
    expect(() =>
      compiledDecodeSync8({
        action: "checkpoint",
        clientRequestId: "req",
        expected: target,
        patch: { boundaries: { append: [], remove: ["boundary"] } },
      }),
    ).toThrow();
  });
});

const witness = {
  ...owner,
  providerSessionId: "session",
  runtimeGeneration: "incarnation",
  runId: "run",
  attemptId: "attempt",
};
const producer = {
  producerId: "captured-producer",
  driver: "codex",
  providerInstanceId: "codex",
  providerSessionId: "session",
  runtimeGeneration: "incarnation",
};
const point = { type: "compaction", nativeTurnId: "native-turn", itemId: "compact" };
const identity = {
  deliveryId: "delivery",
  originalOwner: owner,
  goalId: "goal",
  revision: "1",
  generation: target.generation,
  point,
  witness,
  producer,
};
const preparation = {
  target: { ...target, snapshotDigest: "a".repeat(64) },
  capsuleDigest: "b".repeat(64),
};
const pending = { type: "pending_ticket", identity, deliveryRevision: "0", fenceRevision: "5" };
const prepared = {
  type: "prepared_ticket",
  identity,
  deliveryRevision: "1",
  fenceRevision: "6",
  preparation,
};
const permit = {
  type: "dispatch_permit",
  identity,
  deliveryRevision: "2",
  fenceRevision: "7",
  preparation,
  permitId: "permit",
  dispatchAttemptId: "dispatch-attempt",
  rpcRequestId: "rpc",
};
const gate = {
  outcome: "hold",
  reason: "delivery_unknown",
  fenceRevision: "8",
  deliveryIds: ["delivery"],
};
const reconcile = {
  action: "reconcile_delivery",
  clientRequestId: "owner-request",
  deliveryId: "delivery",
  expectedOriginalOwner: owner,
  expectedDeliveryRevision: "3",
  expectedFenceRevision: "8",
  expectedCurrentRecord: null,
  reason: "Explicitly accept this attempt's uncertainty",
  evidenceRefs: ["observation:1"],
  disposition: { type: "accept_unknown_for_one_recovery", recoveryTarget: preparation.target },
};

const compiledIs9 = Schema.is(ContinuityPendingTicket);
const compiledIs10 = Schema.is(ContinuityPreparedTicket);
const compiledIs11 = Schema.is(ContinuityDispatchPermit);
const compiledIs12 = Schema.is(ContinuityDeliverySettlement);
const compiledIs13 = Schema.is(DeliveryPhase);
const compiledIs14 = Schema.is(ContinuityDeliveryReceipt);
const compiledIs15 = Schema.is(DeliveryGate);
const compiledIs16 = Schema.is(ContinuityRecoveryTransitionResult);
const compiledIs17 = Schema.is(ReconcileDeliveryRequest);
const compiledIs18 = Schema.is(ReconcileDeliveryResult);
const compiledIs19 = Schema.is(ContinuityRecoveryAuthorization);

describe("durable recovery delivery contracts", () => {
  it("separates pending and prepared tickets from the single-use dispatch permit", () => {
    expect(compiledIs9(pending)).toBe(true);
    expect(compiledIs10(prepared)).toBe(true);
    expect(compiledIs11(pending)).toBe(false);
    expect(compiledIs11(prepared)).toBe(false);
    expect(compiledIs11(permit)).toBe(true);
    expect(compiledIs11({ ...permit, rpcRequestId: undefined })).toBe(false);
    expect(
      compiledIs10({
        ...prepared,
        preparation: { target: preparation.target },
      }),
    ).toBe(false);
  });

  it("binds acknowledgement evidence to the original producer, execution and RPC", () => {
    const evidence = {
      outcome: "acknowledged",
      rpcRequestId: "rpc",
      acknowledgementRef: "ack:1",
      witness,
      producer,
    };
    expect(compiledIs12({ permit, evidence })).toBe(true);
    for (const incomplete of [
      { ...evidence, producer: undefined },
      { ...evidence, witness: owner },
      { ...evidence, rpcRequestId: undefined },
      { ...evidence, acknowledgementRef: undefined },
    ])
      expect(compiledIs12({ permit, evidence: incomplete })).toBe(false);
    expect(
      compiledIs12({
        permit,
        evidence: { outcome: "unknown", cause: "lost_response" },
      }),
    ).toBe(true);
    expect(compiledIs12({ permit, evidence: { outcome: "not_sent" } })).toBe(false);
    expect(
      compiledIs12({
        permit,
        evidence: {
          outcome: "not_sent",
          proof: { type: "protocol_no_effect", rpcRequestId: "rpc", responseRef: "response" },
        },
      }),
    ).toBe(true);
  });

  it("retains all phases and independent revisions without treating progress as a delivery counter", () => {
    for (const phase of [
      "pending",
      "prepared",
      "dispatching",
      "acknowledged",
      "not_sent",
      "unknown",
    ]) {
      expect(compiledIs13(phase)).toBe(true);
      const receipt = {
        identity,
        deliveryRevision: "9007199254740994",
        fenceRevision: "12",
        phase,
        preparation: phase === "pending" ? null : preparation,
        events: [
          {
            eventId: "event",
            deliveryRevision: "9007199254740994",
            fenceRevision: "12",
            phase,
            transition:
              phase === "pending" ? { eventId: "compaction-start", type: "started" } : null,
            permit: ["dispatching", "acknowledged"].includes(phase) ? permit : null,
            evidence:
              phase === "acknowledged"
                ? {
                    outcome: "acknowledged",
                    rpcRequestId: "rpc",
                    acknowledgementRef: "ack",
                    witness,
                    producer,
                  }
                : phase === "not_sent"
                  ? {
                      outcome: "not_sent",
                      proof: { type: "dispatch_precluded", revocationRef: "revocation" },
                    }
                  : phase === "unknown"
                    ? { outcome: "unknown", cause: "reconstruction" }
                    : null,
          },
        ],
        dispositions: [],
      };
      expect(compiledIs14(receipt)).toBe(true);
      expect(compiledIs14({ ...receipt, deliveryRevision: 1 })).toBe(false);
      if (phase === "dispatching" || phase === "acknowledged") {
        expect(
          compiledIs14({
            ...receipt,
            events: [{ ...receipt.events[0], permit: null }],
          }),
        ).toBe(false);
      }
      if (phase === "not_sent" || phase === "acknowledged" || phase === "unknown") {
        expect(
          compiledIs14({
            ...receipt,
            events: [{ ...receipt.events[0], evidence: null }],
          }),
        ).toBe(false);
      }
    }
    expect(compiledIs13("known_no_effect")).toBe(false);
    expect(compiledIs15(gate)).toBe(true);
    expect(compiledIs15({ ...gate, deliveryIds: [] })).toBe(false);
    expect(compiledIs15({ ...gate, deliveryIds: ["delivery", "delivery"] })).toBe(false);
    expect(compiledIs16({ outcome: "absent", gate })).toBe(true);
    expect(
      compiledDecodeSync8({
        action: "delivery_receipt",
        deliveryId: "delivery",
      }),
    ).toEqual({ action: "delivery_receipt", deliveryId: "delivery" });
  });

  it("allows idle owner expectations while rejecting fabricated authority and quiescence claims", () => {
    const decode = Schema.decodeUnknownSync(ContinuityRequest);
    expect(compiledIs17(reconcile)).toBe(true);
    expect(decode(reconcile).action).toBe("reconcile_delivery");
    for (const forged of [
      { actor: "owner" },
      { callerKind: "owner_client" },
      { witness },
      { authorized: true },
      { quiescent: true },
      { quiescence: { outcome: "confirmed" } },
    ]) {
      expect(() => decode({ ...reconcile, ...forged })).toThrow();
      expect(compiledIs17({ ...reconcile, ...forged })).toBe(false);
    }
    expect(() =>
      decode({
        ...reconcile,
        disposition: { type: "retire_without_redelivery", acknowledged: true },
      }),
    ).toThrow();
    expect(compiledIs17({ ...reconcile, expectedFenceRevision: undefined })).toBe(false);
    expect(
      compiledIs17({
        ...reconcile,
        disposition: { type: "accept_unknown_for_one_recovery" },
      }),
    ).toBe(false);
    expect(
      compiledIs17({
        ...reconcile,
        disposition: { type: "retry_proven_not_sent", recoveryTarget: preparation.target },
      }),
    ).toBe(true);
    expect(
      compiledIs17({
        ...reconcile,
        disposition: { type: "retire_without_redelivery" },
      }),
    ).toBe(true);
  });

  it("records owner decisions independently of native acknowledgement and the effective gate", () => {
    const disposition = {
      dispositionId: "decision",
      request: reconcile,
      actorNamespace: "authenticated_client",
      actorId: "owner",
      deliveryRevision: "4",
      fenceRevision: "9",
      quiescence: { outcome: "unproven" },
      effect: { type: "held", reason: "quiescence_required" },
    };
    expect(compiledIs18({ outcome: "delivery_reconciled", disposition, gate })).toBe(true);
    const authorization = {
      type: "single_use_recovery",
      authorizationId: "grant",
      dispositionId: "decision",
      deliveryId: "delivery",
      originalOwner: owner,
      recoveryTarget: preparation.target,
      fenceRevision: "9",
    };
    expect(compiledIs19(authorization)).toBe(true);
    expect(
      compiledIs18({
        outcome: "delivery_reconciled",
        disposition: {
          ...disposition,
          effect: { type: "recovery_authorized", authorization },
        },
        gate,
      }),
    ).toBe(false);
    expect(
      compiledIs19({
        ...authorization,
        recoveryTarget: { ...target },
      }),
    ).toBe(false);
    expect(
      compiledIs18({
        outcome: "delivery_reconciled",
        disposition: {
          ...disposition,
          quiescence: {
            outcome: "confirmed",
            producer,
            witness,
            dispatchRevocationRef: "revocation",
            nativeSource: {
              type: "paused_and_settled",
              pauseRef: "pause",
              turnSettlementRef: "settlement",
            },
          },
          effect: { type: "recovery_authorized", authorization },
        },
        gate: { ...gate, reason: "recovery_required" },
      }),
    ).toBe(true);
  });
});
