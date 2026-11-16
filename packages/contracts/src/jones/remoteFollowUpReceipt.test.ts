import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ThreadId, TrimmedNonEmptyString } from "../baseSchemas.ts";
import {
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpClientRequestId,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpThreadSendInput,
  OrchestratorMcpThreadSendReceiptLookupInput,
  OrchestratorMcpThreadSendReceiptLookupResult,
  REMOTE_FOLLOW_UP_RECEIPT_VERSION,
} from "../orchestratorMcp.ts";
import { ThreadResultRequestDescriptor } from "./threadResultDependency.ts";

// Frozen 43fd686 input layout, orchestratorMcp.ts blob ff9567e29a2c7f29877f8c8cf01b971de5889ec5.
// Do not derive this layout from the new input: old decoders ignore an unsupported fence.
const PreviousSendInput = Schema.Struct({
  awaitResult: Schema.optional(Schema.Boolean),
  resultRequest: Schema.optional(ThreadResultRequestDescriptor),
  environmentId: OrchestratorMcpEnvironmentTarget,
  threadId: ThreadId,
  message: TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)),
  mode: Schema.optional(Schema.Literals(["auto", "queue", "separate_turn", "steer", "restart"])),
  freshContext: Schema.optional(Schema.Boolean),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
});

// The required pre-follow-up feature layout, independently of the new capability fields.
const PreviousFeatures = Schema.Struct({
  appOwnedSubagents: Schema.Boolean,
  asyncPolling: Schema.Boolean,
  cancellation: Schema.Boolean,
  batchThreadCreation: Schema.Boolean,
  threadManagement: Schema.Boolean,
  incrementalThreadRead: Schema.Boolean,
  scheduledTasks: Schema.Boolean,
  maxBatchThreads: Schema.Number,
});

const oldSend = {
  environmentId: "environment:target",
  threadId: "thread:target",
  message: "Continue the captured work",
  clientRequestId: "request:original",
  awaitResult: false,
  mode: "queue",
  freshContext: false,
};
const oldCapabilities = {
  parentThreadId: null,
  inheritedProviderInstanceId: null,
  inheritedModel: null,
  runtimeMode: "full-access",
  interactionMode: "default",
  providers: [],
  features: {
    appOwnedSubagents: true,
    asyncPolling: true,
    cancellation: true,
    batchThreadCreation: true,
    threadManagement: true,
    incrementalThreadRead: true,
    scheduledTasks: true,
    maxBatchThreads: 10,
  },
};
const lookup = {
  threadId: "thread:target",
  clientRequestId: "request:original",
  requestDigest: "opaque-exact-encoded-send-digest",
};
const receiptIdentity = {
  ...lookup,
  version: REMOTE_FOLLOW_UP_RECEIPT_VERSION,
  environmentId: "environment:target",
};
const sent = {
  threadId: lookup.threadId,
  messageId: "message:original",
  runId: "run:original",
  taskId: "task:original",
  link: "t3-thread://v1/thread:target",
  status: "queued",
  delivery: "queued",
};
const accepted = { ...receiptIdentity, outcome: "accepted", sent };
const decodeLookup = Schema.decodeUnknownSync(OrchestratorMcpThreadSendReceiptLookupInput);
const decodeReceipt = Schema.decodeUnknownSync(OrchestratorMcpThreadSendReceiptLookupResult);
const receiptJson = Schema.toCodecJson(OrchestratorMcpThreadSendReceiptLookupResult);

describe("remote follow-up SEND fence compatibility", () => {
  const decodeSend = Schema.decodeUnknownSync(OrchestratorMcpThreadSendInput);
  const sendJson = Schema.toCodecJson(OrchestratorMcpThreadSendInput);

  it("keeps an old SEND valid without inventing a target creation fence", () => {
    expect(decodeSend(oldSend)).toEqual(oldSend);
    expect(Schema.decodeUnknownSync(sendJson)(JSON.stringify(oldSend))).toEqual(oldSend);
    expect(decodeSend(oldSend)).not.toHaveProperty("expectedThreadCreatedAt");
  });

  it("preserves the exact createdAt string in native input and encoded peer arguments", () => {
    const input = { ...oldSend, expectedThreadCreatedAt: "2026-11-16T19:07:15.123Z" };
    const decoded = decodeSend(input);
    expect(decoded).toEqual(input);
    const encoded = Schema.encodeSync(sendJson)(decoded);
    expect(JSON.parse(encoded)).toEqual(input);
    expect(Schema.decodeUnknownSync(sendJson)(encoded)).toEqual(decoded);
    const replacement = decodeSend({ ...input, expectedThreadCreatedAt: "2026-11-16T19:07:15.124Z" });
    expect(Schema.encodeSync(sendJson)(replacement)).not.toBe(encoded);
    expect(() => decodeSend({ ...input, expectedThreadCreatedAt: 123 })).toThrow();
    expect(() => decodeSend({ ...input, expectedThreadCreatedAt: null })).toThrow();
  });

  it("establishes that an old decoder drops the fence, so schema acceptance cannot prove support", () => {
    const input = { ...oldSend, expectedThreadCreatedAt: "2026-11-16T19:07:15.123Z" };
    expect(Schema.decodeUnknownSync(PreviousSendInput)(input)).toEqual(oldSend);
    expect(Schema.decodeUnknownSync(Schema.toCodecJson(PreviousSendInput))(JSON.stringify(input)))
      .toEqual(oldSend);
  });
});

describe("remote follow-up capability compatibility", () => {
  const decodeCapabilities = Schema.decodeUnknownSync(OrchestratorMcpCapabilitiesResult);
  const capabilitiesJson = Schema.toCodecJson(OrchestratorMcpCapabilitiesResult);

  it("preserves absent capabilities without advertising either mechanism", () => {
    const decoded = decodeCapabilities(oldCapabilities);
    expect(decoded).toEqual(oldCapabilities);
    expect(decoded.features).not.toHaveProperty("remoteFollowUpSendVersion");
    expect(decoded.features).not.toHaveProperty("remoteFollowUpReceiptVersion");
    expect(JSON.parse(Schema.encodeSync(capabilitiesJson)(decoded))).toEqual(oldCapabilities);
  });

  it("round-trips both explicit versions while old feature readers ignore them", () => {
    const features = {
      ...oldCapabilities.features,
      remoteFollowUpSendVersion: REMOTE_FOLLOW_UP_RECEIPT_VERSION,
      remoteFollowUpReceiptVersion: REMOTE_FOLLOW_UP_RECEIPT_VERSION,
    };
    const input = { ...oldCapabilities, features };
    const decoded = decodeCapabilities(input);
    expect(decoded).toEqual(input);
    expect(Schema.decodeUnknownSync(capabilitiesJson)(Schema.encodeSync(capabilitiesJson)(decoded)))
      .toEqual(input);
    expect(Schema.decodeUnknownSync(PreviousFeatures)(features)).toEqual(oldCapabilities.features);
  });

  it.each(["remoteFollowUpSendVersion", "remoteFollowUpReceiptVersion"] as const)(
    "does not infer the other capability from %s or accept an unknown version",
    (field) => {
      const features = { ...oldCapabilities.features, [field]: REMOTE_FOLLOW_UP_RECEIPT_VERSION };
      expect(decodeCapabilities({ ...oldCapabilities, features }).features).toEqual(features);
      expect(() => decodeCapabilities({
        ...oldCapabilities,
        features: { ...features, [field]: "jones.remote-followup-receipt/v2" },
      })).toThrow();
    },
  );
});

describe("remote follow-up receipt identity and strict results", () => {
  it("round-trips only the caller's target, key and digest, without a caller-supplied auth context", () => {
    const codec = Schema.toCodecJson(OrchestratorMcpThreadSendReceiptLookupInput);
    const decoded = decodeLookup(lookup);
    expect(decoded).toEqual(lookup);
    expect(Schema.decodeUnknownSync(codec)(Schema.encodeSync(codec)(decoded))).toEqual(lookup);
    for (const extra of [{ namespace: "other-origin" }, { environmentId: "other-environment" }]) {
      expect(() => decodeLookup({ ...lookup, ...extra })).toThrow();
      expect(() => Schema.decodeUnknownSync(codec)(JSON.stringify({ ...lookup, ...extra }))).toThrow();
    }
    for (const field of ["clientRequestId", "requestDigest"] as const) {
      expect(() => decodeLookup({ ...lookup, [field]: "" })).toThrow();
    }
  });

  it("preserves the exact accepted message/run receipt instead of substituting a current run", () => {
    const decoded = decodeReceipt(accepted);
    expect(decoded).toEqual(accepted);
    expect(Schema.decodeUnknownSync(receiptJson)(Schema.encodeSync(receiptJson)(decoded)))
      .toEqual(accepted);
    if (decoded.outcome === "accepted") {
      expect(decoded.sent.messageId).toBe("message:original");
      expect(decoded.sent.runId).toBe("run:original");
    }
  });

  it.each(["unknown", "rejected"] as const)("retains %s as a distinct null-sent outcome", (outcome) => {
    const receipt = { ...receiptIdentity, outcome, sent: null };
    expect(decodeReceipt(receipt)).toEqual(receipt);
    expect(Schema.decodeUnknownSync(receiptJson)(JSON.stringify(receipt))).toEqual(receipt);
  });

  it.each([
    { ...accepted, sent: null },
    { ...receiptIdentity, outcome: "accepted" },
    { ...accepted, outcome: "unknown" },
    { ...accepted, outcome: "rejected" },
    { ...accepted, sent: { ...sent, threadId: "thread:replacement" } },
    { ...accepted, sent: { ...sent, messageId: null } },
    { ...accepted, sent: { ...sent, runId: null } },
    { ...accepted, version: "jones.remote-followup-receipt/v2" },
    { ...accepted, version: undefined },
    { ...accepted, environmentId: undefined },
    { ...accepted, threadId: undefined },
    { ...accepted, requestDigest: "" },
    { ...accepted, clientRequestId: "" },
    { ...accepted, namespace: "other-origin" },
    { ...accepted, sent: { ...sent, latestRunId: "run:newer" } },
    { ...receiptIdentity, outcome: "missing", sent: null },
    { ...receiptIdentity, outcome: "unknown", sent: null, namespace: "other-origin" },
  ])("rejects inconsistent or augmented receipt shape %# in native and peer JSON", (input) => {
    expect(() => decodeReceipt(input)).toThrow();
    expect(() => Schema.decodeUnknownSync(receiptJson)(JSON.stringify(input))).toThrow();
  });
});
