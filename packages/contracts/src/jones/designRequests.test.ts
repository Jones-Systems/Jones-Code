import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  DESIGN_REQUEST_HELD_REASONS,
  DESIGN_REQUEST_PROTOCOL,
  DesignRequestBindings,
  DesignRequestGalleryMessage,
  DesignRequestReceipt,
  DesignRequestRoute,
} from "./designRequests.ts";

const decode = Schema.decodeUnknownSync(DesignRequestGalleryMessage, { onExcessProperty: "error" });
const nonceJ = "a".repeat(32);
const nonceG = "b".repeat(32);
const hello = {
  type: "jones.design-request.hello",
  protocol: DESIGN_REQUEST_PROTOCOL,
  nonceJ,
  nonceG,
  projectKey: "http://100.113.248.16:4339/",
  galleryVersion: 1,
};

describe("jones-design-request/1 messages", () => {
  it("accepts the contract shapes and rejects unknown members, protocols and weak nonces", () => {
    expect(decode(hello)).toEqual(hello);
    expect(() => decode({ ...hello, extra: true })).toThrow();
    expect(() => decode({ ...hello, protocol: "jones-design-request/2" })).toThrow();
    expect(() => decode({ ...hello, nonceG: "abc" })).toThrow();
    expect(() => decode({ ...hello, galleryVersion: 2 })).toThrow();
    const submit = {
      type: "jones.design-request.submit",
      protocol: DESIGN_REQUEST_PROTOCOL,
      nonceJ,
      nonceG,
      projectKey: hello.projectKey,
      requestNonce: "c".repeat(32),
      packet: { any: "packet" },
      attachmentData: [{ id: "photo-1", dataUrl: "data:image/png;base64,AA==" }],
      expectedRouteToken: "d".repeat(32),
      attempt: 1,
    };
    expect(decode(submit)).toEqual(submit);
    // The gallery owns attempt numbering (1..20) and always sends attachmentData, empty without images.
    expect(decode({ ...submit, attachmentData: [], attempt: 20 })).toMatchObject({ attempt: 20 });
    for (const attempt of [0, 21, 1.5, "1"]) expect(() => decode({ ...submit, attempt })).toThrow();
    const { attempt: _attempt, ...withoutAttempt } = submit;
    expect(() => decode(withoutAttempt)).toThrow();
    const { attachmentData: _data, ...withoutData } = submit;
    expect(() => decode(withoutData)).toThrow();
    const reconcile = {
      type: "jones.design-request.reconcile",
      protocol: DESIGN_REQUEST_PROTOCOL,
      nonceJ,
      nonceG,
      requestNonce: "c".repeat(32),
      threadId: "thread-1",
      commandId: `gallery-req:packet-1:${"e".repeat(32)}:a1`,
      messageId: `gallery-msg:packet-1:${"e".repeat(32)}:a1`,
      packetId: "packet-1",
      digest: `sha256:${"e".repeat(64)}`,
    };
    expect(decode(reconcile)).toEqual(reconcile);
    const { digest: _digest, ...withoutDigest } = reconcile;
    expect(() => decode(withoutDigest)).toThrow();
    expect(() => decode({ ...reconcile, digest: "sha256:e" })).toThrow();
    expect(() =>
      decode({ ...submit, attachmentData: [{ id: "x", dataUrl: "y", bytes: 1 }] }),
    ).toThrow();
    // A bearer credential or any other extra envelope member is refused.
    expect(() => decode({ ...submit, authorization: "Bearer x" })).toThrow();
  });

  it("keeps route and receipt states closed", () => {
    const route = Schema.decodeUnknownSync(DesignRequestRoute, { onExcessProperty: "error" });
    expect(
      route({
        type: "jones.design-request.route",
        protocol: DESIGN_REQUEST_PROTOCOL,
        nonceJ,
        nonceG,
        state: "held",
        reason: "multiple-primary",
      }).reason,
    ).toBe("multiple-primary");
    expect(() =>
      route({
        type: "jones.design-request.route",
        protocol: DESIGN_REQUEST_PROTOCOL,
        nonceJ,
        nonceG,
        state: "held",
        reason: "guessed-thread",
      }),
    ).toThrow();
    const receipt = Schema.decodeUnknownSync(DesignRequestReceipt, { onExcessProperty: "error" });
    const accepted = {
      type: "jones.design-request.receipt",
      protocol: DESIGN_REQUEST_PROTOCOL,
      nonceJ,
      nonceG,
      requestNonce: "c".repeat(32),
      packetId: "packet-1",
      digest: `sha256:${"e".repeat(64)}`,
      status: "accepted",
      destination: {
        jonesOrigin: "http://jones.example.ts.net",
        workstreamId: "ws-1",
        threadId: "thread-1",
        commandId: "gallery-req:packet-1:eeee:a1",
        messageId: "gallery-msg:packet-1:eeee:a1",
      },
      delivery: "queued",
    };
    expect(receipt(accepted)).toEqual(accepted);
    expect(() => receipt({ ...accepted, status: "sent" })).toThrow();
    // Every receipt, including a reconcile after a fresh page load, echoes the full digest.
    const { digest: _digest, ...withoutDigest } = accepted;
    expect(() => receipt(withoutDigest)).toThrow();
    expect(() => receipt({ ...accepted, packetId: "unknown packet" })).toThrow();
    expect(new Set(DESIGN_REQUEST_HELD_REASONS).size).toBe(DESIGN_REQUEST_HELD_REASONS.length);
  });

  it("stores bindings strictly", () => {
    const bindings = Schema.decodeUnknownSync(DesignRequestBindings, { onExcessProperty: "error" });
    const binding = {
      bindingId: "f".repeat(32),
      galleryOrigin: "http://100.113.248.16:4339",
      projectKey: "form2",
      workstreamId: "ws-1",
    };
    expect(bindings({ version: 1, bindings: [binding] }).bindings).toHaveLength(1);
    expect(() => bindings({ version: 1, bindings: [{ ...binding, threadId: "t" }] })).toThrow();
  });
});
