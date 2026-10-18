import { describe, expect, it } from "vite-plus/test";

import {
  acceptGalleryMessage,
  designRequestNonce,
  isPairableGalleryOrigin,
  parseGalleryOrigin,
  type DesignRequestChannelState,
} from "./channel.ts";

const opener = { name: "gallery" };
const otherWindow = { name: "other" };
const nonceJ = "1".repeat(32);
const nonceG = "2".repeat(32);
const state: DesignRequestChannelState = {
  galleryOrigin: "http://100.113.248.16:4339",
  opener,
  nonceJ,
  nonceG: null,
};
const hello = {
  type: "jones.design-request.hello",
  protocol: "jones-design-request/1",
  nonceJ,
  nonceG,
  projectKey: "form2",
  galleryVersion: 1,
};
const reconcile = {
  type: "jones.design-request.reconcile",
  protocol: "jones-design-request/1",
  nonceJ,
  nonceG,
  requestNonce: "3".repeat(32),
  threadId: "thread-1",
  commandId: `gallery-req:p:${"a".repeat(32)}:a1`,
  messageId: `gallery-msg:p:${"a".repeat(32)}:a1`,
  packetId: "p",
  digest: `sha256:${"a".repeat(64)}`,
};
const event = (data: unknown, overrides: Partial<{ origin: string; source: unknown }> = {}) => ({
  origin: state.galleryOrigin,
  source: opener,
  data,
  ...overrides,
});

describe("gallery opener handshake", () => {
  it("accepts hello only from the exact opener, origin and Jones nonce", () => {
    expect(acceptGalleryMessage(state, event(hello))).toEqual(hello);
    expect(
      acceptGalleryMessage(state, event(hello, { origin: "http://100.113.248.16:4340" })),
    ).toBeNull();
    expect(acceptGalleryMessage(state, event(hello, { source: otherWindow }))).toBeNull();
    expect(acceptGalleryMessage(state, event({ ...hello, nonceJ: "9".repeat(32) }))).toBeNull();
    expect(
      acceptGalleryMessage({ ...state, opener: null }, event(hello, { source: null })),
    ).toBeNull();
    expect(acceptGalleryMessage(state, event({ ...hello, token: "Bearer x" }))).toBeNull();
    expect(acceptGalleryMessage(state, event("not an object"))).toBeNull();
  });

  it("requires the gallery nonce from hello on every later message", () => {
    expect(acceptGalleryMessage(state, event(reconcile))).toBeNull();
    const paired = { ...state, nonceG };
    expect(acceptGalleryMessage(paired, event(reconcile))).toEqual(reconcile);
    expect(
      acceptGalleryMessage(paired, event({ ...reconcile, nonceG: "4".repeat(32) })),
    ).toBeNull();
    // A second hello cannot replace the paired gallery nonce.
    expect(acceptGalleryMessage(paired, event({ ...hello, nonceG: "4".repeat(32) }))).toBeNull();
  });

  it("parses only exact http(s) origins and offers loopback and tailnet origins for pairing", () => {
    expect(parseGalleryOrigin("http://100.113.248.16:4339")).toBe("http://100.113.248.16:4339");
    expect(parseGalleryOrigin("http://100.113.248.16:4339/")).toBeNull();
    expect(parseGalleryOrigin("http://user@host.ts.net")).toBeNull();
    expect(parseGalleryOrigin("javascript:alert(1)")).toBeNull();
    expect(parseGalleryOrigin(null)).toBeNull();
    expect(isPairableGalleryOrigin("http://localhost:4339")).toBe(true);
    expect(isPairableGalleryOrigin("http://100.113.248.16:4339")).toBe(true);
    expect(isPairableGalleryOrigin("https://mac-mini.tail1234.ts.net")).toBe(true);
    expect(isPairableGalleryOrigin("http://100.128.0.1")).toBe(false);
    expect(isPairableGalleryOrigin("https://example.com")).toBe(false);
  });

  it("draws nonces from getRandomValues as 32 hex characters", () => {
    expect(designRequestNonce((bytes) => bytes.fill(171))).toBe("ab".repeat(16));
    expect(designRequestNonce()).toMatch(/^[a-f0-9]{32}$/);
  });
});
