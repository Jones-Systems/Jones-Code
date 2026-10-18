import { expect, it } from "vite-plus/test";
import {
  decodeDesktopNativeOwnerMessage,
  sameDesktopNativeOwnerBinding,
} from "./desktopNativeOwner.ts";

const binding = {
  home: "/home",
  databasePath: "/home/userdata/statev2.sqlite",
  profile: "/profile",
  environmentId: "environment",
  version: "version",
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  manifestSha256: "c".repeat(64),
};
const request = { protocol: 1, sequence: 1, nonce: "d".repeat(64), childPid: 42, binding };
it("decodes the bounded challenge and preserves its exact admitted paths", () => {
  expect(decodeDesktopNativeOwnerMessage(JSON.stringify(request), false)).toEqual(request);
  expect(sameDesktopNativeOwnerBinding(binding, { ...binding, databasePath: "/other" })).toBe(
    false,
  );
});
it.each([
  { ...request, extra: true },
  { ...request, sequence: 0 },
  { ...request, nonce: "short" },
  { ...request, binding: { ...binding, profile: "" } },
])("refuses malformed owner messages", (value) => {
  expect(() => decodeDesktopNativeOwnerMessage(JSON.stringify(value), false)).toThrow();
});
it("refuses oversized input and a response without a run identity", () => {
  expect(() => decodeDesktopNativeOwnerMessage(" ".repeat(8193), false)).toThrow();
  expect(() => decodeDesktopNativeOwnerMessage(JSON.stringify(request), true)).toThrow();
});
