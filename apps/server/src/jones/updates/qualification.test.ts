import { expect, it } from "vite-plus/test";
import { isJonesRuntime } from "./qualification.ts";

const source = {
  repository: "Jones-Systems/Jones-Code",
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
const input = {
  version: "0.0.44-preview.20261002.36963972634",
  buildMetadata: { jonesSource: source },
  qualifiedRuntimeReceipt: false,
};

it("keeps stable Release and unstamped upstream previews on their Release channel", () => {
  expect(isJonesRuntime({ ...input, version: "0.0.44" })).toBe(false);
  expect(isJonesRuntime({ ...input, buildMetadata: {} })).toBe(false);
});
it("recognizes qualified Jones build stamps before native launcher bootstrap", () => {
  expect(isJonesRuntime(input)).toBe(true);
  expect(isJonesRuntime({ ...input, version: `${input.version}.2` })).toBe(true);
});
it("recognizes a verified installed baseline receipt without a new producer stamp", () => {
  expect(isJonesRuntime({ ...input, buildMetadata: {}, qualifiedRuntimeReceipt: true })).toBe(true);
});
it.each([
  { ...source, repository: "t3tools/t3code" },
  { ...source, sha: "unknown" },
  { ...source, tree: "unknown" },
])("rejects an unqualified stamp: %j", (jonesSource) => {
  expect(isJonesRuntime({ ...input, buildMetadata: { jonesSource } })).toBe(false);
});
