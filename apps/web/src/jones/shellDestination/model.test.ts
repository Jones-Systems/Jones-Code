import { describe, expect, it } from "vite-plus/test";
import {
  hasLegacyDestinationMetadata,
  resolveShellDestination,
  shellCommandEligibility,
  type DestinationEnvironment,
} from "./model";

const destination: DestinationEnvironment = {
  environmentId: "exact-mini-id",
  label: "Jones’s Mini",
  serverLabel: "Mac Mini",
  phase: "connected",
  cwd: "/destination",
  canOperate: true,
};
const fence = { _tag: "Valid" as const, environmentId: destination.environmentId };

describe("shell destination resolution", () => {
  it.each([
    ["ready", "source", destination],
    ["self", destination.environmentId, destination],
    ["connecting", "source", { ...destination, phase: "connecting" }],
    ["connecting", "source", { ...destination, cwd: null }],
    ["offline", "source", { ...destination, phase: "offline" }],
    ["offline", "source", { ...destination, phase: "error" }],
    ["no-permission", "source", { ...destination, canOperate: false }],
  ] as const)("resolves %s", (status, source, environment) => {
    expect(resolveShellDestination(fence, source, [environment]).status).toBe(status);
  });
  it("never resolves a prefix, claimed label, or absent ID", () => {
    for (const environmentId of ["exact-mini", destination.label, "absent"]) {
      expect(
        resolveShellDestination({ ...fence, environmentId, label: destination.label }, "source", [
          destination,
        ]),
      ).toMatchObject({
        status: "unknown",
        label: "Unknown computer",
        claimedLabel: destination.label,
        cwd: null,
      });
    }
    expect(
      resolveShellDestination({ _tag: "Invalid", reason: "bad metadata" }, "source", [destination]),
    ).toMatchObject({ status: "invalid", reason: "bad metadata" });
  });
  it("normalizes label hints against catalog and server without using them as identity", () => {
    for (const label of ["  JONES'S   MINI ", "Ｍａｃ Mini", "mac mini"]) {
      expect(resolveShellDestination({ ...fence, label }, "source", [destination]).status).toBe(
        "ready",
      );
    }
    expect(
      resolveShellDestination({ ...fence, label: "Laptop" }, "source", [destination]),
    ).toMatchObject({ status: "label-mismatch", label: destination.label, claimedLabel: "Laptop" });
  });
});

describe("single command guards", () => {
  it("accepts one complete line and reserves the carriage return in the bound", () => {
    expect(shellCommandEligibility("  printf 'hello'\n", true)).toEqual({
      command: "printf 'hello'",
      reason: null,
    });
    expect(shellCommandEligibility(`${"x".repeat(65_534)}\n`, true).reason).toBeNull();
    expect(shellCommandEligibility(`${"x".repeat(65_535)}\n`, true).reason).not.toBeNull();
  });
  it.each([
    "",
    "echo ok",
    "\n",
    "echo one\necho two\n",
    "echo \\\n",
    "echo\tbad\n",
    "echo\r\n",
    "echo\u0000bad\n",
    "echo\u202ebad\n",
    "echo\u200bbad\n",
    "echo\u2028bad\n",
  ])("rejects unsafe/incomplete code %j", (code) => {
    expect(shellCommandEligibility(code, true).reason).not.toBeNull();
  });
  it("rejects streaming and unfinished blocks", () =>
    expect(shellCommandEligibility("echo ok\n", false).reason).not.toBeNull());
  it("requires the dedicated grammar for destination metadata on legacy shells", () => {
    expect(hasLegacyDestinationMetadata("bash", 'environment="mini"')).toBe(true);
    expect(hasLegacyDestinationMetadata("sh", "title=ok environment =mini")).toBe(true);
    expect(hasLegacyDestinationMetadata("bash", "title=environment")).toBe(false);
    expect(hasLegacyDestinationMetadata("python", "environment=mini")).toBe(false);
  });
});
