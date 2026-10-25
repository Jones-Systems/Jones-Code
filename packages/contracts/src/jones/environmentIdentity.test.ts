import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { ExecutionEnvironmentDescriptor } from "../environment.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
} as const;

describe("ExecutionEnvironmentDescriptor", () => {
  it("accepts older descriptors without claiming a Jones source", () => {
    expect(decodeDescriptor(descriptor).jonesSource).toBeUndefined();
    const jonesSource = {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    };
    expect(decodeDescriptor({ ...descriptor, jonesSource }).jonesSource).toEqual(jonesSource);
    expect(() =>
      decodeDescriptor({ ...descriptor, jonesSource: { ...jonesSource, sha: "unknown" } }),
    ).toThrow();
  });
});
