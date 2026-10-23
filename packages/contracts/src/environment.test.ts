import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ExecutionEnvironmentCapabilities, ExecutionEnvironmentDescriptor } from "./environment.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
} as const;

describe("ExecutionEnvironmentDescriptor", () => {
  it("keeps workstream operations absent on old descriptors and decodes the advertised contract", () => {
    expect(decodeDescriptor(descriptor)).toEqual(descriptor);
    expect(decodeDescriptor(descriptor).capabilities.workstreamOperations).toBeUndefined();
    const workstreamOperations = { contract: "workstream-operations/1.0.0" };
    const advertised = {
      ...descriptor,
      capabilities: { ...descriptor.capabilities, workstreamOperations },
    };
    expect(decodeDescriptor(advertised)).toEqual(advertised);
    expect(() =>
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          workstreamOperations: { contract: "workstream-operations/2.0.0" },
        },
      }),
    ).toThrow();
  });

  it("retains default excess-property stripping for old and new capability decoders", () => {
    const { workstreamOperations: _operations, ...legacyFields } =
      ExecutionEnvironmentCapabilities.fields;
    const decodeLegacy = Schema.decodeUnknownSync(Schema.Struct(legacyFields));
    const advertised = {
      ...descriptor.capabilities,
      workstreamOperations: { contract: "workstream-operations/1.0.0", futureOption: true },
      futureCapability: true,
    };
    expect(decodeLegacy(advertised)).toEqual(descriptor.capabilities);
    expect(decodeDescriptor({ ...descriptor, capabilities: advertised }).capabilities).toEqual({
      ...descriptor.capabilities,
      workstreamOperations: { contract: "workstream-operations/1.0.0" },
    });
    expect(Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities)({})).toEqual({
      repositoryIdentity: false,
    });
  });

  it("round-trips old descriptors and accepts an optional build time", () => {
    expect(decodeDescriptor(descriptor)).toEqual(descriptor);
    const stamped = { ...descriptor, jonesBuildStartedAt: "2026-10-20T21:13:43Z" };
    expect(decodeDescriptor(stamped)).toEqual(stamped);
    expect(() =>
      decodeDescriptor({ ...descriptor, jonesBuildStartedAt: "2026-02-30T12:00:00Z" }),
    ).toThrow();
  });
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

  it("keeps settled cleanup optional alongside current worktree capabilities", () => {
    expect(decodeDescriptor(descriptor).capabilities.settledWorktreeCleanup).toBeUndefined();
    const capabilities = {
      settledWorktreeCleanup: true,
      storageCleanupRun: true,
      projectWorktreeCleanup: true,
      worktreesDirectory: true,
      threadAutoSettlementScope: true,
    };
    expect(decodeDescriptor({ ...descriptor, capabilities }).capabilities).toMatchObject(
      capabilities,
    );
  });

  it("decodes old, recognized and future manual installation descriptors", () => {
    expect(decodeDescriptor(descriptor).capabilities.serverInstallation).toBeUndefined();
    for (const installation of [{ kind: "npx" }, { kind: "npm-global", prefix: "/opt/node" }]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, serverInstallation: installation },
        }).capabilities.serverInstallation,
      ).toEqual(installation);
    }
    for (const installation of [{ kind: "future-manager" }, { kind: "npm-global" }]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, serverInstallation: installation },
        }).capabilities.serverInstallation,
      ).toBeUndefined();
    }
  });
  it("requires an advertised required-worktree bootstrap capability", () => {
    expect(decodeDescriptor(descriptor).capabilities.requiredWorktreeBootstrap).toBeUndefined();
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, requiredWorktreeBootstrap: true },
      }).capabilities.requiredWorktreeBootstrap,
    ).toBe(true);
  });

  it("treats a missing pull-request capability as unsupported under version skew", () => {
    expect(decodeDescriptor(descriptor).capabilities.pullRequests).toBeUndefined();
  });

  it("preserves an advertised pull-request capability", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, pullRequests: true },
      }).capabilities.pullRequests,
    ).toBe(true);
  });

  it("treats a missing attachment upload capability as unsupported", () => {
    expect(decodeDescriptor(descriptor).capabilities.attachmentUploads).toBeUndefined();
  });

  it("preserves an advertised attachment upload capability", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, attachmentUploads: true },
      }).capabilities.attachmentUploads,
    ).toBe(true);
  });

  it("preserves the server's generic attachment upload limit", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          fileAttachments: { maxUploadBytes: 50 * 1024 * 1024 },
        },
      }).capabilities.fileAttachments,
    ).toEqual({ maxUploadBytes: 50 * 1024 * 1024 });
  });

  it("treats missing server-resolved command context as unsupported", () => {
    expect(decodeDescriptor(descriptor).capabilities.serverResolvedCommandContext).toBeUndefined();
  });

  it("preserves advertised server-resolved command context", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          serverResolvedCommandContext: true,
        },
      }).capabilities.serverResolvedCommandContext,
    ).toBe(true);
  });
});
