import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ExecutionEnvironmentDescriptor } from "./environment.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
} as const;

describe("ExecutionEnvironmentDescriptor", () => {
  const nativeBootstrapCreation = {
    submissionSchema: "t3.native-bootstrap-submission/v1",
    preparationSchema: "voice.t3-bootstrap-preparation/v1",
    observationSchema: "t3.native-creation-observation/v2",
    guardRequired: true,
  };

  it("keeps absent native bootstrap creation unsupported without inventing advertisement", () => {
    const decoded = decodeDescriptor(descriptor);
    expect(decoded.capabilities.nativeBootstrapCreation).toBeUndefined();
    expect(Object.hasOwn(decoded.capabilities, "nativeBootstrapCreation")).toBe(false);
    expect(Schema.encodeSync(ExecutionEnvironmentDescriptor)(decoded)).toEqual(descriptor);
    expect(() =>
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, nativeBootstrapCreation: null },
      }),
    ).toThrow();
  });

  it("round-trips the actual V2 observation advertisement while preserving V1 submission and preparation", () => {
    const wire = {
      ...descriptor,
      capabilities: { ...descriptor.capabilities, nativeBootstrapCreation },
    };
    const decoded = decodeDescriptor(wire);
    expect(decoded.capabilities.nativeBootstrapCreation).toEqual(nativeBootstrapCreation);
    expect(Schema.encodeSync(ExecutionEnvironmentDescriptor)(decoded)).toEqual(wire);
    const json = Schema.toCodecJson(ExecutionEnvironmentDescriptor);
    expect(Schema.encodeSync(json)(Schema.decodeUnknownSync(json)(wire))).toEqual(wire);
  });

  it("rejects mismatched observation versions, altered compatibility inputs and capability authority overrides", () => {
    for (const extra of [
      { observationSchema: "t3.native-creation-observation/v1" },
      { observationSchema: "t3.native-creation-observation/v3" },
      { submissionSchema: "t3.native-bootstrap-submission/v2" },
      { preparationSchema: "voice.t3-bootstrap-preparation/v2" },
      { guardRequired: false },
      { grantId: "caller-grant" },
      { actorSessionId: "caller" },
    ])
      expect(() =>
        decodeDescriptor({
          ...descriptor,
          capabilities: {
            ...descriptor.capabilities,
            nativeBootstrapCreation: { ...nativeBootstrapCreation, ...extra },
          },
        }),
      ).toThrow();
  });

  it("omits saved accounting on older servers and preserves explicit support", () => {
    expect(decodeDescriptor(descriptor).capabilities.savedTokenAccounting).toBeUndefined();
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, savedTokenAccounting: true },
      }).capabilities.savedTokenAccounting,
    ).toBe(true);
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
