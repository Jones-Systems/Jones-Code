import { describe, expect, it, vi } from "vite-plus/test";
import {
  AuthSessionId,
  type EnvironmentSessionPrincipalShape,
  type T3PlacementResult,
  type ThreadRegistryThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import {
  makeVoiceReviewNativeReadPort,
  validateVoiceReviewNativePlacementResult,
} from "./native.ts";

const principal: EnvironmentSessionPrincipalShape = {
  sessionId: AuthSessionId.make("native-reader"),
  subject: "reader",
  method: "bearer-access-token",
  scopes: new Set(["orchestration:read"]),
};
const key = '["native-host","native-environment","native-thread"]';
const identity = { source_instance_id: "native-environment", native_thread_id: "native-thread" };
const projection = [{ thread_key: key, identity }];
const thread = (thread_key: string): ThreadRegistryThread => ({
  thread_key,
  registration: null,
  summary: null,
  activity: null,
  freshness: {},
  associations: [],
});
const result: T3PlacementResult = {
  page: {
    inventory_sha256: "a".repeat(64),
    context: {
      owner_id: "owner",
      principal_id: "principal",
      authorization_revision: 1,
      server_generation: 1,
      registry_version: 1,
    },
    items: [],
    next_cursor: null,
  },
  trustedEnvironments: [],
  readiness: "ready",
};

describe("voice native placement composition", () => {
  it("uses only exact native projection keys and ignores registry identity claims", () => {
    const port = makeVoiceReviewNativeReadPort({
      gateway: { readThreadPlacements: () => Effect.succeed(result) },
      readIdentities: () => projection,
    });
    const forged = {
      ...thread('["other-host","native-environment","native-thread"]'),
      registration: { thread_id: "native-thread", environment: "native-environment" },
    };
    expect(Array.from(port.identities([thread(key), forged]))).toEqual([[key, identity]]);
    expect(port.identities([thread("native-thread")]).size).toBe(0);
  });
  it("enforces incoming read scope before the qualified gateway", async () => {
    const read = vi.fn(() => Effect.succeed(result));
    const projectionRead = vi.fn(() => projection);
    const port = makeVoiceReviewNativeReadPort({
      gateway: { readThreadPlacements: read },
      readIdentities: projectionRead,
    });
    await expect(port.read({ ...principal, scopes: new Set() }, [identity])).rejects.toThrow();
    expect(read).not.toHaveBeenCalled();
    expect(projectionRead).not.toHaveBeenCalled();
    expect(await port.read(principal, [identity])).toEqual(result);
    expect(read).toHaveBeenCalledWith({ identities: [identity] });
  });
  it("rechecks the native projection and holds unknown or revoked identities without transport", async () => {
    const read = vi.fn(() => Effect.succeed(result));
    let current = projection;
    const port = makeVoiceReviewNativeReadPort({
      gateway: { readThreadPlacements: read },
      readIdentities: () => current,
    });
    const identities = Array.from(port.identities([thread(key)]).values());
    current = [];
    await expect(port.read(principal, identities)).rejects.toThrow();
    await expect(
      port.read(principal, [{ ...identity, source_instance_id: "other-environment" }]),
    ).rejects.toThrow();
    expect(read).not.toHaveBeenCalled();
  });
  it("rejects ambiguous native bindings rather than selecting by host, title or id similarity", () => {
    const port = makeVoiceReviewNativeReadPort({
      gateway: { readThreadPlacements: () => Effect.succeed(result) },
      readIdentities: () => [
        ...projection,
        { thread_key: '["another-host","native-environment","native-thread"]', identity },
      ],
    });
    expect(() => port.identities([thread(key)])).toThrow();
  });
});

describe("voice native placement trust", () => {
  it("requires current native authority and store generation for every displayed membership", () => {
    const now = Date.parse("2026-10-02T12:00:00Z");
    const placement = {
      membership_id: "membership",
      workstream_id: "workstream",
      native_reference_id: "reference",
      kind: "primary" as const,
      ...identity,
      attestation_version: 1,
      attested_at: "2026-10-02T11:00:00Z",
      expires_at: "2026-10-02T13:00:00Z",
      evidence_sha256: "a".repeat(64),
      source_binding_version: 1,
      authority_namespace: "native-authority",
      store_generation: 3,
    };
    const current = {
      ...result,
      page: { ...result.page, items: [placement] },
      trustedEnvironments: [
        {
          environmentId: identity.source_instance_id,
          authorityNamespace: "native-authority",
          storeGeneration: 3,
        },
      ],
    };
    expect(validateVoiceReviewNativePlacementResult(current, now)).toEqual(current);
    for (const changed of [
      { ...current, readiness: "trust-provider-required" as const },
      { ...current, trustedEnvironments: [] },
      {
        ...current,
        trustedEnvironments: [{ ...current.trustedEnvironments[0]!, authorityNamespace: "other" }],
      },
      {
        ...current,
        trustedEnvironments: [{ ...current.trustedEnvironments[0]!, storeGeneration: 4 }],
      },
      {
        ...current,
        page: { ...current.page, items: [{ ...placement, expires_at: "2026-10-02T12:00:00Z" }] },
      },
      {
        ...current,
        page: { ...current.page, items: [{ ...placement, attested_at: "2026-10-02T12:30:00Z" }] },
      },
    ])
      expect(() => validateVoiceReviewNativePlacementResult(changed, now)).toThrow();
  });
});
