import { describe, expect, it, vi } from "vite-plus/test";
import {
  AuthSessionId,
  type EnvironmentSessionPrincipalShape,
  type ThreadRegistryThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { makeVoiceReviewNativeReadPort } from "./native.ts";
import type { VoiceReviewPersonalMembership } from "./bridge.ts";

const principal: EnvironmentSessionPrincipalShape = {
  sessionId: AuthSessionId.make("personal-reader"),
  subject: "reader",
  method: "bearer-access-token",
  scopes: new Set(["orchestration:read"]),
};
const key = '["native-host","native-environment","native-thread"]';
const identity = { source_instance_id: "native-environment", native_thread_id: "native-thread" };
const projection = [{ thread_key: key, identity }];
const membership: VoiceReviewPersonalMembership = {
  workstream_ref: "native:legacy-workstream",
  workstream_id: "legacy-workstream",
  name: "Synthetic personal group",
  environment_id: "native-environment",
  thread_id: "native-thread",
  revision: 4,
};
const thread = (thread_key: string): ThreadRegistryThread => ({
  thread_key,
  registration: null,
  summary: null,
  activity: null,
  freshness: {},
  associations: [],
});

describe("voice personal membership read adapter", () => {
  it("uses exact local keys and ignores foreign registry identity claims", () => {
    const port = makeVoiceReviewNativeReadPort({
      readThread: () => Effect.succeed(membership),
      readIdentities: () => projection,
    });
    const forged = {
      ...thread('["other-host","native-environment","native-thread"]'),
      registration: { thread_id: "native-thread", environment: "native-environment" },
    };
    expect(Array.from(port.identities([thread(key), forged]))).toEqual([[key, identity]]);
    expect(port.identities([thread("native-thread")]).size).toBe(0);
  });
  it("requires ordinary read scope before touching local state", async () => {
    const read = vi.fn(() => Effect.succeed(membership));
    const projectionRead = vi.fn(() => projection);
    const port = makeVoiceReviewNativeReadPort({
      readThread: read,
      readIdentities: projectionRead,
    });
    await expect(port.read({ ...principal, scopes: new Set() }, [identity])).rejects.toThrow();
    expect(read).not.toHaveBeenCalled();
    expect(projectionRead).not.toHaveBeenCalled();
    expect(await port.read(principal, [identity])).toEqual([membership]);
    expect(read).toHaveBeenCalledExactlyOnceWith(identity);
  });
  it("refuses removed, foreign and duplicate identities before reading memberships", async () => {
    const read = vi.fn(() => Effect.succeed(membership));
    let current = projection;
    const port = makeVoiceReviewNativeReadPort({ readThread: read, readIdentities: () => current });
    const identities = Array.from(port.identities([thread(key)]).values());
    current = [];
    await expect(port.read(principal, identities)).rejects.toThrow();
    current = projection;
    await expect(
      port.read(principal, [{ ...identity, source_instance_id: "other" }]),
    ).rejects.toThrow();
    await expect(port.read(principal, [identity, identity])).rejects.toThrow();
    expect(read).not.toHaveBeenCalled();
  });
  it("rejects ambiguous deployment bindings", () => {
    const port = makeVoiceReviewNativeReadPort({
      readThread: () => Effect.succeed(membership),
      readIdentities: () => [
        ...projection,
        { thread_key: '["another-host","native-environment","native-thread"]', identity },
      ],
    });
    expect(() => port.identities([thread(key)])).toThrow();
  });
  it("omits a resolved unassigned thread without inventing a placement", async () => {
    const port = makeVoiceReviewNativeReadPort({
      readThread: () => Effect.succeed(null),
      readIdentities: () => projection,
    });
    expect(await port.read(principal, [identity])).toEqual([]);
  });
});
