import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  RemoteAdmissionGrant,
  RemoteAdmissionGrantDuration,
  RemoteAdmissionGrantUpdate,
  RemoteAdmissionGrantRevoke,
  RemoteAdmissionGrantRestore,
  RemoteAdmissionGrantRestoreResult,
  RemoteAdmissionAccessBasis,
  RemoteAdmissionBlockedReason,
} from "./remoteAdmissionReview.ts";

const grant = {
  grantId: "successor",
  sourceAdmissionId: "admission",
  binding: {
    receiverEnvironmentId: "receiver",
    sessionId: "session",
    originEnvironmentId: "origin",
    originThreadId: "origin-thread",
    destinationThreadId: "destination",
    operation: "t3_thread_send",
    runtimeMode: "full-access",
    interactionMode: "default",
  },
  state: "revoked",
  generation: 3,
  expiresAt: "2026-11-17T12:00:00Z",
  createdAt: "2026-11-16T12:00:00Z",
  updatedAt: "2026-11-16T13:00:00Z",
  revokedAt: "2026-11-16T13:00:00Z",
  predecessorGrantId: "predecessor",
  serverNow: "2026-11-16T14:00:00Z",
};

const decodeUnknownGrantDuration = Schema.decodeUnknownSync(RemoteAdmissionGrantDuration);
const decodeUnknownGrantUpdate = Schema.decodeUnknownSync(RemoteAdmissionGrantUpdate);
const decodeGrantRevoke = Schema.decodeSync(RemoteAdmissionGrantRevoke);
const decodeUnknownGrantRestore = Schema.decodeUnknownSync(RemoteAdmissionGrantRestore);
const decodeUnknownGrantRestoreResult = Schema.decodeUnknownSync(RemoteAdmissionGrantRestoreResult);
const decodeUnknownGrant = Schema.decodeUnknownSync(RemoteAdmissionGrant);
const decodeUnknownAccessBasis = Schema.decodeUnknownSync(RemoteAdmissionAccessBasis);
const decodeUnknownBlockedReason = Schema.decodeUnknownSync(RemoteAdmissionBlockedReason);

describe("remote admission access contracts", () => {
  it("supports exactly the accepted durations", () => {
    const decode = decodeUnknownGrantDuration;
    for (const duration of [3600, 86400, 172800, 259200, 604800])
      expect(decode(duration)).toBe(duration);
    for (const duration of [0, 60, 86399, 86400.5, "86400", 604801])
      expect(() => decode(duration)).toThrow();
  });

  it("requires exact generation on updates/revokes and a command key on restore", () => {
    const update = { grantId: "grant", expectedGeneration: 1, durationSeconds: 3600 };
    expect(decodeUnknownGrantUpdate(update)).toEqual(update);
    expect(
      decodeGrantRevoke({
        grantId: "grant",
        expectedGeneration: 1,
      }),
    ).toEqual({ grantId: "grant", expectedGeneration: 1 });
    for (const expectedGeneration of [undefined, 0, -1, 1.5])
      expect(() =>
        decodeUnknownGrantUpdate({ ...update, expectedGeneration }),
      ).toThrow();
    expect(() => decodeUnknownGrantRestore(update)).toThrow();
    expect(
      decodeUnknownGrantRestore({ ...update, commandKey: "command" })
        .commandKey,
    ).toBe("command");
  });

  it("can return the current revoked successor on an idempotent restore replay", () => {
    const result = { predecessorGrantId: "predecessor", commandKey: "command", successor: grant };
    expect(decodeUnknownGrantRestoreResult(result)).toEqual(result);
    expect(() =>
      decodeUnknownGrant({
        ...grant,
        binding: { ...grant.binding, sessionId: undefined },
      }),
    ).toThrow();
    expect(() =>
      decodeUnknownGrant({
        ...grant,
        binding: { ...grant.binding, operation: "t3_thread_send_attachments" },
      }),
    ).toThrow();
  });

  it("keeps lease generation and one-message permit revision distinct", () => {
    const decode = decodeUnknownAccessBasis;
    expect(decode({ kind: "lease", grantId: "grant", generation: 3 })).toEqual({
      kind: "lease",
      grantId: "grant",
      generation: 3,
    });
    const permit = { kind: "permit", bindingDigest: "b".repeat(64), scopeRevision: 4 };
    expect(decode(permit)).toEqual(permit);
    expect(() => decode({ kind: "permit", grantId: "grant", generation: 3 })).toThrow();
    expect(() => decode({ ...permit, scopeRevision: 0 })).toThrow();
    for (const reason of [
      "access_revoked",
      "access_expired",
      "session_not_bound",
      "mode_exceeds",
      "operation_not_covered",
      "identity_unresolved",
    ])
      expect(decodeUnknownBlockedReason(reason)).toBe(reason);
  });
});
