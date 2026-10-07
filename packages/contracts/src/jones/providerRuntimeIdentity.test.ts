import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  RuntimeIdentityAttestation,
  RuntimeIdentityObservation,
  ProviderRuntimeBinding,
} from "./providerRuntimeIdentity.ts";

const decodeIdentity = Schema.decodeUnknownSync(RuntimeIdentityAttestation);
const decodeObservation = Schema.decodeUnknownSync(RuntimeIdentityObservation);
const decodeBinding = Schema.decodeUnknownSync(ProviderRuntimeBinding);

describe("provider runtime identity", () => {
  it("keeps requested routing separate from observed runtime identity", () => {
    const identity = decodeIdentity({
      runtimeGeneration: "actual-process-1",
      evidenceRevision: 4,
      requested: {
        providerInstanceId: "codex",
        providerDriver: "codex",
        model: "requested",
        serviceTier: "priority",
      },
      observed: {
        backend: { status: "observed", value: "native-backend", sourceEvent: "codex.thread/open" },
        model: { status: "unknown" },
        account: { status: "unavailable", reason: "The native protocol does not bind an account." },
        serviceTier: { status: "unavailable", reason: "No native tier was reported." },
      },
    });
    expect(identity.requested.model).toBe("requested");
    expect(identity.observed.model).toEqual({ status: "unknown" });
    expect(identity.observed.account.status).toBe("unavailable");
    expect(identity.observed.serviceTier.status).toBe("unavailable");
  });

  it.each([
    { status: "observed", value: "", sourceEvent: "native" },
    { status: "observed", value: "native", sourceEvent: " " },
    { status: "unavailable", reason: " " },
  ])("rejects unusable native evidence: %j", (observation) => {
    expect(() => decodeObservation(observation)).toThrow();
  });

  it("requires a producer generation for an identity-bearing native binding", () => {
    const binding = {
      threadId: "app",
      providerThreadId: "provider-thread",
      providerSessionId: "logical-session",
      providerInstanceId: "codex",
      driver: "codex",
      nativeThreadId: "native-thread",
    };
    expect(() => decodeBinding(binding)).toThrow();
    expect(
      decodeBinding({ ...binding, runtimeGeneration: "actual-process" }).runtimeGeneration,
    ).toBe("actual-process");
  });
});
