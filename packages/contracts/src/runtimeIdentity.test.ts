import { assert, it } from "@effect/vitest";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ObservedRuntimeIdentity, RuntimeIdentityAttestation, RuntimeIdentityObservation } from "./runtimeIdentity.ts";

const observed = {
  backend: { status: "observed", value: "openai", sourceEvent: "codex.thread/open" },
  model: { status: "unknown" },
  account: { status: "unavailable", reason: "No provider event binds the account." },
  serviceTier: { status: "unknown" },
};
const requested = {
  providerInstanceId: "codex_work",
  providerDriver: "codex",
  model: "gpt-5.6-sol",
  serviceTier: "priority",
};

it("keeps requested routing separate from provider-attested identity", () => {
  const wire = { runtimeGeneration: "runtime-generation-1", requested, observed };
  const attestation = Schema.decodeUnknownSync(RuntimeIdentityAttestation)(wire);
  assert.deepEqual<unknown>(attestation.observed, observed);
  assert.strictEqual(attestation.requested.model, "gpt-5.6-sol");
  assert.strictEqual(attestation.runtimeGeneration, "runtime-generation-1");
  assert.deepEqual<unknown>(Schema.encodeSync(RuntimeIdentityAttestation)(attestation), wire);
});

it("preserves omitted generations and nullable requested service tiers", () => {
  const wire = { requested: { ...requested, serviceTier: null }, observed };
  const decoded = Schema.decodeUnknownSync(RuntimeIdentityAttestation)(wire);
  assert.strictEqual(decoded.runtimeGeneration, undefined);
  assert.strictEqual(decoded.requested.serviceTier, null);
  assert.deepEqual<unknown>(Schema.encodeSync(RuntimeIdentityAttestation)(decoded), wire);
  for (const runtimeGeneration of [1, null, "", "   "]) {
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(RuntimeIdentityAttestation)({ ...wire, runtimeGeneration })));
  }
});

it("requires source evidence for observed dimensions and reasons for unavailable dimensions", () => {
  const decode = Schema.decodeUnknownOption(RuntimeIdentityObservation);
  assert.isTrue(Option.isNone(decode({ status: "observed", value: "model-1" })));
  assert.isTrue(Option.isNone(decode({ status: "unavailable" })));
  assert.isTrue(Option.isNone(decode({ status: "qualified", value: "model-1" })));
  assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ObservedRuntimeIdentity)({ model: observed.model })));
});
