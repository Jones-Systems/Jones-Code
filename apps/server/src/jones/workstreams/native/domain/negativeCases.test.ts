import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Read only the frozen conformance bytes in this test checkout.
import * as NodeFS from "node:fs";
const negativesRaw = NodeFS.readFileSync(
  new URL(
    "../../../../../../../packages/contracts/src/workstreams-fixtures/negative-cases.json.fixture",
    import.meta.url,
  ),
  "utf8",
);
import { validateCommand, validateExternalValue } from "./validation.ts";

const strict = { onExcessProperty: "error" as const };
function isStrict(schema: Schema.ConstraintDecoder<unknown, never>): (value: unknown) => boolean {
  const decode = Schema.decodeUnknownSync(schema);
  return (value) => {
    try {
      decode(value, strict);
      return true;
    } catch {
      return false;
    }
  };
}
const validators = new Map<string, (value: unknown) => boolean>([
  ["Command", isStrict(Wire.WorkstreamCommand)],
  ["Workstream", isStrict(Wire.Workstream)],
  ["NativeIdentity", isStrict(Wire.WorkstreamNativeIdentity)],
  ["PrLocator", isStrict(Wire.WorkstreamPrLocator)],
  ["MembershipEpisode", isStrict(Wire.MembershipEpisode)],
  ["DeclarationRevision", isStrict(Wire.DeclarationRevision)],
  ["LifecycleDeclaration", isStrict(Wire.WorkstreamLifecycleDeclaration)],
  ["ProgressCondition", isStrict(Wire.WorkstreamProgress)],
  ["DeliveryState", isStrict(Wire.WorkstreamDelivery)],
  ["FreshnessState", isStrict(Wire.WorkstreamFreshness)],
  ["RegistrationVerification", isStrict(Wire.WorkstreamRegistrationVerification)],
  ["PrObservation", isStrict(Wire.WorkstreamPrObservation)],
  ["CommittedReceipt", isStrict(Wire.WorkstreamCommittedReceipt)],
  ["PendingReceipt", isStrict(Wire.WorkstreamPendingReceipt)],
  ["TerminalReceipt", isStrict(Wire.WorkstreamTerminalReceipt)],
  ["Error", isStrict(Wire.WorkstreamError)],
  ["WorkstreamPage", isStrict(Wire.WorkstreamPage)],
]);
const corpus = JSON.parse(negativesRaw) as {
  cases: readonly { name: string; schema: string; value: unknown }[];
};
describe("all frozen v1 negative cases", () => {
  it("accounts for the complete corpus without filtering unknown schemas", () =>
    expect(corpus.cases).toHaveLength(27));
  it.each(corpus.cases)("$name", (fixture) => {
    const name = fixture.schema.slice(fixture.schema.lastIndexOf("/") + 1);
    const validator = validators.get(name);
    if (!validator) throw new Error("Unmapped negative schema: " + name);
    expect(validator(fixture.value)).toBe(false);
    if (name === "Command")
      expect(() => validateCommand(fixture.value as Wire.WorkstreamCommand)).toThrow(
        "invalid_request",
      );
  });
  it("validates external calendar timestamps as well as their wire shape", () => {
    expect(() =>
      validateExternalValue("observation", {
        native_reference_id: "reference-one",
        observation_version: 1,
        attempted_at: "2026-02-30T00:00:00Z",
        outcome: "unavailable",
        retry_after_seconds: null,
        last_success: null,
        command_id: "command:0000000000000001",
      }),
    ).toThrow("invalid_request");
  });
});
