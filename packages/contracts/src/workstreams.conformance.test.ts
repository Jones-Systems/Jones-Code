import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous conformance fixture hashing preserves canonical wire bytes.
import * as NodeCrypto from "node:crypto";

import countsRaw from "./workstreams-fixtures/registry-counts.conformance.json.fixture?raw";
import conformanceRaw from "./workstreams-fixtures/conformance.json.fixture?raw";
import negativesRaw from "./workstreams-fixtures/negative-cases.json.fixture?raw";
import semanticRaw from "./workstreams-fixtures/semantic-cases.json.fixture?raw";
import {
  Workstream,
  WorkstreamCapabilities,
  WorkstreamCountsCapabilities,
  WorkstreamRegistryCounts,
  WORKSTREAM_COUNTS_MANIFEST_SHA256,
  WorkstreamCommand,
  WorkstreamDelivery,
  WorkstreamFreshness,
  WorkstreamLifecycleDeclaration,
  WorkstreamNativeIdentity,
  NativeReference,
  WorkstreamEdge,
  WorkstreamAuditEvent,
  WorkstreamOwnerGrant,
  WorkstreamError,
  WorkstreamSession,
  WorkstreamCommittedReceipt,
  WorkstreamPendingReceipt,
  WorkstreamRejectedReceipt,
  WorkstreamTerminalReceipt,
  WorkstreamProgress,
  WorkstreamPrLocator,
  WorkstreamRegistrationVerification,
  DeclarationRevision,
  MembershipEpisode,
  WorkstreamDeclarationPage,
  WorkstreamDetail,
  WorkstreamEdgePage,
  WorkstreamHistoryPage,
  WorkstreamMembershipPage,
  WorkstreamPage,
  WorkstreamPrObservation,
  WorkstreamReceipt,
  WorkstreamReferenceDetail,
  WorkstreamReferencePage,
} from "./workstreams.ts";

const options = { onExcessProperty: "error" as const };
const conformance = JSON.parse(conformanceRaw) as FixtureCorpus;
const negatives = JSON.parse(negativesRaw) as FixtureCorpus;
const semantic = JSON.parse(semanticRaw) as {
  readonly cases: ReadonlyArray<{
    readonly name: string;
    readonly when?: { readonly timestamp?: string };
  }>;
};
interface FixtureCorpus {
  readonly cases: ReadonlyArray<{
    readonly name: string;
    readonly schema: string;
    readonly value: unknown;
  }>;
}
const compiledDecodeUnknownSync1 = Schema.decodeUnknownSync(WorkstreamCommand);
const compiledDecodeUnknownSync2 = Schema.decodeUnknownSync(Workstream);
const compiledDecodeUnknownSync3 = Schema.decodeUnknownSync(NativeReference);
const compiledDecodeUnknownSync4 = Schema.decodeUnknownSync(WorkstreamNativeIdentity);
const compiledDecodeUnknownSync5 = Schema.decodeUnknownSync(WorkstreamPrLocator);
const compiledDecodeUnknownSync6 = Schema.decodeUnknownSync(MembershipEpisode);
const compiledDecodeUnknownSync7 = Schema.decodeUnknownSync(DeclarationRevision);
const compiledDecodeUnknownSync8 = Schema.decodeUnknownSync(WorkstreamEdge);
const compiledDecodeUnknownSync9 = Schema.decodeUnknownSync(WorkstreamLifecycleDeclaration);
const compiledDecodeUnknownSync10 = Schema.decodeUnknownSync(WorkstreamProgress);
const compiledDecodeUnknownSync11 = Schema.decodeUnknownSync(WorkstreamDelivery);
const compiledDecodeUnknownSync12 = Schema.decodeUnknownSync(WorkstreamFreshness);
const compiledDecodeUnknownSync13 = Schema.decodeUnknownSync(WorkstreamRegistrationVerification);
const compiledDecodeUnknownSync14 = Schema.decodeUnknownSync(WorkstreamPrObservation);
const compiledDecodeUnknownSync15 = Schema.decodeUnknownSync(WorkstreamReceipt);
const compiledDecodeUnknownSync16 = Schema.decodeUnknownSync(WorkstreamCommittedReceipt);
const compiledDecodeUnknownSync17 = Schema.decodeUnknownSync(WorkstreamPendingReceipt);
const compiledDecodeUnknownSync18 = Schema.decodeUnknownSync(WorkstreamRejectedReceipt);
const compiledDecodeUnknownSync19 = Schema.decodeUnknownSync(WorkstreamTerminalReceipt);
const compiledDecodeUnknownSync20 = Schema.decodeUnknownSync(WorkstreamAuditEvent);
const compiledDecodeUnknownSync21 = Schema.decodeUnknownSync(WorkstreamPage);
const compiledDecodeUnknownSync22 = Schema.decodeUnknownSync(WorkstreamReferencePage);
const compiledDecodeUnknownSync23 = Schema.decodeUnknownSync(WorkstreamMembershipPage);
const compiledDecodeUnknownSync24 = Schema.decodeUnknownSync(WorkstreamDeclarationPage);
const compiledDecodeUnknownSync25 = Schema.decodeUnknownSync(WorkstreamEdgePage);
const compiledDecodeUnknownSync26 = Schema.decodeUnknownSync(WorkstreamHistoryPage);
const compiledDecodeUnknownSync27 = Schema.decodeUnknownSync(WorkstreamDetail);
const compiledDecodeUnknownSync28 = Schema.decodeUnknownSync(WorkstreamReferenceDetail);
const compiledDecodeUnknownSync29 = Schema.decodeUnknownSync(WorkstreamCapabilities);
const compiledDecodeUnknownSync30 = Schema.decodeUnknownSync(WorkstreamOwnerGrant);
const compiledDecodeUnknownSync31 = Schema.decodeUnknownSync(WorkstreamError);
const compiledDecodeUnknownSync32 = Schema.decodeUnknownSync(WorkstreamSession);

const supported = new Map<string, (value: unknown) => unknown>([
  ["Command", (value) => compiledDecodeUnknownSync1(value, options)],
  ["Workstream", (value) => compiledDecodeUnknownSync2(value, options)],
  ["NativeReference", (value) => compiledDecodeUnknownSync3(value, options)],
  ["NativeIdentity", (value) => compiledDecodeUnknownSync4(value, options)],
  ["PrLocator", (value) => compiledDecodeUnknownSync5(value, options)],
  ["MembershipEpisode", (value) => compiledDecodeUnknownSync6(value, options)],
  ["DeclarationRevision", (value) => compiledDecodeUnknownSync7(value, options)],
  ["Edge", (value) => compiledDecodeUnknownSync8(value, options)],
  ["LifecycleDeclaration", (value) => compiledDecodeUnknownSync9(value, options)],
  ["ProgressCondition", (value) => compiledDecodeUnknownSync10(value, options)],
  ["DeliveryState", (value) => compiledDecodeUnknownSync11(value, options)],
  ["FreshnessState", (value) => compiledDecodeUnknownSync12(value, options)],
  ["RegistrationVerification", (value) => compiledDecodeUnknownSync13(value, options)],
  ["PrObservation", (value) => compiledDecodeUnknownSync14(value, options)],
  ["Receipt", (value) => compiledDecodeUnknownSync15(value, options)],
  ["CommittedReceipt", (value) => compiledDecodeUnknownSync16(value, options)],
  ["PendingReceipt", (value) => compiledDecodeUnknownSync17(value, options)],
  ["RejectedReceipt", (value) => compiledDecodeUnknownSync18(value, options)],
  ["TerminalReceipt", (value) => compiledDecodeUnknownSync19(value, options)],
  ["AuditEvent", (value) => compiledDecodeUnknownSync20(value, options)],
  ["WorkstreamPage", (value) => compiledDecodeUnknownSync21(value, options)],
  ["ReferencePage", (value) => compiledDecodeUnknownSync22(value, options)],
  ["MembershipPage", (value) => compiledDecodeUnknownSync23(value, options)],
  ["DeclarationPage", (value) => compiledDecodeUnknownSync24(value, options)],
  ["EdgePage", (value) => compiledDecodeUnknownSync25(value, options)],
  ["HistoryPage", (value) => compiledDecodeUnknownSync26(value, options)],
  ["WorkstreamDetail", (value) => compiledDecodeUnknownSync27(value, options)],
  ["ReferenceDetail", (value) => compiledDecodeUnknownSync28(value, options)],
  ["Capabilities", (value) => compiledDecodeUnknownSync29(value, options)],
  ["OwnerGrant", (value) => compiledDecodeUnknownSync30(value, options)],
  ["Error", (value) => compiledDecodeUnknownSync31(value, options)],
  ["BrowserSession", (value) => compiledDecodeUnknownSync32(value, options)],
]);

const schemaName = (reference: string) => reference.slice(reference.lastIndexOf("/") + 1);
const fixtureFor = (name: string): unknown => {
  const fixture = conformance.cases.find((candidate) => schemaName(candidate.schema) === name);
  if (fixture === undefined) throw new Error(`Missing ${name} conformance fixture`);
  return structuredClone(fixture.value);
};
const commandFor = (operation: string): Record<string, any> => {
  const fixture = conformance.cases.find(
    (candidate) =>
      schemaName(candidate.schema) === "Command" &&
      (candidate.value as { action?: { operation?: string } }).action?.operation === operation,
  );
  if (fixture === undefined) throw new Error(`Missing ${operation} command fixture`);
  return structuredClone(fixture.value) as Record<string, any>;
};

const compiledDecodeSync33 = Schema.decodeSync(WorkstreamRegistrationVerification);

describe("frozen accepted Workstream v1 fixtures", () => {
  it("preserves all immutable source bytes", () => {
    const sha256 = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
    expect(sha256(conformanceRaw)).toBe(
      "a907dfa46f18a1930d0bd176571743d4932ae6dd13fc96421d5dec8e63bd6aa1",
    );
    expect(sha256(negativesRaw)).toBe(
      "d4893333b797c53249f37cd9cc04c605ca54eeed542dcab0c62f6a6c91093d50",
    );
    expect(sha256(semanticRaw)).toBe(
      "1d939c25a2f9b9aca0191b390ba8739fcd8f6d2e40bb0f1e6ae389c2eb05a6fa",
    );
  });

  it("accepts every fixture for a T3-supported DTO decoder", () => {
    const exercised = new Set<string>();
    for (const fixture of conformance.cases) {
      const name = schemaName(fixture.schema);
      const decode = supported.get(name);
      if (decode === undefined) continue;
      expect(() => decode(fixture.value), fixture.name).not.toThrow();
      exercised.add(name);
    }
    expect(exercised).toEqual(
      new Set(conformance.cases.map((fixture) => schemaName(fixture.schema))),
    );
  });

  it("rejects every relevant negative fixture", () => {
    let exercised = 0;
    for (const fixture of negatives.cases) {
      const decode = supported.get(schemaName(fixture.schema));
      if (decode === undefined) continue;
      expect(() => decode(fixture.value), fixture.name).toThrow();
      exercised += 1;
    }
    expect(exercised).toBe(negatives.cases.length);
  });

  it("rejects out-of-contract receipt effects and cardinality", () => {
    const receipt = fixtureFor("CommittedReceipt") as Record<string, any>;
    const mutations = [
      (value: Record<string, any>) =>
        value.effects.membership_ids.push("membership-two", "membership-three"),
      (value: Record<string, any>) =>
        value.effects.workstream_versions.push(
          { workstream_id: "ws-two", version: 1 },
          { workstream_id: "ws-three", version: 1 },
        ),
      (value: Record<string, any>) => (value.effects.workstream_versions[0].version = 0),
      (value: Record<string, any>) => (value.effects.declaration_revision = 0),
      (value: Record<string, any>) => (value.effects.coordination_disposition.disposition = "done"),
      (value: Record<string, any>) =>
        (value.effects.native_settlement = {
          native_reference_id: "ref-t3-thread",
          native_action: "archive",
          outcome: "committed",
        }),
      (value: Record<string, any>) =>
        (value.effects.registration = {
          state: "quarantined",
          attestation_version: 1,
          attested_at: null,
          expires_at: null,
          evidence: null,
        }),
    ];
    for (const mutate of mutations) {
      const candidate = structuredClone(receipt);
      mutate(candidate);
      expect(() => compiledDecodeUnknownSync16(candidate, options)).toThrow();
    }
  });

  it("rejects malformed receipt envelopes, errors, observations, and audit history", () => {
    const committed = fixtureFor("CommittedReceipt") as Record<string, any>;
    for (const mutate of [
      (value: Record<string, any>) => (value.request_sha256 = "not-a-digest"),
      (value: Record<string, any>) => (value.server_generation = 0),
      (value: Record<string, any>) => (value.accepted_at = "2026-09-12"),
      (value: Record<string, any>) => (value.operation = "settle_everything"),
    ]) {
      const candidate = structuredClone(committed);
      mutate(candidate);
      expect(() => compiledDecodeUnknownSync16(candidate, options)).toThrow();
    }

    const pending = fixtureFor("PendingReceipt") as Record<string, any>;
    pending.retry_after_seconds = 3601;
    expect(() => compiledDecodeUnknownSync17(pending, options)).toThrow();

    const error = fixtureFor("Error") as Record<string, any>;
    error.status = 500;
    expect(() => compiledDecodeUnknownSync31(error, options)).toThrow();

    const observation = fixtureFor("PrObservation") as Record<string, any>;
    observation.last_success.observed_at = "yesterday";
    expect(() => compiledDecodeUnknownSync14(observation, options)).toThrow();

    const audit = fixtureFor("AuditEvent") as Record<string, any>;
    for (const mutate of [
      (value: Record<string, any>) => (value.command_id = "short"),
      (value: Record<string, any>) => (value.occurred_at = "yesterday"),
      (value: Record<string, any>) => (value.workstream_versions[0].version = 0),
      (value: Record<string, any>) =>
        value.membership_ids.push("membership-two", "membership-three"),
      (value: Record<string, any>) => (value.declaration_revision = 0),
    ]) {
      const candidate = structuredClone(audit);
      mutate(candidate);
      expect(() => compiledDecodeUnknownSync20(candidate, options)).toThrow();
    }
  });

  it("enforces registration state conditionals in both directions", () => {
    const quarantined = {
      state: "quarantined" as const,
      attestation_version: 0,
      attested_at: null,
      expires_at: null,
      evidence: null,
    };
    expect(() => compiledDecodeSync33(quarantined, options)).not.toThrow();
    expect(() =>
      compiledDecodeSync33({ ...quarantined, attestation_version: 1 }, options),
    ).toThrow();
    expect(() => compiledDecodeSync33({ ...quarantined, state: "attested" }, options)).toThrow();
  });

  it("enforces conditional explanation fields and positive settlement fencing", () => {
    const disposition = commandFor("set_coordination_disposition");
    disposition.action.other_disposition = "unexpected explanation";
    expect(() => compiledDecodeUnknownSync1(disposition, options)).toThrow();

    const otherDisposition = commandFor("set_coordination_disposition");
    otherDisposition.action.disposition = "other";
    otherDisposition.action.other_disposition = null;
    expect(() => compiledDecodeUnknownSync1(otherDisposition, options)).toThrow();

    const settlement = commandFor("request_native_t3_settlement");
    settlement.action.expected_attestation_version = 0;
    expect(() => compiledDecodeUnknownSync1(settlement, options)).toThrow();

    const membership = fixtureFor("MembershipEpisode") as Record<string, any>;
    membership.closed = {
      at: "2026-09-12T00:00:01.000Z",
      actor: { principal_id: "principal-synthetic" },
      command_id: "synthetic-cmd-close-membership",
      registry_version: 2,
      reason: "removed",
      other_reason: "unexpected explanation",
    };
    expect(() => compiledDecodeUnknownSync6(membership, options)).toThrow();
  });

  it("accepts reattaching a primary membership through the shared attach shape", () => {
    const command = commandFor("attach_primary");
    command.action.operation = "reattach_primary";
    expect(() => compiledDecodeUnknownSync1(command, options)).not.toThrow();
  });

  it("rejects impossible UTC calendar and clock timestamps from the frozen semantics", () => {
    const semanticTimestamp = semantic.cases.find(
      (candidate) => candidate.name === "schema-valid invalid calendar timestamp rejected",
    )?.when?.timestamp;
    expect(semanticTimestamp).toBe("2026-02-30T00:00:00Z");

    const workstream = fixtureFor("Workstream") as Record<string, any>;
    for (const invalidTimestamp of [
      semanticTimestamp,
      "2026-09-12T24:00:00Z",
      "2026-09-12T00:60:00Z",
      "2026-09-12T00:00:60Z",
    ]) {
      const candidate = structuredClone(workstream);
      candidate.updated_at = invalidTimestamp;
      expect(() => compiledDecodeUnknownSync2(candidate, options)).toThrow();
    }
  });

  it("requires a positive declaration registry version", () => {
    const declaration = fixtureFor("DeclarationRevision") as Record<string, any>;
    declaration.registry_version = 0;
    expect(() => compiledDecodeUnknownSync7(declaration, options)).toThrow();
  });

  it("caps every decoded collection page at 100 items", () => {
    const pages = [
      [WorkstreamPage, fixtureFor("WorkstreamPage")],
      [WorkstreamReferencePage, fixtureFor("ReferencePage")],
      [WorkstreamMembershipPage, fixtureFor("MembershipPage")],
      [WorkstreamDeclarationPage, fixtureFor("DeclarationPage")],
      [WorkstreamEdgePage, fixtureFor("EdgePage")],
    ] as const;
    for (const [schema, source] of pages) {
      const page = source as Record<string, any>;
      page.items = Array.from({ length: 101 }, () => structuredClone(page.items[0]));
      expect(() => Schema.decodeUnknownSync(schema)(page, options)).toThrow();
    }
    const history = fixtureFor("HistoryPage") as Record<string, any>;
    history.items = Array.from({ length: 101 }, () => fixtureFor("AuditEvent"));
    expect(() => compiledDecodeUnknownSync26(history, options)).toThrow();
  });
});

const compiledDecodeUnknownSync34 = Schema.decodeUnknownSync(WorkstreamRegistryCounts);
const compiledDecodeUnknownSync35 = Schema.decodeUnknownSync(WorkstreamCountsCapabilities);

describe("independently negotiated registry counts", () => {
  const fixture = JSON.parse(countsRaw) as {
    manifest_sha256: string;
    conformance: FixtureCorpus & { source: { commit: string; sha256: string } };
    negative: FixtureCorpus & { source: { commit: string; sha256: string } };
  };
  it("pins the provider revision and exact source corpus hashes", () => {
    expect(fixture.manifest_sha256).toBe(WORKSTREAM_COUNTS_MANIFEST_SHA256);
    expect(fixture.conformance.source).toMatchObject({
      commit: "675e21251773325ec4d20b5de743cc183086278a",
      sha256: "b28c57077fde370ac34e879c2a17f4c72af68cff7893d3aa41c6b6c79503853c",
    });
    expect(fixture.negative.source).toMatchObject({
      commit: "675e21251773325ec4d20b5de743cc183086278a",
      sha256: "b9272fc85eef9d87fa4a941f60a8f623860138a03f9066e52006f888ecee2945",
    });
  });
  it.each(fixture.conformance.cases)("accepts provider example: $name", (entry) => {
    const decoded = entry.schema.endsWith("/RegistryCounts")
      ? compiledDecodeUnknownSync34(entry.value, options)
      : compiledDecodeUnknownSync35(entry.value, options);
    expect(decoded).toEqual(entry.value);
    if (entry.schema.endsWith("/Capabilities"))
      expect(() => compiledDecodeUnknownSync29(entry.value, options)).toThrow();
  });
  it.each(fixture.negative.cases)("rejects provider example: $name", (entry) => {
    expect(() => compiledDecodeUnknownSync34(entry.value, options)).toThrow();
  });
  const counts = fixture.conformance.cases.find((entry) =>
    entry.schema.endsWith("/RegistryCounts"),
  )!.value as Record<string, unknown>;
  it.each([
    { active: 10 },
    { unknown_lifecycle: 10 },
    { active: 8, unknown_lifecycle: 2 },
    { total: Number.MAX_SAFE_INTEGER + 1 },
    { active: 1.5 },
    { active: -1 },
    { observed_at: "2026-02-30T00:00:00Z" },
    { authority_effect: "write" },
    { context: { owner_id: "owner.synthetic", server_generation: 0, registry_version: 7 } },
  ])("rejects invalid counts %j", (invalid) => {
    expect(() => compiledDecodeUnknownSync34({ ...counts, ...invalid }, options)).toThrow();
  });
  it("accepts an empty registry and safe-integer boundary", () => {
    for (const total of [0, Number.MAX_SAFE_INTEGER])
      expect(
        compiledDecodeUnknownSync34(
          {
            ...counts,
            total,
            active: total,
            unknown_lifecycle: 0,
          },
          options,
        ).total,
      ).toBe(total);
  });
});
