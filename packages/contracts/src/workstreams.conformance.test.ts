import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "node:crypto";

import conformanceRaw from "./workstreams-fixtures/conformance.json.fixture?raw";
import negativesRaw from "./workstreams-fixtures/negative-cases.json.fixture?raw";
import semanticRaw from "./workstreams-fixtures/semantic-cases.json.fixture?raw";
import {
  Workstream,
  WorkstreamCapabilities,
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
const supported = new Map<string, (value: unknown) => unknown>([
  ["Command", (value) => Schema.decodeUnknownSync(WorkstreamCommand)(value, options)],
  ["Workstream", (value) => Schema.decodeUnknownSync(Workstream)(value, options)],
  ["NativeReference", (value) => Schema.decodeUnknownSync(NativeReference)(value, options)],
  ["NativeIdentity", (value) => Schema.decodeUnknownSync(WorkstreamNativeIdentity)(value, options)],
  ["PrLocator", (value) => Schema.decodeUnknownSync(WorkstreamPrLocator)(value, options)],
  ["MembershipEpisode", (value) => Schema.decodeUnknownSync(MembershipEpisode)(value, options)],
  ["DeclarationRevision", (value) => Schema.decodeUnknownSync(DeclarationRevision)(value, options)],
  ["Edge", (value) => Schema.decodeUnknownSync(WorkstreamEdge)(value, options)],
  [
    "LifecycleDeclaration",
    (value) => Schema.decodeUnknownSync(WorkstreamLifecycleDeclaration)(value, options),
  ],
  ["ProgressCondition", (value) => Schema.decodeUnknownSync(WorkstreamProgress)(value, options)],
  ["DeliveryState", (value) => Schema.decodeUnknownSync(WorkstreamDelivery)(value, options)],
  ["FreshnessState", (value) => Schema.decodeUnknownSync(WorkstreamFreshness)(value, options)],
  [
    "RegistrationVerification",
    (value) => Schema.decodeUnknownSync(WorkstreamRegistrationVerification)(value, options),
  ],
  ["PrObservation", (value) => Schema.decodeUnknownSync(WorkstreamPrObservation)(value, options)],
  ["Receipt", (value) => Schema.decodeUnknownSync(WorkstreamReceipt)(value, options)],
  [
    "CommittedReceipt",
    (value) => Schema.decodeUnknownSync(WorkstreamCommittedReceipt)(value, options),
  ],
  ["PendingReceipt", (value) => Schema.decodeUnknownSync(WorkstreamPendingReceipt)(value, options)],
  [
    "RejectedReceipt",
    (value) => Schema.decodeUnknownSync(WorkstreamRejectedReceipt)(value, options),
  ],
  [
    "TerminalReceipt",
    (value) => Schema.decodeUnknownSync(WorkstreamTerminalReceipt)(value, options),
  ],
  ["AuditEvent", (value) => Schema.decodeUnknownSync(WorkstreamAuditEvent)(value, options)],
  ["WorkstreamPage", (value) => Schema.decodeUnknownSync(WorkstreamPage)(value, options)],
  ["ReferencePage", (value) => Schema.decodeUnknownSync(WorkstreamReferencePage)(value, options)],
  ["MembershipPage", (value) => Schema.decodeUnknownSync(WorkstreamMembershipPage)(value, options)],
  [
    "DeclarationPage",
    (value) => Schema.decodeUnknownSync(WorkstreamDeclarationPage)(value, options),
  ],
  ["EdgePage", (value) => Schema.decodeUnknownSync(WorkstreamEdgePage)(value, options)],
  ["HistoryPage", (value) => Schema.decodeUnknownSync(WorkstreamHistoryPage)(value, options)],
  ["WorkstreamDetail", (value) => Schema.decodeUnknownSync(WorkstreamDetail)(value, options)],
  [
    "ReferenceDetail",
    (value) => Schema.decodeUnknownSync(WorkstreamReferenceDetail)(value, options),
  ],
  ["Capabilities", (value) => Schema.decodeUnknownSync(WorkstreamCapabilities)(value, options)],
  ["OwnerGrant", (value) => Schema.decodeUnknownSync(WorkstreamOwnerGrant)(value, options)],
  ["Error", (value) => Schema.decodeUnknownSync(WorkstreamError)(value, options)],
  ["BrowserSession", (value) => Schema.decodeUnknownSync(WorkstreamSession)(value, options)],
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
      expect(() =>
        Schema.decodeUnknownSync(WorkstreamCommittedReceipt)(candidate, options),
      ).toThrow();
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
      expect(() =>
        Schema.decodeUnknownSync(WorkstreamCommittedReceipt)(candidate, options),
      ).toThrow();
    }

    const pending = fixtureFor("PendingReceipt") as Record<string, any>;
    pending.retry_after_seconds = 3601;
    expect(() => Schema.decodeUnknownSync(WorkstreamPendingReceipt)(pending, options)).toThrow();

    const error = fixtureFor("Error") as Record<string, any>;
    error.status = 500;
    expect(() => Schema.decodeUnknownSync(WorkstreamError)(error, options)).toThrow();

    const observation = fixtureFor("PrObservation") as Record<string, any>;
    observation.last_success.observed_at = "yesterday";
    expect(() => Schema.decodeUnknownSync(WorkstreamPrObservation)(observation, options)).toThrow();

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
      expect(() => Schema.decodeUnknownSync(WorkstreamAuditEvent)(candidate, options)).toThrow();
    }
  });

  it("enforces registration state conditionals in both directions", () => {
    const quarantined = {
      state: "quarantined",
      attestation_version: 0,
      attested_at: null,
      expires_at: null,
      evidence: null,
    };
    expect(() =>
      Schema.decodeUnknownSync(WorkstreamRegistrationVerification)(quarantined, options),
    ).not.toThrow();
    expect(() =>
      Schema.decodeUnknownSync(WorkstreamRegistrationVerification)(
        { ...quarantined, attestation_version: 1 },
        options,
      ),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(WorkstreamRegistrationVerification)(
        { ...quarantined, state: "attested" },
        options,
      ),
    ).toThrow();
  });

  it("enforces conditional explanation fields and positive settlement fencing", () => {
    const disposition = commandFor("set_coordination_disposition");
    disposition.action.other_disposition = "unexpected explanation";
    expect(() => Schema.decodeUnknownSync(WorkstreamCommand)(disposition, options)).toThrow();

    const otherDisposition = commandFor("set_coordination_disposition");
    otherDisposition.action.disposition = "other";
    otherDisposition.action.other_disposition = null;
    expect(() => Schema.decodeUnknownSync(WorkstreamCommand)(otherDisposition, options)).toThrow();

    const settlement = commandFor("request_native_t3_settlement");
    settlement.action.expected_attestation_version = 0;
    expect(() => Schema.decodeUnknownSync(WorkstreamCommand)(settlement, options)).toThrow();

    const membership = fixtureFor("MembershipEpisode") as Record<string, any>;
    membership.closed = {
      at: "2026-09-12T00:00:01.000Z",
      actor: { principal_id: "principal-synthetic" },
      command_id: "synthetic-cmd-close-membership",
      registry_version: 2,
      reason: "removed",
      other_reason: "unexpected explanation",
    };
    expect(() => Schema.decodeUnknownSync(MembershipEpisode)(membership, options)).toThrow();
  });

  it("accepts reattaching a primary membership through the shared attach shape", () => {
    const command = commandFor("attach_primary");
    command.action.operation = "reattach_primary";
    expect(() => Schema.decodeUnknownSync(WorkstreamCommand)(command, options)).not.toThrow();
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
      expect(() => Schema.decodeUnknownSync(Workstream)(candidate, options)).toThrow();
    }
  });

  it("requires a positive declaration registry version", () => {
    const declaration = fixtureFor("DeclarationRevision") as Record<string, any>;
    declaration.registry_version = 0;
    expect(() => Schema.decodeUnknownSync(DeclarationRevision)(declaration, options)).toThrow();
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
    expect(() => Schema.decodeUnknownSync(WorkstreamHistoryPage)(history, options)).toThrow();
  });
});
