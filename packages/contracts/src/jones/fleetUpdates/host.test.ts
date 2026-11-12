import * as Schema from "effect/Schema";
import * as SchemaAST from "effect/SchemaAST";
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "../../baseSchemas.ts";
import { EnvironmentAuthenticatedAuth, EnvironmentHttpApi } from "../../environmentHttp.ts";
import {
  FleetEnrollment,
  FleetFollowerBlocker,
  FleetFollowerStatus,
  FleetHostError,
  FleetHostOperation,
  FleetHostStatus,
  FleetOperationId,
  FleetPlanDirective,
  FleetPlanDirectiveInput,
  FleetSourceSha,
} from "./host.ts";

const directiveId = "11111111-1111-4111-8111-111111111111";
const enrollmentId = "22222222-2222-4222-8222-222222222222";
const environmentId = "synthetic-follower";
const directive = {
  directiveId,
  targetSource: "a".repeat(40),
  targetTree: "b".repeat(40),
  mode: "manual-review",
  issuedAt: "2026-10-27T04:00:00Z",
} as const;

// The pre-plan follower fields and closed state set from 67ea4142f3e2.
const LegacyFollowerStatus = Schema.Struct({
  protocol: Schema.Literal(1),
  leaderSource: Schema.NullOr(FleetSourceSha),
  leaderPublishedAt: Schema.optionalKey(Schema.String),
  state: Schema.Literals([
    "unavailable",
    "waiting-for-leader",
    "current",
    "preparing",
    "prepare-blocked",
    "waiting-for-quiet",
    "activating",
    "reconciling",
  ]),
  reason: Schema.optionalKey(Schema.String),
  quiet: Schema.optionalKey(
    Schema.Struct({
      inputIdleMs: Schema.Number,
      requiredInputIdleMs: Schema.Number,
      blockers: Schema.Array(FleetFollowerBlocker),
      evaluatedAt: Schema.String,
    }),
  ),
  notBefore: Schema.optionalKey(Schema.String),
  operationId: Schema.optionalKey(FleetOperationId),
});

// Update compatibility is covered by the frozen decoder in environmentHttpGroups.test.ts.
const LegacyHostWithoutUpdate = Schema.Struct({
  operationProtocol: Schema.NullOr(Schema.Literal(1)),
  environmentId: EnvironmentId,
  enrollment: Schema.NullOr(FleetEnrollment),
  operation: Schema.NullOr(FleetHostOperation),
  update: Schema.Null,
  follower: Schema.optionalKey(LegacyFollowerStatus),
});

const compiledDecodeSync1 = Schema.decodeSync(FleetPlanDirective);
const compiledDecodeUnknownSync2 = Schema.decodeUnknownSync(FleetPlanDirective);
const compiledDecodeUnknownSync3 = Schema.decodeUnknownSync(FleetHostStatus);
const compiledDecodeUnknownSync4 = Schema.decodeUnknownSync(LegacyHostWithoutUpdate);
const compiledDecodeUnknownSync5 = Schema.decodeUnknownSync(LegacyFollowerStatus);
const compiledDecodeUnknownSync6 = Schema.decodeUnknownSync(FleetFollowerStatus);
const compiledIs7 = Schema.is(FleetHostError);

describe("fleet manual-review plan directives", () => {
  it("requires exact target, environment, enrollment and prior-directive CAS binding", () => {
    const input = {
      directiveId,
      enrollmentId,
      environmentId,
      repository: "Jones-Systems/Jones-Code",
      targetSource: directive.targetSource,
      targetTree: directive.targetTree,
      expectedDirectiveId: null,
    };
    const decode = Schema.decodeUnknownSync(FleetPlanDirectiveInput);
    expect(decode(input)).toEqual(input);
    expect(decode({ ...input, expectedDirectiveId: enrollmentId })).toEqual({
      ...input,
      expectedDirectiveId: enrollmentId,
    });
    for (const field of Object.keys(input)) {
      const incomplete: Record<string, unknown> = { ...input };
      delete incomplete[field];
      expect(() => decode(incomplete)).toThrow();
    }
    for (const patch of [
      { repository: "another/repository" },
      { environmentId: "" },
      { targetTree: "invalid" },
      { targetSource: "a".repeat(39) },
      { expectedDirectiveId: "invalid" },
    ])
      expect(() => decode({ ...input, ...patch })).toThrow();
    expect(compiledDecodeSync1(directive)).toEqual(directive);
    expect(() => compiledDecodeUnknownSync2({ ...directive, mode: "automatic" })).toThrow();
  });

  it("decodes old and new follower/host payloads in both directions without new states", () => {
    const follower = { protocol: 1, leaderSource: null, state: "waiting-for-leader" };
    const old = {
      operationProtocol: 1,
      environmentId,
      enrollment: null,
      operation: null,
      update: null,
      follower,
    };
    const modern = {
      ...old,
      planDirectiveProtocol: 1,
      follower: { ...follower, planDirective: directive },
    };
    expect(compiledDecodeUnknownSync3(old)).toEqual(old);
    expect(compiledDecodeUnknownSync3(modern)).toEqual(modern);
    expect(compiledDecodeUnknownSync4(modern)).toEqual(old);
    expect(compiledDecodeUnknownSync5(modern.follower)).toEqual(follower);
    expect(compiledDecodeUnknownSync3(old).planDirectiveProtocol).toBeUndefined();
    for (const state of [
      "unavailable",
      "waiting-for-leader",
      "current",
      "preparing",
      "prepare-blocked",
      "waiting-for-quiet",
      "activating",
      "reconciling",
    ]) {
      const value = { ...modern.follower, state };
      expect(compiledDecodeUnknownSync6(value)).toEqual(value);
      expect(compiledDecodeUnknownSync5(value)).toEqual({ ...follower, state });
    }
    expect(() => compiledDecodeUnknownSync3({ ...modern, planDirectiveProtocol: 2 })).toThrow();
    expect(() => compiledDecodeUnknownSync6({ ...follower, state: "manual-review" })).toThrow();
  });

  it("decodes existing host error reasons into tagged instances and rejects new reasons", () => {
    const decode = Schema.decodeUnknownSync(FleetHostError);
    const encode = Schema.encodeSync(FleetHostError);
    for (const reason of [
      "binding",
      "not-enrolled",
      "bootstrap-required",
      "conflict",
      "storage",
    ] as const) {
      const wire = { _tag: "FleetHostError", reason, message: "Synthetic refusal" };
      const error = new FleetHostError({ reason, message: wire.message });
      expect(compiledIs7(error)).toBe(true);
      const decoded = decode(wire);
      expect(decoded).toBeInstanceOf(FleetHostError);
      expect(compiledIs7(decoded)).toBe(true);
      expect(encode(decoded)).toEqual(wire);
    }
    for (const reason of ["plan-managed", "", "unknown"])
      expect(() => decode({ _tag: "FleetHostError", reason, message: "x" })).toThrow();
  });

  it("registers the authenticated PUT with host status success and the existing 409 errors", () => {
    const { planDirective, enroll } = EnvironmentHttpApi.groups.jonesFleetUpdates.endpoints;
    expect([planDirective.method, planDirective.path]).toEqual([
      "PUT",
      "/api/jones-fleet-updates/plan-directive",
    ]);
    expect(planDirective.middlewares.has(EnvironmentAuthenticatedAuth)).toBe(true);
    expect(planDirective.headers?.ast).toEqual(enroll.headers?.ast);
    expect([...planDirective.success].map((schema) => schema.ast)).toEqual([
      Schema.toCodecJson(FleetHostStatus).ast,
    ]);
    expect([...planDirective.error].map((schema) => schema.ast)).toEqual(
      [...enroll.error].map((schema) => schema.ast),
    );
    expect(
      [...planDirective.error].some(
        (schema) => SchemaAST.resolveAt<number>("httpApiStatus")(schema.ast) === 409,
      ),
    ).toBe(true);
  });
});
