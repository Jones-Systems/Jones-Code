import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  NativeReference,
  WorkstreamCommand,
  WorkstreamMembershipPage,
  WorkstreamReferenceDetail,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ServerEnvironmentIdentity } from "../../../../environment/ServerEnvironment.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import * as ProviderTest from "../../nativeProvider/testFixtures.ts";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as Operations from "../operations/memberOperations.ts";
import * as Attestation from "../providers/localAttestation.ts";
import * as Reads from "../read/memberRead.ts";
import { RegistryError } from "../registry/session.ts";
import type { ReadResult } from "../read/routes.ts";
import * as Admission from "./memberAdmission.ts";
import * as Enrollments from "./memberEnrollment.ts";
import type { MemberWriteTarget } from "./memberRecordedCaller.ts";
import * as Source from "./memberSourcePorts.ts";

const decodeReference = Schema.decodeUnknownSync(NativeReference);
const decodeReferenceDetail = Schema.decodeUnknownSync(WorkstreamReferenceDetail);
const decodeMemberships = Schema.decodeUnknownSync(WorkstreamMembershipPage);
const decodeCaller = Schema.decodeUnknownEffect(Admission.MemberRecordedWriteCallerV2);
const binding = ProviderTest.binding;
const environmentId = binding.source_instance_id;
const registryId = "11111111-1111-4111-8111-111111111111";
const origin = { kind: "agent" as const };
const limits = { runtimeMode: "auto" as const, interactionMode: "default" as const };
const thread = {
  environment_id: environmentId,
  thread_id: ProviderTest.attestationRequest.identity.native_id,
};
const target: MemberWriteTarget = { kind: "thread", ...thread };
const identity = { ...ProviderTest.attestationRequest.identity, source_instance_id: environmentId };
const context = { owner_id: binding.owner_id, server_generation: 1, registry_version: 3 };
const envelope = {
  contract: "workstreams-read/1.0.0" as const,
  registry: {
    ...context,
    registry_instance_id: registryId,
    canonical_origin: binding.registry_origin,
  },
  provenance: {
    authority: "native-home" as const,
    home_environment_id: "home:test",
    served_by_environment_id: "home:test",
    source: "live" as const,
    observed_at: ProviderTest.now,
    stale: false,
  },
  coverage: { registry: "complete" as const, sources: [] },
};
const recorded = (writeTarget = target): Admission.MemberRecordedWriteCallerV2 => ({
  schema: "jones-workstreams-member-recorded-write-caller/2",
  local: {
    schema: "jones-workstreams-member-local-write-caller/2",
    environment_id: environmentId,
    auth: {
      kind: "agent",
      calling_thread_id: "caller:test",
      provider_instance_id: "codex:test",
      mcp_credential_id: "credential:test",
      run_id: "run:original",
      provider_thread_id: "provider-thread:test",
      provider_session_id: "provider-session:test",
    },
    origin,
    ceiling: {
      permissions: ["workstreams:read", "workstreams:write"],
      scopes: [],
      capabilities: ["orchestration"],
      limits,
    },
    session: null,
    request_namespace: "request:test",
    invocation_issued_at: 1,
    link_origin: null,
    link_session: null,
    captured_at: 1,
    target: writeTarget,
  },
  association: {
    enrollment_id: binding.enrollment_id,
    enrollment_session_id: binding.session_id,
    intent_sha256: "a".repeat(64),
    home: {
      registry_instance_id: registryId,
      owner_id: binding.owner_id,
      home_environment_id: "home:test",
      server_generation: 1,
    },
    canonical_origin: binding.registry_origin,
    principal_id: binding.principal_id,
    singleton_binding_version: 1,
    pin_bound_revision: 2,
    source_binding_version: 3,
    source: {
      environment_id: environmentId,
      authority_namespace: binding.authority_namespace,
      store_generation: binding.store_generation,
    },
    link_generation: "opaque:member-to-home",
    member_pin_id: "member-pin:test",
    home_pin_id: "home-pin:test",
    home_pin_bound_revision: 4,
  },
  original_outbound_ceiling: {
    access: "auto",
    runtime_mode: "auto",
    interaction_mode: "default",
    permissions: ["workstreams:read", "workstreams:write"],
    scopes: ["orchestration:read", "orchestration:operate"],
  },
});
const operation = () => ({
  contract: "workstream-operations/1.0.0" as const,
  operation_id: "operation:member-source-01",
  kind: "set_primary" as const,
  origin,
  thread,
  target: { kind: "unassigned" as const, registry_id: registryId, owner_id: binding.owner_id },
  expected: { kind: "any" as const },
});
const command = (action: WorkstreamCommand["action"]): WorkstreamCommand => ({
  command_id: "command:member-source-01",
  expected_server_generation: 1,
  expected_registry_version: 3,
  action,
});
const reference = (nativeIdentity = identity) =>
  decodeReference({
    native_reference_id: "reference:test",
    owner_id: binding.owner_id,
    identity: nativeIdentity,
    pr_locator: null,
    registration: {
      state: "quarantined",
      attestation_version: 0,
      attested_at: null,
      expires_at: null,
      evidence: null,
    },
    created_at: ProviderTest.now,
    created_by: { principal_id: binding.principal_id },
    created_registry_version: 1,
  });
const referenceResult = (nativeIdentity = identity): ReadResult => ({
  route: "reference",
  envelope,
  authorization_revision: 1,
  value: decodeReferenceDetail({
    context,
    reference: reference(nativeIdentity),
    latest_observation: null,
  }),
});
const memberships = (
  present: boolean,
  next: string | null = null,
  workstreamId = "workstream:test",
): ReadResult => ({
  route: "memberships",
  envelope,
  authorization_revision: 1,
  value: decodeMemberships({
    context,
    next_cursor: next,
    items: present
      ? [
          {
            membership_id: "membership:test",
            workstream_id: workstreamId,
            native_reference_id: "reference:test",
            kind: "primary",
            opened: {
              at: ProviderTest.now,
              actor: { principal_id: binding.principal_id },
              command_id: "command:membership-01",
              registry_version: 1,
            },
            closed: null,
          },
        ]
      : [],
  }),
});
const unexpected = () => Effect.die("Unexpected synthetic adapter port");

// Admission and enrollment are explicit synthetic ports; the native provider and wire validation are real.
const fixture = () => {
  const native = ProviderTest.makeProviderFixture();
  const provider = makeWorkstreamsNativeProvider(native.ports, "v2");
  const state = {
    reads: [] as Parameters<Reads.MemberReads["Service"]["read"]>[0][],
    read: (
      _request: Parameters<Reads.MemberReads["Service"]["read"]>[0],
    ): Effect.Effect<ReadResult, RegistryError> => unexpected(),
    record: recorded(),
    binding: Option.some(binding),
    captures: [] as MemberWriteTarget[],
    enrollmentSessions: [] as string[],
    checks: 0,
    revoked: false,
    narrowed: false,
    providerCalls: 0,
    invalidProof: false,
    afterProvider: () => {},
  };
  const readmit: Admission.MemberAdmission["Service"]["readmitWrite"] = (input) =>
    Effect.gen(function* () {
      const record = yield* decodeCaller(input).pipe(
        Effect.mapError((cause) => new RegistryError({ code: "forbidden", cause })),
      );
      return {
        record,
        ceiling: record.local.ceiling,
        limits,
        checkCurrent: () =>
          Effect.suspend(() => {
            state.checks++;
            return state.revoked
              ? Effect.fail(new RegistryError({ code: "forbidden" }))
              : Effect.succeed(
                  state.narrowed
                    ? { ...limits, runtimeMode: "approval-required" as const }
                    : limits,
                );
          }),
      };
    });
  const admission = Admission.MemberAdmission.of({
    captureWrite: (value) =>
      Effect.sync(() => {
        state.captures.push(value);
        return state.record;
      }),
    readmitWrite: readmit,
    readmitWriteRetained: readmit,
    captureOwnPlacement: unexpected,
    readmitOwnPlacement: unexpected,
    capture: unexpected,
    readmitRetained: unexpected,
    readmit: unexpected,
    current: unexpected,
  });
  const enrollments = Enrollments.MemberEnrollments.of({
    ownerCurrent: unexpected(),
    validateSource: unexpected,
    inspectSession: unexpected,
    reserveSession: unexpected,
    validateLocal: unexpected,
    readProof: unexpected,
    getValidatedBySessionId: unexpected,
    getValidatedByEnrollmentId: unexpected,
    materialize: unexpected,
    getBySessionId: (id) =>
      Effect.sync(() => {
        state.enrollmentSessions.push(id);
        return state.binding;
      }),
  });
  const ports = Layer.mergeAll(
    Layer.succeed(ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make(environmentId)),
    }),
    Layer.succeed(Reads.MemberReads, {
      read: (request) => {
        state.reads.push(request);
        return state.read(request);
      },
    }),
    Source.requestAdmissionLayer.pipe(
      Layer.provide(Layer.succeed(Admission.MemberAdmission, admission)),
    ),
    Layer.succeed(Enrollments.MemberEnrollments, enrollments),
    Layer.succeed(Attestation.NativeProvider, {
      ...provider,
      attest: (enrolled, request) =>
        Effect.gen(function* () {
          state.providerCalls++;
          expect(enrolled).toEqual(binding);
          const result = yield* provider.attest(enrolled, request);
          state.afterProvider();
          return state.invalidProof && result.state === "attested"
            ? { ...result, attestation: { ...result.attestation, evidence_sha256: "f".repeat(64) } }
            : result;
        }),
    }),
  );
  return { state, native, layer: Source.layer.pipe(Layer.provideMerge(ports)) };
};

it.effect("resolves direct own-thread registration and metadata without any home lookup", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const targets = yield* Forwarder.MemberCommandTargets;
    expect(
      yield* targets.resolve(
        command({ operation: "register_reference", identity, pr_locator: null }),
      ),
    ).toEqual(target);
    expect(
      yield* targets.resolve(
        command({
          operation: "register_reference",
          identity: { ...identity, provider: "issue" },
          pr_locator: null,
        }),
      ),
    ).toEqual({ kind: "metadata" });
    expect(
      yield* targets.resolve(
        command({
          operation: "create_workstream",
          name: "Test",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        }),
      ),
    ).toEqual({ kind: "metadata" });
    expect(f.state.reads).toEqual([]);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["source", "kind", "id-kind", "account"] as const)(
  "refuses a foreign or unsupported T3 %s target before admission",
  (variant) => {
    const f = fixture();
    const foreign = {
      ...identity,
      ...(variant === "source" ? { source_instance_id: "foreign:environment" } : {}),
      ...(variant === "kind" ? { resource_kind: "issue" } : {}),
      ...(variant === "id-kind" ? { id_kind: "external" as const } : {}),
      ...(variant === "account"
        ? {
            account_provenance: {
              kind: "account" as const,
              account_id: "account:test",
              account_namespace: "account:test",
            },
          }
        : {}),
    };
    return Effect.gen(function* () {
      expect(
        yield* (yield* Forwarder.MemberCommandTargets)
          .resolve(
            command({ operation: "register_reference", identity: foreign, pr_locator: null }),
          )
          .pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
      expect(f.state.reads).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(["remove", "move"] as const)(
  "resolves %s from complete membership pages and exact reference identity",
  (kind) => {
    const f = fixture();
    f.state.read = (request) =>
      Effect.succeed(
        request.route === "reference"
          ? referenceResult()
          : memberships(f.state.reads.length > 1, f.state.reads.length === 1 ? "next_page" : null),
      );
    return Effect.gen(function* () {
      const action =
        kind === "remove"
          ? {
              operation: "remove_membership" as const,
              workstream_id: "workstream:test",
              expected_version: 1,
              membership_id: "membership:test",
            }
          : {
              operation: "move_primary" as const,
              source_workstream_id: "workstream:test",
              expected_source_version: 1,
              source_membership_id: "membership:test",
              destination_workstream_id: "workstream:destination",
              expected_destination_version: 1,
            };
      expect(yield* (yield* Forwarder.MemberCommandTargets).resolve(command(action))).toEqual(
        target,
      );
      expect(f.state.reads).toEqual([
        { route: "memberships", params: { workstream_id: "workstream:test", limit: 100 } },
        {
          route: "memberships",
          params: { workstream_id: "workstream:test", limit: 100, cursor: "next_page" },
        },
        { route: "reference", params: { native_reference_id: "reference:test" } },
      ]);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(["missing", "cursor", "workstream", "reference", "malformed"] as const)(
  "never turns %s target lookup failure into metadata authority",
  (problem) => {
    const f = fixture();
    f.state.read = (request) =>
      Effect.succeed(
        request.route === "reference"
          ? problem === "reference"
            ? {
                ...referenceResult(),
                value: {
                  ...decodeReferenceDetail(referenceResult().value),
                  reference: { ...reference(), native_reference_id: "reference:other" },
                },
              }
            : { ...referenceResult(), value: {} }
          : memberships(
              problem !== "missing" && problem !== "cursor",
              problem === "cursor" ? "same_page" : null,
              problem === "workstream" ? "workstream:other" : "workstream:test",
            ),
      );
    return Effect.gen(function* () {
      const error = yield* (yield* Forwarder.MemberCommandTargets)
        .resolve(
          command({
            operation: "remove_membership",
            workstream_id: "workstream:test",
            expected_version: 1,
            membership_id: "membership:test",
          }),
        )
        .pipe(Effect.flip);
      expect(error.code).toBe(
        problem === "missing"
          ? "not_found"
          : problem === "cursor"
            ? "cursor_invalid"
            : "invalid_response",
      );
      expect(f.state.captures).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "uses the actual source provider and original enrollment for fresh bounded evidence without settlement",
  () => {
    const f = fixture();
    return Effect.gen(function* () {
      const result = yield* (yield* Operations.MemberOperationEvidence).capture(operation());
      expect(result.state).toBe("attested");
      expect(result.request.identity).toEqual(identity);
      expect(f.state.captures).toEqual([target]);
      expect(f.state.enrollmentSessions).toEqual([binding.session_id]);
      expect(f.state.checks).toBe(4);
      expect(f.state.providerCalls).toBe(1);
      expect(f.native.calls).toEqual([]);
      expect(f.native.attempts.size).toBe(0);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each([
  "environment",
  "owner",
  "registry",
  "origin",
  "target",
  "enrollment",
  "generation",
] as const)("refuses %s evidence association before consulting the provider", (problem) => {
  const f = fixture();
  const input = operation();
  if (problem === "environment")
    input.thread = { ...thread, environment_id: "foreign:environment" };
  if (problem === "owner") input.target = { ...input.target, owner_id: "foreign:owner" };
  if (problem === "registry")
    input.target = { ...input.target, registry_id: "22222222-2222-4222-8222-222222222222" };
  if (problem === "origin")
    f.state.record = {
      ...f.state.record,
      local: { ...f.state.record.local, origin: { kind: "app" } },
    };
  if (problem === "target") f.state.record = recorded({ ...target, thread_id: "other:thread" });
  if (problem === "enrollment") f.state.binding = Option.none();
  if (problem === "generation")
    f.state.binding = Option.some({ ...binding, store_generation: binding.store_generation + 1 });
  return Effect.gen(function* () {
    expect(
      yield* (yield* Operations.MemberOperationEvidence).capture(input).pipe(Effect.flip),
    ).toMatchObject({ code: "forbidden" });
    expect(f.state.providerCalls).toBe(0);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["revoked", "narrowed", "proof"] as const)(
  "refuses %s after provider observation without releasing evidence",
  (problem) => {
    const f = fixture();
    f.state.afterProvider = () => {
      f.state.revoked = problem === "revoked";
      f.state.narrowed = problem === "narrowed";
    };
    f.state.invalidProof = problem === "proof";
    return Effect.gen(function* () {
      const error = yield* (yield* Operations.MemberOperationEvidence)
        .capture(operation())
        .pipe(Effect.flip);
      expect(error.code).toBe(problem === "proof" ? "provider_mismatch" : "forbidden");
      expect(f.state.providerCalls).toBe(1);
      expect(f.native.calls).toEqual([]);
      expect(f.native.attempts.size).toBe(0);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("pins the original write caller across source evidence and forwarding capture", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const first = f.state.record;
    yield* (yield* Operations.MemberOperationEvidence).capture(operation());
    f.state.record = {
      ...first,
      local: {
        ...first.local,
        auth: {
          ...first.local.auth,
          kind: "agent",
          calling_thread_id: "caller:test",
          provider_instance_id: "codex:test",
          mcp_credential_id: "credential:new",
          run_id: "run:replacement",
          provider_thread_id: "provider-thread:new",
          provider_session_id: "provider-session:new",
        },
      },
    };
    const captured = yield* (yield* Admission.MemberAdmission).captureWrite(target);
    expect(captured).toBe(first);
    expect(f.state.captures).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});

it.effect("serializes concurrent capture and refuses a second write target in the request", () => {
  const f = fixture();
  return Effect.gen(function* () {
    const admission = yield* Admission.MemberAdmission;
    const records = yield* Effect.all(
      [admission.captureWrite(target), admission.captureWrite(target)],
      { concurrency: "unbounded" },
    );
    expect(records[0]).toBe(records[1]);
    expect(f.state.captures).toHaveLength(1);
    expect(yield* admission.captureWrite({ kind: "metadata" }).pipe(Effect.flip)).toMatchObject({
      code: "forbidden",
    });
    f.state.revoked = true;
    expect(yield* admission.captureWrite(target).pipe(Effect.flip)).toMatchObject({
      code: "forbidden",
    });
    expect(f.state.captures).toHaveLength(1);
  }).pipe(Effect.provide(f.layer));
});
