import { expect, it } from "@effect/vitest";
import { EnvironmentId, WorkstreamsNativeSettlementRequest } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HomeWire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as MemberWire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import * as Test from "../store/sql.testkit.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Source from "./memberSourceReadAdmission.ts";
import { RegistryError } from "../registry/session.ts";
import * as Provider from "../providers/localAttestation.ts";
import { NativeProviderAttempts } from "../../nativeProvider/attemptRepository.ts";
import { makeProviderFixture, request as template } from "../../nativeProvider/testFixtures.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
const decodeSettlement = Schema.decodeUnknownEffect(WorkstreamsNativeSettlementRequest);
const decodeBound = Schema.decodeUnknownEffect(MemberWire.WorkstreamsMemberEnrollmentBoundV1);
const decodeMemberCaller = Schema.decodeUnknownEffect(Admission.MemberRecordedCallerV1);
const decodeUnknownSyncHomeWireWorkstreamsHomeBindBodyV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindBodyV2,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentIntentV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentIntentV1,
);
const decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindCommittedV2,
);
const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const sourceAuthority = {
  environment_id: "member:test",
  authority_namespace: "member-store",
  store_generation: 1,
};
const homeAuthority = {
  environment_id: "home:test",
  authority_namespace: "home-store",
  store_generation: 1,
};
const build = {
  repository: "Jones-Systems/Jones-Code" as const,
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
const issuance = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "member-enrollment:test",
  registry_origin: "https://native.workstreams.invalid",
  context: {
    owner_id: Test.scope.owner_id,
    principal_id: "principal:test",
    source_instance_id: "member:test",
    authority_namespace: "member-store",
    store_generation: 1,
    enrollment_id: "member-enrollment:test",
    protocol: "workstreams-t3-provider/1.0.0",
    build,
  },
  session: {
    session_id: "provider-session:test",
    issued_at: Test.now,
    expires_at: "2026-01-02T00:00:00.000Z",
  },
};
const syntheticSession = {
  session_id: "member-link:test",
  subject: "mcp-client",
  method: "bearer-access-token",
  issued_at: Test.now,
  expires_at: "2026-01-31T00:00:00.000Z",
  peer_link: true,
  ceiling: {
    access: "full-access",
    runtime_mode: "approval-required",
    interaction_mode: "default",
    scopes: [
      "orchestration:read",
      "orchestration:operate",
      "settings:write",
      "source-control:write",
    ],
    permissions: ["workstreams:read", "workstreams:write"],
  },
};
const makeIntent = (session: unknown = syntheticSession) => {
  const assertion = (atHome: boolean) => ({
    schema: "jones-workstreams-peer-session-pin/1",
    pin_id: atHome ? "home-pin:test" : "member-pin:test",
    revision: 1,
    receiver_authority: atHome ? homeAuthority : sourceAuthority,
    session: atHome ? { ...syntheticSession, session_id: "home-link:test" } : session,
    peer_environment_id: atHome ? "member:test" : "home:test",
    roles: [atHome ? "member_home" : "home_source"],
    expected_home: home,
    source_authority: sourceAuthority,
    source_enrollment_id: issuance.enrollment_id,
    source_binding_version: null,
    singleton_binding_version: null,
    state: "prepared",
    expires_at: "2026-01-01T00:20:00.000Z",
    setup_request_id: atHome ? "home-setup:test" : "member-setup:test",
    owner_qualification_digest: "c".repeat(64),
    bind_receipt: null,
  });
  const preconditions = { registry_version: 9, grant_id: "grant:test", grant_version: 1 };
  const setup = (atHome: boolean) =>
    JSON.stringify(
      {
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion: assertion(atHome),
        expected: preconditions,
      },
      null,
      2,
    ) + "\n";
  const homeBytes = setup(true),
    memberBytes = setup(false);
  const body = HomeWire.canonicalWorkstreamsHomeBindBody(
    decodeUnknownSyncHomeWireWorkstreamsHomeBindBodyV2({
      bind_id: "bind:test",
      expected_home: home,
      expected_canonical_origin: issuance.registry_origin,
      expected_home_authority: homeAuthority,
      source: { authority: sourceAuthority, enrollment: issuance },
      home_setup: {
        pin_id: "home-pin:test",
        prepared_revision: 1,
        setup_request_id: "home-setup:test",
        setup_request_sha256: sha(homeBytes),
      },
      member_setup: {
        pin_id: "member-pin:test",
        prepared_revision: 1,
        setup_request_id: "member-setup:test",
        outbound_link_generation: "opaque-link-generation:1",
        expected_singleton_version: null,
        singleton_binding_version: 1,
      },
      expected: { principal_id: "principal:test", ...preconditions, source_binding: null },
      rebind: false,
    }),
  );
  return decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentIntentV1({
    issuance_body: JSON.stringify(issuance, null, 2) + "\n",
    bind_request_body: body,
    bind_request_sha256: sha(body),
    home_setup: {
      contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
      kind: "prepared",
      request: {
        contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
        expected_home: home,
        home_setup_request_id: "home-setup:test",
      },
      canonical_origin: issuance.registry_origin,
      home_authority: homeAuthority,
      setup_request_body: homeBytes,
      pin: { ...assertion(true), setup_request_sha256: sha(homeBytes) },
    },
    member_setup_request_body: memberBytes,
    member_setup_request_sha256: sha(memberBytes),
  });
};
const homeResult = (intent = makeIntent()) =>
  decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2({
    contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
    kind: "committed",
    request_body: intent.bind_request_body,
    receipt: {
      principal_id: "principal:test",
      binding_version: 1,
      registry_version: 10,
      request_sha256: intent.bind_request_sha256,
    },
    home_pin_bound_revision: 2,
  });
const record: Admission.MemberRecordedCallerV1 = {
  schema: "jones-workstreams-member-recorded-caller/1",
  local: {
    schema: "jones-workstreams-member-local-caller/1",
    auth: { kind: "bearer", session_id: "local", subject: "owner", method: "bearer-access-token" },
    origin: { kind: "app" },
    ceiling: {
      permissions: ["workstreams:read"],
      scopes: ["orchestration:read"],
      capabilities: [],
      limits: { runtimeMode: "approval-required", interactionMode: "default" },
    },
    session: { issued_at: 0, expires_at: 10000 },
    environment_id: "member:test",
    request_namespace: null,
    invocation_issued_at: null,
    link_origin: null,
    link_session: null,
    captured_at: 0,
  },
  association: {
    enrollment_id: "enrollment",
    enrollment_session_id: "session",
    intent_sha256: "a".repeat(64),
    home,
    canonical_origin: "https://home.invalid",
    principal_id: "principal",
    singleton_binding_version: 1,
    pin_bound_revision: 2,
    source_binding_version: 1,
    source: {
      environment_id: "member:test",
      authority_namespace: "namespace",
      store_generation: 1,
    },
    link_generation: "opaque-original",
  },
};
// M/pin/caller boundaries below are synthetic. The actual NativeProvider lookup implementation
// proves absence stays unknown without reserve/startDispatch/engine dispatch.
it.effect.each([
  "lookup",
  "digest",
  "command",
  "principal",
  "thread",
  "generation",
  "prepared",
  "pin-revoked",
  "caller-revoked",
  "cloned-scope",
] as const)("retained source %s is bound and lookup-only", (change) =>
  Effect.gen(function* () {
    const intent = makeIntent();
    const r = homeResult(intent);
    const original: Admission.MemberRecordedCallerV1 = {
      ...record,
      local: {
        ...record.local,
        auth: {
          kind: "agent",
          calling_thread_id: template.identity.native_id,
          provider_instance_id: "codex",
          mcp_credential_id: "original-credential",
          run_id: "original-run",
          provider_thread_id: "original-provider-thread",
          provider_session_id: "original-native-session",
        },
        session: null,
      },
      association: {
        ...record.association,
        home,
        canonical_origin: issuance.registry_origin,
        principal_id: "principal:test",
        enrollment_id: issuance.enrollment_id,
        enrollment_session_id: issuance.session.session_id,
        source: sourceAuthority,
        link_generation: "opaque-link-generation:1",
      },
    };
    const request = yield* decodeSettlement({
      ...template,
      owner_id: home.owner_id,
      principal_id: "principal:test",
      identity: { ...template.identity, source_instance_id: "member:test" },
      expected_authority_namespace: "member-store",
      expected_store_generation: 1,
    });
    const bytes = JSON.stringify(request);
    const input = {
      command_id: request.command_id,
      command_request_sha256: request.request_sha256,
      request_body: bytes,
      request_sha256: sha(bytes),
      caller: original,
    };
    if (change === "digest") input.request_sha256 = "f".repeat(64);
    if (change === "command") input.command_id = "other";
    if (change === "principal")
      input.caller = {
        ...original,
        association: { ...original.association, principal_id: "other" },
      };
    if (change === "thread")
      input.caller = {
        ...original,
        local: {
          ...original.local,
          auth: {
            ...original.local.auth,
            kind: "agent",
            calling_thread_id: "other",
            provider_instance_id: "codex",
            mcp_credential_id: "original-credential",
            run_id: "original-run",
            provider_thread_id: "original-provider-thread",
            provider_session_id: "original-native-session",
          },
        },
      };
    const bound = yield* decodeBound({
      schema: MemberWire.WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA,
      state: "bound",
      revision: 2,
      intent,
      intent_sha256: sha(MemberWire.canonicalWorkstreamsMemberEnrollmentIntent(intent)),
      home_result: r,
      local_binding: { pin_bound_revision: 2, singleton_binding_version: 1 },
    });
    const prepared = PinRecords.decodeSetupRequest(intent.member_setup_request_body).pin;
    const pin = {
      ...prepared,
      revision: 2,
      state: change === "prepared" ? ("prepared" as const) : ("bound" as const),
      source_binding_version: 1,
      singleton_binding_version: 1,
      bind_receipt: r.receipt,
    };
    if (change === "generation") pin.expected_home = { ...home, server_generation: 2 };
    const invocation: Mcp.McpInvocationScope = {
      environmentId: EnvironmentId.make("member:test"),
      capabilities: new Set(["orchestration"]),
      issuedAt: 0,
      requestNamespace: "incoming-home",
      thread: undefined,
      client: {
        sessionId: syntheticSession.session_id,
        label: "home",
        linked: true,
        access: "full-access",
      },
    };
    const native = makeProviderFixture();
    let lookups = 0;
    const actualProvider = makeWorkstreamsNativeProvider(
      {
        ...native.ports,
        build: Effect.succeed(Option.some(build)),
        authority: {
          readCurrent: Effect.succeed({
            environmentId: "member:test",
            authorityNamespace: "member-store",
            storeGeneration: 1,
          }),
        },
      },
      "v2",
    );
    const ports = Layer.mergeAll(
      Layer.mock(Admission.MemberAdmission)({
        readmitRetained: (caller) =>
          decodeMemberCaller(caller).pipe(
            Effect.orDie,
            Effect.map((decoded) => ({
              record: decoded,
              limits: record.local.ceiling.limits!,
              checkCurrent: () =>
                change === "caller-revoked"
                  ? Effect.fail(new RegistryError({ code: "forbidden" }))
                  : Effect.succeed(record.local.ceiling.limits!),
            })),
          ),
      }),
      Layer.mock(Members.MemberEnrollments)({
        getValidatedByEnrollmentId: () => Effect.succeed(Option.some(bound)),
      }),
      Layer.succeed(Pins.PeerSessionPins, {
        forInvocation: (scope, selection) => {
          expect(selection).toEqual({ role: "home_source" });
          if (scope !== invocation || change === "cloned-scope")
            return Effect.fail(new Pins.PeerSessionError({ code: "unauthenticated" }));
          return Effect.succeed({
            pin,
            ceiling: prepared.session.ceiling,
            checkCurrent: () =>
              change === "pin-revoked"
                ? Effect.fail(new Pins.PeerSessionError({ code: "not_current" }))
                : Effect.void,
          });
        },
      }),
      Layer.succeed(NativeProviderAttempts, {
        ...native.ports.attempts,
        reserve: () => Effect.die("lookup reserved"),
        startDispatch: () => Effect.die("lookup dispatched"),
      }),
      Layer.succeed(Provider.NativeProvider, {
        ...actualProvider,
        settle: () => Effect.die("must never settle"),
        lookup: (...args) => {
          lookups++;
          return actualProvider.lookup(...args);
        },
      }),
    );
    const result = yield* Effect.gen(function* () {
      return yield* (yield* Source.MemberSourceReadAdmission).lookup(input);
    }).pipe(
      Effect.provide(Source.layer.pipe(Layer.provide(ports))),
      Effect.provideService(Mcp.McpInvocationContext, invocation),
      Effect.result,
    );
    if (change === "lookup") {
      expect(result).toMatchObject({
        _tag: "Success",
        success: { state: "unknown", reason: "attempt_not_found" },
      });
      expect(lookups).toBe(1);
    } else {
      expect(result._tag).toBe("Failure");
      expect(lookups).toBe(0);
    }
    expect(native.calls).toHaveLength(0);
    expect(native.attempts.size).toBe(0);
  }),
);
