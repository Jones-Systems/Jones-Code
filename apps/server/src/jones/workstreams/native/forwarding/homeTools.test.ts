// @effect-diagnostics nodeBuiltinImport:off -- Retain synthetic configuration fixtures in external TMPDIR for root cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { WorkstreamsNativeAttestationRequest } from "@t3tools/contracts";
import { EnvironmentId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Database from "../store/sql.ts";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as AuthRows from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import migration from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Records from "../store/peerSessionPinStore.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Registries from "../store/registryStore.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Setup from "../runtime/peerSessionPinSetup.ts";
import * as Owner from "../runtime/sourceEnrollment.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Guard from "./guard.ts";
import * as Boundary from "./peerWriteAdmission.ts";
import * as Home from "./homeTools.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Supplied from "./suppliedEvidence.ts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import { digest, forwardChildIds } from "../operations/forwardedHomeOperation.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import forwardMigration from "../../../persistence/Migrations/130_JonesWorkstreamsForwardReceipts.ts";
import * as Authentication from "../runtime/peerMcpAuthentication.ts";
import * as Session from "../registry/session.ts";
import * as DateTime from "effect/DateTime";

const encodeForwardWire = Schema.encodeSync(
  Schema.fromJsonString(Forward.WorkstreamsForwardRequest),
);
const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const receiver = {
  environmentId: "home:test",
  authorityNamespace: "receiver-store",
  storeGeneration: 1,
};
const signing = Layer.succeed(
  Secrets.ServerSecretStore,
  Secrets.ServerSecretStore.of({
    get: () => Effect.succeed(Option.some(Test.keyBytes)),
    getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
    set: () => Effect.die("unexpected set"),
    create: () => Effect.die("unexpected create"),
    remove: () => Effect.die("unexpected remove"),
  }),
);
const services = (options: { filename?: string; keys?: Map<string, Uint8Array> } = {}) =>
  Layer.mergeAll(Records.layer, Authentication.layer).pipe(
    Layer.provideMerge(Pins.nativeLayer),
    Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
    Layer.provideMerge(Test.testLayer(options)),
    Layer.provideMerge(
      Config.layerTest(
        process.cwd(),
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-home-config-")),
      ),
    ),
    Layer.provideMerge(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* Test.seed;
  yield* migration;
  const auth = yield* Auth.EnvironmentAuth,
    native = yield* Pins.NativePeerSessions,
    records = yield* Records.PeerSessionPinStore,
    sql = yield* SqlClient.SqlClient,
    rows = yield* AuthRows.AuthSessionRepository;
  const issued = yield* auth.issueMcpClientSession({
    label: "forged-label-is-not-authority",
    access: "full-access",
    peerLink: true,
    client: { deviceType: "bot" },
  });
  const scope: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("home:test"),
    issuedAt: Date.parse(Test.now),
    requestNamespace: "synthetic-request",
    capabilities: new Set(["orchestration"]),
    thread: undefined,
    client: {
      sessionId: issued.sessionId,
      label: "different-label",
      access: "full-access",
      linked: true,
    },
  };
  const request = {
    cookies: {},
    headers: { authorization: `Bearer ${issued.token}` },
  } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
  yield* (yield* Authentication.PeerMcpAuthentication).register(request, scope);
  const facts = yield* native.current(scope);
  const assertion = {
    schema: "jones-workstreams-peer-session-pin/1" as const,
    pin_id: "pin:test",
    revision: 1,
    receiver_authority: {
      environment_id: receiver.environmentId,
      authority_namespace: receiver.authorityNamespace,
      store_generation: 1,
    },
    session: facts.session,
    peer_environment_id: "source:test",
    roles: ["member_home"] as const,
    expected_home: home,
    source_authority: {
      environment_id: "source:test",
      authority_namespace: "source-store",
      store_generation: 1,
    },
    source_enrollment_id: "enrollment:test",
    source_binding_version: null,
    singleton_binding_version: null,
    state: "prepared" as const,
    expires_at: "2026-01-01T00:20:00.000Z",
    setup_request_id: "setup:test",
    owner_qualification_digest: "b".repeat(64),
    bind_receipt: null,
  };
  const body = JSON.stringify({
    schema: "jones-workstreams-peer-pin-setup/1",
    assertion,
    expected: { registry_version: 1, grant_id: "grant:test", grant_version: 1 },
  });
  const decoded = Records.decodeSetupRequest(body);
  const registry = yield* Registries.RegistryStore;
  yield* registry.appendPlain(Test.scope, "principals", {
    principal_id: "principal:test",
    kind: "environment",
    environment_id: "source:test",
    provenance: "{}",
  });
  yield* registry.appendPlain(Test.scope, "grants", {
    grant_id: "grant:test",
    principal_id: "principal:test",
    grant_version: 1,
    permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
    state: "active",
  });
  const receipt = {
    principal_id: "principal:test",
    binding_version: 1,
    registry_version: 2,
    request_sha256: "c".repeat(64),
  };
  let current = { ...receiver };
  const authority = Layer.succeed(Authority.NativeStoreAuthority, {
    readCurrent: Effect.suspend(() => Effect.succeed(current)),
    trustProvider: { read: () => Effect.die("unused") },
  } as unknown as Authority.NativeStoreAuthority["Service"]);
  const qualifier = Layer.succeed(Setup.PeerPinSetupQualification, {
    qualify: (exact: string, digest: string) =>
      exact === body && digest === decoded.pin.setup_request_sha256
        ? Effect.succeed({
            invocation: scope,
            owner_qualification_digest: assertion.owner_qualification_digest,
            checkCurrent: () => Effect.void,
          })
        : Effect.fail(new Pins.PeerSessionError({ code: "owner_unqualified" })),
    verifyBinding: (_p: unknown, r: unknown, v: number) =>
      JSON.stringify(r) === JSON.stringify(receipt) && v === 1
        ? Effect.void
        : Effect.fail(new Pins.PeerSessionError({ code: "conflict" })),
  });
  const nativeHome = {
    ...home,
    canonical_origin: "https://native.workstreams.invalid",
    served_by_environment_id: "home:test",
  };
  const originalKeys = yield* Keys.WorkstreamsKeyProvider;
  let keyCalls = 0,
    writerKeyViolations = 0,
    keysUnavailable = false;
  let nativeCalls = 0,
    authorityCalls = 0,
    sourceCalls = 0;
  let keyWait: Effect.Effect<void> = Effect.void;
  let sourceWait: Effect.Effect<void> = Effect.void;
  let authorityWait: Effect.Effect<void> = Effect.void;
  let beforeWriter: Effect.Effect<void> = Effect.void;
  const originalSource = new Map<string, string>();
  const sourceWireBodies = new Map<string, string>();
  let sourceDigestOverride: string | null = null;
  const watchedNative = Pins.NativePeerSessions.of({
    ...native,
    current: (invocation) =>
      Effect.sync(() => {
        nativeCalls++;
      }).pipe(Effect.andThen(native.current(invocation))),
  });
  const outside = () =>
    Effect.gen(function* () {
      keyCalls++;
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) {
        writerKeyViolations++;
        throw new Error("External key lookup in writer");
      }
      yield* keyWait;
    });
  const cachedInput = Keys.WorkstreamsKeyProvider.of({
    activeKeyId: (scope) => outside().pipe(Effect.andThen(originalKeys.activeKeyId(scope))),
    get: (id) =>
      outside().pipe(
        Effect.andThen(
          keysUnavailable
            ? Effect.fail(new Keys.KeyUnavailable({ reason: "missing" }))
            : originalKeys.get(id),
        ),
      ),
    indexKey: (id) => outside().pipe(Effect.andThen(originalKeys.indexKey(id))),
  });
  let sourceRevoked = false,
    readbackUnqualified = false;
  let onSourceCurrent: Effect.Effect<void> = Effect.void;
  const check = () =>
    Effect.suspend(() =>
      onSourceCurrent.pipe(
        Effect.andThen(
          sourceRevoked
            ? Effect.fail(new Session.RegistryError({ code: "source_revoked" }))
            : Effect.void,
        ),
      ),
    );
  // External current owner/store and original-source lookup are synthetic qualification fixtures.
  // Auth, NativePeerSessions, pins, Guard, request proof, domain, ledger, crypto and SQLite are real.
  const mounted = Layer.mergeAll(
    Setup.layer,
    Home.layer.pipe(
      Layer.provide(Layer.succeed(Keys.WorkstreamsKeyProvider, cachedInput)),
      Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
      Layer.provide(Layer.succeed(Pins.NativePeerSessions, watchedNative)),
    ),
  ).pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(Layer.succeed(NativeHome, nativeHome)),
    Layer.provideMerge(authority),
    Layer.provideMerge(qualifier),
    Layer.provideMerge(Layer.succeed(Owner.SourceSetupOwner, { verifyUnchanged: Effect.void })),
    Layer.provideMerge(
      Layer.succeed(Boundary.ReceivingAuthorityFence, {
        prepare: () =>
          Effect.sync(() => {
            authorityCalls++;
          }).pipe(
            Effect.andThen(Effect.suspend(() => authorityWait)),
            Effect.as({
              authority: receiver,
              checkCurrent: () =>
                current.storeGeneration === 1
                  ? Effect.void
                  : Effect.fail(new Session.RegistryError({ code: "authority_changed" })),
            }),
          ),
      }),
    ),
    Layer.provideMerge(
      Layer.succeed(Supplied.SourceForwardQualification, {
        prepare: (request, pin) =>
          Effect.gen(function* () {
            sourceCalls++;
            yield* sourceWait;
            const prepared = beforeWriter;
            beforeWriter = Effect.void;
            yield* prepared;
            const retained = originalSource.get(request.identity.source_operation_id);
            if (retained !== undefined && retained !== canonicalJson(request))
              return yield* new Session.RegistryError({ code: "source_mismatch" });
            if (!sourceWireBodies.has(request.identity.forward_id))
              sourceWireBodies.set(request.identity.forward_id, encodeForwardWire(request));
            return yield* request.identity.source_enrollment_session_id ===
              "source-enrollment-session" &&
            request.identity.source_enrollment_id === pin.source_enrollment_id &&
            request.identity.member_pin_id === "member-pin:test" &&
            request.identity.member_pin_bound_revision === 2 &&
            pin.pin_id === "pin:test" &&
            pin.revision === 2
              ? Effect.as(check(), {
                  forward_request_sha256:
                    sourceDigestOverride ?? digest(encodeForwardWire(request)),
                  checkCurrent: check,
                })
              : Effect.fail(new Session.RegistryError({ code: "source_mismatch" }));
          }),
        prepareReadback: (request, pin) =>
          readbackUnqualified || !sourceWireBodies.has(request.identity.forward_id)
            ? Effect.succeed({ state: "unknown" as const, reason: "not_found" as const })
            : request.identity.source_enrollment_session_id === "source-enrollment-session" &&
                request.identity.source_enrollment_id === pin.source_enrollment_id &&
                request.identity.member_pin_id === "member-pin:test" &&
                request.identity.member_pin_bound_revision === 2 &&
                pin.pin_id === "pin:test" &&
                pin.revision === 2
              ? Effect.as(check(), {
                  state: "authorized" as const,
                  forward_request_sha256:
                    sourceDigestOverride ??
                    digest(sourceWireBodies.get(request.identity.forward_id)!),
                  checkCurrent: check,
                })
              : Effect.fail(new Session.RegistryError({ code: "source_mismatch" })),
      }),
    ),
  );
  const bind = Effect.gen(function* () {
    const setup = yield* Setup.PeerSessionPinSetup;
    yield* registry.appendPlain(Test.scope, "source_bindings", {
      provider: "t3",
      source_instance_id: "source:test",
      binding_version: 1,
      authority_namespace: "source-store",
      store_generation: 1,
      state: "active",
    });
    return yield* setup.bind(assertion.pin_id, 1, receipt, 1);
  });
  return {
    auth,
    native,
    records,
    rows,
    sql,
    scope,
    request,
    body,
    pin: decoded.pin,
    receipt,
    registry,
    mounted,
    nativeHome,
    bind,
    revokeKey: () => {
      keysUnavailable = true;
    },
    withholdReadback: (value: boolean) => {
      readbackUnqualified = value;
    },
    keyEvidence: () => ({ keyCalls, writerKeyViolations }),
    observeSourceCurrent: (effect: Effect.Effect<void>) => {
      onSourceCurrent = effect;
    },
    preparationEvidence: () => ({ nativeCalls, authorityCalls, sourceCalls, keyCalls }),
    holdKeys: (wait: Effect.Effect<void>) => {
      keyWait = wait;
    },
    holdSource: (wait: Effect.Effect<void>) => {
      sourceWait = wait;
    },
    holdAuthority: (wait: Effect.Effect<void>) => {
      authorityWait = wait;
    },
    afterPrefetch: (effect: Effect.Effect<void>) => {
      beforeWriter = effect;
    },
    sourceDigest: (value: string | null) => {
      sourceDigestOverride = value;
    },
    retainSource: (request: Forward.WorkstreamsForwardRequest) => {
      originalSource.set(request.identity.source_operation_id, canonicalJson(request));
      sourceWireBodies.set(request.identity.forward_id, encodeForwardWire(request));
    },
    revokeSource: () => {
      sourceRevoked = true;
    },
    restore: () => {
      current = { ...current, storeGeneration: 2 };
    },
  };
});

const identityFor = (
  f: Effect.Success<typeof fixture>,
  forwardId: string,
  requestDigest: string,
  commandIds: readonly string[],
) => ({
  forward_id: "jwf-" + digest("source:test\0" + forwardId).slice(0, 48),
  source_operation_id: forwardId,
  parent_operation_id: null,
  command_ids: commandIds,
  request_sha256: requestDigest,
  recorded_caller_sha256: "d".repeat(64),
  expected_home: home,
  canonical_origin: "https://native.workstreams.invalid",
  source_authority: {
    environment_id: "source:test",
    authority_namespace: "source-store",
    store_generation: 1,
  },
  source_enrollment_id: "enrollment:test",
  source_enrollment_session_id: "source-enrollment-session",
  principal_id: "principal:test",
  singleton_binding_version: 1,
  source_binding_version: 1,
  member_pin_id: "member-pin:test",
  member_pin_bound_revision: 2,
  outbound_link_generation: "opaque-link-generation",
});
// The transport adapter supplies the exact authenticator-registered invocation per call.
const decodeForwardRequest = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardRequest);
const encodeAttestationRequest = Schema.encodeEffect(
  Schema.fromJsonString(WorkstreamsNativeAttestationRequest),
);
const requestTools = Effect.map(Home.HomeTools, (tools) => ({
  forward: Effect.fn("HomeToolsTest.forward")(function* (input: unknown) {
    const invocation = yield* Mcp.McpInvocationContext;
    return yield* tools
      .forward(input, invocation)
      .pipe(Effect.provideService(Layer.CurrentMemoMap, Layer.makeMemoMapUnsafe()));
  }),
  readback: Effect.fn("HomeToolsTest.readback")(function* (input: unknown) {
    const invocation = yield* Mcp.McpInvocationContext;
    return yield* tools
      .readback(input, invocation)
      .pipe(Effect.provideService(Layer.CurrentMemoMap, Layer.makeMemoMapUnsafe()));
  }),
}));
const createRequest = (
  f: Effect.Success<typeof fixture>,
  id = "operation-create-000001",
  whitespace = false,
) =>
  Effect.gen(function* () {
    const op = {
      contract: "workstream-operations/1.0.0" as const,
      operation_id: id,
      origin: { kind: "agent" as const },
      kind: "create_workstream" as const,
      registry: { registry_id: home.registry_instance_id, owner_id: home.owner_id },
      workstream: {
        name: "protected-workstream-sentinel",
        lifecycle: "active" as const,
        progress: { state: "unknown" as const },
        sort_order: 0,
      },
    };
    const body = JSON.stringify(op, null, whitespace ? 2 : undefined),
      sha = digest(body);
    const provisional = {
      contract:
        Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT as typeof Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
      kind: "operation" as const,
      identity: identityFor(f, id, sha, []),
      request: {
        expected_home: home,
        relay: {
          source_environment_id: "source:test",
          origin: op.origin,
          linked_origin_environment_id: null,
          hop: 1 as const,
        },
        request: op,
        request_sha256: sha,
      },
      request_body: body,
    };
    return {
      ...provisional,
      identity: { ...provisional.identity, command_ids: yield* forwardChildIds(provisional) },
    };
  });
type FixtureServices =
  | Layer.Success<ReturnType<typeof services>>
  | Layer.Success<Effect.Success<typeof fixture>["mounted"]>
  | Mcp.McpInvocationContext;
const run = <E>(
  test: (f: Effect.Success<typeof fixture>) => Effect.Effect<void, E, FixtureServices>,
  options: { filename?: string; keys?: Map<string, Uint8Array> } = {},
) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      yield* (yield* Setup.PeerSessionPinSetup).apply(f.body, 0);
      yield* f.bind;
      yield* forwardMigration;
      yield* test(f);
    }).pipe(Effect.provideService(Mcp.McpInvocationContext, f.scope), Effect.provide(f.mounted));
  }).pipe(Effect.provide(services(options)));

it.effect(
  "genuine registered peer commits create with raw children and canonical public digest; exact replay never reexecutes",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f, "operation-create-000001", true);
        expect(request.identity.forward_id).toMatch(/^jwf-[a-f0-9]{48}$/);
        expect(request.identity.forward_id).not.toBe(request.identity.source_operation_id);
        expect(request.identity.source_operation_id).toBe(request.request.request.operation_id);
        const result = yield* tools.forward(request);
        expect(result.kind).toBe("operation");
        if (result.kind !== "operation") return;
        expect(result.result.request_sha256).toBe(digest(canonicalJson(request.request.request)));
        expect(result.result.request_sha256).not.toBe(request.identity.request_sha256);
        expect(result.result.children.map((c) => c.command_id)).toEqual(
          request.identity.command_ids,
        );
        expect(result.result.state).toBe("committed");
        expect(f.keyEvidence().keyCalls).toBeGreaterThan(0);
        expect(f.keyEvidence().writerKeyViolations).toBe(0);
        const before = yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records");
        const ledger = yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts");
        expect(ledger).toMatchObject([
          { forward_id: request.identity.forward_id, state: "completed" },
        ]);
        expect(result.identity).toEqual(request.identity);
        expect(result.identity.member_pin_id).toBe("member-pin:test");
        expect(result.home.incoming_peer_pin_id).toBe("pin:test");
        expect(result.identity.member_pin_id).not.toBe(result.home.incoming_peer_pin_id);
        expect(result.result.operation_id).toBe(request.identity.source_operation_id);
        expect(yield* tools.forward(request)).toEqual(result);
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts")).toEqual(
          ledger,
        );
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records")).toEqual(before);
        const readback = yield* tools.readback({
          contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          identity: request.identity,
        });
        expect(readback).toEqual({
          contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          identity: request.identity,
          state: "present",
          result,
        });
        const changed = yield* createRequest(f, "operation-create-000001", false);
        expect((yield* Effect.result(tools.forward(changed)))._tag).toBe("Failure");
        expect(yield* f.sql.unsafe("SELECT state FROM jones_workstreams_forward_receipts")).toEqual(
          [{ state: "completed" }],
        );
      }),
    ),
);

it.effect(
  "raw command and appearance replay use authoritative stores and retained encrypted parent results",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools;
        const body = JSON.stringify({
          command_id: "command-create-00000001",
          expected_server_generation: 1,
          expected_registry_version: 2,
          action: {
            operation: "create_workstream",
            name: "Raw protected sentinel",
            lifecycle: "active",
            progress: { state: "unknown" },
            sort_order: 1,
          },
        });
        const request = {
          contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          kind: "command" as const,
          identity: identityFor(f, "command-create-00000001", digest(body), [
            "command-create-00000001",
          ]),
          request: {
            expected_home: home,
            relay: {
              source_environment_id: "source:test",
              origin: { kind: "agent" as const },
              linked_origin_environment_id: null,
              hop: 1 as const,
            },
            body,
            body_sha256: digest(body),
          },
        };
        const result = yield* tools.forward(request);
        expect(result.kind).toBe("command");
        if (result.kind !== "command" || result.result.state !== "committed") return;
        const id = result.result.effects.workstream_versions[0]!.workstream_id;
        const appearance = {
          command_id: "appearance-command-00001",
          expected_server_generation: 1,
          workstream_id: id,
          expected_version: 0,
          border_color: "#112233",
        };
        const appearanceBody = JSON.stringify(appearance, null, 1);
        const a = {
          contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          kind: "appearance" as const,
          identity: identityFor(f, appearance.command_id, digest(appearanceBody), [
            appearance.command_id,
          ]),
          request: {
            expected_home: home,
            relay: request.request.relay,
            request: appearance,
            request_body: appearanceBody,
            request_sha256: digest(appearanceBody),
          },
        };
        const first = yield* tools.forward(a);
        expect(first.kind).toBe("appearance");
        expect(yield* tools.forward(a)).toEqual(first);
        expect((yield* f.registry.list(Test.scope, "appearance"))[0]!.value).toEqual({
          workstream_id: id,
          border_color: "#112233",
          version: 1,
        });
        const rows = yield* f.sql.unsafe<{ request_envelope: string; result_envelope: string }>(
          "SELECT request_envelope,result_envelope FROM jones_workstreams_forward_receipts",
        );
        expect(JSON.stringify(rows)).not.toContain("protected");
        expect(JSON.stringify(rows)).not.toContain("#112233");
        expect(yield* tools.forward(request)).toEqual(result);
        expect(request.identity.source_operation_id).toBe(result.result.command_id);
        expect(a.identity.source_operation_id).toBe(appearance.command_id);
        for (const [input, retained] of [
          [request, result],
          [a, first],
        ] as const) {
          expect(input.identity.forward_id).toMatch(/^jwf-[a-f0-9]{48}$/);
          expect(input.identity.forward_id).not.toBe(input.identity.source_operation_id);
          expect(
            yield* tools.readback({
              contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
              identity: input.identity,
            }),
          ).toMatchObject({ state: "present", result: retained });
          const changed = {
            ...input,
            identity: { ...input.identity, source_operation_id: "substituted-command-0001" },
          };
          expect(yield* tools.forward(changed).pipe(Effect.flip)).toMatchObject({
            code: "invalid_request",
          });
        }
      }),
    ),
);

it.effect.each([
  "scope-clone",
  "session-revoked",
  "read-only",
  "plan",
  "source-revoked",
  "home-revoked",
  "foreign-source",
  "double-relay",
  "bad-digest",
  "bad-child",
] as const)("forward refuses %s before any child or parent persists", (fault) =>
  run((f) =>
    Effect.gen(function* () {
      const tools = yield* requestTools;
      let request = yield* createRequest(f);
      if (fault === "session-revoked")
        yield* f.rows.revoke({
          sessionId: f.pin.session.session_id as Parameters<typeof f.rows.revoke>[0]["sessionId"],
          revokedAt: DateTime.makeUnsafe(Test.now),
        });
      if (fault === "read-only") Object.assign(f.scope.client!, { access: "read-only" });
      if (fault === "plan") Object.assign(f.scope.client!, { interactionMode: "plan" });
      if (fault === "source-revoked") f.revokeSource();
      if (fault === "home-revoked") f.restore();
      if (fault === "foreign-source")
        request = {
          ...request,
          identity: {
            ...request.identity,
            source_authority: { ...request.identity.source_authority, environment_id: "foreign" },
          },
        };
      if (fault === "double-relay") Object.assign(request.request.relay, { hop: 2 });
      if (fault === "bad-digest")
        request = { ...request, request_body: request.request_body + " " };
      if (fault === "bad-child")
        request = {
          ...request,
          identity: { ...request.identity, command_ids: ["wrong-child-command-0001"] },
        };
      const execution = tools.forward(request);
      const result = yield* Effect.result(
        fault === "scope-clone"
          ? execution.pipe(Effect.provideService(Mcp.McpInvocationContext, { ...f.scope }))
          : execution,
      );
      expect(result._tag).toBe("Failure");
      expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts")).toHaveLength(
        0,
      );
      expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records")).toHaveLength(0);
    }),
  ),
);

it.effect(
  "unknown readback stays unknown; raw external verification never gets a home-local recovery caller",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        expect(
          yield* tools.readback({
            contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
            identity: request.identity,
          }),
        ).toMatchObject({ state: "unknown", reason: "not_found" });
        const body = JSON.stringify({
          command_id: "external-command-000001",
          expected_server_generation: 1,
          expected_registry_version: 2,
          action: {
            operation: "verify_reference",
            native_reference_id: "reference",
            expected_attestation_version: 0,
          },
        });
        const raw = {
          contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          kind: "command" as const,
          identity: identityFor(f, "external-command-000001", digest(body), [
            "external-command-000001",
          ]),
          request: {
            expected_home: home,
            relay: request.request.relay,
            body,
            body_sha256: digest(body),
          },
        };
        expect(yield* tools.forward(raw).pipe(Effect.flip)).toMatchObject({
          code: "peer_recovery_unmounted",
        });
        expect(
          yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts"),
        ).toHaveLength(0);
      }),
    ),
);

it.effect("closed fixed handlers reject unsupported protocols and unknown selector fields", () =>
  run((f) =>
    Effect.gen(function* () {
      const tools = yield* requestTools,
        request = yield* createRequest(f);
      expect(
        yield* tools.forward({ ...request, tool: "arbitrary_callback" }).pipe(Effect.flip),
      ).toMatchObject({ code: "unsupported_protocol" });
      expect(
        yield* tools
          .readback({ contract: "workstreams-native-forwarding/2", identity: request.identity })
          .pipe(Effect.flip),
      ).toMatchObject({ code: "unsupported_protocol" });
    }),
  ),
);

const primaryRequest = (
  f: Effect.Success<typeof fixture>,
  id: string,
  target: string,
  expected: { kind: "any" } | { kind: "primary"; workstream_id: string },
) =>
  Effect.gen(function* () {
    const op = {
      contract: "workstream-operations/1.0.0" as const,
      operation_id: id,
      origin: { kind: "agent" as const },
      kind: "set_primary" as const,
      thread: { environment_id: "source:test", thread_id: "native-thread-000001" },
      target: {
        kind: "workstream" as const,
        registry_id: home.registry_instance_id,
        owner_id: home.owner_id,
        workstream_id: target,
      },
      expected,
    };
    const body = JSON.stringify(op),
      sha = digest(body);
    const attestationRequest = {
      owner_id: home.owner_id,
      principal_id: "principal:test",
      identity: {
        provider: "t3" as const,
        source_instance_id: "source:test",
        native_id: op.thread.thread_id,
        resource_kind: "thread" as const,
        id_kind: "internal" as const,
        account_provenance: { kind: "not_account_scoped" as const },
      },
      expected_authority_namespace: "source-store",
      expected_store_generation: 1,
    };
    const encoded = yield* encodeAttestationRequest(attestationRequest);
    const at = Test.now,
      expires = "2026-01-01T00:05:00.000Z";
    const evidence = {
      protocol: "workstreams-t3-provider/2.0.0" as const,
      state: "attested" as const,
      request: attestationRequest,
      attestation: {
        provider: "t3" as const,
        source_instance_id: "source:test",
        native_id: op.thread.thread_id,
        authority_namespace: "source-store",
        store_generation: 1,
        evidence_sha256: digest(encoded + "\n" + at + "\n" + expires),
      },
      attested_at: at,
      expires_at: expires,
    };
    const provisional = {
      contract:
        Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT as typeof Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
      kind: "operation" as const,
      identity: identityFor(f, id, sha, []),
      request: {
        expected_home: home,
        relay: {
          source_environment_id: "source:test",
          origin: op.origin,
          linked_origin_environment_id: null,
          hop: 1 as const,
        },
        request: op,
        request_sha256: sha,
        evidence,
      },
      request_body: body,
    };
    return {
      ...provisional,
      identity: { ...provisional.identity, command_ids: yield* forwardChildIds(provisional) },
    };
  });
it.effect(
  "atomic supplied placement and multi-child primary move preserve history and both workstream CAS versions",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools;
        const a = yield* tools.forward(yield* createRequest(f, "operation-create-00000A")),
          b = yield* tools.forward(yield* createRequest(f, "operation-create-00000B"));
        if (
          a.kind !== "operation" ||
          a.result.state !== "committed" ||
          b.kind !== "operation" ||
          b.result.state !== "committed"
        )
          return;
        const aid = a.result.result.workstream_id!,
          bid = b.result.result.workstream_id!;
        const attach = yield* primaryRequest(f, "operation-attach-000001", aid, { kind: "any" });
        const first = yield* tools.forward(attach);
        expect(first.kind === "operation" && first.result.state).toBe("committed");
        if (first.kind !== "operation") return;
        expect(first.result.children.map((c) => c.operation)).toEqual([
          "register_reference",
          "verify_reference",
          "attach_primary",
        ]);
        const move = yield* primaryRequest(f, "operation-move-00000001", bid, {
          kind: "primary",
          workstream_id: aid,
        });
        const moved = yield* tools.forward(move);
        expect(moved.kind === "operation" && moved.result.state).toBe("committed");
        const episodes = yield* f.registry.list(Test.scope, "memberships");
        expect(episodes).toHaveLength(2);
        expect(
          episodes.filter((m) => (m.value as { closed: unknown }).closed === null),
        ).toHaveLength(1);
        expect(
          episodes.find((m) => (m.value as { closed: unknown }).closed === null)!.metadata
            .workstream_id,
        ).toBe(bid);
        const records = yield* f.registry.list(Test.scope, "records");
        expect(records.find((r) => r.metadata.workstream_id === aid)!.metadata.version).toBe(3);
        expect(records.find((r) => r.metadata.workstream_id === bid)!.metadata.version).toBe(2);
        const history = yield* f.sql.unsafe("SELECT * FROM jones_workstreams_history");
        expect(history.length).toBeGreaterThanOrEqual(8);
        expect(yield* tools.forward(attach)).toEqual(first);
        expect(yield* tools.forward(move)).toEqual(moved);
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_history")).toEqual(history);
        expect(
          yield* f.sql.unsafe("SELECT * FROM jones_workstreams_external_reservations"),
        ).toHaveLength(0);
      }),
    ),
);
it.effect.each([
  "stale-evidence",
  "foreign-evidence",
  "parent-id",
  "source-enrollment-session",
] as const)(
  "atomic placement refuses %s and leaves no parent, reference or partial membership",
  (fault) =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          created = yield* tools.forward(yield* createRequest(f));
        if (created.kind !== "operation" || created.result.state !== "committed") return;
        const request = yield* primaryRequest(
          f,
          "operation-failed-000001",
          created.result.result.workstream_id!,
          { kind: "any" },
        );
        if (fault === "stale-evidence")
          Object.assign(request.request.evidence, { expires_at: Test.now });
        if (fault === "foreign-evidence")
          Object.assign(request.request.evidence.attestation, { native_id: "foreign-thread" });
        if (fault === "parent-id")
          Object.assign(request.identity, { parent_operation_id: "different-parent-00001" });
        if (fault === "source-enrollment-session")
          Object.assign(request.identity, {
            source_enrollment_session_id: "foreign-source-session",
          });
        const before = yield* f.sql.unsafe(
          "SELECT registry_version FROM jones_workstreams_registries",
        );
        expect((yield* Effect.result(tools.forward(request)))._tag).toBe("Failure");
        expect(yield* f.registry.list(Test.scope, "references")).toHaveLength(0);
        expect(yield* f.registry.list(Test.scope, "memberships")).toHaveLength(0);
        expect(
          yield* f.sql.unsafe("SELECT registry_version FROM jones_workstreams_registries"),
        ).toEqual(before);
        expect(
          yield* f.sql.unsafe("SELECT forward_id FROM jones_workstreams_forward_receipts"),
        ).toEqual([{ forward_id: created.identity.forward_id }]);
      }),
    ),
);

it.effect.each(["membership-failure", "grant-revoked", "source-binding"] as const)(
  "a late %s rolls back registration, verification, membership, history and parent together",
  (fault) =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          created = yield* tools.forward(yield* createRequest(f));
        if (created.kind !== "operation" || created.result.state !== "committed") return;
        const request = yield* primaryRequest(
          f,
          "operation-rollback-0001",
          created.result.result.workstream_id!,
          { kind: "any" },
        );
        const before = {
          registry: yield* f.sql.unsafe("SELECT * FROM jones_workstreams_registries"),
          history: yield* f.sql.unsafe("SELECT * FROM jones_workstreams_history"),
          receipts: yield* f.sql.unsafe("SELECT * FROM jones_workstreams_receipts"),
        };
        if (fault === "membership-failure")
          yield* f.sql.unsafe(
            "CREATE TEMP TRIGGER jones_test_late_barrier AFTER INSERT ON jones_workstreams_memberships BEGIN SELECT RAISE(ABORT,'synthetic late child barrier'); END",
          );
        if (fault === "grant-revoked")
          yield* f.sql
            .unsafe(`CREATE TEMP TRIGGER jones_test_late_barrier AFTER INSERT ON jones_workstreams_memberships BEGIN
      INSERT INTO jones_workstreams_grants(registry_instance_id,owner_id,grant_id,principal_id,grant_version,permissions,state)
      SELECT registry_instance_id,owner_id,grant_id,principal_id,grant_version+1,'["workstreams:read"]','revoked'
      FROM jones_workstreams_grants WHERE grant_version=1; END`);
        if (fault === "source-binding")
          yield* f.sql
            .unsafe(`CREATE TEMP TRIGGER jones_test_late_barrier AFTER INSERT ON jones_workstreams_memberships BEGIN
      INSERT INTO jones_workstreams_source_bindings(registry_instance_id,owner_id,provider,source_instance_id,binding_version,authority_namespace,store_generation,state)
      SELECT registry_instance_id,owner_id,provider,source_instance_id,binding_version+1,authority_namespace,store_generation,'revoked'
      FROM jones_workstreams_source_bindings WHERE binding_version=1; END`);
        const failure = yield* tools.forward(request).pipe(Effect.flip);
        expect(failure.code).not.toBe("supplied_evidence_mismatch");
        expect(failure.code).not.toBe("supplied_verification_hook_unmounted");
        expect(yield* f.registry.list(Test.scope, "references")).toHaveLength(0);
        expect(yield* f.registry.list(Test.scope, "memberships")).toHaveLength(0);
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_registries")).toEqual(
          before.registry,
        );
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_history")).toEqual(
          before.history,
        );
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_receipts")).toEqual(
          before.receipts,
        );
        expect(
          yield* f.sql.unsafe("SELECT forward_id FROM jones_workstreams_forward_receipts"),
        ).toEqual([{ forward_id: created.identity.forward_id }]);
      }),
    ),
);
it.effect(
  "precondition conflict records an exact parent result without advancing children or workstream versions",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          created = yield* tools.forward(yield* createRequest(f));
        if (created.kind !== "operation" || created.result.state !== "committed") return;
        const request = yield* primaryRequest(
          f,
          "operation-conflict-0001",
          created.result.result.workstream_id!,
          { kind: "primary", workstream_id: "wrong-workstream" },
        );
        const before = yield* f.sql.unsafe(
          "SELECT registry_version FROM jones_workstreams_registries",
        );
        const result = yield* tools.forward(request);
        expect(result.kind === "operation" && result.result.state).toBe("conflict");
        if (result.kind === "operation") expect(result.result.children).toEqual([]);
        expect(yield* f.registry.list(Test.scope, "references")).toHaveLength(0);
        expect(
          yield* f.sql.unsafe("SELECT registry_version FROM jones_workstreams_registries"),
        ).toEqual(before);
        expect(yield* tools.forward(request)).toEqual(result);
      }),
    ),
);

it.effect("unavailable content key fails before parent or child acceptance", () =>
  run((f) =>
    Effect.gen(function* () {
      const tools = yield* requestTools,
        request = yield* createRequest(f);
      f.revokeKey();
      expect(yield* tools.forward(request).pipe(Effect.flip)).toMatchObject({
        code: "key_unavailable",
      });
      expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts")).toHaveLength(
        0,
      );
      expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records")).toHaveLength(0);
    }),
  ),
);

it.effect(
  "private write proof refuses copied callers/scopes and replacement native registration",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const request = yield* createRequest(f);
        const boundaryGraph = Boundary.sessionLayer.pipe(
          Layer.provideMerge(
            Boundary.layer.pipe(
              Layer.provideMerge(Guard.layer.pipe(Layer.provideMerge(Pins.layer))),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const boundary = yield* Boundary.PeerWriteBoundary,
            session = yield* Session.Session;
          const proof = yield* boundary.current(request);
          yield* session.authorize(proof.caller, "workstreams:write");
          expect(
            (yield* Effect.result(session.authorize({ ...proof.caller }, "workstreams:write")))
              ._tag,
          ).toBe("Failure");
          expect((yield* Effect.result(boundary.checkCurrent({ ...proof })))._tag).toBe("Failure");
          expect(
            (yield* Effect.result(
              boundary
                .checkCurrent(proof)
                .pipe(Effect.provideService(Mcp.McpInvocationContext, { ...f.scope })),
            ))._tag,
          ).toBe("Failure");
          yield* f.native.authenticate(f.request, f.scope);
          expect((yield* Effect.result(boundary.checkCurrent(proof)))._tag).toBe("Failure");
        }).pipe(
          Effect.provide(boundaryGraph),
          Effect.provideService(Boundary.CurrentSourceForwardFence, {
            forward_request_sha256: "a".repeat(64),
            checkCurrent: () => Effect.void,
          }),
        );
      }),
    ),
);

it.effect(
  "preflight unknown negotiates without a source intent and never releases a retained result",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        f.withholdReadback(true);
        const input = {
          contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
          identity: request.identity,
        };
        expect(yield* tools.readback(input)).toMatchObject({
          state: "unknown",
          reason: "not_found",
        });
        expect(
          yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts"),
        ).toHaveLength(0);
        f.withholdReadback(false);
        const committed = yield* tools.forward(request);
        f.withholdReadback(true);
        expect(yield* tools.readback(input)).toMatchObject({
          state: "unknown",
          reason: "not_found",
        });
        f.withholdReadback(false);
        expect(yield* tools.readback(input)).toMatchObject({ state: "present", result: committed });
      }),
    ),
);

it.effect.each(["forward", "readback"] as const)(
  "ambient actual SqlClient transaction refuses %s before native, authority, source or key preparation",
  (method) =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools;
        const request = yield* createRequest(f);
        const before = f.preparationEvidence();
        const input =
          method === "forward"
            ? request
            : {
                contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                identity: request.identity,
              };
        const execution =
          method === "forward"
            ? tools.forward(input).pipe(Effect.asVoid)
            : tools.readback(input).pipe(Effect.asVoid);
        const failure = yield* f.sql.withTransaction(execution).pipe(Effect.flip);
        expect(failure).toMatchObject({ code: "preparation_in_transaction" });
        expect(f.preparationEvidence()).toEqual(before);
        expect(
          yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts"),
        ).toHaveLength(0);
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records")).toHaveLength(0);
      }),
    ),
);

it.effect.each(["key", "authority", "source"] as const)(
  "an unrelated same-client writer commits while external %s preparation waits",
  (stage) =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const wait = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );
        if (stage === "key") f.holdKeys(wait);
        if (stage === "authority") f.holdAuthority(wait);
        if (stage === "source") f.holdSource(wait);
        yield* f.sql.unsafe("CREATE TEMP TABLE jones_home_writer_probe(value INTEGER)");
        const pending = yield* tools.forward(request).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* f.sql.withTransaction(
          f.sql.unsafe("INSERT INTO jones_home_writer_probe VALUES (1)"),
        );
        expect(yield* f.sql.unsafe("SELECT value FROM jones_home_writer_probe")).toEqual([
          { value: 1 },
        ]);
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending);
        expect(result.kind === "operation" && result.result.state).toBe("committed");
        expect(f.keyEvidence().writerKeyViolations).toBe(0);
      }),
    ),
);

const rotateHome = (f: Effect.Success<typeof fixture>) =>
  Effect.gen(function* () {
    yield* f.sql.unsafe(
      "UPDATE jones_workstreams_keys SET state='decrypt_only' WHERE key_id='test-home'",
    );
    yield* f.sql.unsafe(
      "INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) SELECT 'test-home-next',registry_instance_id,'home','active',created_at FROM jones_workstreams_keys WHERE key_id='test-home'",
    );
  });
it.effect.each(["before-writer", "held-source"] as const)(
  "active HOME ID drift %s fails without refill or retry",
  (stage) =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        // Retain the formerly accepted ID form to isolate rotation from the distinct-ID regression.
        request.identity.forward_id = request.identity.source_operation_id;
        const beforeCalls = f.keyEvidence().keyCalls;
        let failure: Session.RegistryError;
        if (stage === "before-writer") {
          f.afterPrefetch(rotateHome(f).pipe(Effect.orDie));
          failure = yield* tools.forward(request).pipe(Effect.flip);
        } else {
          const entered = yield* Deferred.make<void>(),
            release = yield* Deferred.make<void>();
          f.holdSource(
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          );
          const pending = yield* tools.forward(request).pipe(Effect.result, Effect.forkChild);
          yield* Deferred.await(entered);
          yield* f.sql.withTransaction(rotateHome(f));
          const preparedCalls = f.keyEvidence().keyCalls;
          yield* Deferred.succeed(release, undefined);
          const outcome = yield* Fiber.join(pending);
          expect(outcome._tag).toBe("Failure");
          if (outcome._tag !== "Failure") return;
          failure = outcome.failure;
          expect(f.keyEvidence().keyCalls).toBe(preparedCalls);
        }
        expect(failure).toMatchObject({ code: "key_unavailable" });
        expect(f.preparationEvidence().authorityCalls).toBe(1);
        expect(f.preparationEvidence().sourceCalls).toBe(1);
        expect(f.keyEvidence()).toEqual({ keyCalls: beforeCalls + 5, writerKeyViolations: 0 });
        expect(
          yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts"),
        ).toHaveLength(0);
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records")).toHaveLength(0);
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_history")).toHaveLength(0);
      }),
    ),
);
it.effect("same active HOME ID at writer admission still commits", () =>
  run((f) =>
    Effect.gen(function* () {
      const tools = yield* requestTools,
        request = yield* createRequest(f);
      f.afterPrefetch(
        f.sql
          .unsafe("SELECT active_key_id FROM jones_workstreams_registries")
          .pipe(Effect.asVoid, Effect.orDie),
      );
      const result = yield* tools.forward(request);
      expect(result.kind === "operation" && result.result.state).toBe("committed");
      expect(f.keyEvidence().writerKeyViolations).toBe(0);
    }),
  ),
);
it.effect.each(["source-id", "caller", "bytes"] as const)(
  "retained full forwarding identity refuses changed %s and leaves ledger/result unchanged",
  (fault) =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        const result = yield* tools.forward(request);
        const before = yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts");
        const changed = structuredClone(request);
        if (fault === "source-id")
          changed.identity.source_operation_id = "different-operation-0001";
        if (fault === "caller") changed.identity.recorded_caller_sha256 = "e".repeat(64);
        if (fault === "bytes") {
          changed.request_body += " ";
          changed.identity.request_sha256 = digest(changed.request_body);
          changed.request.request_sha256 = changed.identity.request_sha256;
          changed.identity.command_ids = yield* forwardChildIds(changed);
        }
        expect(yield* tools.forward(changed).pipe(Effect.flip)).toMatchObject({
          code: fault === "source-id" ? "unsupported_protocol" : "idempotency_conflict",
        });
        expect(
          yield* tools
            .readback({
              contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
              identity: changed.identity,
            })
            .pipe(Effect.flip),
        ).toMatchObject({ code: "idempotency_conflict" });
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts")).toEqual(
          before,
        );
        expect(yield* tools.forward(request)).toEqual(result);
      }),
    ),
);
it.effect(
  "synthetic exact source qualification refuses a substituted forward ID before acceptance",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        f.retainSource(request);
        const changed = structuredClone(request);
        changed.identity.forward_id = "jwf-" + "e".repeat(48);
        expect(yield* tools.forward(changed).pipe(Effect.flip)).toMatchObject({
          code: "source_mismatch",
        });
        expect(
          yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts"),
        ).toHaveLength(0);
        const result = yield* tools.forward(request);
        expect(yield* tools.forward(request)).toEqual(result);
      }),
    ),
);

it.effect(
  "distinct forwarding IDs remain encrypted in DB/WAL and exact raw/result associations survive reopen",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-home-reopen-"));
    const filename = NodePath.join(directory, "synthetic.sqlite");
    let identity: Forward.WorkstreamsForwardIdentity;
    let exactBody: string;
    let exactRaw: string;
    let committed: Forward.WorkstreamsForwardResult;
    return run(
      (f) =>
        Effect.gen(function* () {
          yield* f.sql.unsafe("PRAGMA journal_mode=WAL");
          const tools = yield* requestTools,
            request = yield* createRequest(f, "operation-reopen-00001", true);
          identity = request.identity;
          exactBody = JSON.stringify(yield* decodeForwardRequest(request));
          exactRaw = request.request_body;
          committed = yield* tools.forward(request);
          expect(yield* tools.forward(request)).toEqual(committed);
          for (const path of [filename, filename + "-wal"]) {
            expect(NodeFS.existsSync(path)).toBe(true);
            const bytes = NodeFS.readFileSync(path);
            expect(bytes.includes(Buffer.from("protected-workstream-sentinel"))).toBe(false);
            expect(bytes.includes(Buffer.from(request.request_body))).toBe(false);
          }
        }),
      { filename },
    ).pipe(
      Effect.andThen(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient,
            db = yield* Database.NativeSql;
          const rows = yield* sql.unsafe<{
            forward_id: string;
            wire_sha256: string;
            request_envelope: string;
            result_envelope: string;
          }>("SELECT * FROM jones_workstreams_forward_receipts");
          expect(rows).toHaveLength(1);
          expect(rows[0]!.forward_id).toBe(identity.forward_id);
          const context = {
            ...Test.scope,
            record_id: canonicalJson([
              identity.source_authority.environment_id,
              identity.forward_id,
            ]),
            revision: 1,
          };
          const retained = yield* db.openJson(
            { ...context, purpose: "forward_request" },
            rows[0]!.request_envelope,
          );
          expect(retained).toBe(exactBody);
          expect(rows[0]!.wire_sha256).toBe(digest(exactBody));
          expect(
            (
              JSON.parse(retained as string) as Extract<
                Forward.WorkstreamsForwardRequest,
                { kind: "operation" }
              >
            ).request_body,
          ).toBe(exactRaw);
          const saved = yield* db.openJson(
            { ...context, purpose: "forward_result" },
            rows[0]!.result_envelope,
          );
          expect(typeof saved === "string" && JSON.parse(saved)).toEqual(committed);
          expect(
            (JSON.parse(retained as string) as Forward.WorkstreamsForwardRequest).identity
              .source_operation_id,
          ).toBe("operation-reopen-00001");
          expect(
            (yield* Effect.result(
              db.openJson(
                { ...context, record_id: context.record_id + "changed", purpose: "forward_result" },
                rows[0]!.result_envelope,
              ),
            ))._tag,
          ).toBe("Failure");
        }).pipe(Effect.provide(Test.testLayer({ filename }))),
      ),
    );
  },
);

it.effect("retained old ciphertext keys remain readable after a later active ID rotation", () =>
  run(
    (f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        const result = yield* tools.forward(request);
        const before = yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts");
        yield* f.sql.withTransaction(
          rotateHome(f).pipe(
            Effect.andThen(
              f.sql.unsafe(
                "UPDATE jones_workstreams_registries SET active_key_id='test-home-next',registry_version=registry_version+1",
              ),
            ),
          ),
        );
        expect(yield* tools.forward(request)).toEqual(result);
        expect(
          yield* tools.readback({
            contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
            identity: request.identity,
          }),
        ).toMatchObject({ state: "present", result });
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts")).toEqual(
          before,
        );
        expect(f.keyEvidence().writerKeyViolations).toBe(0);
      }),
    {
      keys: new Map([
        ...Test.secrets(),
        ["workstreams-content-key-test-home-next", Uint8Array.from({ length: 32 }, () => 7)],
      ]),
    },
  ),
);
it.effect("fresh raw command source ID must match the exact command ID before acceptance", () =>
  run((f) =>
    Effect.gen(function* () {
      const tools = yield* requestTools,
        base = yield* createRequest(f);
      const command = {
        command_id: "command-source-check-01",
        expected_server_generation: 1,
        expected_registry_version: 2,
        action: {
          operation: "create_workstream",
          name: "protected",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        },
      };
      const body = JSON.stringify(command);
      const request = {
        contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
        kind: "command" as const,
        identity: identityFor(f, "different-source-id-001", digest(body), [command.command_id]),
        request: {
          expected_home: home,
          relay: base.request.relay,
          body,
          body_sha256: digest(body),
        },
      };
      expect(yield* tools.forward(request).pipe(Effect.flip)).toMatchObject({
        code: "invalid_request",
      });
      expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_forward_receipts")).toHaveLength(
        0,
      );
      expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records")).toHaveLength(0);
    }),
  ),
);

it.effect(
  "late active-ID drift rolls back a visibly committed child, registry/history and parent together",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const tools = yield* requestTools,
          request = yield* createRequest(f);
        const before = yield* f.sql.unsafe("SELECT * FROM jones_workstreams_registries");
        let observedCommittedChildInsideWriter = false;
        f.observeSourceCurrent(
          Effect.gen(function* () {
            const inWriter = Option.isSome(yield* Effect.serviceOption(f.sql.transactionService));
            const rows = yield* f.sql.unsafe<{ count: number }>(
              "SELECT COUNT(*) AS count FROM jones_workstreams_records",
            );
            const receipts = yield* f.sql.unsafe<{ state: string }>(
              "SELECT state FROM jones_workstreams_receipts",
            );
            if (
              !observedCommittedChildInsideWriter &&
              inWriter &&
              rows[0]!.count === 1 &&
              receipts.some((r) => r.state === "committed")
            ) {
              observedCommittedChildInsideWriter = true;
              // This synthetic bounded SQL fence injects drift only after observing the committed child.
              yield* rotateHome(f);
            }
          }).pipe(Effect.orDie),
        );
        const calls = f.keyEvidence().keyCalls;
        expect(yield* tools.forward(request).pipe(Effect.flip)).toMatchObject({
          code: "invalid_receipt",
        });
        expect(observedCommittedChildInsideWriter).toBe(true);
        expect(f.keyEvidence()).toEqual({ keyCalls: calls + 5, writerKeyViolations: 0 });
        expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_registries")).toEqual(before);
        for (const table of ["records", "receipts", "history", "forward_receipts"]) {
          expect(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_" + table)).toHaveLength(0);
        }
        expect(
          yield* f.sql.unsafe("SELECT key_id,state FROM jones_workstreams_keys WHERE scope='home'"),
        ).toEqual([{ key_id: "test-home", state: "active" }]);
      }),
    ),
);

it.effect("source envelope digest binds new publication and retained result release", () =>
  run((f) =>
    Effect.gen(function* () {
      const tools = yield* requestTools;
      const request = yield* createRequest(f);
      f.retainSource(request);
      f.sourceDigest("a".repeat(64));
      expect(yield* Effect.result(tools.forward(request))).toMatchObject({
        _tag: "Failure",
        failure: { code: "source_request_conflict" },
      });
      expect(yield* f.sql`SELECT forward_id FROM jones_workstreams_forward_receipts`).toEqual([]);
      f.sourceDigest(null);
      const result = yield* tools.forward(request);
      expect(result).toMatchObject({ kind: "operation", result: { state: "committed" } });
      const read = {
        contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
        identity: request.identity,
      };
      f.sourceDigest("b".repeat(64));
      expect(yield* Effect.result(tools.readback(read))).toMatchObject({
        _tag: "Failure",
        failure: { code: "source_request_conflict" },
      });
      expect(yield* f.sql`SELECT state FROM jones_workstreams_forward_receipts`).toEqual([
        { state: "completed" },
      ]);
      f.sourceDigest(null);
      expect(yield* tools.readback(read)).toMatchObject({ state: "present", result });
    }),
  ),
);
