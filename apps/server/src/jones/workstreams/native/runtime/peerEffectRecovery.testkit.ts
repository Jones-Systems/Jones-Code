// @effect-diagnostics nodeBuiltinImport:off -- Retained synthetic fixtures; root owns cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import { ServiceLauncherHostProcess } from "../../../../cloud/serviceLauncherClient.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../../../../cloud/serviceProtocol.ts";
import pinsMigration from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import parentMigration from "../../../persistence/Migrations/130_JonesWorkstreamsForwardReceipts.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Db from "../store/sql.ts";
import * as Index from "../crypto/identityIndex.ts";
import * as Store from "../store/effectStore.ts";
import * as PinsStore from "../store/peerSessionPinStore.ts";
import * as Registry from "../store/registryStore.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Pins from "./peerSessionPins.ts";
import * as CurrentFixture from "./currentAuthority.testkit.ts";
import * as Origins from "./peerEffectOrigin.ts";
import * as Retained from "./retainedPeerSession.ts";
import * as Envelope from "./peerEffectEnvelope.ts";
import type { ExternalIntent, StoredPrObservation } from "../domain/state.ts";

export const scope = Test.scope;
const home = {
  ...Test.scope,
  canonical_origin: "https://native.workstreams.invalid",
  home_environment_id: "home:test",
  served_by_environment_id: "home:test",
  server_generation: 1,
};
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const hash = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const ceiling = {
  access: "full-access",
  runtime_mode: "auto",
  interaction_mode: "default",
  scopes: ["orchestration:read", "orchestration:operate"],
  permissions: ["workstreams:read", "workstreams:write"],
} as const;
const signing = Layer.succeed(
  Secrets.ServerSecretStore,
  Secrets.ServerSecretStore.of({
    get: () => Effect.succeedSome(Test.keyBytes),
    getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
    set: () => Effect.die("Unexpected signing secret write"),
    create: () => Effect.die("Unexpected signing secret creation"),
    remove: () => Effect.die("Unexpected signing secret removal"),
  }),
);
type FixtureOperation = "request_native_t3_settlement" | "verify_reference" | "refresh_linked_pr";
const makeSeed = (
  operation: FixtureOperation,
  previous: StoredPrObservation | null,
  accepted: boolean,
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(Test.now));
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* Test.seed;
    yield* pinsMigration;
    yield* parentMigration;
    const sql = yield* SqlClient.SqlClient;
    const db = yield* Db.NativeSql;
    const registry = yield* Registry.RegistryStore;
    const auth = yield* Auth.EnvironmentAuth;
    const native = yield* Pins.NativePeerSessions;
    const records = yield* PinsStore.PeerSessionPinStore;
    const sessions = yield* Sessions.AuthSessionRepository;
    const issued = yield* auth.issueMcpClientSession({
      label: "synthetic-original-peer",
      access: "full-access",
      peerLink: true,
      client: { deviceType: "bot" },
    });
    const invocation: Mcp.McpInvocationScope = {
      environmentId: Wire.EnvironmentId.make("home:test"),
      issuedAt: Date.parse(Test.now),
      requestNamespace: "synthetic-peer-effect",
      capabilities: new Set(["orchestration"]),
      thread: undefined,
      client: {
        sessionId: issued.sessionId,
        label: "attribution only",
        access: "full-access",
        linked: true,
      },
    };
    const request = {
      cookies: {},
      headers: { authorization: "Bearer " + issued.token },
    } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
    yield* native.authenticate(request, invocation);
    const registration = yield* native.current(invocation);
    yield* registry.appendPlain(Test.scope, "principals", {
      principal_id: "principal:test",
      kind: "environment",
      environment_id: "source:test",
      provenance: "{}",
    });
    const grant = {
      owner_id: home.owner_id,
      principal_id: "principal:test",
      grant_id: "grant:test",
      grant_version: 1,
      permissions: ["workstreams:read", "workstreams:write"] as const,
      state: "active" as const,
    };
    yield* registry.appendPlain(Test.scope, "grants", {
      grant_id: grant.grant_id,
      principal_id: grant.principal_id,
      grant_version: grant.grant_version,
      state: grant.state,
      permissions: encode(grant.permissions),
    });
    yield* registry.appendPlain(Test.scope, "source_bindings", {
      provider: "t3",
      source_instance_id: "source:test",
      binding_version: 1,
      authority_namespace: "source-store",
      store_generation: 3,
      state: "active",
    });
    if (operation === "refresh_linked_pr") {
      yield* registry.appendPlain(Test.scope, "source_bindings", {
        provider: "github",
        source_instance_id: "github:test",
        binding_version: 1,
        authority_namespace: "github-authority",
        store_generation: 1,
        state: "active",
      });
    }
    const context = (yield* registry.context(home))!;
    const assertion = {
      schema: "jones-workstreams-peer-session-pin/1",
      pin_id: "pin:receiving-home",
      revision: 1,
      receiver_authority: {
        environment_id: "home:test",
        authority_namespace: "t3-native:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        store_generation: 1,
      },
      session: registration.session,
      peer_environment_id: "source:test",
      roles: ["member_home"],
      expected_home: {
        registry_instance_id: home.registry_instance_id,
        owner_id: home.owner_id,
        home_environment_id: home.home_environment_id,
        server_generation: 1,
      },
      source_authority: {
        environment_id: "source:test",
        authority_namespace: "source-store",
        store_generation: 3,
      },
      source_enrollment_id: "enrollment:original",
      source_binding_version: null,
      singleton_binding_version: null,
      state: "prepared",
      expires_at: "2026-01-01T00:20:00.000Z",
      setup_request_id: "setup:original",
      owner_qualification_digest: "b".repeat(64),
      bind_receipt: null,
    };
    const decoded = PinsStore.decodeSetupRequest(
      encode({
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion,
        expected: {
          registry_version: context.registry_version,
          grant_id: grant.grant_id,
          grant_version: 1,
        },
      }),
    );
    yield* records.insert({
      pin: decoded.pin,
      request_body: encode({
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion,
        expected: {
          registry_version: context.registry_version,
          grant_id: grant.grant_id,
          grant_version: 1,
        },
      }),
    });
    const pin = {
      ...decoded.pin,
      state: "bound" as const,
      revision: 2,
      source_binding_version: 1,
      singleton_binding_version: 1,
      bind_receipt: {
        principal_id: "principal:test",
        binding_version: 1,
        registry_version: context.registry_version,
        request_sha256: "c".repeat(64),
      },
    };
    yield* records.transition(pin, 1);
    const command: Wire.WorkstreamCommand = {
      command_id: "effect:peer-origin-original-0001",
      expected_server_generation: 1,
      expected_registry_version: context.registry_version,
      action:
        operation === "verify_reference"
          ? {
              operation: "verify_reference",
              native_reference_id: "ref:test",
              expected_attestation_version: 0,
            }
          : operation === "refresh_linked_pr"
            ? {
                operation: "refresh_linked_pr",
                workstream_id: "workstream:test",
                expected_version: 1,
                membership_id: "membership:test",
                expected_observation_version: previous?.observation_version ?? 0,
              }
            : {
                operation: "request_native_t3_settlement",
                native_reference_id: "ref:test",
                expected_attestation_version: 1,
                native_action: "settle",
              },
    };
    const body = " \n" + encode(command) + "\n";
    const receipt = {
      ...Test.pending(command.command_id, body),
      receipt: {
        ...Test.pending(command.command_id, body).receipt,
        operation: command.action.operation,
      },
    };
    const identity: Forward.WorkstreamsForwardIdentity = {
      forward_id: "jwf-original-peer-forward-0001",
      source_operation_id: command.command_id,
      parent_operation_id: null,
      command_ids: [command.command_id],
      request_sha256: hash(body),
      recorded_caller_sha256: "a".repeat(64),
      expected_home: assertion.expected_home,
      canonical_origin: home.canonical_origin,
      source_authority: assertion.source_authority,
      source_enrollment_id: assertion.source_enrollment_id,
      source_enrollment_session_id: "session:source-enrollment",
      principal_id: "principal:test",
      singleton_binding_version: 1,
      source_binding_version: 1,
      member_pin_id: "pin:source-member",
      member_pin_bound_revision: 2,
      outbound_link_generation: "source-to-home:original",
    };
    const forward: Forward.WorkstreamsForwardRequest = {
      contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
      kind: "command",
      identity,
      request: {
        expected_home: identity.expected_home,
        body,
        body_sha256: hash(body),
        relay: {
          hop: 1,
          source_environment_id: "source:test",
          origin: { kind: "agent" },
          linked_origin_environment_id: null,
        },
      },
    };
    const wire = "\n" + encode(forward) + "\n";
    let intent: ExternalIntent = {
      kind: operation === "request_native_t3_settlement" ? "native_t3_settlement" : operation,
      command,
      owner_id: home.owner_id,
      actor: { principal_id: "principal:test" },
      authorization: {
        owner_id: home.owner_id,
        actor: { principal_id: "principal:test" },
        grant,
        server_generation: 1,
      },
      request_sha256: hash(body),
      accepted_at: Test.now,
      admitted_registry_version: context.registry_version,
      reference: {
        native_reference_id: "ref:test",
        owner_id: home.owner_id,
        identity: {
          provider: "t3",
          source_instance_id: "source:test",
          resource_kind: "thread",
          id_kind: "internal",
          native_id: "thread:original",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: null,
        registration: {
          state: "verification-pending",
          attestation_version: 0,
          attested_at: null,
          expires_at: null,
          evidence: null,
        },
        created_at: Test.now,
        created_by: { principal_id: "principal:test" },
        created_registry_version: 1,
      },
      source_binding: {
        provider: "t3",
        source_instance_id: "source:test",
        authority_namespace: "source-store",
        store_generation: 3,
        state: "active",
      },
      membership: null,
      previous_observation: previous,
    };
    if (operation === "refresh_linked_pr") {
      const reference: Wire.NativeReference = {
        ...intent.reference,
        identity: {
          provider: "github",
          source_instance_id: "github:test",
          resource_kind: "pull_request",
          id_kind: "external",
          native_id: "jones/example#7",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: {
          host: "github.com",
          repository_owner: "jones",
          repository_name: "example",
          number: 7,
        },
        registration: {
          state: "attested",
          attestation_version: 1,
          attested_at: Test.now,
          expires_at: "2026-01-01T00:05:00.000Z",
          evidence: {
            provider: "github",
            source_instance_id: "github:test",
            native_id: "jones/example#7",
            authority_namespace: "github-authority",
            store_generation: 1,
            evidence_sha256: "e".repeat(64),
          },
        },
      };
      const sourceBinding: ExternalIntent["source_binding"] = {
        provider: "github",
        source_instance_id: "github:test",
        authority_namespace: "github-authority",
        store_generation: 1,
        state: "active",
      };
      const membership: Wire.MembershipEpisode = {
        membership_id: "membership:test",
        workstream_id: "workstream:test",
        native_reference_id: "ref:test",
        kind: "primary",
        opened: {
          at: Test.now,
          actor: intent.actor,
          command_id: "seed:membership-original",
          registry_version: 1,
        },
        closed: null,
      };
      intent = { ...intent, reference, source_binding: sourceBinding, membership };
      const workstream: Wire.Workstream = {
        workstream_id: "workstream:test",
        owner_id: home.owner_id,
        name: "Synthetic PR workstream",
        lifecycle: "active",
        progress: { state: "unknown" },
        delivery: "none-observed",
        freshness: "unknown",
        sort_order: 0,
        version: 1,
        created_at: Test.now,
        updated_at: Test.now,
        created_by: intent.actor,
        updated_by: intent.actor,
      };
      yield* registry.put(
        Test.scope,
        "records",
        {
          metadata: { workstream_id: workstream.workstream_id, version: 1, sort_order: 0 },
          value: workstream,
        },
        null,
      );
      yield* registry.put(
        Test.scope,
        "references",
        {
          metadata: {
            native_reference_id: reference.native_reference_id,
            provider: reference.identity.provider,
            resource_kind: reference.identity.resource_kind,
            id_kind: reference.identity.id_kind,
            identity_index: yield* (yield* Index.WorkstreamsIdentityIndex).digest(
              Test.scope,
              reference.identity,
            ),
            legacy_identity_sha256: null,
            attestation_version: reference.registration.attestation_version,
            registration_state: reference.registration.state,
            created_by_principal: intent.actor.principal_id,
            routing_source_instance_id: null,
            routing_native_id: null,
            routing_authority_namespace: null,
            routing_store_generation: null,
            routing_attested_at: null,
            routing_expires_at: null,
            routing_evidence_sha256: null,
          },
          value: reference,
        },
        null,
      );
      yield* registry.put(
        Test.scope,
        "memberships",
        {
          metadata: {
            membership_id: membership.membership_id,
            workstream_id: membership.workstream_id,
            native_reference_id: reference.native_reference_id,
            kind: membership.kind,
            opened_registry_version: 1,
            closed_registry_version: null,
            closed_at: null,
          },
          value: membership,
        },
        null,
      );
      if (previous)
        yield* registry.put(
          Test.scope,
          "pr_observations",
          {
            metadata: {
              native_reference_id: previous.native_reference_id,
              observation_version: previous.observation_version,
              attempted_at: previous.attempted_at,
              outcome: previous.outcome,
            },
            value: previous,
          },
          null,
        );
    }
    const envelope = yield* Envelope.wrapPeerEffectIntent(
      encode(intent),
      body,
      hash(body),
      {
        schema: "jones-workstreams-peer-effect-recovery/2",
        kind: "source-recorded-forward",
        incoming_peer_pin_id: pin.pin_id,
        incoming_peer_pin_revision: pin.revision,
        source_readmission: {
          contract: "workstreams-source-readmission/2.0.0",
          accepted_forward: identity,
          command_id: command.command_id,
          command_request_sha256: hash(body),
        },
        original_ceiling: ceiling,
      },
      hash(wire),
      "home-to-source:original",
    );
    // Synthetic acceptance uses the real encrypted stores and parent schema in one outer transaction.
    // This does not qualify the not-yet-mounted initial PEER acceptance producer.
    const parentEnvelope = yield* db.sealJson(
      {
        ...Test.scope,
        purpose: "forward_request",
        record_id: canonical([identity.source_authority.environment_id, identity.forward_id]),
        revision: 1,
      },
      wire,
    );
    const effects = yield* Store.EffectStore;
    if (accepted)
      yield* db.transaction(
        Test.scope,
        Effect.gen(function* () {
          yield* sql`INSERT INTO jones_workstreams_forward_receipts ${sql.insert({
            ...Test.scope,
            source_environment_id: "source:test",
            forward_id: identity.forward_id,
            request_sha256: hash(body),
            identity_sha256: hash(canonical(identity)),
            wire_sha256: hash(wire),
            state: "accepted",
            request_envelope: parentEnvelope,
            result_envelope: null,
          })}`;
          yield* effects.reserve(Test.scope, receipt, {
            command_id: command.command_id,
            principal_id: "principal:test",
            native_reference_id: "ref:test",
            effect: command.action.operation as FixtureOperation,
            server_generation: 1,
            admitted_registry_version: context.registry_version,
            intent,
            peer_recovery: envelope,
          });
        }),
      );
    return {
      sql,
      db,
      registry,
      auth,
      sessions,
      native,
      records,
      invocation,
      registration,
      authenticateOriginal: () => native.authenticate(request, invocation),
      command,
      sessionId: issued.sessionId,
      envelope,
      pin,
      identity,
      receipt,
      intent,
      forward,
      wire,
      effects,
    };
  });
type Seed = Effect.Success<ReturnType<typeof makeSeed>>;
export const withFixture = <A, E, R>(
  use: (
    fixture: Seed & {
      baseDir: string;
      keys: Map<string, Uint8Array>;
      setSecretHook: (hook: (name: string) => Effect.Effect<void>) => void;
    },
  ) => Effect.Effect<A, E, R>,
  operation: FixtureOperation = "request_native_t3_settlement",
  previous: StoredPrObservation | null = null,
  options: { readonly accepted: boolean } = { accepted: true },
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const parent = process.env.NW_PEER_EFFECT_EVIDENCE_DIR ?? NodeOS.tmpdir();
      NodeFS.mkdirSync(parent, { recursive: true });
      const baseDir = NodeFS.realpathSync(
        NodeFS.mkdtempSync(NodePath.join(parent, "peer-effect-")),
      );
      NodeFS.chmodSync(baseDir, 0o700);
      const state = NodePath.join(baseDir, "userdata");
      NodeFS.mkdirSync(state, { mode: 0o700 });
      const database = NodePath.join(state, "statev2.sqlite");
      const keys = Test.secrets();
      let secretHook: (name: string) => Effect.Effect<void> = () => Effect.void;
      const secrets = Secrets.ServerSecretStore.of({
        get: (name) =>
          Effect.suspend(() => secretHook(name)).pipe(
            Effect.as(Option.fromNullishOr(keys.get(name))),
          ),
        set: () => Effect.die("Unexpected content key write"),
        create: () => Effect.die("Unexpected content key creation"),
        remove: () => Effect.die("Unexpected key removal"),
        getOrCreateRandom: () => Effect.die("Unexpected automatic key generation"),
      });
      return yield* Effect.gen(function* () {
        // The file is created by the actual selected SqlClient before owner qualification.
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE writer_probe(value TEXT)`;
        const current = yield* CurrentFixture.prepare({
          home: baseDir,
          database,
          context: {
            source_instance_id: "home:test",
            authority_namespace: "t3-native:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            store_generation: 1,
          },
        });
        const config = Context.get(
          yield* Layer.build(Config.layerTest(baseDir, baseDir)),
          Config.ServerConfig,
        );
        const host = ServiceLauncherHostProcess.of({
          connected: true,
          send: () => {
            throw new Error("No update permitted");
          },
          on: () => undefined,
          off: () => undefined,
        });
        const runtime = Layer.mergeAll(
          Layer.succeed(Config.ServerConfig, config),
          Layer.succeed(ServiceLauncherHostProcess, host),
          Layer.succeed(HostProcess.Environment, {
            [SERVICE_LAUNCHER_CONTEXT_ENV]: encode({
              protocol: SERVICE_LAUNCHER_PROTOCOL,
              childVersion: "0.0.0-preview.20261002.100",
            }),
          }),
        );
        const service = Layer.mergeAll(Origins.layer, Retained.layer).pipe(
          Layer.provideMerge(PinsStore.layer),
          Layer.provideMerge(
            Pins.nativeLayer.pipe(Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing)))),
          ),
          Layer.provideMerge(current),
          Layer.provideMerge(Authority.layer),
          Layer.provideMerge(Environment.layerIdentity),
          Layer.provideMerge(Layer.succeed(NativeHome, home)),
          Layer.provideMerge(runtime),
        );
        return yield* Effect.gen(function* () {
          const fixture = yield* makeSeed(operation, previous, options.accepted);
          return yield* use({
            ...fixture,
            baseDir,
            keys,
            setSecretHook: (hook) => {
              secretHook = hook;
            },
          });
        }).pipe(Effect.provide(service));
      }).pipe(Effect.provide(Test.testLayer({ filename: database, secrets })));
    }),
  ).pipe(Effect.provide(NodeServices.layer));
