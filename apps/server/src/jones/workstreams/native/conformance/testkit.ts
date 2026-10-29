import { workstreamsHttpResponseHeaders } from "@t3tools/contracts/jones/workstreamsHttp";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
// @effect-diagnostics nodeBuiltinImport:off -- Fresh synthetic databases are retained under task-owned evidence; cleanup belongs to root.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";

import * as Wire from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as Clock from "effect/Clock";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as TestClock from "effect/testing/TestClock";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as Schema from "effect/Schema";
import { FetchHttpClient, HttpClient } from "effect/http";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as Fixture from "../store/sql.testkit.ts";
import * as Admin from "../cli.ts";
import * as Operations from "../../operations/service.ts";
import * as OperationStore from "../store/operationStore.ts";
import * as Startup from "../runtime/startup.ts";
import * as Entry from "../runtime/entry.ts";
import * as Admission from "../runtime/admission.ts";
import { RecordedCallerFence } from "../runtime/recordedCaller.ts";
import * as CurrentAuthorityFixture from "../runtime/currentAuthority.testkit.ts";
import * as RegistrationContext from "../../registrationContext/service.ts";
import * as Runtime from "../../runtimeIntegration/native.ts";
import * as Placements from "../registry/placements.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../../../../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../../../../orchestration-v2/ThreadManagementService.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeAttempts from "../../nativeProvider/attemptRepository.ts";
import attemptsMigration from "../../../persistence/Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import proofsMigration from "../../../persistence/Migrations/107_JonesWorkstreamsSettlementProofs.ts";
import nativeMigration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import enrollmentMigration from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import * as SourceSetup from "../runtime/sourceSetup.ts";
import * as SourceEnrollment from "../runtime/sourceEnrollment.ts";
import * as SourceRecords from "../store/sourceEnrollmentStore.ts";
import { NativeProviderEnrollmentError } from "../../nativeProvider/enrollment.ts";
import { enrollmentRequest as originalEnrollmentRequest } from "../../enrollment/testFixtures.ts";
import { makeProviderFixture } from "../../nativeProvider/testFixtures.ts";
import {
  makeWorkstreamsNativeProvider,
  type NativeProviderPorts,
} from "../../nativeProvider/service.ts";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "../../../../workstreams/http.ts";

const enrollmentRequest = {
  ...originalEnrollmentRequest,
  context: {
    ...originalEnrollmentRequest.context,
    authority_namespace: "t3-native:12345678-1234-4234-8234-123456789abc",
  },
};

export const registry = {
  ...Fixture.scope,
  owner_id: enrollmentRequest.context.owner_id,
  canonical_origin: enrollmentRequest.registry_origin,
  home_environment_id: enrollmentRequest.context.source_instance_id,
  server_generation: 1,
};
export const principal = {
  sessionId: Wire.AuthSessionId.make("startup-http"),
  subject: "synthetic-local-browser",
  method: "bearer-access-token" as const,
  scopes: new Set([Wire.AuthOrchestrationReadScope, Wire.AuthOrchestrationOperateScope]),
};
export const fresh = () => {
  const parent = NodePath.join(NodeOS.tmpdir(), "nw-s1-ingress-fixtures");
  NodeFS.mkdirSync(parent, { recursive: true });
  const home = NodeFS.mkdtempSync(NodePath.join(parent, "entry-")),
    database = NodePath.join(home, "userdata", "statev2.sqlite");
  NodeFS.chmodSync(home, 0o700);
  NodeFS.mkdirSync(NodePath.dirname(database), { mode: 0o700 });
  return {
    home,
    database,
    layer: Admin.layer.pipe(
      Layer.provideMerge(
        Fixture.testLayer({
          filename: database,
          keys: new Map([...Fixture.secrets(), ["server-signing-key", Fixture.keyBytes]]),
        }),
      ),
      Layer.provideMerge(NodeServices.layer),
    ),
  };
};
export const seed = Effect.fn("NativeIngress.seed")(function* (
  f: ReturnType<typeof fresh>,
  initialize = true,
  options: {
    nativeOutcome?: "committed" | "denied" | "pending";
    callerExpiryMs?: number;
    ownerId?: string;
  } = {},
) {
  const selectedEnrollment = {
    ...enrollmentRequest,
    context: { ...enrollmentRequest.context, owner_id: options.ownerId ?? registry.owner_id },
  };
  const selectedRegistry = { ...registry, owner_id: selectedEnrollment.context.owner_id };
  yield* TestClock.setTime(Date.parse(selectedEnrollment.session.issued_at));
  if (initialize) {
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* attemptsMigration;
    yield* proofsMigration;
    yield* nativeMigration;
    yield* enrollmentMigration;
    yield* (yield* Admin.NativeAdmin).initialize({
      registry: selectedRegistry,
      principal_id: selectedEnrollment.context.principal_id,
      content_key_id: "test-home",
      local_key_id: "test-local",
    });
  }
  const sessions = yield* AuthSessions.make;
  const issuedAt = DateTime.makeUnsafe(selectedEnrollment.session.issued_at),
    expiresAt = DateTime.makeUnsafe(
      options.callerExpiryMs === undefined
        ? selectedEnrollment.session.expires_at
        : Date.parse(selectedEnrollment.session.issued_at) + options.callerExpiryMs,
    );
  const client = {
    label: null,
    ipAddress: null,
    userAgent: null,
    deviceType: "unknown" as const,
    os: null,
    browser: null,
  };
  if (initialize)
    yield* sessions.create({
      ...principal,
      scopes: [...principal.scopes],
      issuedAt,
      expiresAt,
      client,
    });
  if (initialize)
    yield* sessions.create({
      ...principal,
      sessionId: Wire.AuthSessionId.make("startup-mcp"),
      subject: "mcp-client",
      scopes: [...principal.scopes],
      issuedAt,
      expiresAt,
      client,
    });
  if (initialize)
    yield* sessions.create({
      ...principal,
      sessionId: Wire.AuthSessionId.make("startup-cli"),
      subject: "cli-issued-session",
      scopes: [...principal.scopes],
      issuedAt,
      expiresAt,
      client,
    });
  const provider = makeProviderFixture();
  const currentOwner = yield* CurrentAuthorityFixture.prepare({
    ...f,
    context: selectedEnrollment.context,
  });
  const sql = yield* SqlClient.SqlClient;
  const attempts = yield* NativeAttempts.NativeProviderAttempts.pipe(
    Effect.provide(NativeAttempts.NativeProviderAttemptsLive),
  );
  const authority: Authority.NativeStoreAuthority["Service"] = {
    ...provider.ports.authority,
    readCurrent: Effect.succeed({
      environmentId: selectedEnrollment.context.source_instance_id,
      authorityNamespace: selectedEnrollment.context.authority_namespace,
      storeGeneration: selectedEnrollment.context.store_generation,
    }),
    trustProvider: {
      readTrustSnapshot: () => ({
        readiness: "ready",
        trustedEnvironments: [
          {
            environmentId: selectedRegistry.home_environment_id,
            authorityNamespace: selectedEnrollment.context.authority_namespace,
            storeGeneration: selectedEnrollment.context.store_generation,
          },
        ],
      }),
    },
  };
  const nativeSources = yield* SourceEnrollment.NativeSourceEnrollments.pipe(
    Effect.provide(
      SourceEnrollment.layer.pipe(
        Layer.provide(
          Layer.succeed(SourceEnrollment.SourceSetupOwner, { verifyUnchanged: Effect.void }),
        ),
        Layer.provideMerge(SourceRecords.layer),
        Layer.provide(Layer.succeed(AuthSessions.AuthSessionRepository, sessions)),
        Layer.provide(
          Layer.succeed(SourceEnrollment.SourceIssuer, {
            authority,
            build: Effect.succeed(Option.some(selectedEnrollment.context.build)),
          }),
        ),
      ),
    ),
  );
  if (initialize)
    yield* Effect.gen(function* () {
      yield* (yield* SourceSetup.NativeSourceSetup).apply({
        schema: "jones-code.native-source-setup/v1",
        enrollment: selectedEnrollment,
        expected: {
          registry_instance_id: selectedRegistry.registry_instance_id,
          owner_id: selectedRegistry.owner_id,
          home_environment_id: selectedRegistry.home_environment_id,
          home_generation: 1,
          registry_version: 1,
          singleton_binding_version: 1,
          grant_id: "grant:" + selectedEnrollment.context.principal_id,
          grant_version: 1,
          source_binding: null,
        },
        rebind: false,
      });
    }).pipe(
      Effect.provide(SourceSetup.layer.pipe(Layer.provide(SourceRecords.layer))),
      Effect.provideService(SourceEnrollment.NativeSourceEnrollments, nativeSources),
      Effect.provideService(SourceEnrollment.SourceSetupOwner, { verifyUnchanged: Effect.void }),
    );
  const runtime: Runtime.NativeWorkstreamsRuntime["Service"] = {
    provider: makeWorkstreamsNativeProvider({ ...provider.ports, authority }),
    providerV2: makeWorkstreamsNativeProvider(
      {
        ...provider.ports,
        authority,
        attempts,
        engine: {
          dispatch: Effect.fn("NativeIngress.syntheticDispatch")(function* (
            command: Parameters<NativeProviderPorts["engine"]["dispatch"]>[0],
          ) {
            if (command.type !== "thread.settle" && command.type !== "thread.unsettle")
              throw new Error("Unexpected synthetic command");
            const attempt = (yield* sql<{
              dispatch_started_at: string | null;
            }>`SELECT dispatch_started_at FROM workstreams_native_attempts WHERE native_command_id = ${command.commandId}`.pipe(
              Effect.orDie,
            ))[0];
            if (!attempt || attempt.dispatch_started_at === null)
              throw new Error("Provider dispatch preceded durable admission");
            provider.calls.push({
              commandId: command.commandId,
              threadId: command.threadId,
              action: command.type,
            });
            if (options.nativeOutcome === "denied")
              provider.receipts.set(command.commandId, {
                commandId: command.commandId,
                aggregateKind: "thread",
                aggregateId: command.threadId,
                commandType: command.type,
                acceptedAt: selectedEnrollment.session.issued_at,
                resultSequence: 1,
                status: "rejected",
                error: "Synthetic source policy refusal",
              });
            else if (options.nativeOutcome !== "pending")
              provider.commit(
                command.commandId,
                command.type === "thread.settle" ? "settle" : "unsettle",
              );
            return { sequence: 1, storedEvents: [] };
          }),
        },
      },
      "v2",
    ),
    enrollments: {
      getBySessionId: (id) =>
        nativeSources
          .getBySessionId(id)
          .pipe(Effect.mapError(() => new NativeProviderEnrollmentError())),
    },
  };
  const config = {
    mode: "native-home",
    registry: selectedRegistry,
    bootstrap: {
      home: f.home,
      database: f.database,
      key_provider: "server-secret-store",
      source_session_id: selectedEnrollment.session.session_id,
    },
  };
  const selection = yield* Startup.select({
    text: canonicalJson(config),
    legacyActivationPresent: false,
    home: f.home,
    database: f.database,
    environmentId: selectedRegistry.home_environment_id,
  });
  if (selection.home === null) return yield* Effect.die(selection.problem);
  const services = Startup.homeServicesLayer({ ...selection, home: selection.home }).pipe(
    Layer.provideMerge(currentOwner),
    Layer.provide(
      Layer.succeed(Admission.RecordedThreadLookup, {
        getThreadShell: (id) =>
          Effect.succeed(id === "thread-synthetic" ? liveThreadShell(id) : null),
        getThreadRecords: () => Effect.fail(new RecordedCallerFence({ code: "not_granted" })),
      }),
    ),
    Layer.provide(Layer.succeed(SourceEnrollment.NativeSourceEnrollments, nativeSources)),
    Layer.provide(Layer.succeed(Runtime.NativeWorkstreamsRuntime, runtime)),
    Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, authority)),
    Layer.provide(Layer.succeed(Placements.PlacementTrust, authority.trustProvider)),
    Layer.provide(Layer.succeed(AuthSessions.AuthSessionRepository, sessions)),
    Layer.provide(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(
          Wire.EnvironmentId.make(selectedRegistry.home_environment_id),
        ),
      }),
    ),
    Layer.provide(
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getThreadShell: (id) =>
          Effect.succeed(id === "thread-synthetic" ? liveThreadShell(id) : null),
      }),
    ),
    Layer.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: (id) =>
          Effect.succeed(id === "thread-synthetic" ? liveThreadShell(id) : null),
      }),
    ),
  );
  return {
    services,
    sessions,
    provider,
    nativeSources,
    authority,
    selection,
    registry: selectedRegistry,
  };
});

import { expect } from "@effect/vitest";
import * as ServerSecretStore from "../../../../auth/ServerSecretStore.ts";
import * as EnvironmentAuth from "../../../../auth/EnvironmentAuth.ts";
import { layerAuthenticatedAuth } from "../../../../auth/http.ts";
import * as ServerConfig from "../../../../config.ts";
import * as Store from "../store/registryStore.ts";
import * as Local from "../registry/localTransport.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import { canonicalJson } from "../domain/validation.ts";
import type {
  WorkstreamCreateRequest,
  WorkstreamSetPrimaryRequest,
  WorkstreamOperationRequest,
} from "@t3tools/contracts/jones/workstreamOperations";

export {
  Effect,
  Layer,
  Wire,
  Operations,
  Gateway,
  Store,
  OperationStore,
  Startup,
  Entry,
  TestClock,
  DateTime,
  enrollmentRequest,
  canonicalJson,
};
export const scope = {
  registry_instance_id: registry.registry_instance_id,
  owner_id: registry.owner_id,
};
export const app = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal));

export const harnessFor = (gateway: Gateway.WorkstreamGateway["Service"]) =>
  Effect.sync(() => {
    let sequence = 0;
    const command = (action: Wire.WorkstreamCommand["action"]) =>
      Effect.map(gateway.readSession(), (session) => ({
        command_id: "ingress-command-" + String(++sequence).padStart(16, "0"),
        expected_server_generation: session.serverGeneration,
        expected_registry_version: session.registryVersion,
        action,
      }));
    const submit = (action: Wire.WorkstreamCommand["action"]) =>
      Effect.flatMap(command(action), (value) => gateway.submit(value));
    const committed = (action: Wire.WorkstreamCommand["action"]) =>
      Effect.map(submit(action), (receipt) => {
        expect(receipt.state, canonicalJson(receipt)).toBe("committed");
        if (receipt.state !== "committed") throw new Error(canonicalJson(receipt));
        return receipt;
      });
    const create = (name = "Synthetic ingress Workstream", sort_order = 0) =>
      Effect.map(
        committed({
          operation: "create_workstream",
          name,
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order,
        }),
        (receipt) => receipt.effects.workstream_versions[0]!.workstream_id,
      );
    const version = (id: string) => Effect.map(gateway.readDetail(id), (d) => d.workstream.version);
    const register = (nativeId = "thread-synthetic") =>
      Effect.map(
        committed({
          operation: "register_reference",
          identity: {
            provider: "t3",
            source_instance_id: registry.home_environment_id,
            resource_kind: "thread",
            id_kind: "internal",
            native_id: nativeId,
            account_provenance: { kind: "not_account_scoped" },
          },
          pr_locator: null,
        }),
        (receipt) => receipt.effects.native_reference_id!,
      );
    const verify = (id: string, expected_attestation_version = 0) =>
      committed({
        operation: "verify_reference",
        native_reference_id: id,
        expected_attestation_version,
      });
    const attach = Effect.fn("NativeIngress.attach")(function* (
      id: string,
      ref: string,
      secondary = false,
    ) {
      return yield* committed({
        operation: secondary ? "link_secondary" : "attach_primary",
        workstream_id: id,
        expected_version: yield* version(id),
        native_reference_id: ref,
      });
    });
    return { gateway, command, submit, committed, create, version, register, verify, attach };
  });

export const createOperation = (
  id: string,
  origin: WorkstreamOperationRequest["origin"] = { kind: "app" },
): WorkstreamCreateRequest => ({
  contract: "workstream-operations/1.0.0",
  operation_id: id,
  origin,
  kind: "create_workstream",
  registry: { registry_id: registry.registry_instance_id, owner_id: registry.owner_id },
  workstream: {
    name: "Synthetic " + id,
    lifecycle: "active",
    progress: { state: "unknown" },
    sort_order: 0,
  },
});
export const assignOperation = (
  id: string,
  workstream: string | null,
  origin: WorkstreamOperationRequest["origin"] = { kind: "app" },
): WorkstreamSetPrimaryRequest => ({
  contract: "workstream-operations/1.0.0",
  operation_id: id,
  origin,
  kind: "set_primary",
  thread: { environment_id: registry.home_environment_id, thread_id: "thread-synthetic" },
  target:
    workstream === null
      ? {
          kind: "unassigned",
          registry_id: registry.registry_instance_id,
          owner_id: registry.owner_id,
        }
      : {
          kind: "workstream",
          registry_id: registry.registry_instance_id,
          owner_id: registry.owner_id,
          workstream_id: workstream,
        },
  expected: { kind: "unassigned" },
});
export const revokeGrant = Effect.fn("NativeIngress.revokeGrant")(function* (
  permissions: readonly string[] = ["workstreams:read", "workstreams:write"],
  state = "revoked",
) {
  const store = yield* Store.RegistryStore;
  const grants = yield* store.listPlain(scope, "grants");
  const latest = grants.at(-1)!;
  yield* store.appendPlain(scope, "grants", {
    principal_id: enrollmentRequest.context.principal_id,
    grant_id: latest.grant_id!,
    grant_version: Number(latest.grant_version) + 1,
    permissions: canonicalJson(permissions),
    state,
  });
});

// Frozen 200-only endpoint metadata and classifier from b9719594ecd942e2370dedfba32680f36df292a7.
// Receipt, payload, params and error codecs are byte-identical to that parent; no new endpoint metadata is reused.
const {
  WorkstreamReceipt,
  EnvironmentAuthenticatedAuth,
  T3WorkstreamCommandPollParams,
  T3WorkstreamCommandRequest,
  EnvironmentHttpConflictError,
  EnvironmentRequestInvalidError,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
} = Wire;
const OptionalBearerHeaders = Schema.Struct({
  authorization: Schema.optionalKey(Schema.String),
  dpop: Schema.optionalKey(Schema.String),
});
const EnvironmentWorkstreamSnapshotErrors = [
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
const EnvironmentWorkstreamCommandErrors = [
  EnvironmentRequestInvalidError,
  EnvironmentHttpConflictError,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
] as const;
class LegacyWorkstreamsHttpApi extends HttpApiGroup.make("workstreams")
  .add(
    HttpApiEndpoint.get("command", "/api/workstreams/commands/:commandId", {
      headers: OptionalBearerHeaders,
      params: T3WorkstreamCommandPollParams,
      success: WorkstreamReceipt,
      error: EnvironmentWorkstreamSnapshotErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("submit", "/api/workstreams/commands", {
      headers: OptionalBearerHeaders,
      payload: T3WorkstreamCommandRequest,
      success: WorkstreamReceipt,
      error: EnvironmentWorkstreamCommandErrors,
    }).middleware(EnvironmentAuthenticatedAuth),
  ) {}
export class LegacyHttpApi extends HttpApi.make("environment").add(LegacyWorkstreamsHttpApi) {}

const isHttpConflict = Schema.is(EnvironmentHttpConflictError);
const isInvalidRequest = Schema.is(EnvironmentRequestInvalidError);
const isScopeRequired = Schema.is(EnvironmentScopeRequiredError);

/** Only these decoded POST refusals prove that the gateway did not submit the command. */
export function legacyWorkstreamCommandNoEffect(cause: unknown): "stale" | "denied" | null {
  if (isHttpConflict(cause) && cause.message === "workstream_command_not_current") return "stale";
  if (isInvalidRequest(cause) || isScopeRequired(cause)) return "denied";
  return null;
}

class NativeTestApi extends HttpApi.make("environment").add(
  Wire.EnvironmentHttpApi.groups.workstreams,
) {}
export const http = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig.pipe(
    Effect.provide(ServerConfig.layerTest(process.cwd(), process.cwd())),
  );
  const auth = yield* EnvironmentAuth.EnvironmentAuth.pipe(
    Effect.provide(
      EnvironmentAuth.layer.pipe(
        Layer.provide(
          Layer.succeed(ServerSecretStore.ServerSecretStore, {
            get: () => Effect.succeed(Option.some(Fixture.keyBytes)),
            getOrCreateRandom: () => Effect.succeed(Fixture.keyBytes),
            set: () => Effect.die("Unexpected auth key write"),
            create: () => Effect.die("Unexpected auth key create"),
            remove: () => Effect.die("Unexpected auth key removal"),
          }),
        ),
        Layer.provide(Layer.succeed(ServerConfig.ServerConfig, config)),
        Layer.provide(
          Layer.succeed(Environment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(Wire.EnvironmentId.make(registry.home_environment_id)),
          }),
        ),
      ),
    ),
  );
  const issued = yield* auth.issueSession({
    subject: principal.subject,
    scopes: [...principal.scopes],
  });
  const clock = yield* Clock.Clock;
  const middleware = yield* Wire.EnvironmentAuthenticatedAuth.pipe(
    Effect.provide(layerAuthenticatedAuth),
    Effect.provideService(EnvironmentAuth.EnvironmentAuth, auth),
  );
  const entry = yield* Entry.Entry;
  const routes = HttpApiBuilder.layer(NativeTestApi).pipe(
    Layer.provide(workstreamHttpApiLayer),
    Layer.provide(Layer.succeed(Entry.Entry, entry)),
    Layer.provide(Layer.succeed(Gateway.WorkstreamGateway, yield* Gateway.WorkstreamGateway)),
    Layer.provide(
      Layer.succeed(Operations.WorkstreamOperations, yield* Operations.WorkstreamOperations),
    ),
    Layer.provide(
      Layer.succeed(RegistrationContext.WorkstreamsRegistrationContext, entry.registration),
    ),
    Layer.provide(
      Layer.succeed(Wire.EnvironmentAuthenticatedAuth, (effect, options) =>
        middleware(effect, options).pipe(Effect.provideService(Clock.Clock, clock)),
      ),
    ),
    Layer.provide(workstreamResponseHeadersLayer),
    Layer.provideMerge(
      HttpPlatform.layer.pipe(
        Layer.provideMerge(NodeServices.layer),
        Layer.provideMerge(Etag.layerWeak),
      ),
    ),
    Layer.provide(NodeServices.layer),
  );
  const web = yield* Effect.acquireRelease(
    Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
    (value) => Effect.promise(() => value.dispose()),
  );
  const request = (
    path: string,
    payload?: unknown,
    authenticated = true,
    extra: Record<string, string> = {},
  ) =>
    Effect.promise(() =>
      web.handler(
        new Request("http://synthetic/api/workstreams" + path, {
          method: payload === undefined ? "GET" : "POST",
          headers: {
            "content-type": "application/json",
            ...(authenticated ? { authorization: "Bearer " + issued.token } : {}),
            ...extra,
          },
          ...(payload === undefined ? {} : { body: canonicalJson(payload) }),
        }),
      ),
    );
  const makeClient = (modern: boolean) =>
    Effect.flatMap(HttpClient.HttpClient, (httpClient) =>
      HttpApiClient.group(NativeTestApi, {
        group: "workstreams",
        baseUrl: "http://synthetic",
        httpClient: modern
          ? HttpClient.mapRequest(
              httpClient,
              HttpClientRequest.setHeaders(workstreamsHttpResponseHeaders),
            )
          : httpClient,
      }),
    ).pipe(
      Effect.provide(
        FetchHttpClient.layer.pipe(
          Layer.provide(
            Layer.succeed(FetchHttpClient.Fetch, (input, init) =>
              web.handler(new Request(input, init)),
            ),
          ),
        ),
      ),
    );
  const client = yield* makeClient(false);
  const modernClient = yield* makeClient(true);
  const oldClient = yield* Effect.flatMap(HttpClient.HttpClient, (httpClient) =>
    HttpApiClient.group(LegacyHttpApi, {
      group: "workstreams",
      baseUrl: "http://synthetic",
      httpClient,
    }),
  ).pipe(
    Effect.provide(
      FetchHttpClient.layer.pipe(
        Layer.provide(
          Layer.succeed(FetchHttpClient.Fetch, (input, init) =>
            web.handler(new Request(input, init)),
          ),
        ),
      ),
    ),
  );
  return {
    request,
    modernRequest: (path: string, payload?: unknown) =>
      request(path, payload, true, workstreamsHttpResponseHeaders),
    modernClient,
    oldClient,
    auth,
    sessionId: issued.sessionId,
    client,
    headers: { authorization: "Bearer " + issued.token },
  };
});
export const json = (response: Response) =>
  Effect.promise(() => response.json() as Promise<unknown>);

import * as GitHub from "../providers/githubPrObserver.ts";
import * as Resolver from "../providers/referenceResolver.ts";
import * as Runner from "../registry/effectRunner.ts";
export { GitHub };
export const withGitHub = Effect.fn("NativeIngress.withGitHub")(function* <A, E, R>(
  transport: GitHub.GitHubPrTransport["Service"],
  body: (gateway: Gateway.WorkstreamGateway["Service"]) => Effect.Effect<A, E, R>,
) {
  const entry = yield* Entry.Entry;
  const providers = Resolver.layer.pipe(
    Layer.provideMerge(
      GitHub.layer.pipe(
        Layer.provide(Layer.succeed(GitHub.GitHubPrTransport, transport)),
        Layer.provide(
          Layer.succeed(GitHub.GitHubPrAccess, {
            source_instance_id: "github-synthetic",
            authorization_header: "Bearer synthetic-port-only",
            user_agent: "native-ingress-qualification",
          }),
        ),
      ),
    ),
    Layer.provide(
      Layer.succeed(Resolver.ReferenceSources, {
        github: {
          owner_id: registry.owner_id,
          principal_id: enrollmentRequest.context.principal_id,
          source_instance_id: "github-synthetic",
          authority_namespace: "github-authority",
          store_generation: 1,
        },
        selectT3: () => Effect.die("GitHub-only fixture must not route a T3 reference"),
      }),
    ),
  );
  const local = yield* Local.LocalTransport.pipe(
    Effect.provide(
      Layer.fresh(
        Local.layer.pipe(
          Layer.provideMerge(Runner.layer.pipe(Layer.provideMerge(providers))),
          Layer.provide(
            Layer.succeed(Placements.PlacementTrust, {
              readTrustSnapshot: () => ({
                readiness: "ready" as const,
                trustedEnvironments: [
                  {
                    environmentId: registry.home_environment_id,
                    authorityNamespace: enrollmentRequest.context.authority_namespace,
                    storeGeneration: enrollmentRequest.context.store_generation,
                  },
                ],
              }),
            }),
          ),
          Layer.provide(
            Layer.succeed(NativeHome, {
              ...registry,
              served_by_environment_id: registry.home_environment_id,
            }),
          ),
        ),
      ),
    ),
  );
  const adapter = yield* local.forCaller({
    principal_id: enrollmentRequest.context.principal_id,
    source_environment_id: registry.home_environment_id,
    binding_version: 1,
    origin: { kind: "app" },
  });
  yield* (yield* Store.RegistryStore).appendPlain(scope, "source_bindings", {
    provider: "github",
    source_instance_id: "github-synthetic",
    binding_version: 1,
    authority_namespace: "github-authority",
    store_generation: 1,
    state: "active",
  });
  return yield* entry.run("workstreams:write", null, () => body(adapter.gateway));
});

export const harness = Effect.flatMap(Gateway.WorkstreamGateway, harnessFor);
