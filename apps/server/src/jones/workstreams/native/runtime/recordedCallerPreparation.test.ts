// @effect-diagnostics nodeBuiltinImport:off -- Synthetic owner/build/SQLite fixtures are retained under the external task evidence root for ROOT cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeEvents from "node:events";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Wire from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { DEFAULT_SIGNAL_EXPORT } from "@t3tools/shared/observability";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../../config.ts";
import * as ProcessRunner from "../../../../processRunner.ts";
import * as ServerEnvironment from "../../../../environment/ServerEnvironment.ts";
import * as NativeAuthority from "../../../../environment/NativeStoreAuthority.ts";
import { initializeNativeStoreAuthority } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Ownership from "../../../../serverOwnership.ts";
import { ServiceLauncherHostProcess } from "../../../../cloud/serviceLauncherClient.ts";
import {
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_LAUNCHER_CONTEXT_ENV,
} from "../../../../cloud/serviceProtocol.ts";
import * as Qualified from "../../../cloud/qualifiedRuntime.ts";
import * as Updates from "../../../updates/UpdateAdmissionFence.ts";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import migration125 from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import migration126 from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import { enrollmentRequest } from "../../enrollment/testFixtures.ts";
import { deriveNativeEnrollmentSession } from "../../enrollment/request.ts";
import * as Admin from "../cli.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as RegistrySession from "../registry/session.ts";
import * as Registries from "../store/registryStore.ts";
import * as Records from "../store/sourceEnrollmentStore.ts";
import * as Fixture from "../store/sql.testkit.ts";
import * as Startup from "./startup.ts";
import * as Enrollments from "./sourceEnrollment.ts";
import * as CurrentAuthority from "./currentAuthority.ts";
import * as Preparation from "./recordedCallerPreparation.ts";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const version = "0.0.0-preview.20261002.100";
const environmentId = "synthetic-environment";
const home = {
  ...Fixture.scope,
  canonical_origin: "https://native.workstreams.invalid",
  home_environment_id: environmentId,
  served_by_environment_id: environmentId,
  server_generation: 1,
};
const selection: Startup.Selection = {
  mode: "native-home",
  config: null,
  home,
  sourceSessionId: enrollmentRequest.session.session_id,
  problem: null,
};
// External launcher transport and installed-build envelope are synthetic. CurrentAuthority,
// both owner locks, selected SQL, encryption, enrollment validation and auth-session SQL are real.
const retainedHome = Effect.gen(function* () {
  const parent = process.env.NW_CALLER_PREPARATION_EVIDENCE_DIR ?? NodeOS.tmpdir();
  NodeFS.mkdirSync(parent, { recursive: true });
  const baseDir = NodeFS.realpathSync(NodeFS.mkdtempSync(NodePath.join(parent, "caller-owner-")));
  NodeFS.chmodSync(baseDir, 0o700);
  const paths = yield* Config.deriveServerPaths(baseDir, undefined);
  const release = NodePath.join(baseDir, "runtime", "versions", version);
  for (const directory of [paths.stateDir, paths.authorityStateDir, release])
    NodeFS.mkdirSync(directory, { recursive: true, mode: 0o700 });
  NodeFS.writeFileSync(paths.environmentIdPath, environmentId + "\n", { mode: 0o600 });
  NodeFS.writeFileSync(
    NodePath.join(baseDir, "runtime", "service-state.json"),
    JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: version }),
    { mode: 0o600 },
  );
  NodeFS.writeFileSync(NodePath.join(release, "t3"), "synthetic qualified executable", {
    mode: 0o700,
  });
  const authority = initializeNativeStoreAuthority(paths.authorityStateDir, environmentId);
  NodeFS.writeFileSync(
    NodePath.join(release, Qualified.QUALIFIED_RUNTIME_RECEIPT),
    JSON.stringify({
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version,
      sourceSha: "a".repeat(40),
      sourceTree: "b".repeat(40),
      installedSourceSha: "c".repeat(40),
      runId: 101,
      runAttempt: 1,
      artifactId: 102,
      workflow: ".github/workflows/artifact-cli-linux.yml",
      artifactDigest: "sha256:" + "d".repeat(64),
      archiveSha256: "e".repeat(64),
      platform: "linux",
      architecture: "x64",
      payloadSha256: yield* Effect.promise(() =>
        Qualified.qualifiedPayloadDigest(
          release,
          "linux",
          ".github/workflows/artifact-cli-linux.yml",
        ),
      ),
    }),
    { mode: 0o600 },
  );
  const config = Config.make({
    ...paths,
    baseDir,
    cwd: baseDir,
    mode: "web",
    port: 0,
    host: undefined,
    logLevel: "Error",
    traceMinLevel: "Info",
    traceTimingEnabled: false,
    traceBatchWindowMs: 200,
    traceMaxBytes: 1048576,
    traceMaxFiles: 1,
    otlpTracesUrl: undefined,
    otlpMetricsUrl: undefined,
    otlpLogsUrl: undefined,
    otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
    otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
    otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
    otelEnvironment: OtelEnvironment.none,
    staticDir: undefined,
    devUrl: undefined,
    devAllowedOrigins: [],
    noBrowser: true,
    startupPresentation: "headless",
    desktopBootstrapToken: undefined,
    autoBootstrapProjectFromCwd: false,
    logWebSocketEvents: false,
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
  });
  const transport = new NodeEvents.EventEmitter();
  const launcher = {
    connected: true,
    send: () => {
      throw new Error("No launcher handoff in caller fixture");
    },
    on: (event: "message" | "disconnect", listener: (...args: readonly unknown[]) => void) => {
      transport.on(event, listener);
    },
    off: (event: "message" | "disconnect", listener: (...args: readonly unknown[]) => void) => {
      transport.off(event, listener);
    },
  };
  return { config, authority, launcher, transport };
});
const withFixture = <A, E, R>(
  use: (f: {
    readonly callers: Preparation.RecordedCallerPreparation["Service"];
    readonly sessions: AuthSessions.AuthSessionRepository["Service"];
    readonly sql: SqlClient.SqlClient;
    readonly registries: Registries.RegistryStore["Service"];
    readonly principal: Wire.EnvironmentAuthenticatedPrincipal["Service"];
    readonly transport: NodeEvents.EventEmitter;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(Fixture.now));
      const f = yield* retainedHome;
      const env = {
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          childVersion: version,
        }),
      };
      const authorityGraph = CurrentAuthority.layer.pipe(
        Layer.provideMerge(
          NativeAuthority.layer.pipe(Layer.provideMerge(ServerEnvironment.layerIdentity)),
        ),
        Layer.provideMerge(Updates.layer),
      );
      const graph = Preparation.layer.pipe(
        Layer.provideMerge(authorityGraph),
        Layer.provideMerge(RegistrySession.layer),
        Layer.provideMerge(Records.layer),
        Layer.provideMerge(AuthSessions.layer),
        Layer.provideMerge(Admin.layer),
        Layer.provideMerge(Fixture.testLayer({ filename: f.config.dbPath })),
        Layer.provide(Layer.succeed(Config.ServerConfig, f.config)),
        Layer.provide(Layer.succeed(NativeHome, home)),
        Layer.provide(Layer.succeed(Startup.Startup, selection)),
        Layer.provideMerge(NodeServices.layer),
      );
      return yield* Effect.gen(function* () {
        const owner = yield* Ownership.acquireServerOwnership(f.config.serverRuntimeStatePath);
        return yield* Effect.gen(function* () {
          yield* runMigrations({ toMigrationInclusive: 54 });
          yield* migration125;
          yield* migration126;
          NodeFS.chmodSync(f.config.dbPath, 0o600);
          yield* (yield* Admin.NativeAdmin).initialize({
            registry: {
              ...Fixture.scope,
              canonical_origin: home.canonical_origin,
              home_environment_id: environmentId,
              server_generation: 1,
            },
            principal_id: Fixture.binding.principal_id,
            content_key_id: "test-home",
            local_key_id: "test-local",
          });
          const sessions = yield* AuthSessions.AuthSessionRepository;
          const registries = yield* Registries.RegistryStore;
          const records = yield* Records.SourceEnrollmentStore;
          const sql = yield* SqlClient.SqlClient;
          const request: Enrollments.NativeSourceSetupRequest = {
            schema: "jones-code.native-source-setup/v1",
            enrollment: {
              ...enrollmentRequest,
              registry_origin: home.canonical_origin,
              context: {
                ...enrollmentRequest.context,
                owner_id: home.owner_id,
                principal_id: Fixture.binding.principal_id,
                source_instance_id: environmentId,
                authority_namespace: f.authority.authority_namespace,
                store_generation: f.authority.store_generation,
              },
            },
            expected: {
              ...Fixture.scope,
              home_environment_id: environmentId,
              home_generation: 1,
              registry_version: 1,
              singleton_binding_version: 1,
              grant_id: "grant:principal:test",
              grant_version: 1,
              source_binding: null,
            },
            rebind: false,
          };
          yield* registries.appendPlain(Fixture.scope, "source_bindings", {
            provider: "t3",
            source_instance_id: environmentId,
            binding_version: 1,
            authority_namespace: f.authority.authority_namespace,
            store_generation: f.authority.store_generation,
            state: "active",
          });
          // Source-binding insertion advances the registry via migration 125.
          yield* sessions.createIfAbsent(deriveNativeEnrollmentSession(request.enrollment));
          const proof: Enrollments.NativeSourceEnrollmentV1 = {
            schema: "jones-code.native-source-enrollment/v1",
            request,
            request_sha256: Enrollments.sourceSetupDigest(request),
            receipt: { registry_version: 2, source_binding_version: 1 },
          };
          yield* records.insert({
            ...Fixture.scope,
            enrollment_id: request.enrollment.enrollment_id,
            session_id: request.enrollment.session.session_id,
            request_sha256: proof.request_sha256,
            proof,
          });
          const sessionId = Wire.AuthSessionId.make("prepared-http-original");
          const scopes = [Wire.AuthOrchestrationReadScope, Wire.AuthOrchestrationOperateScope];
          const caller = {
            sessionId,
            subject: "prepared-owner",
            method: "browser-session-cookie" as const,
            scopes,
            client: {
              label: null,
              ipAddress: null,
              userAgent: null,
              deviceType: "unknown" as const,
              os: null,
              browser: null,
            },
            issuedAt: DateTime.makeUnsafe(request.enrollment.session.issued_at),
            expiresAt: DateTime.makeUnsafe(request.enrollment.session.expires_at),
          };
          yield* sessions.create(caller);
          const principal = { ...caller, scopes: new Set(scopes) };
          const callers = yield* Preparation.RecordedCallerPreparation;
          return yield* use({
            callers,
            sessions,
            sql,
            registries,
            principal,
            transport: f.transport,
          }).pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal));
        }).pipe(
          Effect.provide(graph),
          Effect.provideService(Ownership.ServerOwnershipLifetime, owner.lifetime),
        );
      }).pipe(
        Effect.provideService(ServiceLauncherHostProcess, f.launcher),
        Effect.provideService(HostProcess.Environment, env),
      );
    }),
  ).pipe(Effect.provide(ProcessRunner.layer.pipe(Layer.provideMerge(NodeServices.layer))));

it.effect(
  "genuine scoped owner and fresh enrollment/caller graph capture inside the selected writer",
  () =>
    withFixture((f) =>
      Effect.gen(function* () {
        const escaped = yield* f.callers.withPrepared((callers) =>
          f.sql.withTransaction(
            Effect.gen(function* () {
              expect(Option.isSome(yield* Effect.serviceOption(f.sql.transactionService))).toBe(
                true,
              );
              const fs = process.getBuiltinModule("node:fs");
              const unexpected = () => {
                throw new Error("External filesystem access inside caller writer");
              };
              const spies = [
                vi.spyOn(fs, "openSync").mockImplementation(unexpected),
                vi.spyOn(fs, "readFileSync").mockImplementation(unexpected),
                vi.spyOn(fs, "lstatSync").mockImplementation(unexpected),
                vi.spyOn(fs, "realpathSync").mockImplementation(unexpected),
              ];
              try {
                const record = yield* callers.captureCaller();
                expect(record.auth).toMatchObject({
                  kind: "bearer",
                  session_id: f.principal.sessionId,
                });
                expect(record.source.enrollment_session_id).toBe(selection.sourceSessionId);
                const caller = yield* callers.readmitRecorded(record);
                yield* caller.check("workstreams:write", null);
                expect(yield* callers.visible(record, null)).toBe(true);
                return { callers, caller, record };
              } finally {
                for (const spy of spies) spy.mockRestore();
              }
            }),
          ),
        );
        expect(yield* Effect.result(escaped.callers.captureCaller())).toMatchObject({
          _tag: "Failure",
        });
        expect(yield* Effect.result(escaped.caller.check("workstreams:write", null))).toMatchObject(
          { _tag: "Failure" },
        );
        expect(yield* escaped.callers.visible(escaped.record, null)).toBe(false);
      }),
    ),
);
it.effect(
  "ambient refusal precedes external preparation and callback errors retain their identity",
  () =>
    withFixture((f) =>
      Effect.gen(function* () {
        const result = yield* f.sql.withTransaction(
          Effect.result(f.callers.withPrepared(() => Effect.die("Unexpected callback"))),
        );
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "RecordedCallerFence", code: "unavailable" },
        });
        const original = { _tag: "DeliberateOuterAbort" };
        const abortResult = yield* Effect.result(
          f.callers.withPrepared(() => Effect.fail(original)),
        );
        expect(abortResult).toMatchObject({ _tag: "Failure", failure: original });
        if (Result.isFailure(abortResult)) expect(abortResult.failure).toBe(original);
      }),
    ),
);
it.effect("current session revocation refuses without replacing original HTTP identity", () =>
  withFixture((f) =>
    f.callers.withPrepared((callers) =>
      f.sql.withTransaction(
        Effect.gen(function* () {
          const record = yield* callers.captureCaller();
          yield* f.sessions.revoke({
            sessionId: f.principal.sessionId,
            revokedAt: DateTime.makeUnsafe(Fixture.now),
          });
          expect(yield* Effect.result(callers.readmitRecorded(record))).toMatchObject({
            _tag: "Failure",
            failure: { code: "not_granted" },
          });
        }),
      ),
    ),
  ),
);
it.effect("latest grant rows are rechecked after preparation without a grant cache", () =>
  withFixture((f) =>
    f.callers.withPrepared((callers) =>
      f.sql.withTransaction(
        Effect.gen(function* () {
          const record = yield* callers.captureCaller();
          yield* f.registries.appendPlain(Fixture.scope, "grants", {
            principal_id: Fixture.binding.principal_id,
            grant_version: 2,
            grant_id: "revoked-current",
            permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
            state: "revoked",
          });
          expect(yield* Effect.result(callers.readmitRecorded(record))).toMatchObject({
            _tag: "Failure",
            failure: { code: "not_granted" },
          });
        }),
      ),
    ),
  ),
);
it.effect(
  "original HTTP requested scopes only narrow and a replaced principal object refuses",
  () =>
    withFixture((f) =>
      Effect.gen(function* () {
        const narrow = { ...f.principal, scopes: new Set([Wire.AuthOrchestrationReadScope]) };
        yield* f.callers
          .withPrepared((callers) =>
            f.sql.withTransaction(
              Effect.gen(function* () {
                const record = yield* callers.captureCaller();
                expect(record.ceiling.scopes).toEqual([Wire.AuthOrchestrationReadScope]);
                const caller = yield* callers.readmitRecorded(record);
                expect(yield* Effect.result(caller.check("workstreams:write", null))).toMatchObject(
                  { _tag: "Failure" },
                );
                expect(
                  yield* Effect.result(
                    callers.captureCaller().pipe(
                      Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
                        ...narrow,
                      }),
                    ),
                  ),
                ).toMatchObject({ _tag: "Failure", failure: { code: "identity_changed" } });
              }),
            ),
          )
          .pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, narrow));
      }),
    ),
);

it.effect(
  "blocked actual build/payload qualification leaves an unrelated writer on the same SQL client",
  () =>
    withFixture((f) =>
      Effect.gen(function* () {
        yield* f.sql`CREATE TABLE caller_preparation_probe(value TEXT)`;
        let entered!: () => void;
        let release!: () => void;
        const enteredPromise = new Promise<void>((resolve) => {
          entered = resolve;
        });
        const gate = new Promise<void>((resolve) => {
          release = resolve;
        });
        const original = Qualified.currentQualifiedRuntimeBinding;
        const spy = vi
          .spyOn(Qualified, "currentQualifiedRuntimeBinding")
          .mockImplementation(async (...args) => {
            entered();
            await gate;
            return original(...args);
          });
        try {
          const pending = yield* f.callers
            .withPrepared((callers) => f.sql.withTransaction(callers.captureCaller()))
            .pipe(Effect.forkChild);
          yield* Effect.promise(() => enteredPromise);
          yield* f.sql.withTransaction(
            f.sql`INSERT INTO caller_preparation_probe VALUES ('committed-during-build-wait')`,
          );
          expect(yield* f.sql`SELECT value FROM caller_preparation_probe`).toHaveLength(1);
          release();
          expect((yield* Fiber.join(pending)).auth).toMatchObject({
            session_id: f.principal.sessionId,
          });
        } finally {
          release();
          spy.mockRestore();
        }
      }),
    ),
);

it.effect.each(["source", "singleton", "enrollment_session"] as const)(
  "current %s is revalidated inside acceptance",
  (drift) =>
    withFixture((f) =>
      f.callers.withPrepared((callers) =>
        f.sql.withTransaction(
          Effect.gen(function* () {
            const record = yield* callers.captureCaller();
            if (drift === "source")
              yield* f.registries.appendPlain(Fixture.scope, "source_bindings", {
                provider: "t3",
                source_instance_id: environmentId,
                binding_version: 2,
                authority_namespace: record.source.authority_namespace,
                store_generation: record.source.store_generation,
                state: "revoked",
              });
            else if (drift === "singleton")
              yield* f.sql`UPDATE jones_workstreams_binding_state SET state = 'fenced', binding_version = binding_version + 1 WHERE singleton = 1`;
            else
              yield* f.sessions.revoke({
                sessionId: Wire.AuthSessionId.make(record.source.enrollment_session_id),
                revokedAt: DateTime.makeUnsafe(Fixture.now),
              });
            expect(yield* Effect.result(callers.readmitRecorded(record))).toMatchObject({
              _tag: "Failure",
              failure: { code: "not_granted" },
            });
          }),
        ),
      ),
    ),
);

it.effect(
  "an observed inner write and caller validation roll back together after deliberate outer abort",
  () =>
    withFixture((f) =>
      Effect.gen(function* () {
        yield* f.sql`CREATE TABLE caller_preparation_probe(value TEXT)`;
        let observed = false;
        const failure = { _tag: "DeliberateObservedAbort" };
        const result = yield* Effect.result(
          f.callers.withPrepared((callers) =>
            f.sql.withTransaction(
              Effect.gen(function* () {
                const record = yield* callers.captureCaller();
                const caller = yield* callers.readmitRecorded(record);
                yield* caller.check("workstreams:write", null);
                yield* f.sql`INSERT INTO caller_preparation_probe VALUES ('visible-inside-writer')`;
                expect(yield* f.sql`SELECT value FROM caller_preparation_probe`).toHaveLength(1);
                observed = true;
                yield* caller.check("workstreams:write", null);
                return yield* Effect.fail(failure);
              }),
            ),
          ),
        );
        expect(observed).toBe(true);
        expect(result).toMatchObject({ _tag: "Failure", failure });
        if (Result.isFailure(result)) expect(result.failure).toBe(failure);
        expect(yield* f.sql`SELECT value FROM caller_preparation_probe`).toHaveLength(0);
      }),
    ),
);

it.effect(
  "the original launcher incarnation cannot revive a prepared caller after disconnect",
  () =>
    withFixture((f) =>
      Effect.gen(function* () {
        const result = yield* Effect.result(
          f.callers.withPrepared((callers) =>
            f.sql.withTransaction(
              Effect.gen(function* () {
                const record = yield* callers.captureCaller();
                const caller = yield* callers.readmitRecorded(record);
                f.transport.emit("disconnect");
                expect(yield* Effect.result(caller.check("workstreams:write", null))).toMatchObject(
                  { _tag: "Failure" },
                );
              }),
            ),
          ),
        );
        expect(result).toMatchObject({ _tag: "Failure" });
      }),
    ),
);
