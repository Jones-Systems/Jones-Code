// @effect-diagnostics nodeBuiltinImport:off -- Synthetic build/launcher metadata and real SQLite ownership scopes; fixtures retained for root cleanup.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlClient from "@t3tools/shared/nodeSqliteClient";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { initializeNativeStoreAuthority } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Ownership from "../../../../serverOwnership.ts";
import * as ProcessRunner from "../../../../processRunner.ts";
import { ServiceLauncherHostProcess } from "../../../../cloud/serviceLauncherClient.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../../../../cloud/serviceProtocol.ts";
import * as Updates from "../../../updates/UpdateAdmissionFence.ts";
import { syntheticT3Home, syntheticVersion } from "../../enrollment/testHome.ts";
import * as Current from "../runtime/currentAuthority.ts";

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

const fixture = Effect.gen(function* () {
  const retained = yield* Scope.make();
  const f = yield* syntheticT3Home("nw-current-authority-").pipe(
    Effect.provideService(Scope.Scope, retained),
  );
  const db = new NodeSqlite.DatabaseSync(f.dbPath);
  db.exec("CREATE TABLE writer_probe(value TEXT)");
  db.close();
  NodeFS.chmodSync(f.dbPath, 0o600);
  initializeNativeStoreAuthority(f.authorityDir, "synthetic-environment");
  return f;
});
const withRuntime = <A, E, R>(
  use: (input: {
    current: Current.CurrentAuthority["Service"];
    sql: SqlClient.SqlClient;
    updates: Updates.UpdateAdmissionFence["Service"];
    disconnect: () => void;
    baseDir: string;
    authorityDir: string;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    // Explicit directory form has no Config.layerTest temporary-scope cleanup.
    const configContext = yield* Layer.build(Config.layerTest(f.baseDir, f.baseDir));
    const config = Context.get(configContext, Config.ServerConfig);
    const disconnects = new Set<(...args: ReadonlyArray<unknown>) => void>();
    let connected = true;
    const host = ServiceLauncherHostProcess.of({
      get connected() {
        return connected;
      },
      send: () => true,
      on: (event, listener) => {
        if (event === "disconnect") disconnects.add(listener);
      },
      off: (event, listener) => {
        if (event === "disconnect") disconnects.delete(listener);
      },
    });
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // This is the real original server-owner SQLite lock and its root-owned scope token.
      const ownership = yield* Ownership.acquireServerOwnership(config.serverRuntimeStatePath);
      const updates = yield* Updates.make;
      return yield* Effect.gen(function* () {
        const current = yield* Current.CurrentAuthority;
        return yield* use({
          current,
          sql,
          updates,
          baseDir: f.baseDir,
          authorityDir: f.authorityDir,
          disconnect: () => {
            connected = false;
            for (const listener of disconnects) listener();
          },
        });
      }).pipe(
        Effect.provide(
          Current.layer.pipe(
            Layer.provide(Authority.layer.pipe(Layer.provide(Environment.layerIdentity))),
          ),
        ),
        Effect.provideService(Ownership.ServerOwnershipLifetime, ownership.lifetime),
        Effect.provideService(Updates.UpdateAdmissionFence, updates),
      );
    }).pipe(
      Effect.provide(NodeSqlClient.layer({ filename: f.dbPath })),
      Effect.provideService(Config.ServerConfig, config),
      Effect.provideService(ServiceLauncherHostProcess, host),
      Effect.provideService(HostProcess.Environment, {
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          childVersion: syntheticVersion,
        }),
      }),
      Effect.provideService(HostProcess.HomeDirectory, f.baseDir),
      Effect.provide(ProcessRunner.layer),
      Effect.scoped,
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

import { EnvironmentId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Receiving from "./receivingAuthority.ts";
import * as Session from "../registry/session.ts";

// These are real EnvironmentAuth and NativePeerSessions registrations in retained synthetic SQL,
// not accepted rows or copied invocation objects. Tokens stay in memory and never enter evidence logs.
const registrationLayer = Pins.nativeLayer.pipe(
  Layer.provideMerge(
    Auth.layer.pipe(
      Layer.provideMerge(Environment.layerIdentity),
      Layer.provide(
        Layer.succeed(
          Secrets.ServerSecretStore,
          Secrets.ServerSecretStore.of({
            get: () => Effect.succeed(Option.some(new Uint8Array(32).fill(17))),
            getOrCreateRandom: () => Effect.succeed(new Uint8Array(32).fill(17)),
            set: () => Effect.die("unexpected synthetic secret mutation"),
            create: () => Effect.die("unexpected synthetic secret mutation"),
            remove: () => Effect.die("unexpected synthetic secret mutation"),
          }),
        ),
      ),
    ),
  ),
);
it.effect(
  "receiving fence retains the exact original authenticator registration and invocation scope",
  () =>
    withRuntime(({ sql }) =>
      Effect.gen(function* () {
        yield* runMigrations({ toMigrationInclusive: 54 });
        const auth = yield* Auth.EnvironmentAuth;
        const native = yield* Pins.NativePeerSessions;
        const issued = yield* auth.issueMcpClientSession({
          label: "synthetic-receiving-test",
          access: "full-access",
          peerLink: true,
          client: { deviceType: "bot" },
        });
        const invocation: Mcp.McpInvocationScope = {
          environmentId: EnvironmentId.make("synthetic-environment"),
          issuedAt: DateTime.toEpochMillis(yield* DateTime.now),
          requestNamespace: "receiving-authority-qualification",
          capabilities: new Set(["orchestration"]),
          thread: undefined,
          client: {
            sessionId: issued.sessionId,
            label: "synthetic",
            access: "full-access",
            linked: true,
          },
        };
        const request = {
          cookies: {},
          headers: { authorization: `Bearer ${issued.token}` },
        } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
        yield* native.authenticate(request, invocation);
        let escaped: Effect.Effect<void, Session.RegistryError> | undefined;
        yield* Receiving.withPreparedReceivingAuthority(invocation, (fence) =>
          Effect.gen(function* () {
            expect((yield* fence.prepare({ ...invocation }).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            const prepared = yield* fence.prepare(invocation);
            escaped = prepared.checkCurrent();
            yield* sql.withTransaction(prepared.checkCurrent());
            expect(
              (yield* prepared
                .checkCurrent()
                .pipe(
                  Effect.provideService(Mcp.McpInvocationContext, { ...invocation }),
                  Effect.result,
                ))._tag,
            ).toBe("Failure");
            // Reauthentication replaces registration with identical tuple/session bytes.
            yield* native.authenticate(request, invocation);
            yield* native.current(invocation);
            expect((yield* prepared.checkCurrent().pipe(Effect.result))._tag).toBe("Failure");
          }),
        );
        expect((yield* escaped!.pipe(Effect.result))._tag).toBe("Failure");
        expect(
          (yield* Receiving.withPreparedReceivingAuthority(
            { ...invocation },
            () => Effect.void,
          ).pipe(Effect.result))._tag,
        ).toBe("Failure");
      }).pipe(Effect.provide(registrationLayer)),
    ),
);
it.effect("receiving preparation refuses actual ambient SQL before authenticator lookup", () =>
  withRuntime(({ sql }) =>
    Effect.gen(function* () {
      const native = yield* Pins.NativePeerSessions;
      const spy = vi.spyOn(native, "current");
      const invocation: Mcp.McpInvocationScope = {
        environmentId: EnvironmentId.make("synthetic-environment"),
        issuedAt: 0,
        requestNamespace: "unregistered-refusal-only",
        capabilities: new Set(),
        thread: undefined,
        client: undefined,
      };
      try {
        expect(
          (yield* sql
            .withTransaction(
              Receiving.withPreparedReceivingAuthority(invocation, () => Effect.void),
            )
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(spy).not.toHaveBeenCalled();
      } finally {
        spy.mockRestore();
      }
    }).pipe(Effect.provide(registrationLayer)),
  ),
);

import { NativeHome } from "../mode/authorityMode.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Home from "./homeForwardPort.ts";
import * as Supplied from "./suppliedEvidence.ts";

it.effect("receiving service wrapper refuses an ambient writer before authentication or keys", () =>
  withRuntime(({ sql, current }) =>
    Effect.gen(function* () {
      const native = yield* Pins.NativePeerSessions;
      const authentication = vi.spyOn(native, "current");
      let keyCalls = 0;
      const keyRefusal = Effect.suspend(() => {
        keyCalls++;
        return Effect.fail(new Keys.KeyUnavailable({ reason: "missing" }));
      });
      const refusal = Effect.fail(new Session.RegistryError({ code: "unavailable" }));
      const tools = yield* Home.HomeTools.pipe(
        Effect.provide(
          Receiving.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(NativeHome, {
                  registry_instance_id: "11111111-1111-4111-8111-111111111111",
                  owner_id: "synthetic-owner",
                  home_environment_id: "synthetic-environment",
                  server_generation: 1,
                  canonical_origin: "https://native.workstreams.invalid",
                  served_by_environment_id: "synthetic-environment",
                }),
                Layer.succeed(Keys.WorkstreamsKeyProvider, {
                  activeKeyId: () => keyRefusal,
                  get: () => keyRefusal,
                  indexKey: () => keyRefusal,
                }),
                Layer.succeed(Supplied.SourceForwardQualification, {
                  prepare: () => refusal,
                  prepareReadback: () => refusal,
                }),
                Layer.succeed(Session.Session, { authorize: () => refusal }),
                Layer.succeed(Current.CurrentAuthority, current),
              ),
            ),
          ),
        ),
      );
      const invocation: Mcp.McpInvocationScope = {
        environmentId: EnvironmentId.make("synthetic-environment"),
        issuedAt: 0,
        requestNamespace: "wrapper-ambient-refusal",
        capabilities: new Set(),
        thread: undefined,
        client: undefined,
      };
      try {
        for (const operation of [
          tools.forward({}, invocation).pipe(Effect.asVoid),
          tools.readback({}, invocation).pipe(Effect.asVoid),
        ]) {
          expect(yield* sql.withTransaction(operation).pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
            failure: { code: "unavailable" },
          });
        }
        expect(authentication).not.toHaveBeenCalled();
        expect(keyCalls).toBe(0);
      } finally {
        authentication.mockRestore();
      }
    }).pipe(Effect.provide(registrationLayer)),
  ),
);
