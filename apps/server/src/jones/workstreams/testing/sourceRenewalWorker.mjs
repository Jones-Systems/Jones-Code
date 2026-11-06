import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

// Imported by mock controls without bootstrapping an artifact or loading production readers.
export function makeWorkerClose(finalizers) {
  let completion;
  return () =>
    (completion ??= (async () => {
      const results = await Promise.allSettled(finalizers.map(async (finalize) => finalize()));
      const errors = results
        .filter((result) => result.status === "rejected")
        .map((result) => result.reason);
      if (errors.length) throw new AggregateError(errors, "Artifact worker cleanup failed");
    })());
}

async function main() {
  // This adapter runs in a separate immutable native module graph for each source artifact.
  const root = NodeFS.realpathSync(process.argv[2]);
  const require = NodeModule.createRequire(NodePath.join(root, "apps/server/package.json"));
  const dependency = (name) => import(NodeURL.pathToFileURL(require.resolve(name)).href);
  const source = (name) => import(NodeURL.pathToFileURL(NodePath.join(root, name)).href);
  const Effect = await dependency("effect/Effect");
  const Layer = await dependency("effect/Layer");
  const Scope = await dependency("effect/Scope");
  const Exit = await dependency("effect/Exit");
  const Option = await dependency("effect/Option");
  const Clock = await dependency("effect/Clock");
  const Schema = await dependency("effect/Schema");
  const SqlClient = await dependency("effect/sql/SqlClient");
  const DateTime = await dependency("effect/DateTime");
  const base = "apps/server/src/";
  const Native = await source(`${base}jones/workstreams/nativeProvider/service.ts`);
  const Fixture = await source(`${base}jones/workstreams/nativeProvider/testFixtures.ts`);
  const Enrollments = await source(`${base}jones/workstreams/enrollment/service.ts`);
  const Credentials = await source(`${base}jones/workstreams/enrollment/credentialFile.ts`);
  const Sessions = await source(`${base}persistence/AuthSessions.ts`);
  const SessionStore = await source(`${base}auth/SessionStore.ts`);
  const SecretStore = await source(`${base}auth/ServerSecretStore.ts`);
  const Migrations = await source(`${base}persistence/Migrations.ts`);
  const Migration = await source(
    `${base}jones/persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts`,
  );
  const Sqlite = await source("packages/shared/src/nodeSqliteClient.ts");
  const Loopback = await source(`${base}jones/workstreams/testing/loopbackControlPlane.ts`);
  const observed = Option.getOrThrow(await Effect.runPromise(Native.readNativeProviderBuild));
  const decodeSession = Schema.decodeUnknownSync(Sessions.CreateAuthSessionInput);
  const liveClock = Effect.runSync(Clock.Clock);
  let millis = Date.parse("2026-01-01T00:00:00.000Z");
  const clock = {
    ...liveClock,
    currentTimeMillisUnsafe: () => millis,
    currentTimeMillis: Effect.sync(() => millis),
    currentTimeNanosUnsafe: () => BigInt(millis) * 1000000n,
    currentTimeNanos: Effect.sync(() => BigInt(millis) * 1000000n),
  };
  const scope = Effect.runSync(Scope.make());
  const run = async (effect) => {
    const result = await Effect.runPromise(
      effect.pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(Clock.Clock, clock),
        Effect.result,
      ),
    );
    if (result._tag === "Failure") throw result.failure;
    return result.success;
  };
  let store;
  let environment;

  const digest = (file) =>
    NodeCrypto.createHash("sha256")
      .update(NodeFS.readFileSync(NodePath.join(root, file)))
      .digest("hex");
  const identity = {
    root,
    pid: process.pid,
    build: observed,
    provider: NodePath.join(root, `${base}jones/workstreams/nativeProvider/service.ts`),
    registration: NodePath.join(root, `${base}jones/workstreams/registrationContext/service.ts`),
    providerSha256: digest(`${base}jones/workstreams/nativeProvider/service.ts`),
    registrationSha256: digest(`${base}jones/workstreams/registrationContext/service.ts`),
    manifestSha256: digest("apps/server/package.json"),
    effect: NodeFS.realpathSync(require.resolve("effect/Effect")),
    contracts: NodeFS.realpathSync(
      NodeURL.fileURLToPath(import.meta.resolve("@t3tools/contracts")),
    ),
    runtime: { version: process.version, executable: NodeFS.realpathSync(process.execPath) },
  };
  const close = makeWorkerClose([
    () => environment?.close(),
    () => Effect.runPromise(Scope.close(scope, Exit.void)),
  ]);
  async function dispatch(command, args) {
    if (command === "identity") return identity;
    if (command === "close") {
      await close();
      return { closed: true };
    }
    if (command === "open") {
      NodeAssert.equal(store, undefined);
      NodeAssert.deepEqual(args.build, observed, "consumer identity mismatch before database open");
      NodeAssert.equal(args.manifestSha256, identity.manifestSha256);
      NodeAssert.equal(args.providerSha256, identity.providerSha256);
      NodeAssert.equal(args.registrationSha256, identity.registrationSha256);
      store = await run(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            Sqlite.layer({
              filename: NodePath.join(args.stateRoot, `${args.sourceId}.sqlite`),
              readerWorker: false,
              checkpointWorker: false,
            }),
          );
          yield* Migrations.runMigrations({ toMigrationInclusive: 54 }).pipe(
            Effect.provide(context),
          );
          yield* Migration.default.pipe(Effect.provide(context));
          const sql = yield* SqlClient.SqlClient.pipe(Effect.provide(context));
          const sessions = yield* Sessions.make.pipe(Effect.provide(context));
          const authority = {
            readCurrent: Effect.succeed({
              environmentId: args.sourceId,
              authorityNamespace: "synthetic-authority",
              storeGeneration: 1,
            }),
          };
          const enrollments = yield* Enrollments.makeNativeEnrollments({
            authority,
            build: Native.readNativeProviderBuild,
          }).pipe(
            Effect.provide(context),
            Effect.provideService(Sessions.AuthSessionRepository, sessions),
          );
          const materialize = yield* SessionStore.makeReservedBearerSessionMaterializer.pipe(
            Effect.provideService(Sessions.AuthSessionRepository, sessions),
            Effect.provideService(SecretStore.ServerSecretStore, {
              get: () => Effect.succeedSome(new Uint8Array(32).fill(7)),
              set: () => Effect.die("fixture key mutation"),
              create: () => Effect.die("fixture key mutation"),
              getOrCreateRandom: () => Effect.die("fixture key mutation"),
              remove: () => Effect.die("fixture key mutation"),
            }),
          );
          const { build: _discardedBuild, ...ports } = Fixture.makeProviderFixture().ports;
          const providerPorts = {
            ...ports,
            authority,
            engine: { dispatch: () => Effect.die("renewal must not dispatch") },
          };
          return {
            sql,
            sessions,
            enrollments,
            materialize,
            provider: Native.makeWorkstreamsNativeProvider(providerPorts),
            providerV2: Native.makeWorkstreamsNativeProvider(providerPorts, "v2"),
            stateRoot: args.stateRoot,
            sourceId: args.sourceId,
          };
        }),
      );
      return identity;
    }
    NodeAssert.ok(store, "database not opened");
    const credential = (request) =>
      Credentials.makeNativeCredentialWriter({
        baseDir: store.stateRoot,
        credentialPath: NodePath.join(store.stateRoot, `${request.enrollment_id}.json`),
      });
    switch (command) {
      case "environment": {
        NodeAssert.equal(environment, undefined);
        const control = {
          activation: {
            ...args.activation,
            value: { ...args.activation.value, baseUrl: new URL(args.activation.value.baseUrl) },
          },
        };
        environment = await Loopback.startEnvironment(control, store.sourceId, "embedded");
        return { origin: environment.origin };
      }
      case "request":
        return {
          schema: "jones-code.workstreams-native-enrollment/v1",
          enrollment_id: `${store.sourceId}-${args.suffix}`,
          registry_origin: "https://workstreams.test",
          context: {
            ...Fixture.enrolledContext,
            owner_id: "owner-fixture",
            principal_id: "22222222-2222-4222-8222-222222222222",
            source_instance_id: store.sourceId,
            authority_namespace: "synthetic-authority",
            store_generation: 1,
            enrollment_id: `${store.sourceId}-${args.suffix}`,
            build: observed,
          },
          session: {
            session_id: `${store.sourceId}-${args.suffix}-session`,
            issued_at: "2026-01-01T00:00:00.000Z",
            expires_at: "2026-01-02T00:00:00.000Z",
          },
        };
      case "rows":
        return run(
          Effect.gen(function* () {
            return {
              sessions: yield* store.sql`SELECT * FROM auth_sessions ORDER BY session_id`,
              enrollments:
                yield* store.sql`SELECT * FROM workstreams_native_enrollments ORDER BY enrollment_id`,
            };
          }),
        );
      case "getBySessionId":
        return run(store.enrollments.getBySessionId(args).pipe(Effect.map(Option.getOrNull)));
      case "inspect":
      case "inspectExisting":
      case "reserve":
        return run(store.enrollments[command](args));
      case "context":
      case "contextV2":
        return run(store[command === "context" ? "provider" : "providerV2"].context(args));
      case "observe":
        return run(credential(args).observe());
      case "publish":
        return run(credential(args.request).publish(args.token, args.preimage));
      case "materialize":
        return run(store.materialize(decodeSession(args)));
      case "revoke":
        return run(
          store.sessions.revoke({ ...args, revokedAt: DateTime.makeUnsafe(args.revokedAt) }),
        );
      default:
        throw new Error("unsupported source-renewal command");
    }
  }
  let chain = Promise.resolve();
  const respond = (message, closeAfter) => {
    if (!process.connected) return;
    process.send(message, (error) => {
      if (error) process.exitCode = 1;
      if (closeAfter && process.connected) process.disconnect();
    });
  };
  process.on("message", (message) => {
    chain = chain
      .then(async () => {
        try {
          millis = message.millis;
          const value = await dispatch(message.command, message.args);
          respond({ id: message.id, value }, message.command === "close");
        } catch (error) {
          if (message.command === "close") process.exitCode = 1;
          respond(
            {
              id: message.id,
              error: {
                message: String(error.message).slice(0, 512),
                code: error.code,
                _tag: error._tag,
                cleanupErrors:
                  error instanceof AggregateError
                    ? error.errors
                        .slice(0, 8)
                        .map((failure) => String(failure?.message ?? failure).slice(0, 512))
                    : [],
              },
            },
            message.command === "close",
          );
        }
      })
      .catch(() => {
        process.exitCode = 1;
      });
  });
  process.on("disconnect", () => {
    void close().then(
      () => {
        process.exitCode ??= 0;
      },
      () => {
        process.exitCode = 1;
      },
    );
  });
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  await main();
}
