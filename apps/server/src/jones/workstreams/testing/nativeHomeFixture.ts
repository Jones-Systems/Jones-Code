// @effect-diagnostics nodeBuiltinImport:off -- Explicit synthetic-home test boundary; never imported by application startup.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as EffectNodeCrypto from "@effect/platform-node/NodeCrypto";
import { AuthSessionId, WorkstreamsNativeBuild } from "@t3tools/contracts";
import * as Net from "@t3tools/shared/Net";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Command } from "effect/cli";
import * as SessionStore from "../../../auth/SessionStore.ts";
import * as Secrets from "../../../auth/ServerSecretStore.ts";
import * as ServerConfig from "../../../config.ts";
import * as Persistence from "../../../persistence/Sqlite.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { resolveCliAuthConfig } from "../../../cli/config.ts";
import * as Option from "effect/Option";
import packageJson from "../../../../package.json" with { type: "json" };
import { SERVICE_LAUNCHER_PROTOCOL } from "../../../cloud/serviceProtocol.ts";
import { initializeNativeStoreAuthorityForBaseDir } from "../../../environment/nativeStoreAuthorityPersistence.ts";
import { QUALIFIED_RUNTIME_RECEIPT, qualifiedPayloadDigest } from "../../cloud/qualifiedRuntime.ts";
import type { RegistryInit } from "../native/cli.ts";
import type { NativeConfig } from "../native/mode/config.ts";
import { registryCommand } from "../native/runtime/cli.ts";
import { NativeSourceSetupRequest } from "../native/runtime/sourceEnrollment.ts";

export class NativeHomeFixtureError extends Schema.TaggedError<
  NativeHomeFixtureError,
  { readonly brand: unique symbol }
>()("NativeHomeFixtureError", {
  code: Schema.Literals(["unsafe_target", "build_unavailable", "preparation_failed"]),
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  override get message() {
    return `Native home fixture refused: ${this.code}.`;
  }
}

const decodeBuild = Schema.decodeUnknownEffect(WorkstreamsNativeBuild);
const decodeSetup = Schema.decodeUnknownSync(NativeSourceSetupRequest);
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const writeJson = (path: string, value: unknown) =>
  NodeFS.writeFileSync(path, encode(value), { mode: 0o600, flag: "wx" });

/** Populate an existing empty 0700 directory. The caller owns its lifetime, including failed setup. */
export const prepareNativeHomeFixture = Effect.fn("NativeHomeFixture.prepare")(function* (
  home: string,
) {
  const platform = yield* HostProcess.Platform;
  const architecture = yield* HostProcess.Architecture;
  const build = yield* decodeBuild(
    (packageJson as { readonly jonesSource?: unknown }).jonesSource,
  ).pipe(
    Effect.mapError((cause) => new NativeHomeFixtureError({ code: "build_unavailable", cause })),
  );
  yield* Effect.try({
    try: () => {
      const live = NodePath.join(NodeOS.homedir(), ".t3");
      if (
        platform !== "linux" ||
        !NodePath.isAbsolute(home) ||
        NodePath.resolve(home) !== home ||
        home === live ||
        home.startsWith(live + NodePath.sep) ||
        /[\r\n\0]/.test(home)
      )
        throw new Error("Unsafe target");
      const stat = NodeFS.lstatSync(home);
      if (
        stat.isSymbolicLink() ||
        !stat.isDirectory() ||
        stat.uid !== process.getuid?.() ||
        (stat.mode & 0o077) !== 0 ||
        NodeFS.realpathSync(home) !== home ||
        NodeFS.readdirSync(home).length !== 0
      )
        throw new Error("Target must be an empty owned private directory");
      // Exclusive marker prevents a second fixture invocation from adopting a partial home.
      writeJson(NodePath.join(home, "synthetic-native-home.json"), {
        schema: "jones-code.synthetic-native-home/v1",
        build,
        executable: false,
      });
    },
    catch: (cause) => new NativeHomeFixtureError({ code: "unsafe_target", cause }),
  });
  const environmentId = "fixture-environment-" + NodeCrypto.randomUUID();
  const database = NodePath.join(home, "userdata", "statev2.sqlite");
  yield* Effect.tryPromise({
    try: async () => {
      const release = NodePath.join(home, "runtime", "versions", packageJson.version);
      NodeFS.mkdirSync(NodePath.dirname(database), { mode: 0o700 });
      NodeFS.mkdirSync(release, { recursive: true, mode: 0o700 });
      NodeFS.writeFileSync(
        NodePath.join(home, "userdata", "environment-id"),
        environmentId + "\n",
        { mode: 0o600, flag: "wx" },
      );
      writeJson(NodePath.join(home, "runtime", "service-state.json"), {
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: packageJson.version,
      });
      // Synthetic receipt exercises real qualification, but attests no packaged executable or installed runtime.
      NodeFS.writeFileSync(NodePath.join(release, "t3"), "#!/bin/sh\nexit 1\n", {
        mode: 0o700,
        flag: "wx",
      });
      writeJson(NodePath.join(release, QUALIFIED_RUNTIME_RECEIPT), {
        protocol: 1,
        repository: build.repository,
        channel: "jones-main",
        version: packageJson.version,
        sourceSha: build.sha,
        sourceTree: build.tree,
        installedSourceSha: build.sha,
        runId: 1,
        runAttempt: 1,
        artifactId: 1,
        workflow: ".github/workflows/artifact-cli-linux.yml",
        artifactDigest: "sha256:" + "0".repeat(64),
        archiveSha256: "0".repeat(64),
        platform: "linux",
        architecture,
        payloadSha256: await qualifiedPayloadDigest(
          release,
          "linux",
          ".github/workflows/artifact-cli-linux.yml",
        ),
      });
    },
    catch: (cause) => new NativeHomeFixtureError({ code: "preparation_failed", cause }),
  });
  const authority = yield* Effect.try({
    try: () => initializeNativeStoreAuthorityForBaseDir(home, SERVICE_LAUNCHER_PROTOCOL),
    catch: (cause) => new NativeHomeFixtureError({ code: "preparation_failed", cause }),
  });
  const registry: RegistryInit["registry"] = {
    registry_instance_id: NodeCrypto.randomUUID(),
    owner_id: "fixture-owner",
    canonical_origin: "https://native-home-fixture.invalid",
    home_environment_id: environmentId,
    server_generation: 1,
  };
  const init: RegistryInit = {
    registry,
    principal_id: "fixture-principal",
    content_key_id: "fixture-home",
    local_key_id: "fixture-local",
  };
  const run = Command.runWith(registryCommand, { version: packageJson.version });
  const selected = ["--home", home, "--key-provider", "server-secret-store"];
  yield* run(["keys", "init", ...selected, "--request", encode(init)]);
  yield* run(["init", ...selected, "--request", encode(init)]);
  // A first server start normally creates the signing key. Use that same service before standalone issuance.
  const authConfig = yield* resolveCliAuthConfig({ baseDir: Option.some(home) }, Option.none());
  const authLayer = SessionStore.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ServerEnvironment.layerIdentity,
        Secrets.layer,
        Persistence.layerFromPath(database),
      ),
    ),
    Layer.provide(ServerConfig.layer(authConfig)),
  );
  yield* SessionStore.SessionStore.pipe(Effect.provide(authLayer), Effect.scoped);
  const config: NativeConfig = {
    mode: "native-home",
    registry,
    bootstrap: { home, database, key_provider: "server-secret-store" },
  };
  yield* Effect.try({
    try: () => {
      writeJson(NodePath.join(home, "workstreams-native.json"), config);
      // The init connection is closed. Standalone strict plan/readback require a quiesced rollback store.
      const db = new NodeSqlite.DatabaseSync(database);
      try {
        db.exec("PRAGMA journal_mode=DELETE");
      } finally {
        db.close();
      }
    },
    catch: (cause) => new NativeHomeFixtureError({ code: "preparation_failed", cause }),
  });
  const now = yield* DateTime.now;
  const request = decodeSetup({
    schema: "jones-code.native-source-setup/v1",
    enrollment: {
      schema: "jones-code.workstreams-native-enrollment/v1",
      enrollment_id: "fixture-source-enrollment",
      registry_origin: registry.canonical_origin,
      context: {
        owner_id: registry.owner_id,
        principal_id: init.principal_id,
        source_instance_id: environmentId,
        authority_namespace: authority.authority_namespace,
        store_generation: authority.store_generation,
        enrollment_id: "fixture-source-enrollment",
        protocol: "workstreams-t3-provider/1.0.0",
        build,
      },
      session: {
        session_id: AuthSessionId.make("fixture-source-session"),
        issued_at: DateTime.formatIso(now),
        expires_at: DateTime.formatIso(DateTime.add(now, { hours: 24 })),
      },
    },
    expected: {
      registry_instance_id: registry.registry_instance_id,
      owner_id: registry.owner_id,
      home_environment_id: environmentId,
      home_generation: 1,
      registry_version: 1,
      singleton_binding_version: 1,
      grant_id: "grant:" + init.principal_id,
      grant_version: 1,
      source_binding: null,
    },
    rebind: false,
  });
  yield* Effect.try({
    try: () => writeJson(NodePath.join(home, "source-setup-request.json"), request),
    catch: (cause) => new NativeHomeFixtureError({ code: "preparation_failed", cause }),
  });
  for (const action of ["plan", "apply", "readback"] as const)
    yield* run(["source-setup", action, ...selected, "--request", encode(request)]);
  const ready: NativeConfig = {
    ...config,
    bootstrap: { ...config.bootstrap!, source_session_id: request.enrollment.session.session_id },
  };
  yield* Effect.try({
    try: () =>
      NodeFS.writeFileSync(NodePath.join(home, "workstreams-native.json"), encode(ready), {
        mode: 0o600,
      }),
    catch: (cause) => new NativeHomeFixtureError({ code: "preparation_failed", cause }),
  });
  return { home, database, environmentId, build, config: ready, request };
});

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  const home = process.argv[2];
  if (process.argv.length !== 3 || !home) {
    process.stderr.write(
      "Usage: node nativeHomeFixture.ts <existing-empty-private-absolute-home>\n",
    );
    process.exitCode = 2;
  } else {
    await prepareNativeHomeFixture(home)
      .pipe(
        Effect.tap((result) =>
          Console.log(
            encode({
              home: result.home,
              build: result.build,
              source_session_id: result.config.bootstrap?.source_session_id,
              synthetic: true,
            }),
          ),
        ),
        Effect.scoped,
        Effect.provide(Layer.mergeAll(NodeServices.layer, Net.layer, EffectNodeCrypto.layer)),
        Effect.runPromise,
      )
      .catch(() => {
        process.stderr.write(
          "Native home fixture failed; retain the owned target for diagnosis.\n",
        );
        process.exitCode = 1;
      });
  }
}
