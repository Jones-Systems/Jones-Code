// @effect-diagnostics nodeBuiltinImport:off -- Only task-owned synthetic fixture files are created; cleanup remains root-owned.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Config from "../../../../config.ts";
import * as Context from "effect/Context";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Current from "./currentAuthority.ts";
import * as Ownership from "../../../../serverOwnership.ts";
import * as ProcessRunner from "../../../../processRunner.ts";
import { ServiceLauncherHostProcess } from "../../../../cloud/serviceLauncherClient.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "../../../../cloud/serviceProtocol.ts";
import {
  qualifiedPayloadDigest,
  QUALIFIED_RUNTIME_RECEIPT,
} from "../../../cloud/qualifiedRuntime.ts";
import { initializeNativeStoreAuthority } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Updates from "../../../updates/UpdateAdmissionFence.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export const prepare = Effect.fn("NativeCurrentAuthorityFixture.prepare")(function* (f: {
  readonly home: string;
  readonly database: string;
  readonly context: {
    readonly source_instance_id: string;
    readonly authority_namespace: string;
    readonly store_generation: number;
  };
}) {
  const version = "0.0.0-preview.20261002.100";
  const release = NodePath.join(f.home, "runtime", "versions", version);
  const authorityDir = NodePath.join(f.home, "native-store-authority");
  for (const directory of [release, authorityDir]) {
    NodeFS.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  NodeFS.chmodSync(f.database, 0o600);
  NodeFS.writeFileSync(
    NodePath.join(f.home, "userdata", "environment-id"),
    f.context.source_instance_id + "\n",
    { mode: 0o600 },
  );
  NodeFS.writeFileSync(
    NodePath.join(f.home, "runtime", "service-state.json"),
    encodeJson({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: version }),
    { mode: 0o600 },
  );
  NodeFS.writeFileSync(
    NodePath.join(authorityDir, "native-store-authority-v1.json"),
    encodeJson({
      record_version: "t3-native-store-authority/1.0.0",
      environment_id: f.context.source_instance_id,
      authority_namespace: f.context.authority_namespace,
      store_generation: f.context.store_generation,
      state: "active",
      transition_id: null,
    }),
    { mode: 0o600 },
  );
  initializeNativeStoreAuthority(authorityDir, f.context.source_instance_id);
  NodeFS.writeFileSync(NodePath.join(release, "t3"), "synthetic entry executable", { mode: 0o700 });
  NodeFS.writeFileSync(
    NodePath.join(release, QUALIFIED_RUNTIME_RECEIPT),
    encodeJson({
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
        qualifiedPayloadDigest(release, "linux", ".github/workflows/artifact-cli-linux.yml"),
      ),
    }),
    { mode: 0o600 },
  );
  const config = Context.get(
    yield* Layer.build(Config.layerTest(f.home, f.home)),
    Config.ServerConfig,
  );
  expect(config.dbPath).toBe(f.database);
  const ownership = yield* Ownership.acquireServerOwnership(config.serverRuntimeStatePath).pipe(
    Effect.provide(ProcessRunner.layer),
  );
  // Release metadata and launcher transport are synthetic; ownership, authority qualification,
  // selected SQLite and CurrentAuthority remain production services.
  const host = ServiceLauncherHostProcess.of({
    connected: true,
    send: () => {
      throw new Error("No update handoff is permitted by this authority fixture");
    },
    on: () => undefined,
    off: () => undefined,
  });
  return Current.layer.pipe(
    Layer.provide(Layer.succeed(Config.ServerConfig, config)),
    Layer.provide(Layer.succeed(Ownership.ServerOwnershipLifetime, ownership.lifetime)),
    Layer.provide(Updates.layer),
    Layer.provide(Layer.succeed(ServiceLauncherHostProcess, host)),
    Layer.provide(
      Layer.succeed(HostProcess.Environment, {
        [SERVICE_LAUNCHER_CONTEXT_ENV]: encodeJson({
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          childVersion: version,
        }),
      }),
    ),
    Layer.provide(ProcessRunner.layer),
  );
});
