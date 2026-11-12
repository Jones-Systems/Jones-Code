// @effect-diagnostics nodeBuiltinImport:off -- Tests build one cleanup-owned synthetic T3 home per case.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { qualifiedPayloadDigest, QUALIFIED_RUNTIME_RECEIPT } from "../../cloud/qualifiedRuntime.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../../../cloud/serviceProtocol.ts";

// Test files mock package.json with this version and Jones source identity.
export const syntheticVersion = "0.0.0-preview.20261002.100";
export const syntheticBuild = {
  repository: "Jones-Systems/Jones-Code",
  sha: "a".repeat(40),
  tree: "b".repeat(40),
} as const;
export const syntheticEnvironmentId = "synthetic-environment";
export const syntheticAuthorityNamespace = "t3-native:12345678-1234-4234-8234-123456789abc";
export const syntheticStoreGeneration = 7;

export interface SyntheticT3Home {
  readonly baseDir: string;
  readonly authorityDir: string;
  readonly authorityPath: string;
  readonly statePath: string;
  readonly environmentPath: string;
  readonly dbPath: string;
  readonly receiptPath: string;
  readonly secretsDir: string;
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/**
 * A qualified launcher home with active native authority and a receipt-bound release.
 * The caller creates `dbPath`; release removes the whole temporary home.
 */
export const syntheticT3Home = (prefix: string) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const platform = yield* HostProcess.Platform;
      const architecture = yield* HostProcess.Architecture;
      return yield* Effect.tryPromise(async (): Promise<SyntheticT3Home> => {
        const baseDir = NodeFS.realpathSync(
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), prefix)),
        );
        try {
          const authorityDir = NodePath.join(baseDir, "native-store-authority");
          const release = NodePath.join(baseDir, "runtime", "versions", syntheticVersion);
          for (const dir of [NodePath.join(baseDir, "userdata"), authorityDir, release])
            NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
          const statePath = NodePath.join(baseDir, "runtime", "service-state.json");
          NodeFS.writeFileSync(
            statePath,
            encodeJson({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: syntheticVersion }),
            { mode: 0o600 },
          );
          const environmentPath = NodePath.join(baseDir, "userdata", "environment-id");
          NodeFS.writeFileSync(environmentPath, `${syntheticEnvironmentId}\n`, { mode: 0o600 });
          const authorityPath = NodePath.join(authorityDir, "native-store-authority-v1.json");
          NodeFS.writeFileSync(
            authorityPath,
            encodeJson({
              record_version: "t3-native-store-authority/1.0.0",
              environment_id: syntheticEnvironmentId,
              authority_namespace: syntheticAuthorityNamespace,
              store_generation: syntheticStoreGeneration,
              state: "active",
              transition_id: null,
            }),
            { mode: 0o600 },
          );
          NodeFS.writeFileSync(NodePath.join(release, "t3"), "synthetic executable bytes", {
            mode: 0o700,
          });
          const receiptPath = NodePath.join(release, QUALIFIED_RUNTIME_RECEIPT);
          NodeFS.writeFileSync(
            receiptPath,
            encodeJson({
              protocol: 1,
              repository: syntheticBuild.repository,
              channel: "jones-main",
              version: syntheticVersion,
              sourceSha: syntheticBuild.sha,
              sourceTree: syntheticBuild.tree,
              installedSourceSha: "c".repeat(40),
              runId: 101,
              runAttempt: 1,
              artifactId: 102,
              workflow:
                platform === "darwin"
                  ? ".github/workflows/artifact-desktop-mac.yml"
                  : ".github/workflows/artifact-cli-linux.yml",
              artifactDigest: `sha256:${"d".repeat(64)}`,
              archiveSha256: "e".repeat(64),
              platform,
              architecture,
              payloadSha256: await qualifiedPayloadDigest(
                release,
                platform === "darwin" ? "darwin" : "linux",
                platform === "darwin"
                  ? ".github/workflows/artifact-desktop-mac.yml"
                  : ".github/workflows/artifact-cli-linux.yml",
              ),
            }),
            { mode: 0o600 },
          );
          return {
            baseDir,
            authorityDir,
            authorityPath,
            statePath,
            environmentPath,
            dbPath: NodePath.join(baseDir, "userdata", "statev2.sqlite"),
            receiptPath,
            secretsDir: NodePath.join(baseDir, "userdata", "secrets"),
          };
        } catch (error) {
          NodeFS.rmSync(baseDir, { recursive: true, force: true });
          throw error;
        }
      });
    }).pipe(
      Effect.provideService(HostProcess.Platform, HostProcess.Platform.defaultValue()),
      Effect.provideService(HostProcess.Architecture, HostProcess.Architecture.defaultValue()),
    ),
    (home) => Effect.sync(() => NodeFS.rmSync(home.baseDir, { recursive: true, force: true })),
  );

export const writeSyntheticSigningKey = (home: SyntheticT3Home) => {
  NodeFS.mkdirSync(home.secretsDir, { mode: 0o700 });
  NodeFS.writeFileSync(
    NodePath.join(home.secretsDir, "server-signing-key.bin"),
    new Uint8Array(32).fill(7),
    { mode: 0o600 },
  );
};

export const snapshotTree = (directory: string): Record<string, unknown> =>
  Object.fromEntries(
    NodeFS.readdirSync(directory)
      .sort()
      .map((name) => {
        const path = NodePath.join(directory, name);
        const stat = NodeFS.lstatSync(path);
        return [
          name,
          stat.isDirectory()
            ? snapshotTree(path)
            : {
                bytes: NodeFS.readFileSync(path).toString("hex"),
                mode: stat.mode,
                mtime: stat.mtimeMs,
              },
        ];
      }),
  );
