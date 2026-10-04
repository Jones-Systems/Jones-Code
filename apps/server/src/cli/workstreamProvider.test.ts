// @effect-diagnostics nodeBuiltinImport:off -- CLI fixtures use only one cleanup-owned synthetic home and qualified payload.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import { AuthSessionId, WORKSTREAMS_T3_PROVIDER_PROTOCOL } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/unstable/cli";
import { qualifiedPayloadDigest, QUALIFIED_RUNTIME_RECEIPT } from "../cloud/qualifiedRuntime.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../cloud/serviceProtocol.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import enrollmentMigration from "../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import type { NativeEnrollmentRequest } from "../workstreams/enrollment/request.ts";
import { workstreamCommand } from "./workstream.ts";
import { runWorkstreamProviderCliOperation } from "./workstreamProvider.ts";

vi.mock("../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const unexpectedNetwork = Effect.die("Provider enrollment fixture invoked a network operation.");
const cliRuntimeLayer = Layer.mergeAll(
  NodeServices.layer,
  TestConsole.layer,
  Layer.succeed(NetService.NetService, {
    canListenOnHost: () => unexpectedNetwork,
    isPortAvailableOnLoopback: () => unexpectedNetwork,
    hasListenerOnHost: () => unexpectedNetwork,
    reserveLoopbackPort: () => unexpectedNetwork,
    findAvailablePort: () => unexpectedNetwork,
  }),
);
const version = "0.0.0-preview.20261002.100";
const request: NativeEnrollmentRequest = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "synthetic-enrollment",
  registry_origin: "https://registry.invalid",
  context: {
    owner_id: "synthetic-owner",
    principal_id: "synthetic-principal",
    source_instance_id: "synthetic-environment",
    authority_namespace: "t3-native:12345678-1234-4234-8234-123456789abc",
    store_generation: 7,
    enrollment_id: "synthetic-enrollment",
    protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
    build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
  },
  session: {
    session_id: AuthSessionId.make("synthetic-reserved-session"),
    issued_at: "2026-01-01T00:00:00.000Z",
    expires_at: "2026-01-31T00:00:00.000Z",
  },
};
const fixture = Effect.acquireRelease(
  Effect.gen(function* () {
    const platform = yield* HostProcessPlatform;
    const architecture = yield* HostProcessArchitecture;
    return yield* Effect.tryPromise(async () => {
      const baseDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "workstreams-provider-cli-synthetic-"),
      );
      try {
        const authorityDir = NodePath.join(baseDir, "native-store-authority");
        const release = NodePath.join(baseDir, "runtime", "versions", version);
        for (const dir of [NodePath.join(baseDir, "userdata"), authorityDir, release])
          NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
        NodeFS.writeFileSync(
          NodePath.join(baseDir, "runtime", "service-state.json"),
          encodeJson({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: version }),
          { mode: 0o600 },
        );
        NodeFS.writeFileSync(
          NodePath.join(baseDir, "userdata", "environment-id"),
          "synthetic-environment\n",
          { mode: 0o600 },
        );
        NodeFS.writeFileSync(
          NodePath.join(authorityDir, "native-store-authority-v1.json"),
          encodeJson({
            record_version: "t3-native-store-authority/1.0.0",
            environment_id: "synthetic-environment",
            authority_namespace: request.context.authority_namespace,
            store_generation: 7,
            state: "active",
            transition_id: null,
          }),
          { mode: 0o600 },
        );
        NodeFS.writeFileSync(NodePath.join(release, "t3"), "synthetic executable bytes", {
          mode: 0o700,
        });
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
            workflow:
              platform === "darwin"
                ? ".github/workflows/artifact-desktop-mac.yml"
                : ".github/workflows/artifact-cli-linux.yml",
            artifactDigest: `sha256:${"d".repeat(64)}`,
            archiveSha256: "e".repeat(64),
            platform,
            architecture,
            payloadSha256: await qualifiedPayloadDigest(release),
          }),
          { mode: 0o600 },
        );
        const requestFile = NodePath.join(baseDir, "request.json");
        NodeFS.writeFileSync(requestFile, encodeJson(request), { mode: 0o600 });
        return {
          baseDir,
          authorityDir,
          requestFile,
          credentialPath: NodePath.join(baseDir, "userdata", "provider.json"),
          dbPath: NodePath.join(baseDir, "userdata", "state.sqlite"),
          secretsDir: NodePath.join(baseDir, "userdata", "secrets"),
        };
      } catch (error) {
        NodeFS.rmSync(baseDir, { recursive: true, force: true });
        throw error;
      }
    });
  }).pipe(
    Effect.provideService(HostProcessPlatform, HostProcessPlatform.defaultValue()),
    Effect.provideService(HostProcessArchitecture, HostProcessArchitecture.defaultValue()),
  ),
  (f) => Effect.sync(() => NodeFS.rmSync(f.baseDir, { recursive: true, force: true })),
).pipe(
  Effect.tap((f) =>
    Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* enrollmentMigration;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: f.dbPath })), Effect.scoped),
  ),
  Effect.tap((f) => Effect.sync(() => NodeFS.chmodSync(f.dbPath, 0o600))),
  Effect.tap(() => TestClock.setTime(Date.parse(request.session.issued_at))),
);
const snapshot = (directory: string): Record<string, unknown> =>
  Object.fromEntries(
    NodeFS.readdirSync(directory)
      .sort()
      .map((name) => {
        const path = NodePath.join(directory, name);
        const stat = NodeFS.lstatSync(path);
        return [
          name,
          stat.isDirectory()
            ? snapshot(path)
            : {
                bytes: NodeFS.readFileSync(path).toString("hex"),
                mode: stat.mode,
                mtime: stat.mtimeMs,
              },
        ];
      }),
  );

it.effect(
  "provider plan and readback read a qualified synthetic database without creating or changing files",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = snapshot(f.baseDir);
      const planned = yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" });
      assert.strictEqual(planned.state, "planned");
      assert.strictEqual(planned.session_id, request.session.session_id);
      const readback = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "readback",
        expectedCredentialSha256: "d".repeat(64),
      });
      assert.strictEqual(readback.reason, "records_absent");
      assert.deepEqual(snapshot(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(f.secretsDir), false);
    }).pipe(Effect.scoped),
);

it.effect(
  "provider apply recovers a lost reply using the same reserved session and emits only public receipts",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      NodeFS.mkdirSync(f.secretsDir, { mode: 0o700 });
      NodeFS.writeFileSync(
        NodePath.join(f.secretsDir, "server-signing-key.bin"),
        new Uint8Array(32).fill(7),
        { mode: 0o600 },
      );
      const applied = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "apply",
        expectedPriorSha256: null,
      });
      assert.strictEqual(applied.state, "applied");
      const repeated = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "apply",
        expectedPriorSha256: null,
      });
      assert.strictEqual(repeated.state, "unchanged");
      assert.strictEqual(repeated.session_id, applied.session_id);
      assert.strictEqual(repeated.file_post_sha256, applied.file_post_sha256);
      assert.deepEqual(repeated.completed_phases, [
        "native_records_reserved",
        "native_credential_published",
      ]);
      const publicText = encodeJson(repeated);
      for (const forbidden of [
        "Bearer ",
        "authorization_header",
        f.dbPath,
        f.credentialPath,
        request.registry_origin,
      ])
        assert.strictEqual(publicText.includes(forbidden), false);
      assert.strictEqual(
        (yield* runWorkstreamProviderCliOperation({
          ...f,
          operation: "readback",
          expectedCredentialSha256: applied.file_post_sha256!,
        })).state,
        "unchanged",
      );
      assert.deepEqual(NodeFS.readdirSync(NodePath.dirname(f.credentialPath)).sort(), [
        "environment-id",
        "provider.json",
        "secrets",
        "state.sqlite",
      ]);
    }).pipe(Effect.scoped),
);

it.effect("strict provider inspection refuses WAL before constructing the SQL layer", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const db = new NodeSqlite.DatabaseSync(f.dbPath);
    try {
      db.exec("PRAGMA journal_mode=WAL");
    } finally {
      db.close();
    }
    const before = snapshot(f.baseDir);
    const layer = vi.spyOn(NodeSqliteClient, "layer");
    yield* Effect.gen(function* () {
      const planned = yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" });
      assert.strictEqual(planned.reason, "records_unavailable");
      const readback = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "readback",
        expectedCredentialSha256: "d".repeat(64),
      });
      assert.strictEqual(readback.reason, "records_unavailable");
      assert.strictEqual(layer.mock.calls.length, 0);
      assert.deepEqual(snapshot(f.baseDir), before);
    }).pipe(Effect.ensuring(Effect.sync(() => layer.mockRestore())));
  }).pipe(Effect.scoped),
);

it.effect("provider operations refuse a preexisting rollback journal before opening SQLite", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    NodeFS.writeFileSync(`${f.dbPath}-journal`, "synthetic preexisting journal", {
      flag: "wx",
      mode: 0o600,
    });
    const before = snapshot(f.baseDir);
    const layer = vi.spyOn(NodeSqliteClient, "layer");
    yield* Effect.gen(function* () {
      const planned = yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" });
      const applied = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "apply",
        expectedPriorSha256: null,
      });
      const readback = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "readback",
        expectedCredentialSha256: "d".repeat(64),
        sqliteSidecarEffects: "allow",
      });
      for (const receipt of [planned, applied, readback]) {
        assert.strictEqual(receipt.state, "unknown");
        assert.strictEqual(receipt.reason, "qualification_failed");
        assert.deepEqual(receipt.completed_phases, []);
      }
      assert.strictEqual(layer.mock.calls.length, 0);
      assert.deepEqual(snapshot(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(f.secretsDir), false);
    }).pipe(Effect.ensuring(Effect.sync(() => layer.mockRestore())));
  }).pipe(Effect.scoped),
);

it.effect(
  "explicit operational inspection reads a checkpointed WAL store without issuance or record changes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const writer = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        writer.exec("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE)");
      } finally {
        writer.close();
      }
      assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-wal`), false);
      assert.strictEqual(NodeFS.existsSync(`${f.dbPath}-shm`), false);
      const before = NodeFS.readFileSync(f.dbPath);
      assert.strictEqual(
        (yield* runWorkstreamProviderCliOperation({
          ...f,
          operation: "plan",
          sqliteSidecarEffects: "allow",
        })).state,
        "planned",
      );
      assert.strictEqual(
        (yield* runWorkstreamProviderCliOperation({
          ...f,
          operation: "readback",
          sqliteSidecarEffects: "allow",
          expectedCredentialSha256: "d".repeat(64),
        })).reason,
        "records_absent",
      );
      assert.deepEqual(NodeFS.readFileSync(f.dbPath), before);
      assert.strictEqual(NodeFS.existsSync(f.secretsDir), false);
      assert.strictEqual(NodeFS.existsSync(f.credentialPath), false);
      const verification = new NodeSqlite.DatabaseSync(f.dbPath, { readOnly: true });
      try {
        assert.strictEqual(
          verification.prepare("SELECT COUNT(*) AS count FROM auth_sessions").get()?.count,
          0,
        );
        assert.strictEqual(
          verification.prepare("SELECT COUNT(*) AS count FROM workstreams_native_enrollments").get()
            ?.count,
          0,
        );
      } finally {
        verification.close();
      }
    }).pipe(Effect.scoped),
);

it.effect(
  "provider apply supports a qualified WAL store and preserves exact enrollment on retry",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const writer = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        writer.exec("PRAGMA journal_mode=WAL");
      } finally {
        writer.close();
      }
      NodeFS.mkdirSync(f.secretsDir, { mode: 0o700 });
      NodeFS.writeFileSync(
        NodePath.join(f.secretsDir, "server-signing-key.bin"),
        new Uint8Array(32).fill(7),
        { mode: 0o600 },
      );
      const applied = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "apply",
        expectedPriorSha256: null,
      });
      assert.strictEqual(applied.state, "applied");
      const repeated = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "apply",
        expectedPriorSha256: null,
      });
      assert.strictEqual(repeated.state, "unchanged");
      assert.strictEqual(repeated.session_id, request.session.session_id);
      assert.strictEqual(repeated.file_post_sha256, applied.file_post_sha256);
      const inspected = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "readback",
        sqliteSidecarEffects: "allow",
        expectedCredentialSha256: applied.file_post_sha256!,
      });
      assert.strictEqual(inspected.state, "unchanged");
      assert.deepEqual(inspected.completed_phases, [
        "native_records_reserved",
        "native_credential_published",
      ]);
      const verification = new NodeSqlite.DatabaseSync(f.dbPath, { readOnly: true });
      try {
        assert.strictEqual(
          verification.prepare("SELECT COUNT(*) AS count FROM auth_sessions").get()?.count,
          1,
        );
        assert.strictEqual(
          verification.prepare("SELECT COUNT(*) AS count FROM workstreams_native_enrollments").get()
            ?.count,
          1,
        );
      } finally {
        verification.close();
      }
    }).pipe(Effect.scoped),
);

it.effect(
  "provider commands require explicit locations and retain their reserved request instead of issuing a fresh ID",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = snapshot(f.baseDir);
      const run = Command.runWith(workstreamCommand, { version });
      const missing = yield* run(["provider", "plan", "--base-dir", f.baseDir, f.requestFile]).pipe(
        Effect.result,
      );
      assert.strictEqual(missing._tag, "Failure");
      yield* run([
        "provider",
        "plan",
        "--base-dir",
        f.baseDir,
        "--authority-dir",
        f.authorityDir,
        "--credential-path",
        f.credentialPath,
        f.requestFile,
      ]);
      const lines = yield* TestConsole.logLines;
      assert.strictEqual(
        lines.some(
          (line) =>
            typeof line === "string" && line.includes('"session_id":"synthetic-reserved-session"'),
        ),
        true,
      );
      assert.deepEqual(snapshot(f.baseDir), before);
    }).pipe(Effect.provide(cliRuntimeLayer), Effect.scoped),
);

it.effect("the sidecar-effects flag is closed and cannot qualify the selected V2 store", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const db = new NodeSqlite.DatabaseSync(f.dbPath);
    try {
      db.exec("PRAGMA journal_mode=WAL");
    } finally {
      db.close();
    }
    const before = snapshot(f.baseDir);
    const run = Command.runWith(workstreamCommand, { version });
    const locations = [
      "--base-dir",
      f.baseDir,
      "--authority-dir",
      f.authorityDir,
      "--credential-path",
      f.credentialPath,
    ];
    yield* run(["provider", "plan", ...locations, f.requestFile]);
    const strictLines = yield* TestConsole.logLines;
    assert.strictEqual(
      strictLines.some(
        (line) => typeof line === "string" && line.includes('"reason":"qualification_failed"'),
      ),
      true,
    );
    yield* run([
      "provider",
      "plan",
      ...locations,
      "--sqlite-sidecar-effects",
      "allow",
      f.requestFile,
    ]);
    const operationalLines = yield* TestConsole.logLines;
    assert.strictEqual(
      operationalLines
        .slice(strictLines.length)
        .some(
          (line) => typeof line === "string" && line.includes('"reason":"qualification_failed"'),
        ),
      true,
    );
    yield* run([
      "provider",
      "readback",
      ...locations,
      "--sqlite-sidecar-effects",
      "allow",
      "--expected-credential-sha256",
      "d".repeat(64),
      f.requestFile,
    ]);
    const readbackLines = yield* TestConsole.logLines;
    assert.strictEqual(
      readbackLines
        .slice(operationalLines.length)
        .some(
          (line) => typeof line === "string" && line.includes('"reason":"qualification_failed"'),
        ),
      true,
    );
    assert.strictEqual(
      (yield* run([
        "provider",
        "plan",
        ...locations,
        "--sqlite-sidecar-effects",
        "unbounded",
        f.requestFile,
      ]).pipe(Effect.result))._tag,
      "Failure",
    );
    assert.strictEqual(
      (yield* run([
        "provider",
        "apply",
        ...locations,
        "--sqlite-sidecar-effects",
        "allow",
        "--expected-prior-sha256",
        "absent",
        f.requestFile,
      ]).pipe(Effect.result))._tag,
      "Failure",
    );
    assert.deepEqual(snapshot(f.baseDir), before);
    assert.strictEqual(NodeFS.existsSync(f.secretsDir), false);
    assert.strictEqual(NodeFS.existsSync(f.credentialPath), false);
  }).pipe(Effect.provide(cliRuntimeLayer), Effect.scoped),
);

it.effect(
  "provider CLI does not repair missing schemas or initialize missing qualification state",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      NodeFS.unlinkSync(f.dbPath);
      const before = snapshot(f.baseDir);
      assert.strictEqual(
        (yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" })).state,
        "unknown",
      );
      assert.deepEqual(snapshot(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(f.dbPath), false);
      const incompatible = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        incompatible.exec("CREATE TABLE unrelated_fixture (id TEXT)");
      } finally {
        incompatible.close();
      }
      NodeFS.chmodSync(f.dbPath, 0o600);
      const schemaBefore = snapshot(f.baseDir);
      const schemaResult = yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" });
      assert.strictEqual(schemaResult.reason, "records_unavailable");
      assert.deepEqual(snapshot(f.baseDir), schemaBefore);
    }).pipe(Effect.scoped),
);
