// @effect-diagnostics nodeBuiltinImport:off -- CLI fixtures use only one cleanup-owned synthetic home and qualified payload.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NetService from "@t3tools/shared/Net";
import { AuthSessionId, WORKSTREAMS_T3_PROVIDER_PROTOCOL } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import { runMigrations } from "../../../persistence/Migrations.ts";
import enrollmentMigration from "../../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import { workstreamCommand } from "../../../cli/workstream.ts";
import { prepareWorkstreamProviderRequest, runWorkstreamProviderCliOperation } from "./cli.ts";
import { NativeEnrollmentRequest, nativeEnrollmentRequestSha256 } from "./request.ts";
import {
  snapshotTree,
  syntheticAuthorityNamespace,
  syntheticBuild,
  syntheticEnvironmentId,
  syntheticStoreGeneration,
  syntheticT3Home,
  syntheticVersion,
  writeSyntheticSigningKey,
} from "./testHome.ts";

vi.mock("../../../../package.json", () => ({
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
const decodeRequestJson = Schema.decodeUnknownSync(Schema.fromJsonString(NativeEnrollmentRequest));
const decodeCredentialJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ authorization_header: Schema.String })),
);
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
const version = syntheticVersion;
const request: NativeEnrollmentRequest = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "synthetic-enrollment",
  registry_origin: "https://registry.invalid",
  context: {
    owner_id: "synthetic-owner",
    principal_id: "synthetic-principal",
    source_instance_id: syntheticEnvironmentId,
    authority_namespace: syntheticAuthorityNamespace,
    store_generation: syntheticStoreGeneration,
    enrollment_id: "synthetic-enrollment",
    protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
    build: syntheticBuild,
  },
  session: {
    session_id: AuthSessionId.make("synthetic-reserved-session"),
    issued_at: "2026-01-01T00:00:00.000Z",
    expires_at: "2026-01-31T00:00:00.000Z",
  },
};
const migrateSyntheticStore = (dbPath: string) =>
  Effect.gen(function* () {
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* enrollmentMigration;
  }).pipe(
    Effect.provide(NodeSqliteClient.layer({ filename: dbPath })),
    Effect.scoped,
    Effect.andThen(Effect.sync(() => NodeFS.chmodSync(dbPath, 0o600))),
  );
const fixture = syntheticT3Home("workstreams-provider-cli-synthetic-").pipe(
  Effect.map((home) => {
    const requestFile = NodePath.join(home.baseDir, "request.json");
    NodeFS.writeFileSync(requestFile, encodeJson(request), { mode: 0o600 });
    return {
      ...home,
      requestFile,
      credentialPath: NodePath.join(home.baseDir, "userdata", "provider.json"),
    };
  }),
  Effect.tap((f) => migrateSyntheticStore(f.dbPath)),
  Effect.tap(() => TestClock.setTime(Date.parse(request.session.issued_at))),
);

it.effect(
  "provider plan and readback read a qualified synthetic database without creating or changing files",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = snapshotTree(f.baseDir);
      const planned = yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" });
      assert.strictEqual(planned.state, "planned");
      assert.strictEqual(planned.session_id, request.session.session_id);
      const readback = yield* runWorkstreamProviderCliOperation({
        ...f,
        operation: "readback",
        expectedCredentialSha256: "d".repeat(64),
      });
      assert.strictEqual(readback.reason, "records_absent");
      assert.deepEqual(snapshotTree(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(f.secretsDir), false);
    }).pipe(Effect.scoped),
);

it.effect(
  "provider apply recovers a lost reply using the same reserved session and emits only public receipts",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      writeSyntheticSigningKey(f);
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
        "statev2.sqlite",
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
    const before = snapshotTree(f.baseDir);
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
      assert.deepEqual(snapshotTree(f.baseDir), before);
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
    const before = snapshotTree(f.baseDir);
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
      assert.deepEqual(snapshotTree(f.baseDir), before);
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
      writeSyntheticSigningKey(f);
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
      const before = snapshotTree(f.baseDir);
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
      assert.deepEqual(snapshotTree(f.baseDir), before);
    }).pipe(Effect.provide(cliRuntimeLayer), Effect.scoped),
);

it.effect("the sidecar-effects flag is closed and belongs only to plan/readback", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const db = new NodeSqlite.DatabaseSync(f.dbPath);
    try {
      db.exec("PRAGMA journal_mode=WAL");
    } finally {
      db.close();
    }
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
        (line) => typeof line === "string" && line.includes('"reason":"records_unavailable"'),
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
      operationalLines.some(
        (line) => typeof line === "string" && line.includes('"state":"planned"'),
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
      const before = snapshotTree(f.baseDir);
      assert.strictEqual(
        (yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" })).state,
        "unknown",
      );
      assert.deepEqual(snapshotTree(f.baseDir), before);
      assert.strictEqual(NodeFS.existsSync(f.dbPath), false);
      const incompatible = new NodeSqlite.DatabaseSync(f.dbPath);
      try {
        incompatible.exec("CREATE TABLE unrelated_fixture (id TEXT)");
      } finally {
        incompatible.close();
      }
      NodeFS.chmodSync(f.dbPath, 0o600);
      const schemaBefore = snapshotTree(f.baseDir);
      const schemaResult = yield* runWorkstreamProviderCliOperation({ ...f, operation: "plan" });
      assert.strictEqual(schemaResult.reason, "records_unavailable");
      assert.deepEqual(snapshotTree(f.baseDir), schemaBefore);
    }).pipe(Effect.scoped),
);

const prepareInput = (f: { readonly baseDir: string; readonly authorityDir: string }) => ({
  baseDir: f.baseDir,
  authorityDir: f.authorityDir,
  requestFile: NodePath.join(f.baseDir, "prepared-request.json"),
  ownerId: "synthetic-owner",
  principalId: "synthetic-principal",
  enrollmentId: "synthetic-prepared-enrollment",
  registryOrigin: "http://127.0.0.1:8787",
  lifetimeDays: 30,
});

it.effect(
  "prepare binds persisted qualification into a private request and never replaces an earlier one",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const input = prepareInput(f);
      const before = snapshotTree(f.baseDir);
      const prepared = yield* prepareWorkstreamProviderRequest(input);
      const bytes = NodeFS.readFileSync(input.requestFile, "utf8");
      const written = decodeRequestJson(bytes);
      assert.strictEqual(NodeFS.statSync(input.requestFile).mode & 0o777, 0o600);
      assert.deepEqual(written.context, {
        owner_id: "synthetic-owner",
        principal_id: "synthetic-principal",
        source_instance_id: syntheticEnvironmentId,
        authority_namespace: syntheticAuthorityNamespace,
        store_generation: syntheticStoreGeneration,
        enrollment_id: "synthetic-prepared-enrollment",
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        build: syntheticBuild,
      });
      assert.strictEqual(written.session.issued_at, request.session.issued_at);
      assert.strictEqual(written.session.expires_at, "2026-01-31T00:00:00.000Z");
      assert.strictEqual(prepared.request_sha256, nativeEnrollmentRequestSha256(written));
      assert.strictEqual(prepared.session_id, written.session.session_id);
      // Only the new request exists; no database connection, secret or credential was created.
      const after = snapshotTree(f.baseDir);
      delete (after as Record<string, unknown>)["prepared-request.json"];
      assert.deepEqual(after, before);
      const retry = yield* Effect.flip(prepareWorkstreamProviderRequest(input));
      assert.strictEqual(retry.code, "request_exists");
      assert.strictEqual(NodeFS.readFileSync(input.requestFile, "utf8"), bytes);
    }).pipe(Effect.scoped),
);

it.effect("prepare rejects unbounded lifetimes, unsafe origins and unqualified authority", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = prepareInput(f);
    for (const changed of [
      { lifetimeDays: 31 },
      { lifetimeDays: 0 },
      { registryOrigin: "http://localhost:8787" },
      { registryOrigin: "http://registry.invalid" },
      { requestFile: "relative-request.json" },
    ])
      assert.strictEqual(
        (yield* Effect.flip(prepareWorkstreamProviderRequest({ ...input, ...changed }))).code,
        "invalid_request",
      );
    NodeFS.writeFileSync(f.environmentPath, "other-environment\n");
    assert.strictEqual(
      (yield* Effect.flip(prepareWorkstreamProviderRequest(input))).code,
      "qualification_failed",
    );
    assert.strictEqual(NodeFS.existsSync(input.requestFile), false);
  }).pipe(Effect.scoped),
);

it.effect("a prepared request applies, publishes its bearer file and reads back unchanged", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = prepareInput(f);
    yield* prepareWorkstreamProviderRequest(input);
    writeSyntheticSigningKey(f);
    const locations = {
      baseDir: f.baseDir,
      authorityDir: f.authorityDir,
      credentialPath: f.credentialPath,
      requestFile: input.requestFile,
    };
    const applied = yield* runWorkstreamProviderCliOperation({
      ...locations,
      operation: "apply",
      expectedPriorSha256: null,
    });
    assert.strictEqual(applied.state, "applied");
    const credential = decodeCredentialJson(NodeFS.readFileSync(f.credentialPath, "utf8"));
    assert.match(credential.authorization_header, /^Bearer [A-Za-z0-9._~-]+$/);
    assert.strictEqual(NodeFS.statSync(f.credentialPath).mode & 0o777, 0o600);
    const readback = yield* runWorkstreamProviderCliOperation({
      ...locations,
      operation: "readback",
      sqliteSidecarEffects: "allow",
      expectedCredentialSha256: applied.file_post_sha256!,
    });
    assert.strictEqual(readback.state, "unchanged");
    assert.strictEqual(readback.reason, "complete");
  }).pipe(Effect.scoped),
);
