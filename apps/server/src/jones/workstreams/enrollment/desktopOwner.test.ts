// @effect-diagnostics nodeBuiltinImport:off -- Fixtures own only their temporary home and ephemeral loopback listener.
// @effect-diagnostics globalFetch:off -- This test deliberately verifies Node fetch automatic headers against a real owned loopback socket.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { AuthAccessWriteScope } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Context from "effect/Context";
import type * as HttpServerRequest from "effect/http/HttpServerRequest";
import { runMigrations } from "../../../persistence/Migrations.ts";
import enrollmentMigration from "../../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import { readExistingNativeStoreAuthorityState } from "../../../environment/nativeStoreAuthorityPersistence.ts";
import {
  assertLocalDesktopOwnerRequest,
  readDesktopNativeOwnerRequest,
  runDesktopNativeOwnerOperation,
  type DesktopNativeEnrollmentOwner,
} from "./desktopOwner.ts";

vi.mock("../../../../package.json", () => ({
  default: {
    version: "fixture",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const fixture = Effect.acquireRelease(
  Effect.sync(() => {
    const baseDir = NodeFS.realpathSync(
      NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "desktop-enrollment-")),
    );
    try {
      const userdata = NodePath.join(baseDir, "userdata");
      NodeFS.mkdirSync(userdata, { mode: 0o700 });
      NodeFS.mkdirSync(NodePath.join(userdata, "secrets"), { mode: 0o700 });
      NodeFS.writeFileSync(
        NodePath.join(userdata, "secrets", "server-signing-key.bin"),
        new Uint8Array(32).fill(7),
        { mode: 0o600 },
      );
      NodeFS.writeFileSync(NodePath.join(userdata, "environment-id"), "desktop-env\n", {
        mode: 0o600,
      });
      return {
        baseDir,
        authorityDir: NodePath.join(baseDir, "native-store-authority"),
        environmentId: "desktop-env",
        dbPath: NodePath.join(userdata, "statev2.sqlite"),
      };
    } catch (error) {
      NodeFS.rmSync(baseDir, { recursive: true, force: true });
      throw error;
    }
  }),
  (home) =>
    Effect.sync(() => {
      NodeFS.rmSync(home.baseDir, { recursive: true, force: true });
      expect(NodeFS.existsSync(home.baseDir)).toBe(false);
    }),
);

it.effect(
  "enrolls and prepares/plans/applies/readbacks the actual desktop-bound store without launcher files",
  () =>
    Effect.gen(function* () {
      const home = yield* fixture;
      const databaseContext = yield* Layer.build(NodeSqliteClient.layer({ filename: home.dbPath }));
      const sql = Context.get(databaseContext, SqlClient.SqlClient);
      yield* Effect.gen(function* () {
        yield* runMigrations({ toMigrationInclusive: 54 });
        yield* enrollmentMigration;
      }).pipe(Effect.provide(databaseContext));
      NodeFS.chmodSync(home.dbPath, 0o600);
      let live = true;
      const owner: DesktopNativeEnrollmentOwner = {
        binding: {
          home: home.baseDir,
          databasePath: home.dbPath,
          profile: NodePath.join(home.baseDir, "profile"),
          environmentId: home.environmentId,
          version: "fixture",
          sourceSha: "a".repeat(40),
          sourceTree: "b".repeat(40),
          manifestSha256: "c".repeat(64),
        },
        assertCurrent: () => {
          if (!live) throw new Error("owner lost");
        },
        refresh: Effect.void,
      };
      const run = (input: Parameters<typeof runDesktopNativeOwnerOperation>[0]) =>
        runDesktopNativeOwnerOperation(input, owner, home).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
      const enrolled = yield* run({ operation: "authority-enroll" });
      expect(enrolled.schema).toBe("jones-code.workstreams-native-authority-enrolled/v1");
      const before = readExistingNativeStoreAuthorityState(home.authorityDir);
      expect(yield* run({ operation: "authority-enroll" })).toEqual(enrolled);
      const prepared = yield* run({
        operation: "provider-prepare",
        enrollmentId: "laptop",
        ownerId: "owner",
        principalId: "principal",
        registryOrigin: "https://registry.example",
        lifetimeDays: 30,
      });
      expect(prepared.schema).toBe("jones-code.workstreams-native-enrollment-prepared/v1");
      const plan = yield* run({ operation: "provider-plan", enrollmentId: "laptop" });
      expect(plan).toMatchObject({ state: "planned", file_pre_sha256: null });
      const applied = yield* run({
        operation: "provider-apply",
        enrollmentId: "laptop",
        expectedPriorSha256: null,
      });
      expect(applied).toMatchObject({
        state: "applied",
        native_context: {
          source_instance_id: home.environmentId,
          authority_namespace: before.authority_namespace,
          store_generation: 1,
          build: { sha: "a".repeat(40), tree: "b".repeat(40) },
        },
      });
      if (!("file_post_sha256" in applied) || applied.file_post_sha256 === null)
        throw new Error("missing fixture credential hash");
      const readback = yield* run({
        operation: "provider-readback",
        enrollmentId: "laptop",
        expectedCredentialSha256: applied.file_post_sha256,
      });
      expect(readback).toMatchObject({ state: "unchanged", reason: "complete" });
      const receiptJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))([
        enrolled,
        prepared,
        plan,
        applied,
        readback,
      ]);
      expect(receiptJson).not.toContain("authorization_header");
      const privateRoot = NodePath.join(home.baseDir, "workstreams", "native-enrollment");
      expect(NodeFS.statSync(privateRoot).mode & 0o777).toBe(0o700);
      expect(
        NodeFS.statSync(NodePath.join(privateRoot, "laptop.credential.json")).mode & 0o777,
      ).toBe(0o600);
      expect(NodeFS.existsSync(NodePath.join(home.baseDir, "runtime", "service-state.json"))).toBe(
        false,
      );
      expect(readExistingNativeStoreAuthorityState(home.authorityDir)).toEqual(before);
      const requestBytes = NodeFS.readFileSync(NodePath.join(privateRoot, "laptop.request.json"));
      expect(
        (yield* Effect.result(
          run({
            operation: "provider-prepare",
            enrollmentId: "laptop",
            ownerId: "owner",
            principalId: "principal",
            registryOrigin: "https://registry.example",
            lifetimeDays: 30,
          }),
        ))._tag,
      ).toBe("Failure");
      expect(NodeFS.readFileSync(NodePath.join(privateRoot, "laptop.request.json"))).toEqual(
        requestBytes,
      );
      live = false;
      expect((yield* Effect.result(run({ operation: "authority-enroll" })))._tag).toBe("Failure");
      expect(readExistingNativeStoreAuthorityState(home.authorityDir)).toEqual(before);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it("uses the actual loopback socket and accepts Node fetch while rejecting browser/proxy/insufficient authority", async () => {
  const server = NodeHttp.createServer((request, response) => {
    try {
      const token = request.headers.authorization;
      assertLocalDesktopOwnerRequest(
        {
          source: request,
          originalUrl: request.url,
          headers: request.headers,
        } as HttpServerRequest.HttpServerRequest,
        {
          method: token === "Bearer owner" ? "bearer-access-token" : "browser-session-cookie",
          scopes: new Set(
            token === "Bearer owner" ? [AuthAccessWriteScope] : ["orchestration:read"],
          ),
        },
      );
      response.writeHead(200).end();
    } catch {
      response.writeHead(403).end();
    }
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const origin = `http://127.0.0.1:${(server.address() as NodeNet.AddressInfo).port}/api/workstreams/native/owner/v1/operations`;
    expect(
      (await fetch(origin, { method: "POST", headers: { authorization: "Bearer owner" } })).status,
    ).toBe(200);
    for (const headers of [
      {},
      { authorization: "Bearer reader" },
      ...["origin", "cookie", "tailscale-user-login", "x-forwarded-for", "forwarded"].map(
        (name) => ({ authorization: "Bearer owner", [name]: "spoof" }),
      ),
    ])
      expect((await fetch(origin, { method: "POST", headers })).status).toBe(403);
    expect(
      (
        await fetch(`${origin}?alternate=1`, {
          method: "POST",
          headers: { authorization: "Bearer owner" },
        })
      ).status,
    ).toBe(403);
  } finally {
    server.closeAllConnections();
    if (server.listening)
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
  }
});

it.effect("bounds and closes owner request bodies before any operation", () =>
  Effect.gen(function* () {
    const body = (text: string) => Stream.make(new TextEncoder().encode(text));
    expect(yield* readDesktopNativeOwnerRequest(body('{"operation":"authority-enroll"}'))).toEqual({
      operation: "authority-enroll",
    });
    for (const text of [
      '{"operation":"authority-enroll","baseDir":"/other"}',
      "x".repeat(8193),
      '{"operation":"restore"}',
    ])
      expect((yield* Effect.result(readDesktopNativeOwnerRequest(body(text))))._tag).toBe(
        "Failure",
      );
  }),
);
