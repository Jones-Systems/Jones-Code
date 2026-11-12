// @effect-diagnostics nodeBuiltinImport:off -- CLI tests use only freshly allocated synthetic homes; the root retains evidence and owns cleanup.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
import * as NetService from "@t3tools/shared/Net";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import { Command } from "effect/cli";
import * as TestConsole from "effect/testing/TestConsole";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../../../config.ts";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import { RegistryError } from "../registry/session.ts";
import * as SecretStore from "../../../../auth/ServerSecretStore.ts";
import * as Admin from "../cli.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Fixture from "../store/sql.testkit.ts";
import { registryCommand } from "./cli.ts";
import { vi } from "vite-plus/test";
import * as TestClock from "effect/testing/TestClock";
import { enrollmentRequest } from "../../enrollment/testFixtures.ts";
import {
  syntheticT3Home,
  syntheticEnvironmentId,
  syntheticAuthorityNamespace,
  syntheticStoreGeneration,
} from "../../enrollment/testHome.ts";

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

const lockProbe = vi.hoisted(() => ({
  onRead: undefined as undefined | ((filename: string, stage: string, active: boolean) => void),
}));
vi.mock("../../enrollment/trustedCliContext.ts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../enrollment/trustedCliContext.ts")>();
  return {
    ...actual,
    makeTrustedNativeEnrollmentCliContext: (
      ...args: Parameters<typeof actual.makeTrustedNativeEnrollmentCliContext>
    ) =>
      actual.makeTrustedNativeEnrollmentCliContext(...args).pipe(
        Effect.map((trusted) => ({
          ...trusted,
          bindAuthorityToSqlClient: (sql: Parameters<typeof trusted.bindAuthorityToSqlClient>[0]) =>
            trusted.bindAuthorityToSqlClient(sql).pipe(
              Effect.map((authority) => ({
                readCurrent: Effect.gen(function* () {
                  const active = Option.isSome(yield* Effect.serviceOption(sql.transactionService));
                  lockProbe.onRead?.(trusted.sqlite.filename, "before", active);
                  const value = yield* authority.readCurrent;
                  lockProbe.onRead?.(trusted.sqlite.filename, "after", active);
                  return value;
                }),
              })),
            ),
        })),
      ),
  };
});
const decodeCompetingWriter = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ pid: Schema.Number, acquired: Schema.Boolean })),
);
const competingWriter = (filename: string, stage: string) => {
  const child = NodeChildProcess.spawnSync(
    process.execPath,
    [
      "--input-type=commonjs",
      "-e",
      `
    const { DatabaseSync } = require("node:sqlite");
    const db = new DatabaseSync(process.argv[1]);
    let acquired;
    try {
      db.exec("PRAGMA busy_timeout=0; BEGIN IMMEDIATE; CREATE TABLE synthetic_lock_probe (id TEXT); INSERT INTO synthetic_lock_probe VALUES ('competitor'); ROLLBACK");
      acquired = true;
    } catch (error) {
      if (db.isTransaction) db.exec("ROLLBACK");
      if (error.errcode !== 5) throw error;
      acquired = false;
    } finally { db.close(); }
    process.stdout.write(JSON.stringify({ pid: process.pid, acquired }));
  `,
      filename,
    ],
    { encoding: "utf8", timeout: 10000 },
  );
  expect(child.error).toBeUndefined();
  expect(child.signal).toBeNull();
  expect(child.status, child.stderr).toBe(0);
  const result = decodeCompetingWriter(child.stdout);
  expect(child.pid).toBe(result.pid);
  let absent = false;
  try {
    process.kill(result.pid, 0);
  } catch (error) {
    absent =
      typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH";
  }
  expect(absent).toBe(true);
  if (process.env.NW_SOURCE_LOCK_EVIDENCE)
    NodeFS.appendFileSync(
      process.env.NW_SOURCE_LOCK_EVIDENCE,
      JSON.stringify({ stage: "mounted-cli/" + stage, ...result, exit: child.status, absent }) +
        "\n",
    );
  return result.acquired;
};

const fresh = (keys: boolean) => {
  const parent = NodePath.resolve("node_modules/.cache/nw-startup-fixtures");
  NodeFS.mkdirSync(parent, { recursive: true });
  const home = NodeFS.mkdtempSync(NodePath.join(parent, "cli-"));
  if (keys) {
    const secrets = NodePath.join(home, "userdata/secrets");
    NodeFS.mkdirSync(secrets, { recursive: true, mode: 0o700 });
    for (const [name, bytes] of Fixture.secrets())
      NodeFS.writeFileSync(NodePath.join(secrets, name + ".bin"), bytes, { mode: 0o600 });
  }
  return { home, database: NodePath.join(home, "userdata/statev2.sqlite") };
};
const request = {
  registry: {
    ...Fixture.scope,
    canonical_origin: "https://native.workstreams.invalid",
    home_environment_id: "home:test",
    server_generation: 1,
  },
  principal_id: "principal:test",
  content_key_id: "test-home",
  local_key_id: "test-local",
};
const run = Command.runWith(registryCommand, { version: "test" });
const selected = (home: string) => [
  "init",
  "--home",
  home,
  "--key-provider",
  "server-secret-store",
  "--request",
  JSON.stringify(request),
];

it.effect(
  "the real registry init command bootstraps only its explicit home and existing key provider",
  () => {
    const f = fresh(true);
    return Effect.gen(function* () {
      yield* run(selected(f.home));
      const db = new NodeSqlite.DatabaseSync(f.database, { readOnly: true });
      try {
        expect(
          db
            .prepare("SELECT registry_instance_id, owner_id FROM jones_workstreams_registries")
            .all(),
        ).toEqual([Fixture.scope]);
        expect(
          db.prepare("SELECT principal_id FROM jones_workstreams_binding_state").all(),
        ).toEqual([{ principal_id: request.principal_id }]);
        expect(
          db.prepare("SELECT count(*) AS n FROM workstreams_native_enrollments").get(),
        ).toMatchObject({ n: 0 });
      } finally {
        db.close();
      }
      expect((yield* Effect.result(run(selected(f.home))))._tag).toBe("Failure");
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    );
  },
);

it.effect(
  "CLI init refuses missing selections, unknown providers, invalid input and missing keys",
  () => {
    const f = fresh(false);
    return Effect.gen(function* () {
      expect((yield* Effect.result(run(["init", "--request", JSON.stringify(request)])))._tag).toBe(
        "Failure",
      );
      expect(
        (yield* Effect.result(
          run([
            "init",
            "--home",
            f.home,
            "--key-provider",
            "unknown",
            "--request",
            JSON.stringify(request),
          ]),
        ))._tag,
      ).toBe("Failure");
      expect(NodeFS.existsSync(f.database)).toBe(false);
      expect(
        (yield* Effect.result(
          run([
            "init",
            "--home",
            f.home,
            "--key-provider",
            "server-secret-store",
            "--request",
            "{}",
          ]),
        ))._tag,
      ).toBe("Failure");
      expect(NodeFS.existsSync(f.database)).toBe(false);
      expect((yield* Effect.result(run(selected(f.home))))._tag).toBe("Failure");
      const db = new NodeSqlite.DatabaseSync(f.database, { readOnly: true });
      try {
        expect(
          db.prepare("SELECT count(*) AS n FROM jones_workstreams_registries").get(),
        ).toMatchObject({ n: 0 });
      } finally {
        db.close();
      }
      expect(NodeFS.readdirSync(NodePath.join(f.home, "userdata/secrets"))).toEqual([]);
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    );
  },
);

it.effect("mounted source-setup commands refuse an unqualified home without creating state", () => {
  const f = fresh(false);
  return Effect.gen(function* () {
    for (const action of ["plan", "apply", "readback"])
      expect(
        (yield* Effect.result(
          run([
            "source-setup",
            action,
            "--home",
            f.home,
            "--key-provider",
            "server-secret-store",
            "--request",
            "{}",
          ]),
        ))._tag,
      ).toBe("Failure");
    expect(NodeFS.existsSync(f.database)).toBe(false);
    expect(NodeFS.existsSync(NodePath.join(f.home, "userdata/secrets"))).toBe(false);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)));
});

it.effect("keys init creates the init keys once without printing them; init then succeeds", () => {
  const f = fresh(false);
  const keys = (home: string) => [
    "keys",
    "init",
    "--home",
    home,
    "--key-provider",
    "server-secret-store",
    "--request",
    JSON.stringify(request),
  ];
  const secrets = NodePath.join(f.home, "userdata/secrets");
  const names = [
    "workstreams-content-key-test-home",
    "workstreams-content-key-test-local",
    `workstreams-index-key-${request.registry.registry_instance_id}`,
  ];
  return Effect.gen(function* () {
    yield* run(keys(f.home));
    const created = names.map((name) => NodePath.join(secrets, name + ".bin"));
    const bytes = created.map((file) => NodeFS.readFileSync(file));
    for (const [i, file] of created.entries()) {
      expect(bytes[i]!.byteLength).toBe(32);
      expect(NodeFS.statSync(file).mode & 0o777).toBe(0o600);
    }
    expect(new Set(bytes.map((b) => b.toString("hex"))).size).toBe(3);
    const text = (yield* TestConsole.logLines).join("\n");
    expect(JSON.parse(text)).toEqual({ created: names });
    for (const b of bytes) {
      expect(text).not.toContain(b.toString("hex"));
      expect(text).not.toContain(b.toString("base64"));
    }
    expect((yield* Effect.result(run(keys(f.home))))._tag).toBe("Failure");
    expect(created.map((file) => NodeFS.readFileSync(file))).toEqual(bytes);
    expect(NodeFS.existsSync(f.database)).toBe(false);
    yield* run(selected(f.home));
    expect(NodeFS.existsSync(f.database)).toBe(true);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)));
});

it.effect(
  "keys init refuses any existing key, a bad provider or a bad request and creates none",
  () => {
    const f = fresh(false);
    const secrets = NodePath.join(f.home, "userdata/secrets");
    const index = NodePath.join(
      secrets,
      `workstreams-index-key-${request.registry.registry_instance_id}.bin`,
    );
    const keys = (provider: string, body: unknown) => [
      "keys",
      "init",
      "--home",
      f.home,
      "--key-provider",
      provider,
      "--request",
      JSON.stringify(body),
    ];
    return Effect.gen(function* () {
      for (const args of [
        keys("unknown", request),
        keys("server-secret-store", { ...request, local_key_id: request.content_key_id }),
        keys("server-secret-store", { ...request, extra: true }),
      ])
        expect((yield* Effect.result(run(args)))._tag).toBe("Failure");
      expect(NodeFS.existsSync(secrets) ? NodeFS.readdirSync(secrets) : []).toEqual([]);
      NodeFS.mkdirSync(secrets, { recursive: true, mode: 0o700 });
      NodeFS.writeFileSync(index, Buffer.alloc(32, 9), { mode: 0o600 });
      expect((yield* Effect.result(run(keys("server-secret-store", request))))._tag).toBe(
        "Failure",
      );
      expect(NodeFS.readdirSync(secrets)).toEqual([NodePath.basename(index)]);
      expect(NodeFS.readFileSync(index)).toEqual(Buffer.alloc(32, 9));
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    );
  },
);

it.effect("a failed keys init removes only the keys that run created", () => {
  const stored = new Map<string, Uint8Array>([["unrelated", new Uint8Array(32)]]);
  const removed: string[] = [];
  const names = Admin.keyNames(request);
  const secrets = SecretStore.ServerSecretStore.of({
    get: (name) => Effect.succeed(Option.fromNullishOr(stored.get(name))),
    set: () => Effect.die("Unexpected secret write"),
    create: (name, value) =>
      name === names[1]
        ? Effect.fail(
            new SecretStore.SecretStorePersistError({ resource: name, cause: "synthetic" }),
          )
        : Effect.sync(() => void stored.set(name, value)),
    getOrCreateRandom: () => Effect.die("Unexpected automatic key creation"),
    remove: (name) => Effect.sync(() => void (stored.delete(name), removed.push(name))),
  });
  return Effect.gen(function* () {
    const result = yield* Effect.result((yield* Admin.NativeKeyAdmin).initialize(request));
    expect(result._tag === "Failure" && result.failure.code).toBe("unavailable");
    expect(removed).toEqual([names[0]]);
    expect([...stored.keys()]).toEqual(["unrelated"]);
  }).pipe(
    Effect.provide(
      Admin.keyLayer.pipe(
        Layer.provide(
          Layer.mergeAll(Layer.succeed(SecretStore.ServerSecretStore, secrets), NodeServices.layer),
        ),
      ),
    ),
  );
});

const memberRequest = { registry: request.registry, local_key_id: "member-local" };
const memberKeyName = "workstreams-content-key-member-local";
const memberHome = () => {
  const f = fresh(false);
  NodeFS.mkdirSync(NodePath.dirname(f.database), { recursive: true });
  new NodeSqlite.DatabaseSync(f.database).close();
  return { ...f, secrets: NodePath.join(f.home, "userdata/secrets") };
};
const memberKeys = (
  home: string,
  body: unknown = memberRequest,
  provider = "server-secret-store",
) => [
  "keys",
  "member-init",
  "--home",
  home,
  "--key-provider",
  provider,
  "--request",
  JSON.stringify(body),
];
const refusal = <A, E, R>(run: Effect.Effect<A, E, R>) =>
  Effect.map(Effect.result(run), (result) =>
    result._tag === "Failure" ? ((result.failure as { code?: string }).code ?? "failure") : "ok",
  );
const keyRows = (database: string) => {
  const db = new NodeSqlite.DatabaseSync(database, { readOnly: true });
  try {
    return db
      .prepare("SELECT key_id, registry_instance_id, scope, state FROM jones_workstreams_keys")
      .all();
  } finally {
    db.close();
  }
};

it.effect(
  "member-init resume preserves orphan bytes, converges, and resolves the active key",
  () => {
    const f = memberHome();
    const file = NodePath.join(f.secrets, memberKeyName + ".bin");
    const bytes = Buffer.alloc(32, 73);
    NodeFS.mkdirSync(f.secrets, { recursive: true, mode: 0o700 });
    NodeFS.writeFileSync(file, bytes, { mode: 0o600 });
    const provider = Keys.layer.pipe(
      Layer.provide(SecretStore.layer.pipe(Layer.provide(Config.layerTest(process.cwd(), f.home)))),
      Layer.provide(NodeSqliteClient.layer({ filename: f.database })),
    );
    return Effect.gen(function* () {
      expect(yield* refusal(run(memberKeys(f.home)))).toBe("key_exists");
      for (let attempt = 0; attempt < 2; attempt++) {
        yield* run([...memberKeys(f.home), "--resume"]);
        expect(NodeFS.readFileSync(file)).toEqual(bytes);
        expect(keyRows(f.database)).toHaveLength(1);
      }
      const resolved = yield* Effect.gen(function* () {
        const keys = yield* Keys.WorkstreamsKeyProvider;
        const id = yield* keys.activeKeyId({ ...Fixture.scope, key_scope: "local" });
        expect(id).toBe(memberRequest.local_key_id);
        return yield* keys.get(id);
      }).pipe(Effect.provide(provider));
      expect(Buffer.from(resolved)).toEqual(bytes);
      const lines = (yield* TestConsole.logLines).map(String);
      expect(JSON.parse(lines.at(-1)!)).toEqual({ created: [] });
      expect(lines.join("\n")).not.toContain(bytes.toString("base64"));
      NodeFS.rmSync(file);
      for (const resume of [[], ["--resume"]])
        expect(yield* refusal(run([...memberKeys(f.home), ...resume]))).toBe("key_file_missing");
      expect(NodeFS.existsSync(file)).toBe(false);
      expect(keyRows(f.database)).toHaveLength(1);
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    );
  },
);

it.effect("member-init resume refuses malformed files and conflicting rows untouched", () => {
  const f = memberHome();
  const file = NodePath.join(f.secrets, memberKeyName + ".bin");
  NodeFS.mkdirSync(f.secrets, { recursive: true, mode: 0o700 });
  NodeFS.writeFileSync(file, Buffer.alloc(31, 9), { mode: 0o600 });
  return Effect.gen(function* () {
    expect(yield* refusal(run([...memberKeys(f.home), "--resume"]))).toBe("invalid_key");
    expect(NodeFS.readFileSync(file)).toEqual(Buffer.alloc(31, 9));
    expect(keyRows(f.database)).toEqual([]);
    NodeFS.writeFileSync(file, Buffer.alloc(32, 9));
    const db = new NodeSqlite.DatabaseSync(f.database);
    try {
      db.prepare("INSERT INTO jones_workstreams_keys VALUES (?, ?, 'home', 'active', ?)").run(
        memberRequest.local_key_id,
        Fixture.scope.registry_instance_id,
        Fixture.now,
      );
    } finally {
      db.close();
    }
    const before = keyRows(f.database);
    expect(yield* refusal(run([...memberKeys(f.home), "--resume"]))).toBe("key_exists");
    expect(NodeFS.readFileSync(file)).toEqual(Buffer.alloc(32, 9));
    expect(keyRows(f.database)).toEqual(before);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)));
});

it.effect.each([
  "committed",
  "readback-failed",
  "rollback",
  "conflicting",
  "different-at",
] as const)("member-init reconciles transaction failure: %s", (outcome) => {
  const f = memberHome();
  const file = NodePath.join(f.secrets, memberKeyName + ".bin");
  const resources = Layer.mergeAll(
    SecretStore.layer.pipe(Layer.provide(Config.layerTest(process.cwd(), f.home))),
    NodeSqliteClient.layer({ filename: f.database }),
  ).pipe(Layer.provideMerge(NodeServices.layer));
  return Effect.gen(function* () {
    yield* migration;
    const sql = yield* SqlClient.SqlClient;
    let transactionEnded = false;
    const fault = new RegistryError({ code: "synthetic_transaction_failure" });
    // Preserve real SQLite commit/rollback; inject failure only at its acknowledgement boundary.
    const wrapped = new Proxy(sql, {
      get(target, property, receiver) {
        if (property !== "withTransaction") return Reflect.get(target, property, receiver);
        return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          (outcome === "rollback"
            ? sql.withTransaction(effect.pipe(Effect.andThen(Effect.fail(fault))))
            : sql.withTransaction(effect).pipe(Effect.andThen(Effect.fail(fault)))
          ).pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                transactionEnded = true;
              }),
            ),
          );
      },
      apply(target, thisArg, args) {
        if (transactionEnded && outcome === "readback-failed") return Effect.fail(fault);
        const query = Reflect.apply(target, thisArg, args);
        return transactionEnded && (outcome === "conflicting" || outcome === "different-at")
          ? query.pipe(
              Effect.map(() => [
                {
                  key_id: memberRequest.local_key_id,
                  registry_instance_id: Fixture.scope.registry_instance_id,
                  scope: outcome === "conflicting" ? "home" : "local",
                  state: "active",
                  created_at: "different-at",
                },
              ]),
            )
          : query;
      },
    });
    const result = yield* Effect.result(
      Effect.gen(function* () {
        return yield* (yield* Admin.NativeMemberKeyAdmin).initialize(memberRequest);
      }).pipe(
        Effect.provide(
          Admin.memberKeyLayer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, wrapped))),
        ),
      ),
    );
    if (outcome === "committed") {
      expect(result._tag).toBe("Success");
      expect(result._tag === "Success" && result.success).toEqual({ created: [memberKeyName] });
      const keys = yield* Keys.WorkstreamsKeyProvider;
      const id = yield* keys.activeKeyId({ ...Fixture.scope, key_scope: "local" });
      expect(id).toBe(memberRequest.local_key_id);
      expect(Buffer.from(yield* keys.get(id))).toEqual(NodeFS.readFileSync(file));
    } else {
      expect(result._tag === "Failure" && result.failure.code).toBe(
        outcome === "rollback" ? "synthetic_transaction_failure" : "outcome_unknown",
      );
    }
    expect(NodeFS.existsSync(file)).toBe(outcome !== "rollback");
    expect(keyRows(f.database)).toHaveLength(outcome === "rollback" ? 0 : 1);
  }).pipe(Effect.provide(Keys.layer.pipe(Layer.provideMerge(resources))));
});

it.effect(
  "keys member-init provisions the member's local key once and the key provider resolves it",
  () => {
    const f = memberHome();
    const file = NodePath.join(f.secrets, memberKeyName + ".bin");
    // The same activeKeyId/get calls member setup, member startup and peer setup make first.
    const fileSecrets = SecretStore.ServerSecretStore.of({
      get: (name) =>
        Effect.sync(() => {
          const path = NodePath.join(f.secrets, name + ".bin");
          return NodeFS.existsSync(path)
            ? Option.some(Uint8Array.from(NodeFS.readFileSync(path)))
            : Option.none();
        }),
      set: () => Effect.die("Unexpected secret write"),
      create: () => Effect.die("Unexpected secret creation"),
      getOrCreateRandom: () => Effect.die("Unexpected automatic key creation"),
      remove: () => Effect.die("Unexpected key removal"),
    });
    const provider = Keys.layer.pipe(
      Layer.provide(Layer.succeed(SecretStore.ServerSecretStore, fileSecrets)),
      Layer.provide(NodeSqliteClient.layer({ filename: f.database })),
    );
    return Effect.gen(function* () {
      yield* run(memberKeys(f.home));
      const bytes = NodeFS.readFileSync(file);
      expect(bytes.byteLength).toBe(32);
      expect(NodeFS.statSync(file).mode & 0o777).toBe(0o600);
      // Migration logs may precede the single JSON result line.
      const lines = (yield* TestConsole.logLines).map(String),
        text = lines.join("\n");
      expect(JSON.parse(lines.at(-1)!)).toEqual({ created: [memberKeyName] });
      expect(text).not.toContain(bytes.toString("hex"));
      expect(text).not.toContain(bytes.toString("base64"));
      const rows = [
        {
          key_id: "member-local",
          registry_instance_id: Fixture.scope.registry_instance_id,
          scope: "local",
          state: "active",
        },
      ];
      expect(keyRows(f.database)).toEqual(rows);
      const resolved = yield* Effect.gen(function* () {
        const keys = yield* Keys.WorkstreamsKeyProvider;
        const id = yield* keys.activeKeyId({ ...Fixture.scope, key_scope: "local" });
        return { id, bytes: yield* keys.get(id) };
      }).pipe(Effect.provide(provider));
      expect(resolved.id).toBe("member-local");
      expect(Buffer.from(resolved.bytes)).toEqual(bytes);
      for (const body of [memberRequest, { ...memberRequest, local_key_id: "member-local-2" }])
        expect(yield* refusal(run(memberKeys(f.home, body)))).toBe("key_exists");
      expect(NodeFS.readdirSync(f.secrets)).toEqual([memberKeyName + ".bin"]);
      expect(NodeFS.readFileSync(file)).toEqual(bytes);
      expect(keyRows(f.database)).toEqual(rows);
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    );
  },
);

it.effect(
  "keys member-init refuses bad selections, a home registry or existing keys and keeps nothing it created",
  () => {
    const f = memberHome();
    const file = NodePath.join(f.secrets, memberKeyName + ".bin");
    return Effect.gen(function* () {
      const missing = fresh(false);
      for (const [args, code] of [
        [memberKeys(f.home, memberRequest, "unknown"), "explicit_selection_required"],
        [memberKeys(f.home, { ...memberRequest, extra: true }), "failure"],
        [memberKeys(f.home, { ...memberRequest, local_key_id: "bad key" }), "failure"],
        [memberKeys(missing.home), "home_unavailable"],
      ] as const)
        expect(yield* refusal(run(args))).toBe(code);
      expect(NodeFS.existsSync(missing.database)).toBe(false);
      expect(NodeFS.existsSync(NodePath.join(missing.home, "userdata/secrets"))).toBe(false);
      expect(NodeFS.existsSync(f.secrets) ? NodeFS.readdirSync(f.secrets) : []).toEqual([]);
      // An existing key file refuses before any row is written.
      NodeFS.mkdirSync(f.secrets, { recursive: true, mode: 0o700 });
      NodeFS.writeFileSync(file, Buffer.alloc(32, 9), { mode: 0o600 });
      expect(yield* refusal(run(memberKeys(f.home)))).toBe("key_exists");
      expect(NodeFS.readFileSync(file)).toEqual(Buffer.alloc(32, 9));
      expect(keyRows(f.database)).toEqual([]);
      NodeFS.rmSync(file);
      // A failed row write removes the key file this run created.
      const db = new NodeSqlite.DatabaseSync(f.database);
      try {
        db.exec(
          "CREATE TRIGGER synthetic_key_refusal BEFORE INSERT ON jones_workstreams_keys BEGIN SELECT RAISE(ABORT,'synthetic'); END",
        );
      } finally {
        db.close();
      }
      expect(yield* refusal(run(memberKeys(f.home)))).toBe("unavailable");
      expect(NodeFS.readdirSync(f.secrets)).toEqual([]);
      expect(keyRows(f.database)).toEqual([]);
      // A home that holds the registry refuses: its keys come from `keys init` and `init`.
      const home = fresh(true);
      yield* run(selected(home.home));
      expect(
        yield* refusal(
          run(memberKeys(home.home, { ...memberRequest, local_key_id: "other-local" })),
        ),
      ).toBe("already_initialized");
      expect(
        NodeFS.existsSync(
          NodePath.join(home.home, "userdata/secrets/workstreams-content-key-other-local.bin"),
        ),
      ).toBe(false);
    }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    );
  },
);

it.effect("mounted transition commands refuse a home without an existing database", () => {
  const f = fresh(false);
  const pem = NodeCrypto.generateKeyPairSync("ed25519")
    .publicKey.export({ type: "spki", format: "pem" })
    .toString();
  const snapshot = NodePath.join(f.home, "snapshot.enc"),
    key = NodePath.join(f.home, "permit.pem");
  NodeFS.writeFileSync(snapshot, "encrypted", { mode: 0o600 });
  NodeFS.writeFileSync(key, pem, { mode: 0o600 });
  const transitionRequest = JSON.stringify({
    import_run_id: "70000000-0000-4000-8000-000000000001",
    stream_key_id: "test-home",
    permit: { key_id: "operator", header_sha256: "a".repeat(64), signature: "AAAA" },
    permit_key_id: "operator",
    home_environment_id: "home:test",
    active_key_id: "test-home",
    mappings: [],
  });
  return Effect.gen(function* () {
    for (const [action, provider] of [
      ["plan", "server-secret-store"],
      ["apply", "server-secret-store"],
      ["readback", "unknown"],
    ] as const)
      expect(
        (yield* Effect.result(
          run([
            "transition",
            action,
            "--home",
            f.home,
            "--key-provider",
            provider,
            "--request",
            transitionRequest,
            "--snapshot",
            snapshot,
            "--permit-public-key",
            key,
          ]),
        ))._tag,
      ).toBe("Failure");
    expect(NodeFS.existsSync(f.database)).toBe(false);
    expect(NodeFS.existsSync(NodePath.join(f.home, "userdata/secrets"))).toBe(false);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)));
});

it.effect(
  "mounted source-setup excludes competing writers, issues once and replays after reopen",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(Fixture.now));
      const home = yield* syntheticT3Home("nw-source-setup-cli-");
      NodeFS.mkdirSync(home.secretsDir, { mode: 0o700 });
      for (const [name, bytes] of [
        ...Fixture.secrets(),
        ["server-signing-key", Fixture.keyBytes] as const,
      ])
        NodeFS.writeFileSync(NodePath.join(home.secretsDir, name + ".bin"), bytes, { mode: 0o600 });
      const initial = {
        ...request,
        registry: { ...request.registry, home_environment_id: syntheticEnvironmentId },
      };
      yield* run([
        "init",
        "--home",
        home.baseDir,
        "--key-provider",
        "server-secret-store",
        "--request",
        JSON.stringify(initial),
      ]);
      // Standalone ownership requires rollback journaling in this synthetic, stopped runtime.
      const closeWal = new NodeSqlite.DatabaseSync(home.dbPath);
      try {
        closeWal.exec("PRAGMA journal_mode=DELETE");
      } finally {
        closeWal.close();
      }
      const setup = {
        schema: "jones-code.native-source-setup/v1",
        enrollment: {
          ...enrollmentRequest,
          registry_origin: initial.registry.canonical_origin,
          context: {
            ...enrollmentRequest.context,
            owner_id: Fixture.scope.owner_id,
            principal_id: initial.principal_id,
            source_instance_id: syntheticEnvironmentId,
            authority_namespace: syntheticAuthorityNamespace,
            store_generation: syntheticStoreGeneration,
          },
        },
        expected: {
          ...Fixture.scope,
          home_environment_id: syntheticEnvironmentId,
          home_generation: 1,
          registry_version: 1,
          singleton_binding_version: 1,
          grant_id: "grant:principal:test",
          grant_version: 1,
          source_binding: null,
        },
        rebind: false,
      };
      const args = (action: string) => [
        "source-setup",
        action,
        "--home",
        home.baseDir,
        "--key-provider",
        "server-secret-store",
        "--request",
        JSON.stringify(setup),
      ];
      const activeStages: string[] = [];
      lockProbe.onRead = (filename, stage, active) => {
        if (active) {
          activeStages.push(stage);
          expect(competingWriter(filename, stage)).toBe(false);
        }
      };
      yield* Effect.gen(function* () {
        for (const action of ["plan", "apply", "readback", "apply", "readback"])
          yield* run(args(action));
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            lockProbe.onRead = undefined;
          }),
        ),
      );
      expect(activeStages.filter((stage) => stage === "before").length).toBeGreaterThan(0);
      expect(activeStages.filter((stage) => stage === "after").length).toBeGreaterThan(0);
      const db = new NodeSqlite.DatabaseSync(home.dbPath, { readOnly: true });
      try {
        for (const table of [
          "auth_sessions",
          "jones_workstreams_source_enrollments",
          "jones_workstreams_source_bindings",
        ])
          expect(db.prepare("SELECT count(*) AS n FROM " + table).get()).toMatchObject({ n: 1 });
        expect(
          db.prepare("SELECT registry_version FROM jones_workstreams_registries").get(),
        ).toMatchObject({ registry_version: 2 });
        expect(
          db.prepare("SELECT count(*) AS n FROM workstreams_native_enrollments").get(),
        ).toMatchObject({ n: 0 });
      } finally {
        db.close();
      }
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer, NetService.layer)),
    ),
);
