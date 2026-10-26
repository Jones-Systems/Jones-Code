// @effect-diagnostics nodeBuiltinImport:off -- CLI tests use only freshly allocated synthetic homes; the root retains evidence and owns cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { expect, it } from "@effect/vitest";
import * as NetService from "@t3tools/shared/Net";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Command } from "effect/cli";
import * as TestConsole from "effect/testing/TestConsole";
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

it.effect(
  "mounted source-setup issues once through qualified CLI ownership and replays after reopen",
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
      for (const action of ["plan", "apply", "readback", "apply", "readback"])
        yield* run(args(action));
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
