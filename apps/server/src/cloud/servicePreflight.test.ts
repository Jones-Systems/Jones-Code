import {
  decodeServicePreflightResult,
  qualifiedServicePreflightFailure,
} from "./servicePreflight.ts";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  BUILD_MIGRATION_HEAD,
  databaseMigrationHead,
} from "../persistence/databaseMigrationHead.ts";
import { migrationManifest } from "../persistence/Migrations.ts";

import { runServicePreflight } from "./servicePreflight.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";

// regression: #13239 a stale CLI shadowing the updated one kept "updating" to itself
it.each([1])("blocks legacy launcher protocol %i with exact recovery steps", (launcherProtocol) => {
  const result = runServicePreflight({
    databasePath: "/missing/state.sqlite",
    launcherProtocol,
    version: "9.8.7",
  });

  expect(result).toMatchObject({ status: "blocked", version: "9.8.7" });
  const reason = result.status === "blocked" ? result.reason : "";
  expect(reason).toMatch(new RegExp(`\\b${launcherProtocol}\\b`));
  expect(reason).toMatch(new RegExp(`\\b${SERVICE_LAUNCHER_PROTOCOL}\\b`));
  expect(reason).toContain("curl -fsSL https://t3.codes/install.sh | T3CODE_VERSION=9.8.7 sh");
  expect(reason).toContain("~/.local/bin/t3 service install");
  expect(reason).toContain("t3 --version");
  expect(reason).toContain("which -a t3");
});

it.layer(NodeServices.layer)("service preflight database", (it) => {
  it.effect("blocks a database that a newer build migrated", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const databasePath = path.join(
        yield* fs.makeTempDirectoryScoped({ prefix: "t3-preflight-" }),
        "state.sqlite",
      );
      const db = new NodeSqlite.DatabaseSync(databasePath);
      db.exec("CREATE TABLE effect_sql_migrations (migration_id INTEGER PRIMARY KEY, name TEXT)");
      const insert = db.prepare("INSERT INTO effect_sql_migrations VALUES (?, ?)");
      for (const [id, name] of migrationManifest) insert.run(id, name);
      const preflight = () =>
        runServicePreflight({ databasePath, launcherProtocol: SERVICE_LAUNCHER_PROTOCOL });

      expect(preflight().status).toBe("ready");
      expect(databaseMigrationHead(databasePath)).toBe(BUILD_MIGRATION_HEAD);

      const newerId = Math.max(...migrationManifest.map(([id]) => id)) + 1;
      insert.run(newerId, "FromNightly");
      db.close();
      const result = preflight();
      expect(result.status).toBe("blocked");
      expect(result.status === "blocked" ? result.reason : "").toContain(`${newerId}_FromNightly`);
      // Servers report this head, so clients see a database a newer build migrated.
      expect(databaseMigrationHead(databasePath)).toBe(newerId);

      // A database that exists but cannot be read has an unknown history, so it blocks.
      const unreadable = path.join(path.dirname(databasePath), "unreadable.sqlite");
      yield* fs.writeFileString(unreadable, "not a database");
      expect(databaseMigrationHead(unreadable)).toBeUndefined();
      expect(
        runServicePreflight({
          databasePath: unreadable,
          launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
        }).status,
      ).toBe("blocked");
      // A missing database is a fresh install.
      expect(databaseMigrationHead(path.join(path.dirname(databasePath), "missing.sqlite"))).toBe(
        BUILD_MIGRATION_HEAD,
      );
    }),
  );
});

it("advertises only the supplied bundled startup gate capability", () => {
  expect(
    runServicePreflight({
      databasePath: "/missing/state.sqlite",
      launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
      version: "1.2.3",
      startupGateProtocol: 1,
    }),
  ).toEqual({
    status: "ready",
    version: "1.2.3",
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    startupGateProtocol: 1,
  });
});

it("requires historical-receipt capability only for the additional stage format", () => {
  const ready = {
    status: "ready",
    version: "1.2.3",
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    startupGateProtocol: 1,
  };
  const input = { code: 0, stdout: JSON.stringify(ready), version: "1.2.3" };
  expect(qualifiedServicePreflightFailure(input)).toBeUndefined();
  expect(
    qualifiedServicePreflightFailure({ ...input, requiredQualifiedRecoveryProtocol: 1 }),
  ).toContain("qualified-recovery-unsupported");
  const supported = { ...ready, qualifiedRecoveryProtocol: 1 };
  expect(decodeServicePreflightResult(supported)).toEqual(supported);
  expect(
    qualifiedServicePreflightFailure({
      ...input,
      stdout: JSON.stringify(supported),
      requiredQualifiedRecoveryProtocol: 1,
    }),
  ).toBeUndefined();
  for (const qualifiedRecoveryProtocol of [null, undefined, 0, 2, "1", true])
    expect(decodeServicePreflightResult({ ...ready, qualifiedRecoveryProtocol })).toBeUndefined();
});

it("preserves ordinary preflight without inferring qualified support", () => {
  const ready = { status: "ready", version: "1.2.3", launcherProtocol: SERVICE_LAUNCHER_PROTOCOL };
  expect(decodeServicePreflightResult(ready)).toEqual(ready);
  expect(
    qualifiedServicePreflightFailure({ code: 0, stdout: JSON.stringify(ready), version: "1.2.3" }),
  ).toContain("startup-gate-unavailable");
});

it("returns the staged candidate's migration plan and preserves it on decode", () => {
  const migrationPlan = { pendingUpstream: [59], pendingJones: [105] };
  const result = runServicePreflight({
    databasePath: "/missing/state.sqlite",
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    version: "candidate",
    migrationPlanResult: { status: "ready", migrationPlan },
  });
  expect(result).toMatchObject({ status: "ready", migrationPlan });
  expect(decodeServicePreflightResult(result)).toEqual(result);
});

it("blocks installation when candidate migration preflight is blocked", () => {
  expect(
    runServicePreflight({
      databasePath: "/missing/state.sqlite",
      launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
      version: "candidate",
      migrationPlanResult: { status: "blocked", reason: "Candidate older than this database." },
    }),
  ).toEqual({
    status: "blocked",
    version: "candidate",
    reason: "Candidate older than this database.",
  });
});

it("rejects a malformed supplied migration plan", () => {
  expect(
    decodeServicePreflightResult({
      status: "ready",
      version: "candidate",
      launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
      migrationPlan: { pendingUpstream: ["59"], pendingJones: [105] },
    }),
  ).toBeUndefined();
});

it.each([null, undefined, 0, 2, "1", true])(
  "rejects supplied malformed startup gate marker %s",
  (startupGateProtocol) => {
    expect(
      decodeServicePreflightResult({
        status: "ready",
        version: "1.2.3",
        launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
        startupGateProtocol,
      }),
    ).toBeUndefined();
  },
);

it("qualifies only an exact successful candidate with symmetric gate support", () => {
  const result = {
    status: "ready",
    version: "1.2.3",
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    startupGateProtocol: 1,
  };
  const stdout = JSON.stringify(result);
  expect(qualifiedServicePreflightFailure({ code: 0, stdout, version: "1.2.3" })).toBeUndefined();
  for (const input of [
    { code: 1, stdout, version: "1.2.3" },
    { code: 0, stdout, version: "1.2.4" },
    { code: 0, stdout: "not JSON", version: "1.2.3" },
    { code: 0, stdout: JSON.stringify({ ...result, startupGateProtocol: 2 }), version: "1.2.3" },
    { code: 0, stdout: JSON.stringify({ ...result, launcherProtocol: 3 }), version: "1.2.3" },
  ])
    expect(qualifiedServicePreflightFailure(input)).toContain("startup-gate-unavailable");
});

it("preserves a bound candidate database blocker before stopping the active server", () => {
  expect(
    qualifiedServicePreflightFailure({
      code: 1,
      version: "1.0.0",
      stdout: JSON.stringify({
        status: "blocked",
        version: "1.0.0",
        reason: "Candidate older than this database (Jones migration 105).",
      }),
    }),
  ).toBe("Candidate older than this database (Jones migration 105).");
});
