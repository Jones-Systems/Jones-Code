import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import { preserveForeignContent, qualificationSqliteApi } from "./qualification-adapters.mjs";

const root = NodePath.resolve(import.meta.dirname, "../../..");
const require = NodeModule.createRequire(NodePath.join(root, "apps/server/package.json"));

NodeTest.test(
  "receiving SQLite selection imports its real factory without opening a database",
  async () => {
    const obsolete = NodeURL.pathToFileURL(
      NodePath.join(root, "apps/server/src/persistence/Layers/Sqlite.ts"),
    );
    await NodeAssert.rejects(import(obsolete.href), {
      code: "ERR_MODULE_NOT_FOUND",
      url: obsolete.href,
    });
    const api = qualificationSqliteApi(true);
    const sqlClientPath = NodeFS.realpathSync(require.resolve(api.sqlClient));
    NodeAssert.ok(sqlClientPath.startsWith(`${root}/node_modules/`));
    const sqlClient = await import(NodeURL.pathToFileURL(sqlClientPath).href);
    NodeAssert.ok(sqlClient.SqlClient);
    const sqlite = await import(NodeURL.pathToFileURL(NodePath.join(root, api.modulePath)).href);
    const layer = await import(NodeURL.pathToFileURL(require.resolve("effect/Layer")).href);
    NodeAssert.equal(typeof sqlite[api.factory], "function");
    NodeAssert.equal(sqlite.makeSqlitePersistenceLive, undefined);
    NodeAssert.equal(layer.isLayer(sqlite[api.factory](":memory:")), true);
  },
);

NodeTest.test("historical source role keeps the old SQLite API and unstable SQL package", () => {
  NodeAssert.deepEqual(qualificationSqliteApi(false), {
    modulePath: "apps/server/src/persistence/Layers/Sqlite.ts",
    factory: "makeSqlitePersistenceLive",
    sqlClient: "effect/unstable/sql/SqlClient",
  });
});

const capture = () => ({
  content: {
    tables: {
      foreign_events: { count: 1, sha256: "foreign-content" },
      jones_sql_migrations: { count: 7, sha256: "foreign-ledger" },
    },
    definitions: [
      { type: "table", name: "foreign_events", sql: "CREATE TABLE foreign_events(id)" },
    ],
    ledgers: { jones_sql_migrations: [{ id: 7, name: "foreign-007" }] },
  },
});

for (const kind of ["foreign", "successor"]) {
  NodeTest.test(
    `${kind} runs preservation exactly once and permits receiving-owned ledger additions`,
    async () => {
      let calls = 0;
      const before = capture();
      const after = structuredClone(before);
      after.content.tables.jones_sql_migrations = { count: 8, sha256: "receiving-ledger" };
      after.content.ledgers.jones_sql_migrations.push({ id: 100, name: "receiving-owned" });
      const evidence = await preserveForeignContent(kind, async () => {
        calls += 1;
        return { before, after };
      });
      NodeAssert.equal(calls, 1);
      NodeAssert.deepEqual(evidence, {
        unchangedForeignContent: true,
        adoptedForeignFeatures: false,
      });
    },
  );

  for (const mutation of ["content", "definition", "ledger"]) {
    NodeTest.test(`${kind} preservation rejects changed foreign ${mutation}`, async () => {
      const before = capture();
      const after = structuredClone(before);
      if (mutation === "content") after.content.tables.foreign_events.sha256 = "changed";
      if (mutation === "definition") after.content.definitions = [];
      if (mutation === "ledger") after.content.ledgers.jones_sql_migrations[0].name = "adopted";
      await NodeAssert.rejects(
        preserveForeignContent(kind, async () => ({ before, after })),
        { code: "ERR_ASSERTION" },
      );
    });
  }
}

NodeTest.test(
  "ordinary and rollback cases do not enter the foreign preservation phase",
  async () => {
    for (const kind of ["open", "restore", "rollback"]) {
      NodeAssert.equal(
        await preserveForeignContent(kind, () => NodeAssert.fail("unexpected preservation phase")),
        undefined,
      );
    }
  },
);
