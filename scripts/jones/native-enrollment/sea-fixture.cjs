// Test-only SEA entry. It is built into disposable qualification binaries, never the product.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const { spawnSync } = require("node:child_process");
const { DatabaseSync } = require("node:sqlite");
const { isSea } = require("node:sea");

assert.equal(isSea(), true);
assert.equal(process.versions.node, "26.12.1");
assert.equal(process.versions.sqlite, "3.54.0");
const identity = {
  node: process.versions.node,
  sqlite: process.versions.sqlite,
  executable: fs.realpathSync(process.execPath),
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This disposable SEA reports its actual runtime without loading application services.
  platform: process.platform,
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This disposable SEA reports its actual runtime without loading application services.
  architecture: process.arch,
  sea: isSea(),
};
const options = (dbPath, writable) => {
  const uri = pathToFileURL(dbPath);
  uri.search = `mode=${writable ? "rw" : "ro"}&cache=private&vfs=unix`;
  return { filename: uri.href, readOnly: !writable, allowExtension: false };
};
const open = (dbPath, writable) => {
  const { filename, ...config } = options(dbPath, writable);
  return new DatabaseSync(filename, config);
};

if (process.argv[2] === "--wal-child") {
  const dbPath = process.argv[3];
  assert.equal(path.basename(dbPath), "state # ?.sqlite");
  assert.ok(path.basename(path.dirname(dbPath)).startsWith("jones-enrollment-sea-"));
  const db = open(dbPath, true);
  db.exec(
    "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; INSERT INTO qualification VALUES ('wal-only', 'committed-in-wal')",
  );
  // Preserve committed frames in WAL instead of checkpointing on close.
  process.stdout.write(JSON.stringify(identity), () => process.exit(0));
} else {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jones-enrollment-sea-"));
  const cleanup = () => fs.rmSync(root, { recursive: true, force: true });
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.once(signal, () => {
      cleanup();
      process.exit(signal === "SIGINT" ? 130 : 143);
    });
  }
  try {
    if (process.argv[2] === "--fail") throw new Error("synthetic qualification failure");
    if (process.argv[2] === "--hold") {
      process.stdout.write("ready\n");
      setInterval(() => {}, 1000);
    } else {
      const dbPath = path.join(root, "state # ?.sqlite");
      const seed = new DatabaseSync(dbPath);
      seed.exec(
        "CREATE TABLE qualification (id TEXT PRIMARY KEY, value TEXT); INSERT INTO qualification VALUES ('baseline', 'checkpointed')",
      );
      seed.close();
      assert.ok(options(dbPath, true).filename.includes("%23%20%3F.sqlite"));
      assert.equal(
        new URL(options(dbPath, true).filename).search,
        "?mode=rw&cache=private&vfs=unix",
      );
      const before = fs.readFileSync(dbPath);
      const reader = open(dbPath, false);
      try {
        assert.equal(reader.prepare("SELECT count(*) AS count FROM qualification").get().count, 1);
        assert.throws(
          () => reader.exec("INSERT INTO qualification VALUES ('forbidden', 'write')"),
          /readonly/,
        );
        assert.throws(() => reader.enableLoadExtension(true), { code: "ERR_INVALID_STATE" });
        assert.throws(() => reader.loadExtension("/synthetic-must-not-load"), {
          code: "ERR_INVALID_STATE",
        });
      } finally {
        reader.close();
      }
      assert.deepEqual(fs.readFileSync(dbPath), before);
      const child = spawnSync(process.execPath, ["--wal-child", dbPath], {
        encoding: "utf8",
        timeout: 10000,
      });
      assert.equal(child.status, 0, child.stderr);
      assert.deepEqual(JSON.parse(child.stdout), identity);
      assert.equal(fs.existsSync(`${dbPath}-wal`), true);
      assert.equal(fs.readFileSync(dbPath).includes(Buffer.from("committed-in-wal")), false);
      const walBefore = fs.readFileSync(dbPath);
      const walReader = open(dbPath, false);
      try {
        assert.equal(
          walReader.prepare("SELECT value FROM qualification WHERE id='wal-only'").get().value,
          "committed-in-wal",
        );
      } finally {
        walReader.close();
      }
      assert.deepEqual(fs.readFileSync(dbPath), walBefore);
      const writer = open(dbPath, true);
      try {
        writer.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      } finally {
        writer.close();
      }
      assert.equal(fs.readFileSync(dbPath)[18], 2);
      assert.equal(fs.existsSync(`${dbPath}-wal`), false);
      assert.equal(fs.existsSync(`${dbPath}-shm`), false);
      const checkpointBefore = fs.readFileSync(dbPath);
      const checkpointReader = open(dbPath, false);
      try {
        assert.equal(
          checkpointReader.prepare("SELECT count(*) AS count FROM qualification").get().count,
          2,
        );
      } finally {
        checkpointReader.close();
      }
      assert.deepEqual(fs.readFileSync(dbPath), checkpointBefore);
      fs.unlinkSync(dbPath);
      assert.throws(() => open(dbPath, true));
      assert.equal(fs.existsSync(dbPath), false);
      process.stdout.write(`${JSON.stringify({ identity, sqliteChecks: "passed" })}\n`);
    }
  } finally {
    if (process.argv[2] !== "--hold") cleanup();
  }
}
