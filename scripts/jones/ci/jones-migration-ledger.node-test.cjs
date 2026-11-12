const assert = require("node:assert/strict");
const { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { dirname, join, resolve } = require("node:path");
const { test } = require("node:test");
const { RELEASED_PREFIX_REMAPS, checkLedger, run } = require("./jones-migration-ledger.cjs");

const JONES_MANIFEST = "apps/server/src/jones/persistence/JonesMigrations.ts";
const JONES_DIR = "apps/server/src/jones/persistence/Migrations";
const UPSTREAM_MANIFEST = "apps/server/src/persistence/Migrations.ts";
const UPSTREAM_DIR = "apps/server/src/persistence/Migrations";

// Each fixture row is [id, name, moduleFileBase]; the file base defaults to `${id}_Jones${name}`.
const baseRows = [
  [1, "Original"],
  [100, "First"],
  [101, "Second"],
  [140, "WorkstreamsNative", "125_JonesWorkstreamsNative"],
];

// Inserts a row before the 140 remap so the ascending-ID literal stays valid.
const withRow = (row) => [...baseRows.slice(0, 3), row, baseRows[3]];

const fileOf = ([id, name, file]) => file ?? `${String(id).padStart(3, "0")}_Jones${name}`;

function jonesSource(rows) {
  const imports = rows
    .map((row) => `import Migration${row[0]} from "./Migrations/${fileOf(row)}.ts";`)
    .join("\n");
  const entries = rows
    .map(([id, name]) => `  [${id}, ${JSON.stringify(name)}, Migration${id}],`)
    .join("\n");
  return `${imports}\nexport const jonesMigrationEntries = [\n${entries}\n] as const;\n`;
}

function upstreamSource(ids) {
  const rows = ids.map((id) => `  [${id}, "Upstream${id}", Migration${id}],`).join("\n");
  return `const migrationEntries = [\n${rows}\n] as const;\n`;
}

function write(root, path, text = "export default 1;\n") {
  mkdirSync(dirname(join(root, path)), { recursive: true });
  writeFileSync(join(root, path), text);
}

function withTree(
  { rows = baseRows, files = rows.map(fileOf), upstream = [1, 2, 3], upstreamFiles = upstream },
  check,
) {
  const root = mkdtempSync(join(tmpdir(), "jones-migration-ledger-"));
  const cleanup = () => rmSync(root, { recursive: true, force: true });
  const cancelled = () => {
    cleanup();
    process.exit(1);
  };
  process.once("SIGINT", cancelled);
  process.once("SIGTERM", cancelled);
  try {
    write(root, JONES_MANIFEST, jonesSource(rows));
    for (const file of files) write(root, `${JONES_DIR}/${file}.ts`);
    write(root, `${JONES_DIR}/${fileOf(rows[0])}.test.ts`);
    write(root, UPSTREAM_MANIFEST, upstreamSource(upstream));
    for (const id of upstreamFiles)
      write(root, `${UPSTREAM_DIR}/${String(id).padStart(3, "0")}_Upstream${id}.ts`);
    return check(root);
  } finally {
    process.removeListener("SIGINT", cancelled);
    process.removeListener("SIGTERM", cancelled);
    cleanup();
  }
}

const errorsOf = (options) => withTree(options, (root) => checkLedger(root).errors);

test("accepts a one-to-one ledger, a pinned released remap and gaps in the ID sequence", () => {
  assert.deepEqual(errorsOf({}), []);
  withTree({}, (root) => assert.equal(run(root, () => {}).jonesEntries, 4));
});

test("pins only the released prefix remaps", () => {
  assert.deepEqual([...RELEASED_PREFIX_REMAPS.values()], [140, 141, 142, 143, 144, 145]);
  const [wrong] = errorsOf({
    rows: [...baseRows.slice(0, 3), [141, "WorkstreamsNative", "125_JonesWorkstreamsNative"]],
  });
  assert.match(wrong, /125_JonesWorkstreamsNative.*pinned to 140/);
});

test("refuses a module file that no ID maps to", () => {
  const errors = errorsOf({ files: [...baseRows.map(fileOf), "102_JonesOrphan"] });
  assert.deepEqual(errors, ["Module 102_JonesOrphan.ts is not mapped to any Jones migration ID"]);
});

test("refuses an ID whose module file is missing", () => {
  const errors = errorsOf({
    files: baseRows.map(fileOf).filter((file) => file !== "101_JonesSecond"),
  });
  assert.deepEqual(errors, ["Jones migration 101_Second has no module file 101_JonesSecond.ts"]);
});

test("refuses one module bound to two IDs", () => {
  const errors = errorsOf({
    rows: withRow([102, "SecondAgain", "101_JonesSecond"]),
    files: baseRows.map(fileOf),
  });
  assert.ok(
    errors.some((error) => /101_JonesSecond is bound to several Jones IDs: 101, 102/.test(error)),
  );
});

test("refuses a prefix that differs from its ID and a name that differs from its module", () => {
  const prefix = errorsOf({ rows: withRow([102, "Third", "103_JonesThird"]) });
  assert.ok(
    prefix.some((error) =>
      /102_Third maps to module 103_JonesThird, whose prefix is not its ID/.test(error),
    ),
  );
  const name = errorsOf({ rows: withRow([102, "Third", "102_JonesOther"]) });
  assert.ok(
    name.some((error) =>
      /102_Third maps to module 102_JonesOther, expected JonesThird/.test(error),
    ),
  );
});

test("refuses an upstream number in the Jones range or equal to a Jones ID", () => {
  const entered = errorsOf({ upstream: [1, 2, 100], upstreamFiles: [1, 2, 100] });
  assert.ok(
    entered.some((error) => /Upstream migration number 100 entered the Jones range/.test(error)),
  );
  assert.ok(
    entered.some((error) =>
      /Jones migration ID 100 collides with an upstream migration number/.test(error),
    ),
  );
  const fileOnly = errorsOf({ upstream: [1, 2], upstreamFiles: [1, 2, 101] });
  assert.ok(
    fileOnly.some((error) => /Upstream migration number 101 entered the Jones range/.test(error)),
  );
});

test("allows legacy Jones IDs 1-6 beside the same upstream numbers", () => {
  assert.deepEqual(errorsOf({ upstream: [1, 2, 3, 4, 5, 6] }), []);
});

test("rejects a non-literal manifest instead of guessing", () => {
  withTree({}, (root) => {
    write(root, JONES_MANIFEST, "export const jonesMigrationEntries = build();\n");
    assert.throws(() => checkLedger(root), /malformed literal jonesMigrationEntries/);
  });
});

test("unconditional Lint runs the ledger check after the order check", () => {
  const workflow = readFileSync(join(__dirname, "../../../.github/workflows/ci.yml"), "utf8");
  const lint = workflow.match(/^  lint:\n([\s\S]*?)(?=^  [\w-]+:)/m)[1];
  assert.match(
    lint,
    /- name: Check Jones migration order\n        run: node scripts\/jones\/ci\/jones-migration-order\.cjs\n\n      - name: Check Jones migration ledger\n        run: node scripts\/jones\/ci\/jones-migration-ledger\.cjs/,
  );
});

test("accepts the checked-in repository ledger", () => {
  assert.deepEqual(checkLedger(resolve(__dirname, "../../..")).errors, []);
});
