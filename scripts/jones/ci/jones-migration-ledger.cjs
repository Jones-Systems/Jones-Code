const { existsSync, readFileSync, readdirSync } = require("node:fs");
const { join } = require("node:path");
const ts = require("typescript-legacy");
const { MANIFEST, parseManifest } = require("./jones-migration-order.cjs");

const MIGRATIONS_DIR = "apps/server/src/jones/persistence/Migrations";
const UPSTREAM_MANIFEST = "apps/server/src/persistence/Migrations.ts";
const UPSTREAM_MIGRATIONS_DIR = "apps/server/src/persistence/Migrations";

// New Jones IDs start at 100. IDs 1-6 are the released legacy identities, kept in the
// separate jones_sql_migrations ledger, so their numeric overlap with upstream 1-6 is
// by design. IDs 7-99 belong to known foreign histories; parseManifest refuses them.
const JONES_FIRST_ALLOCATED_ID = 100;
const LEGACY_JONES_IDS = new Set([1, 2, 3, 4, 5, 6]);

// Released Jones IDs 140-145 were allocated after their module files were created
// as 125-130, so these filenames intentionally differ from their IDs. This table is
// a closed list: any other prefix that differs from its ID is refused.
const RELEASED_PREFIX_REMAPS = new Map([
  ["125_JonesWorkstreamsNative", 140],
  ["126_JonesWorkstreamsSourceEnrollments", 141],
  ["127_JonesWorkstreamsRollupAttempts", 142],
  ["128_JonesWorkstreamsPeerSessionPins", 143],
  ["129_JonesWorkstreamsMemberBindings", 144],
  ["130_JonesWorkstreamsForwardReceipts", 145],
]);

const MODULE_FILE = /^(\d+)_([A-Za-z0-9]+)\.ts$/;
const MODULE_SPECIFIER = /^\.\/Migrations\/(\d+_[A-Za-z0-9]+)\.ts$/;

function parseSource(path, text) {
  const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
  if (source.parseDiagnostics.length > 0) throw new Error(`${path} does not parse`);
  return source;
}

// Reads [id, name, bindingIdentifier] rows and the binding -> module-file map as data.
// parseManifest has already proved the literal's structure and fail-closed use.
function readJonesRows(text) {
  const source = parseSource(MANIFEST, text);
  const files = new Map();
  for (const node of source.statements) {
    if (!ts.isImportDeclaration(node) || !ts.isStringLiteral(node.moduleSpecifier)) continue;
    const match = MODULE_SPECIFIER.exec(node.moduleSpecifier.text);
    const binding = node.importClause?.name?.text;
    if (match && binding) files.set(binding, match[1]);
  }
  const declaration = source.statements
    .filter(ts.isVariableStatement)
    .flatMap((node) => [...node.declarationList.declarations])
    .find((entry) => ts.isIdentifier(entry.name) && entry.name.text === "jonesMigrationEntries");
  const rows = declaration.initializer.expression.elements.map((entry) => {
    const [id, name, binding] = entry.elements;
    return { id: Number(id.text), name: name.text, binding: binding.text };
  });
  return { rows, files };
}

function readUpstreamIds(text) {
  const source = parseSource(UPSTREAM_MANIFEST, text);
  const declaration = source.statements
    .filter(ts.isVariableStatement)
    .flatMap((node) => [...node.declarationList.declarations])
    .find((entry) => ts.isIdentifier(entry.name) && entry.name.text === "migrationEntries");
  let literal = declaration?.initializer;
  while (literal && (ts.isAsExpression(literal) || ts.isParenthesizedExpression(literal)))
    literal = literal.expression;
  if (!literal || !ts.isArrayLiteralExpression(literal))
    throw new Error(`${UPSTREAM_MANIFEST} has no literal migrationEntries array`);
  const ids = literal.elements.map((entry) => {
    const first = ts.isArrayLiteralExpression(entry) ? entry.elements[0] : undefined;
    if (!first || !ts.isNumericLiteral(first))
      throw new Error(`${UPSTREAM_MANIFEST} has a non-literal migration identity`);
    return Number(first.text);
  });
  if (ids.length === 0) throw new Error(`${UPSTREAM_MANIFEST} lists no migrations`);
  return ids;
}

function moduleFiles(dir) {
  if (!existsSync(dir)) throw new Error(`Missing migration directory ${dir}`);
  return readdirSync(dir)
    .filter((name) => /^\d+_.*\.ts$/.test(name) && !/\.(test|spec)\.ts$/.test(name))
    .map((name) => name.replace(/\.ts$/, ""))
    .sort();
}

/**
 * Checks one tree's Jones ledger. Returns { errors, summary }; never runs migration code.
 */
function checkLedger(root) {
  const errors = [];
  const jonesText = readFileSync(join(root, MANIFEST), "utf8");
  // Shared structure proof: literal, ascending, unique IDs and names, no mutation.
  parseManifest(jonesText);
  const { rows, files } = readJonesRows(jonesText);
  const onDisk = new Set(moduleFiles(join(root, MIGRATIONS_DIR)));

  const byFile = new Map();
  for (const { id, name, binding } of rows) {
    const file = files.get(binding);
    if (file === undefined) {
      errors.push(
        `Jones migration ${id}_${name} binds ${binding}, which is not a Migrations module`,
      );
      continue;
    }
    byFile.set(file, [...(byFile.get(file) ?? []), id]);
    const [, prefix, moduleName] = MODULE_FILE.exec(`${file}.ts`) ?? [];
    if (!onDisk.has(file))
      errors.push(`Jones migration ${id}_${name} has no module file ${file}.ts`);
    if (moduleName !== `Jones${name}`)
      errors.push(`Jones migration ${id}_${name} maps to module ${file}, expected Jones${name}`);
    const remapped = RELEASED_PREFIX_REMAPS.get(file);
    if (remapped === undefined ? Number(prefix) !== id : remapped !== id)
      errors.push(
        `Jones migration ${id}_${name} maps to module ${file}, whose prefix is not its ID` +
          (remapped === undefined ? "" : ` (pinned to ${remapped})`),
      );
  }
  for (const [file, ids] of byFile)
    if (ids.length > 1)
      errors.push(`Module ${file} is bound to several Jones IDs: ${ids.join(", ")}`);
  const bindings = rows.map((row) => row.binding);
  for (const binding of new Set(bindings))
    if (bindings.filter((entry) => entry === binding).length > 1)
      errors.push(`Binding ${binding} is used by several Jones migration IDs`);
  for (const file of onDisk)
    if (!byFile.has(file)) errors.push(`Module ${file}.ts is not mapped to any Jones migration ID`);

  const jonesIds = rows.map((row) => row.id);

  const upstreamIds = readUpstreamIds(readFileSync(join(root, UPSTREAM_MANIFEST), "utf8"));
  const upstreamPrefixes = moduleFiles(join(root, UPSTREAM_MIGRATIONS_DIR)).map((file) =>
    Number(file.split("_")[0]),
  );
  const upstream = new Set([...upstreamIds, ...upstreamPrefixes]);
  for (const id of upstream)
    if (id >= JONES_FIRST_ALLOCATED_ID)
      errors.push(
        `Upstream migration number ${id} entered the Jones range from ${JONES_FIRST_ALLOCATED_ID}`,
      );
  for (const id of jonesIds)
    if (!LEGACY_JONES_IDS.has(id) && upstream.has(id))
      errors.push(`Jones migration ID ${id} collides with an upstream migration number`);

  return {
    errors,
    summary: {
      jonesEntries: rows.length,
      jonesMaximum: Math.max(...jonesIds),
      modulesOnDisk: onDisk.size,
      upstreamEntries: upstreamIds.length,
      upstreamMaximum: Math.max(...upstream),
    },
  };
}

function run(root, log = console.log) {
  const { errors, summary } = checkLedger(root);
  log(
    `Jones migration ledger: ${summary.jonesEntries} IDs (max ${summary.jonesMaximum}) over ` +
      `${summary.modulesOnDisk} modules; upstream ${summary.upstreamEntries} entries (max ${summary.upstreamMaximum})`,
  );
  if (errors.length > 0) throw new Error(`\n- ${errors.join("\n- ")}`);
  return summary;
}

if (require.main === module) {
  try {
    const args = process.argv.slice(2);
    if (args.length === 0) run(process.cwd());
    else if (args.length === 2 && args[0] === "--root") run(args[1]);
    else throw new Error("Usage: jones-migration-ledger.cjs [--root <checkout-or-export>]");
  } catch (error) {
    console.error(`Jones migration ledger refused: ${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = {
  RELEASED_PREFIX_REMAPS,
  checkLedger,
  readJonesRows,
  readUpstreamIds,
  run,
};
