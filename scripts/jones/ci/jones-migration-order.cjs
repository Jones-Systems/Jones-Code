const { execFileSync } = require("node:child_process");
const { readFileSync } = require("node:fs");
const ts = require("typescript-legacy");

const MANIFEST = "apps/server/src/jones/persistence/JonesMigrations.ts";
const REPOSITORY = "Jones-Systems/Jones-Code";
const CANONICAL_URL = `https://github.com/${REPOSITORY}.git`;

// Parse source as data. Neither revision's imports or migration effects may run.
function parseManifest(text) {
  const source = ts.createSourceFile(MANIFEST, text, ts.ScriptTarget.Latest, true);
  const invalid = () => {
    throw new Error("Unsupported or malformed literal jonesMigrationEntries manifest");
  };
  if (source.parseDiagnostics.length > 0) invalid();
  const statements = source.statements.filter(
    (node) =>
      ts.isVariableStatement(node) &&
      node.declarationList.declarations.some(
        (entry) => ts.isIdentifier(entry.name) && entry.name.text === "jonesMigrationEntries",
      ),
  );
  if (statements.length !== 1) invalid();
  const statement = statements[0];
  if (
    statement.declarationList.flags !== ts.NodeFlags.Const ||
    statement.declarationList.declarations.length !== 1 ||
    statement.modifiers?.length !== 1 ||
    statement.modifiers[0].kind !== ts.SyntaxKind.ExportKeyword
  )
    invalid();
  const declaration = statement.declarationList.declarations[0];
  const value = declaration.initializer;
  if (
    declaration.type ||
    !value ||
    !ts.isAsExpression(value) ||
    !ts.isTypeReferenceNode(value.type) ||
    value.type.typeName.getText(source) !== "const" ||
    value.type.typeArguments ||
    !ts.isArrayLiteralExpression(value.expression)
  )
    invalid();

  const imports = new Set(
    source.statements
      .filter(ts.isImportDeclaration)
      .filter(
        (node) =>
          ts.isStringLiteral(node.moduleSpecifier) &&
          /^\.\/Migrations\/\d+_[A-Za-z0-9]+\.ts$/.test(node.moduleSpecifier.text),
      )
      .map((node) => node.importClause?.name?.text)
      .filter(Boolean),
  );
  // Fail closed on mutation, aliasing or a computed manifest consumer. The
  // supported loader passes this one literal directly to runJonesMigrations.
  function visit(node) {
    if (
      ts.isIdentifier(node) &&
      node.text === "jonesMigrationEntries" &&
      node !== declaration.name
    ) {
      const parent = node.parent;
      if (
        !ts.isCallExpression(parent) ||
        !ts.isIdentifier(parent.expression) ||
        parent.expression.text !== "runJonesMigrations" ||
        parent.arguments.length !== 1 ||
        parent.arguments[0] !== node
      )
        invalid();
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  const ids = new Set();
  const names = new Set();
  let prior = 0;
  const entries = value.expression.elements.map((entry) => {
    if (!ts.isArrayLiteralExpression(entry) || entry.elements.length !== 3) invalid();
    const [idNode, nameNode, effect] = entry.elements;
    if (
      !ts.isNumericLiteral(idNode) ||
      !/^\d+$/.test(idNode.getText(source)) ||
      !ts.isStringLiteral(nameNode) ||
      !ts.isIdentifier(effect) ||
      !imports.has(effect.text)
    )
      invalid();
    const id = Number(idNode.text);
    const name = nameNode.text;
    if (
      !Number.isSafeInteger(id) ||
      id < 1 ||
      (id > 6 && id < 100) ||
      !/^[A-Za-z][A-Za-z0-9]*$/.test(name) ||
      ids.has(id) ||
      names.has(name) ||
      id <= prior
    )
      invalid();
    ids.add(id);
    names.add(name);
    prior = id;
    return [id, name];
  });
  if (entries.length === 0) invalid();
  return entries;
}

function compareManifests(baseline, candidate) {
  const next = new Map(candidate);
  for (const [id, name] of baseline) {
    if (next.get(id) !== name) {
      throw new Error(`Baseline Jones migration ${id}_${name} was deleted, renamed or renumbered`);
    }
  }
  const previous = new Map(baseline);
  const maximum = Math.max(...previous.keys());
  const added = candidate.filter(([id]) => !previous.has(id));
  for (const [id, name] of added) {
    if (id <= maximum) {
      throw new Error(
        `Added Jones migration ${id}_${name} must be above baseline maximum ${maximum}`,
      );
    }
  }
  return added;
}

function git(cwd, args, trim = true) {
  try {
    const output = execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 60_000,
      maxBuffer: 2 * 1024 * 1024,
    });
    return trim ? output.trim() : output;
  } catch {
    throw new Error(`Git evidence unavailable for ${args[0]}`);
  }
}

function resolve(cwd, revision) {
  if (typeof revision !== "string" || !revision || revision.startsWith("-")) {
    throw new Error("A baseline and candidate revision are required");
  }
  return git(cwd, ["rev-parse", "--verify", `${revision}^{commit}`]);
}

function qualifyRevisions({ cwd, baseline, candidate, allowUnchanged = false, log = console.log }) {
  try {
    baseline = resolve(cwd, baseline);
    candidate = resolve(cwd, candidate);
  } catch (error) {
    log(`Jones migration evidence unavailable: baseline=${baseline} candidate=${candidate}`);
    throw error;
  }
  log(`Jones migration order: baseline=${baseline} candidate=${candidate}`);
  const before = git(cwd, ["show", `${baseline}:${MANIFEST}`], false);
  const after = git(cwd, ["show", `${candidate}:${MANIFEST}`], false);
  const baselineEntries = parseManifest(before);
  const candidateEntries = parseManifest(after);
  if (allowUnchanged) {
    const common = git(cwd, ["merge-base", baseline, candidate]);
    const commonSource = git(cwd, ["show", `${common}:${MANIFEST}`], false);
    parseManifest(commonSource);
    // An unrelated stale PR need not absorb newly added main migrations. Exact
    // source equality to its common ancestor proves it did not delete any.
    if (commonSource === after) {
      log(
        `Jones manifest unchanged since common ancestor ${common}; no migration changes to admit`,
      );
      return { baseline, candidate, added: [], selection: "unchanged" };
    }
  }
  const added = compareManifests(baselineEntries, candidateEntries);
  log(`Jones migration order accepted: ${added.length} appended identities`);
  return { baseline, candidate, added, selection: "compared" };
}

function fetchCanonicalMain(cwd) {
  git(cwd, ["fetch", "--no-tags", CANONICAL_URL, "refs/heads/main"]);
  return resolve(cwd, "FETCH_HEAD");
}

function eventSha(value) {
  if (typeof value !== "string" || !/^[a-f0-9]{40}$/.test(value) || /^0+$/.test(value)) {
    throw new Error("Missing or invalid exact event revision");
  }
  return value;
}

function runForEvent({
  cwd = process.cwd(),
  env = process.env,
  event,
  fetchMain = fetchCanonicalMain,
  log = console.log,
}) {
  if (env.GITHUB_REPOSITORY !== REPOSITORY)
    throw new Error("Expected canonical Jones-Code repository");
  const candidate = eventSha(env.GITHUB_SHA);
  if (resolve(cwd, "HEAD") !== candidate)
    throw new Error("Checkout differs from exact event candidate");
  let baseline;
  let allowUnchanged = false;
  switch (env.GITHUB_EVENT_NAME) {
    case "pull_request":
      if (
        event.pull_request?.base?.repo?.full_name !== REPOSITORY ||
        typeof event.pull_request.base.ref !== "string" ||
        !event.pull_request.base.ref.trim()
      ) {
        throw new Error("PR migration admission requires a target in the canonical repository");
      }
      // The event's old base SHA and origin/main are not current allocation
      // evidence. Resolve the freshly fetched canonical ref every run.
      baseline = eventSha(fetchMain(cwd));
      allowUnchanged = true;
      break;
    case "push":
      if (
        env.GITHUB_REF !== "refs/heads/main" ||
        event.ref !== env.GITHUB_REF ||
        eventSha(event.after) !== candidate
      ) {
        throw new Error("Expected an exact canonical main push event");
      }
      baseline = eventSha(event.before);
      if (baseline === candidate) throw new Error("Main push cannot compare its result to itself");
      break;
    case "workflow_dispatch":
      baseline = eventSha(fetchMain(cwd));
      allowUnchanged = true;
      break;
    default:
      throw new Error("Unsupported event for Jones migration admission");
  }
  return qualifyRevisions({ cwd, baseline, candidate, allowUnchanged, log });
}

if (require.main === module) {
  try {
    const args = process.argv.slice(2);
    if (
      args.length === 4 &&
      args[0] === "--baseline" &&
      args[2] === "--candidate" &&
      !process.env.GITHUB_ACTIONS
    ) {
      qualifyRevisions({ cwd: process.cwd(), baseline: args[1], candidate: args[3] });
    } else if (args.length === 0) {
      const event = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, "utf8"));
      runForEvent({ event });
    } else {
      throw new Error(
        "Usage: jones-migration-order.cjs [--baseline <revision> --candidate <revision>] (explicit revisions are local only)",
      );
    }
  } catch (error) {
    console.error(`Jones migration order refused: ${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = { MANIFEST, parseManifest, compareManifests, qualifyRevisions, runForEvent };
