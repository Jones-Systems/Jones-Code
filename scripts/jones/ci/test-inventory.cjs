// Vitest finds only its own files; node:test, source and Python tests run only where a
// workflow or script names them. standalone-tests.json classifies each by glob. The checks
// fail on an unclassified test, a stale entry, a runner that no longer names its tests, and
// a named test path that does not exist. They read tracked files, as CI sees them.
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "../../..");
const MANIFEST = "scripts/jones/ci/standalone-tests.json";
const SCHEMA = "jones.standalone-tests/v1";
const KINDS = ["ci", "advisory", "manual"];

// Vendored reference repositories are not Jones tests, and CI's sparse checkout omits them.
const VENDORED = /^\.repos\//;
const STANDALONE_NAME = /\.(?:node-test|source-test)\.[^./]+$|\.test\.py$/;
const NODE_TEST_FILE = /\.test\.[cm]js$/;
const NODE_TEST_IMPORT = /(?:\bfrom|\bimport|\brequire\s*\(|\bimport\s*\()\s*["']node:test["']/;
const TEST_NAME = /\.(?:test|spec|node-test|source-test)\.[A-Za-z0-9]+$/;
const TEST_GLOB = /\.(?:test|spec|node-test|source-test)\./;
const REPOSITORY_DIRECTORY = /^(?:apps|packages|scripts|native|infra|docs|\.github)\//;
const TOKEN_SEPARATOR = /[\s'"`()<>;|&,=:\\]+/;
const TOKEN_CHARACTERS = /^[A-Za-z0-9_.\-/+*]+$/;

function trackedFiles(root = ROOT) {
  const listed = execFileSync("git", ["-C", root, "ls-files", "-z"], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  return listed.split("\0").filter((file) => file && !VENDORED.test(file));
}

function fileReader(root = ROOT) {
  return (file) => {
    try {
      return fs.readFileSync(path.join(root, file), "utf8");
    } catch (error) {
      // A tracked file deleted in the working tree has nothing left to run.
      if (error.code === "ENOENT") return "";
      throw error;
    }
  };
}

// `*` stays inside one path segment and `**` crosses them. Nothing else is special.
function globToRegExp(glob) {
  let source = "";
  for (let index = 0; index < glob.length; index += 1) {
    const char = glob[index];
    if (char === "*" && glob[index + 1] === "*") {
      const directories = glob[index + 2] === "/";
      source += directories ? "(?:.*/)?" : ".*";
      index += directories ? 2 : 1;
    } else if (char === "*") source += "[^/]*";
    else if (char === "?") source += "[^/]";
    else source += char.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  }
  return new RegExp(`^${source}$`);
}

function standaloneTests(files, readText) {
  return files.filter(
    (file) =>
      STANDALONE_NAME.test(file) ||
      (NODE_TEST_FILE.test(file) && NODE_TEST_IMPORT.test(readText(file))),
  );
}

function manifestProblems(manifest) {
  const problems = [];
  const keys = (value) => (value && typeof value === "object" ? Object.keys(value) : []);
  if (keys(manifest).join() !== "schema,entries" || manifest.schema !== SCHEMA)
    return [`${MANIFEST}: expected { "schema": "${SCHEMA}", "entries": [...] }`];
  if (!Array.isArray(manifest.entries) || !manifest.entries.length)
    return [`${MANIFEST}: entries must be a non-empty array`];
  const seen = new Set();
  for (const [index, entry] of manifest.entries.entries()) {
    const label = `${MANIFEST} entry ${index}`;
    const fail = (message) => problems.push(`${label} (${entry?.glob}): ${message}`);
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      problems.push(`${label}: expected an object`);
      continue;
    }
    if (keys(entry).some((key) => !["glob", "kind", "runner", "reason"].includes(key)))
      fail("only glob, kind, runner and reason are allowed");
    if (
      typeof entry.glob !== "string" ||
      !/^[A-Za-z0-9_.\-/*?]+$/.test(entry.glob) ||
      entry.glob.startsWith("/") ||
      entry.glob.split("/").some((part) => !part || part === "." || part === "..")
    )
      fail("glob must be a repository-relative path using only * ** and ? wildcards");
    else if (seen.has(entry.glob)) fail("duplicate glob");
    else seen.add(entry.glob);
    if (!KINDS.includes(entry.kind)) fail(`kind must be one of ${KINDS.join(", ")}`);
    else if (entry.kind === "manual") {
      if (typeof entry.reason !== "string" || !entry.reason.trim())
        fail("a manual entry needs a reason it has no runner");
      if ("runner" in entry) fail("a manual entry has no runner");
    } else {
      if (typeof entry.runner !== "string" || !entry.runner) fail("runner is required");
      if ("reason" in entry) fail("only a manual entry carries a reason");
    }
  }
  return problems;
}

// Each test must match exactly one entry, and each entry must still match a test.
function classify(tests, entries) {
  const problems = [];
  const matchers = entries.map((entry) => ({ entry, regex: globToRegExp(entry.glob) }));
  const matched = new Map(entries.map((entry) => [entry, []]));
  for (const file of tests) {
    const hits = matchers.filter(({ regex }) => regex.test(file));
    if (!hits.length)
      problems.push(`${file}: no entry in ${MANIFEST}; add one with kind ci, advisory or manual`);
    else if (hits.length > 1)
      problems.push(
        `${file}: matches ${hits.length} entries (${hits.map(({ entry }) => entry.glob).join(", ")}); a test needs exactly one`,
      );
    for (const { entry } of hits) matched.get(entry).push(file);
  }
  for (const [entry, files] of matched)
    if (!files.length) problems.push(`${entry.glob}: matches no standalone test; fix or remove it`);
  return { problems, matched };
}

// A ci or advisory runner must name its tests: the glob itself, or every matched file.
// Paths may be relative to the runner's directory, as in a package script.
function runnerProblems(matched, files, readText) {
  const problems = [];
  const tracked = new Set(files);
  for (const [entry, tests] of matched) {
    if (entry.kind === "manual" || !tests.length) continue;
    if (!tracked.has(entry.runner)) {
      problems.push(`${entry.glob}: runner ${entry.runner} is not a tracked file`);
      continue;
    }
    const text = readText(entry.runner);
    const directory = path.posix.dirname(entry.runner);
    const spellings = (name) => [
      name,
      ...(directory !== "." && name.startsWith(`${directory}/`)
        ? [name.slice(directory.length + 1)]
        : []),
    ];
    const names = (name) => spellings(name).some((spelling) => text.includes(spelling));
    if (!names(entry.glob) && !tests.every(names))
      problems.push(
        `${entry.glob}: ${entry.runner} neither contains this glob nor names every test it matches`,
      );
  }
  return problems;
}

// Literal test paths, test globs and repository directories that a text names.
// `bare` also accepts a slashless test name, which a package script resolves
// against its own directory but a workflow comment would only be prose.
function referencedPaths(text, { base = ".", bare = false } = {}) {
  const found = new Map();
  for (const raw of text.split(TOKEN_SEPARATOR)) {
    const token = raw.replace(/^(?:\.\/)+/, "");
    // Absolute paths and the path part of a URL are not repository files.
    if (!token || token.startsWith("/") || !TOKEN_CHARACTERS.test(token)) continue;
    const resolved = path.posix.normalize(path.posix.join(base, token));
    if (resolved.startsWith("..")) continue;
    const last = token.split("/").pop();
    if (token.includes("*")) {
      if (TEST_GLOB.test(last)) found.set(token, { token, resolved, type: "glob" });
    } else if (token.endsWith("/")) {
      if (REPOSITORY_DIRECTORY.test(resolved))
        found.set(token, { token, resolved, type: "directory" });
    } else if (TEST_NAME.test(last) && (bare || token.includes("/"))) {
      found.set(token, { token, resolved, type: "file" });
    }
  }
  return [...found.values()];
}

function referenceProblems(sources, files) {
  const problems = [];
  const tracked = new Set(files);
  for (const { name, text, base, bare } of sources) {
    for (const { token, resolved, type } of referencedPaths(text, { base, bare })) {
      const exists =
        type === "file"
          ? tracked.has(resolved)
          : type === "directory"
            ? files.some((file) => file.startsWith(resolved))
            : files.some((file) => globToRegExp(resolved).test(file));
      if (!exists)
        problems.push(`${name}: ${token} names no tracked ${type === "glob" ? "test" : type}`);
    }
  }
  return problems;
}

// Whole-line comments are prose and may name a path relative to some other directory.
const withoutComments = (text) =>
  text
    .split("\n")
    .filter((line) => !/^\s*#/.test(line))
    .join("\n");

function referenceSources(files, readText) {
  const workflows = files
    .filter((file) => /^\.github\/workflows\/[^/]+\.ya?ml$/.test(file))
    .map((file) => ({
      name: file,
      text: withoutComments(readText(file)),
      base: ".",
      bare: false,
    }));
  const packages = files
    .filter((file) => path.posix.basename(file) === "package.json")
    .map((file) => {
      const scripts = JSON.parse(readText(file)).scripts ?? {};
      return {
        name: `${file} scripts`,
        text: Object.values(scripts).join("\n"),
        base: path.posix.dirname(file),
        bare: true,
      };
    });
  return [...workflows, ...packages];
}

function inventoryProblems({
  root = ROOT,
  files = trackedFiles(root),
  readText = fileReader(root),
} = {}) {
  const manifest = JSON.parse(readText(MANIFEST));
  const invalid = manifestProblems(manifest);
  if (invalid.length) return invalid;
  const tests = standaloneTests(files, readText);
  const { problems, matched } = classify(tests, manifest.entries);
  return [
    ...problems,
    ...runnerProblems(matched, files, readText),
    ...referenceProblems(referenceSources(files, readText), files),
  ];
}

module.exports = {
  MANIFEST,
  classify,
  globToRegExp,
  inventoryProblems,
  manifestProblems,
  referencedPaths,
  referenceProblems,
  runnerProblems,
  standaloneTests,
  trackedFiles,
};

if (require.main === module) {
  const problems = inventoryProblems();
  for (const problem of problems) console.error(problem);
  process.exitCode = problems.length ? 1 : 0;
}
