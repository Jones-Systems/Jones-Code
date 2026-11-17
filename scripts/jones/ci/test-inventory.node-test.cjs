const assert = require("node:assert/strict");
const { test } = require("node:test");
const {
  MANIFEST,
  globToRegExp,
  inventoryProblems,
  manifestProblems,
  referencedPaths,
  standaloneTests,
} = require("./test-inventory.cjs");

const entry = (glob, kind = "ci", runner = ".github/workflows/ci.yml") =>
  kind === "manual" ? { glob, kind, reason: "Run by hand." } : { glob, kind, runner };

// A small repository: texts maps each tracked path to its content.
function problemsFor(texts, entries) {
  const files = {
    ...texts,
    [MANIFEST]: JSON.stringify({ schema: "jones.standalone-tests/v1", entries }),
  };
  return inventoryProblems({
    files: Object.keys(files),
    readText: (file) => files[file] ?? "",
  });
}

const base = {
  ".github/workflows/ci.yml": "      - run: node --test tools/*.node-test.mjs\n",
  "tools/a.node-test.mjs": "",
};

test("the repository's standalone tests are all classified, run and named by real paths", () => {
  assert.deepEqual(inventoryProblems(), []);
});

test("globs keep * inside a segment and let ** cross directories", () => {
  const matches = (glob, file) => globToRegExp(glob).test(file);
  assert.ok(matches("scripts/jones/ci/*.node-test.*", "scripts/jones/ci/a.node-test.cjs"));
  assert.ok(!matches("scripts/jones/ci/*.node-test.*", "scripts/jones/ci/sub/a.node-test.cjs"));
  assert.ok(matches("apps/**/a.test.mjs", "apps/a.test.mjs"));
  assert.ok(matches("apps/**/a.test.mjs", "apps/x/y/a.test.mjs"));
  assert.ok(!matches("a.node-test.mjs", "aXnode-test.mjs"));
});

test("discovery finds node, source and Python tests and only node:test .test.cjs/.mjs files", () => {
  const texts = {
    "a.node-test.mjs": "",
    "b.source-test.mjs": "",
    "c.test.py": "",
    "d.test.mjs": 'import { test } from "node:test";',
    "e.test.cjs": 'const { test } = require("node:test");',
    "f.test.mjs": 'await import("node:test");',
    "g.test.mjs": 'import { it } from "vite-plus/test";',
    "h.test.ts": 'import { test } from "node:test";',
    "i.test.mjs": '// mentions "node:test" only in prose',
    "j.node-test-support.mjs": "",
  };
  assert.deepEqual(
    standaloneTests(Object.keys(texts), (file) => texts[file]),
    ["a.node-test.mjs", "b.source-test.mjs", "c.test.py", "d.test.mjs", "e.test.cjs", "f.test.mjs"],
  );
});

test("a manifest must use the schema, valid globs and a reason only for manual entries", () => {
  const problems = (entries) => manifestProblems({ schema: "jones.standalone-tests/v1", entries });
  assert.deepEqual(problems([entry("a/*.node-test.mjs"), entry("b.test.py", "manual")]), []);
  assert.match(manifestProblems({ entries: [] })[0], /expected \{ "schema"/);
  assert.match(problems([])[0], /non-empty array/);
  assert.match(problems([entry("/abs.node-test.mjs")])[0], /repository-relative/);
  assert.match(problems([entry("../up.node-test.mjs")])[0], /repository-relative/);
  assert.match(problems([entry("a{b,c}.node-test.mjs")])[0], /repository-relative/);
  assert.match(problems([entry("a.node-test.mjs"), entry("a.node-test.mjs")])[0], /duplicate glob/);
  assert.match(problems([{ glob: "a.node-test.mjs", kind: "weekly" }])[0], /kind must be/);
  assert.match(problems([{ glob: "a.node-test.mjs", kind: "ci" }])[0], /runner is required/);
  assert.match(problems([{ glob: "a.test.py", kind: "manual" }])[0], /needs a reason/);
  assert.match(
    problems([{ ...entry("a.test.py", "manual"), runner: "x.yml" }])[0],
    /manual entry has no runner/,
  );
  assert.match(
    problems([{ ...entry("a.node-test.mjs"), reason: "why" }])[0],
    /only a manual entry carries a reason/,
  );
});

test("a test with no entry, two entries, or a stale entry fails", () => {
  assert.deepEqual(problemsFor(base, [entry("tools/*.node-test.mjs")]), []);
  assert.match(
    problemsFor({ ...base, "tools/b.source-test.mjs": "" }, [
      entry("tools/*.node-test.mjs"),
    ]).join(),
    /tools\/b\.source-test\.mjs: no entry/,
  );
  assert.match(
    problemsFor(base, [
      entry("tools/*.node-test.mjs"),
      entry("tools/a.node-test.mjs", "manual"),
    ]).join(),
    /tools\/a\.node-test\.mjs: matches 2 entries/,
  );
  assert.match(
    problemsFor(base, [entry("tools/*.node-test.mjs"), entry("old/*.node-test.mjs")]).join(),
    /old\/\*\.node-test\.mjs: matches no standalone test/,
  );
});

test("a ci or advisory runner must name the glob or every test, in either path spelling", () => {
  const runner = (text, runnerPath = ".github/workflows/ci.yml") => ({
    ...base,
    [runnerPath]: text,
  });
  const entries = (runnerPath) => [entry("tools/*.node-test.mjs", "ci", runnerPath)];
  assert.deepEqual(problemsFor(runner("node --test tools/*.node-test.mjs"), entries()), []);
  assert.deepEqual(problemsFor(runner("node --test tools/a.node-test.mjs"), entries()), []);
  assert.deepEqual(
    problemsFor(
      runner('{"scripts":{"t":"node --test *.node-test.mjs"}}', "tools/package.json"),
      entries("tools/package.json"),
    ),
    [],
  );
  assert.match(
    problemsFor(
      {
        ...runner('{"scripts":{"t":"node --test a.node-test.mjs"}}', "tools/package.json"),
        "tools/c.node-test.mjs": "",
      },
      entries("tools/package.json"),
    ).join(),
    /tools\/package\.json neither contains this glob nor names every test/,
  );
  assert.match(
    problemsFor(runner("run: echo nothing"), entries()).join(),
    /neither contains this glob nor names every test/,
  );
  assert.match(
    problemsFor(base, [entry("tools/*.node-test.mjs", "ci", "missing.yml")]).join(),
    /runner missing\.yml is not a tracked file/,
  );
});

test("references find literal tests, test globs and directories, and skip patterns and prose", () => {
  const text = [
    "run: node --test apps/a/b.test.ts apps/c/ scripts/*.node-test.mjs './x/y.spec.tsx'",
    "regex: '^apps/server/enrollment/|^scripts/gone.test.ts$'",
    'cmd: vp test run $DIR/z.test.ts "${{ matrix }}/w.test.ts" https://example.com/p/q.test.ts',
    "list: apps/d/{one,two}.test.ts bare.test.ts --reporter=json docs/guide.md",
  ].join("\n");
  assert.deepEqual(
    referencedPaths(text).map(({ resolved, type }) => `${type}:${resolved}`),
    [
      "file:apps/a/b.test.ts",
      "directory:apps/c/",
      "glob:scripts/*.node-test.mjs",
      "file:x/y.spec.tsx",
    ],
  );
  assert.deepEqual(
    referencedPaths("node --test core.test.cjs ../vite.config.ts", {
      base: "apps/pkg",
      bare: true,
    }).map(({ resolved }) => resolved),
    ["apps/pkg/core.test.cjs"],
  );
});

test("a workflow or package script naming a missing test, directory or glob fails", () => {
  const workflow = [
    "      # apps/server/prose.test.ts is only mentioned in this comment",
    "      - run: |",
    "          node --test tools/*.node-test.mjs tools/gone/*.test.mjs",
    "          vp test run apps/x/dir/ apps/x/missing.test.ts apps/x/present.test.ts",
  ].join("\n");
  const problems = problemsFor(
    {
      ...base,
      ".github/workflows/ci.yml": workflow,
      "apps/x/present.test.ts": "",
      "package.json": JSON.stringify({ scripts: { t: "node --test tools/nope.node-test.mjs" } }),
      "tools/package.json": JSON.stringify({
        scripts: { t: "node --test a.node-test.mjs b.node-test.mjs" },
      }),
    },
    [entry("tools/*.node-test.mjs")],
  );
  assert.deepEqual(problems, [
    ".github/workflows/ci.yml: tools/gone/*.test.mjs names no tracked test",
    ".github/workflows/ci.yml: apps/x/dir/ names no tracked directory",
    ".github/workflows/ci.yml: apps/x/missing.test.ts names no tracked file",
    "package.json scripts: tools/nope.node-test.mjs names no tracked file",
    "tools/package.json scripts: b.node-test.mjs names no tracked file",
  ]);
});
