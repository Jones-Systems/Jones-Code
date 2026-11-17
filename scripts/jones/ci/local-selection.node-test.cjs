const assert = require("node:assert/strict");
const { test } = require("node:test");
const { decide, readWorkspace } = require("../../../.github/scripts/ci-changes.cjs");
const { SPLIT_PACKAGES, select } = require("./linux-changes.cjs");
const {
  TEST_FILE,
  ciNotes,
  requiresFullQualification,
  splitTests,
} = require("./local-selection.cjs");

const packages = readWorkspace();
const SHARED_KEYS = ["typecheck", "build", "test_web", "test_server", "rust", "release_smoke"];

// The decision scripts/check-changed.ts makes, as it makes it.
function localSelection(files) {
  const { affected, outputs } = decide(files, packages, requiresFullQualification(files));
  const selected = new Set(affected);
  return {
    ...Object.fromEntries(SHARED_KEYS.map((key) => [key, outputs[key]])),
    ...Object.fromEntries(
      Object.entries(SPLIT_PACKAGES).map(([job, name]) => [job, selected.has(name)]),
    ),
  };
}

const CHANGED_FILE_SETS = {
  "docs only": ["docs/operations/development.md"],
  "web source": ["apps/web/src/main.tsx"],
  "server source": ["apps/server/src/server.ts"],
  "desktop source": ["apps/desktop/src/main.ts"],
  "mobile source": ["apps/mobile/src/app.tsx"],
  "client runtime source": ["packages/client-runtime/src/jones/example.ts"],
  "shared package source": ["packages/shared/src/example.ts"],
  "rust native source": ["native/resource-monitor/src/lib.rs"],
  "root manifest": ["package.json"],
  "unknown path": ["unrecognized/file.ts"],
  "ci workflow": [".github/workflows/ci.yml"],
  "ci script": ["scripts/jones/ci/linux-changes.cjs"],
  "ci node-test": ["scripts/jones/ci/local-selection.node-test.cjs"],
  "shard weights": ["scripts/jones/server-shard-weights/select.ts"],
  "progress reporter": ["scripts/jones/ci-test-progress-reporter.ts"],
  "check-changed itself": ["scripts/check-changed.ts"],
  "other jones script": ["scripts/jones/boundary/inventory.json"],
  "mixed docs and ci script": [
    "docs/operations/development.md",
    "scripts/jones/ci/check-results.cjs",
  ],
};

test("local selection equals CI select() for shared keys and split jobs", () => {
  for (const [name, files] of Object.entries(CHANGED_FILE_SETS)) {
    const ci = select(files, packages);
    const local = localSelection(files);
    for (const key of [...SHARED_KEYS, ...Object.keys(SPLIT_PACKAGES)]) {
      assert.equal(local[key], ci[key], `${name}: ${key}`);
    }
  }
});

test("only Jones CI selection scripts add full qualification beyond the upstream selector", () => {
  for (const file of [
    "scripts/jones/ci/linux-changes.cjs",
    "scripts/jones/ci/local-selection.cjs",
    "scripts/jones/server-shard-weights/select.ts",
    "scripts/jones/ci-test-progress-reporter.ts",
  ]) {
    assert.equal(requiresFullQualification([file]), true, file);
    assert.deepEqual(
      localSelection([file]),
      Object.fromEntries(
        [...SHARED_KEYS, ...Object.keys(SPLIT_PACKAGES)].map((key) => [key, true]),
      ),
      file,
    );
  }
  for (const file of [
    "scripts/jones/boundary/inventory.json",
    "scripts/jones/ci-test-progress-reporter.test.ts",
    "scripts/check-changed.ts",
    "apps/web/src/main.tsx",
    "docs/operations/development.md",
  ]) {
    assert.equal(requiresFullQualification([file]), false, file);
  }
  assert.equal(requiresFullQualification(null), true);
  assert.equal(requiresFullQualification([]), false);
});

test("ciNotes names the split jobs CI runs and when CI runs everything", () => {
  const joined = (files) => ciNotes(files, packages).join("\n");
  assert.match(joined(["apps/desktop/src/main.ts"]), /Split CI jobs: .*Test Desktop/);
  assert.match(joined(["apps/mobile/src/app.tsx"]), /Split CI jobs: .*Test Mobile/);
  assert.match(joined(["packages/client-runtime/src/jones/example.ts"]), /Test Client Runtime/);
  const shared = joined(["packages/shared/src/example.ts"]);
  for (const name of ["Test Desktop", "Test Mobile", "Test Client Runtime"]) {
    assert.ok(shared.includes(name), name);
  }
  assert.doesNotMatch(joined(["apps/web/src/main.tsx"]), /Test Desktop|Test Mobile/);
  assert.match(joined(["scripts/jones/ci/check-results.cjs"]), /CI runs every job: this change/);
  assert.doesNotMatch(joined(["docs/operations/development.md"]), /Split CI jobs|every job: this/);
});

test("routes directly runnable CI tests and leaves fixture suites with their declared runners", () => {
  const nodeTests = [
    "scripts/jones/ci/selection.node-test.cjs",
    "scripts/jones/worktree-setup/setup.node-test.mjs",
    "native/process-placement/build.test.mjs",
    ".github\\scripts\\thread-transfer-report.test.cjs",
  ];
  const vitestTests = ["apps/desktop/src/jones/updates/installRetention.test.ts"];
  assert.deepEqual(
    splitTests([
      ...nodeTests,
      ...vitestTests,
      "scripts/jones/native-enrollment/sea-fixture.node-test.mjs",
      "scripts/jones/updates/install-retention.node-test.mjs",
      "apps/desktop/scripts/jones-thread-monitor-mac-smoke.test.mjs",
      ".github/scripts/relay-state-output.test.cjs",
      "scripts/jones/new/example.node-test.mjs",
    ]),
    { nodeTests, vitestTests },
  );
});

test("test-file detection selects node-tests and vitest tests, not their sources", () => {
  for (const file of [
    "apps/server/src/ws.test.ts",
    "apps/web/src/Example.browser.test.tsx",
    "scripts/jones/ci/selection.node-test.cjs",
    "apps/server/src/jones/Example.node-test.mjs",
    ".github/scripts/ci-changes.test.cjs",
  ]) {
    assert.equal(TEST_FILE.test(file), true, file);
  }
  for (const file of [
    "apps/server/src/ws.ts",
    "scripts/jones/ci/linux-changes.cjs",
    "scripts/jones/ci/example.node-test.json",
    "scripts/jones/ci/example.node-tests.mjs",
  ]) {
    assert.equal(TEST_FILE.test(file), false, file);
  }
});
