// Jones-owned local view of the Linux CI selection. scripts/check-changed.ts
// calls these helpers so `vp run check:changed` selects what CI's `select()`
// selects, and names the jobs it cannot run itself.
const { SPLIT_PACKAGES, requiresFullQualification, select } = require("./linux-changes.cjs");

const { globToRegExp } = require("./test-inventory.cjs");
const { entries } = require("./standalone-tests.json");

const TEST_FILE = /\.(?:node-)?test\.[cm]?[jt]sx?$/;
const standaloneRunners = entries.map(({ glob, kind, runner }) => ({
  pattern: globToRegExp(glob),
  // A Vitest wrapper can supply fixtures and call the standalone suite itself.
  directNodeRunner:
    kind === "ci" && (runner === ".github/workflows/ci.yml" || runner.endsWith("/package.json")),
}));

function splitTests(tests) {
  const vitestTests = [];
  const nodeTests = [];
  for (const file of tests) {
    const normalized = file.replaceAll("\\", "/");
    const entry = standaloneRunners.find(({ pattern }) => pattern.test(normalized));
    if (entry) {
      if (entry.directNodeRunner) nodeTests.push(file);
    } else if (!normalized.includes(".node-test.") && !normalized.startsWith(".github/scripts/")) {
      vitestTests.push(file);
    }
  }
  return { vitestTests, nodeTests };
}

const SPLIT_JOB_NAMES = {
  test_desktop: "Test Desktop",
  test_mobile: "Test Mobile",
  test_client_runtime: "Test Client Runtime",
};

// Lines for the check:changed summary: the split CI jobs this change selects,
// and when CI runs everything.
function ciNotes(files, packages) {
  const notes = [];
  const outputs = select(files, packages);
  const split = Object.keys(SPLIT_PACKAGES)
    .filter((job) => outputs[job])
    .map((job) => SPLIT_JOB_NAMES[job] ?? job);
  if (requiresFullQualification(files)) {
    notes.push(
      "  CI runs every job: this change touches scripts/jones/ci/, server-shard-weights/ " +
        "or the test progress reporter",
    );
  } else if (split.length > 0) {
    notes.push(`  Split CI jobs: ${split.join(", ")}`);
  }
  return notes;
}

module.exports = { TEST_FILE, ciNotes, requiresFullQualification, splitTests };
