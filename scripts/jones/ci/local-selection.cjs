// Jones-owned local view of the Linux CI selection. scripts/check-changed.ts
// calls these helpers so `vp run check:changed` selects what CI's `select()`
// selects, and names the jobs it cannot run itself.
const { SPLIT_PACKAGES, requiresFullQualification, select } = require("./linux-changes.cjs");

// vitest runs test files; node:test files must run under `node --test`, since
// vitest passes a failing node:test file. CI runs `*.node-test.*` and the
// .github/scripts tests that way.
const TEST_FILE = /\.(?:node-)?test\.[cm]?[jt]sx?$/;
const NODE_TEST_FILE = /\.node-test\.[cm]?[jt]sx?$|^\.github[/\\]scripts[/\\]/;

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
    notes.push(`  CI also runs: ${split.join(", ")}`);
  }
  notes.push(
    "  CI runs every job for pushes to main and for stacked or non-main-based pull requests",
  );
  return notes;
}

module.exports = { NODE_TEST_FILE, TEST_FILE, ciNotes, requiresFullQualification };
