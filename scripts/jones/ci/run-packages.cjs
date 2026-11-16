const { spawnSync } = require("node:child_process");
const { readWorkspace } = require("../../../.github/scripts/ci-changes.cjs");
const { packageBatch } = require("./linux-changes.cjs");

function selectPackages(input, available) {
  if (!input) return available;
  try {
    const selected = JSON.parse(input);
    if (!Array.isArray(selected) || selected.some((name) => !available.includes(name)))
      return available;
    return [...new Set(selected)];
  } catch {
    return available;
  }
}

function main() {
  const selected = selectPackages(process.env.TEST_PACKAGES, packageBatch(readWorkspace()));
  if (!selected.length) {
    console.log("No affected workspace packages in this batch; standalone tests still run.");
    return;
  }
  const args = [
    "run",
    "--parallel",
    "--concurrency-limit",
    "4",
    ...selected.flatMap((name) => ["--filter", name]),
    "test",
  ];
  console.log("vp", ...args);
  const result = spawnSync("vp", args, { stdio: "inherit" });
  if (result.error) throw result.error;
  process.exitCode = result.status ?? 1;
}

module.exports = { selectPackages };
if (require.main === module) main();
