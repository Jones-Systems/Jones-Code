// Jones adapts upstream package selection to complete-stack qualification and
// the split CI jobs. Keep the imported selector unchanged for future syncs.
const fs = require("node:fs");
const { execFileSync } = require("node:child_process");
const { decide, readWorkspace } = require("../../../.github/scripts/ci-changes.cjs");

const SPLIT_PACKAGES = {
  test_desktop: "@t3tools/desktop",
  test_mobile: "@t3tools/mobile",
  test_client_runtime: "@t3tools/client-runtime",
};
const OWN_TEST_JOB = new Set([
  "t3",
  "@t3tools/web",
  "@t3tools/monorepo",
  ...Object.values(SPLIT_PACKAGES),
]);

function packageBatch(packages) {
  return packages
    .filter((pkg) => pkg.hasTest && !OWN_TEST_JOB.has(pkg.name))
    .map((pkg) => pkg.name);
}

function select(files, packages) {
  // These scripts control selection, execution and sharding across packages.
  const full =
    files === null ||
    files.some((file) =>
      /^scripts\/jones\/(ci\/|server-shard-weights\/|ci-test-progress-reporter\.ts$)/.test(file),
    );
  const { affected, outputs } = decide(files, packages, full);
  const selected = new Set(affected);
  return {
    ...Object.fromEntries(
      ["typecheck", "build", "test_web", "test_server", "rust", "release_smoke"].map((key) => [
        key,
        outputs[key],
      ]),
    ),
    ...Object.fromEntries(
      Object.entries(SPLIT_PACKAGES).map(([job, name]) => [job, selected.has(name)]),
    ),
    // Standalone Node/Python/native tests do not belong to a workspace package.
    // Their job always runs; only its package step consumes this list.
    test_packages: JSON.stringify(packageBatch(packages).filter((name) => selected.has(name))),
  };
}

function changedPaths(eventName, event, run) {
  if (eventName !== "pull_request") return { files: null, reason: "Non-PR full qualification" };
  try {
    const pr = event.pull_request;
    const sha = /^[0-9a-f]{40}$/;
    if (!Number.isSafeInteger(pr?.number) || !sha.test(pr?.head?.sha) || !sha.test(pr?.base?.sha)) {
      throw new Error("Missing exact PR revisions");
    }
    const repository = event.repository?.full_name;
    if (!/^[\w.-]+\/[\w.-]+$/.test(repository ?? ""))
      throw new Error("Missing repository identity");
    const metadata = JSON.parse(run("gh", ["api", `repos/${repository}/pulls/${pr.number}`]));
    if (
      metadata.head?.sha !== pr.head.sha ||
      metadata.base?.sha !== pr.base.sha ||
      metadata.base?.ref !== pr.base.ref ||
      metadata.number !== pr.number
    ) {
      throw new Error("PR metadata changed or is incomplete");
    }
    // A native stack can have a main base on its bottom PR. Ordinary branch
    // stacks can have no native membership. Either form gets a complete run.
    if (
      metadata.stack != null ||
      !event.repository.default_branch ||
      pr.base.ref !== event.repository.default_branch
    ) {
      throw new Error("Stack or non-default PR base requires full qualification");
    }
    const paths = run("git", [
      "diff",
      "--no-renames",
      "--name-only",
      "-z",
      pr.base.sha,
      pr.head.sha,
      "--",
    ]);
    if (paths && !paths.endsWith("\0")) throw new Error("Incomplete changed-path output");
    const files = paths.split("\0").filter(Boolean);
    if (
      files.some(
        (file) => /[\r\n]/.test(file) || file.startsWith("/") || file.split("/").includes(".."),
      )
    ) {
      throw new Error("Ambiguous changed path");
    }
    return {
      files,
      reason: `Exact PR diff ${pr.base.sha}..${pr.head.sha} (renames include both paths)`,
    };
  } catch (error) {
    return { files: null, reason: `Full fallback: ${error.message}` };
  }
}

function main() {
  let change = { files: null, reason: "Missing event evidence; full qualification" };
  try {
    change = changedPaths(
      process.env.GITHUB_EVENT_NAME,
      JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH, "utf8")),
      (command, args) =>
        execFileSync(command, args, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }),
    );
  } catch (error) {
    console.error(`Could not read event: ${error.message}`);
  }
  console.error(change.reason);
  const outputs = select(change.files, readWorkspace());
  const lines =
    Object.entries(outputs)
      .map(([key, value]) => `${key}=${value}`)
      .join("\n") + "\n";
  fs.appendFileSync(process.env.GITHUB_OUTPUT, lines);
  if (process.env.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(
      process.env.GITHUB_STEP_SUMMARY,
      `### Linux CI selection\n\n${change.reason}\n\n\`\`\`text\n${lines}\`\`\`\n`,
    );
  }
}

module.exports = { changedPaths, packageBatch, select };
if (require.main === module) main();
