"use strict";

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const { createRequire } = require("node:module");

const WORKFLOW = ".github/workflows/focused-precheck.yml";
const HELPER = ".github/scripts/focused-precheck.cjs";
const NOT_COVERED =
  "candidate tree, not the PR merge commit; not a required check; not merge approval";
const STATIC = {
  knip: "vp run knip:check",
  boundary: "node scripts/jones/check-boundary.ts --strict --json",
  "vp-check": "vp check",
  typecheck: "vpr typecheck",
};
const ORDINARY_SCRIPT = "vp test run --config ../../vite.config.ts --dir .";
const SPECIAL_SCRIPTS = {
  "apps/server": "vp test run",
  "packages/client-runtime": "vp test run",
  "apps/desktop": "vp test run --passWithNoTests",
  "apps/web": "vp test run --passWithNoTests --project unit --project browser",
};

function requireThat(condition, message) {
  if (!condition) throw new Error(message);
}

function validateRequest(env) {
  const branch = env.DEFAULT_BRANCH;
  requireThat(env.GITHUB_EVENT_NAME === "workflow_dispatch", "manual dispatch required");
  requireThat(branch && env.GITHUB_REF === `refs/heads/${branch}`, "default branch required");
  requireThat(
    env.GITHUB_WORKFLOW_REF === `${env.GITHUB_REPOSITORY}/${WORKFLOW}@refs/heads/${branch}`,
    "unsupported workflow identity",
  );
  requireThat(
    /^[0-9a-f]{40}$/.test(env.GITHUB_SHA || "") &&
      /^[0-9a-f]{40}$/.test(env.GITHUB_WORKFLOW_SHA || ""),
    "missing workflow revision",
  );
  requireThat(
    /^[0-9a-f]{40}$/.test(env.INPUT_CANDIDATE_SHA || ""),
    "candidate must be a lowercase 40-hex SHA",
  );
  const label = env.INPUT_CANDIDATE_BRANCH || "";
  requireThat(
    /^[A-Za-z0-9._/-]{1,200}$/.test(label) && !/^[-/.]|[/.]$|\.\.|\/\/|@\{|\.lock$/.test(label),
    "invalid candidate branch label",
  );
  requireThat(
    ["static", "tests", "static-and-tests"].includes(env.INPUT_PROFILE),
    "invalid profile",
  );
  requireThat(/^[a-z0-9][a-z0-9-]{7,63}$/.test(env.INPUT_REQUEST_ID || ""), "invalid request ID");
  const raw = env.INPUT_TEST_PATHS || "";
  requireThat(raw.length <= 4096 && /^[\x09-\x0d\x20-\x7e]*$/.test(raw), "invalid test path input");
  const paths = [...new Set(raw.split(/[\t\n\v\f\r ]+/).filter(Boolean))].sort();
  requireThat(paths.length <= 20, "at most 20 test paths");
  requireThat(
    env.INPUT_PROFILE === "static" ? paths.length === 0 : paths.length > 0,
    "profile requires a matching test selection",
  );
  for (const file of paths) {
    requireThat(
      /^[A-Za-z0-9._/-]+$/.test(file) &&
        file.split("/").every((part) => part && part !== "." && part !== ".."),
      "non-normalized test path",
    );
    requireThat(
      /^(apps\/(server|desktop|web|mobile)|packages\/[a-z0-9-]+)\/.+\.test\.tsx?$/.test(file),
      "unsupported test surface",
    );
    requireThat(
      !/^packages\/(source-control-testing|oxlint-plugin)\//.test(file),
      "unsupported test package",
    );
    requireThat(
      !(file.startsWith("apps/web/") && /\.browser\./i.test(file)),
      "web browser tests are outside the pilot",
    );
  }
  return {
    request_id: env.INPUT_REQUEST_ID,
    candidate_branch: label,
    candidate_sha_requested: env.INPUT_CANDIDATE_SHA,
    profile: env.INPUT_PROFILE,
    test_paths: paths,
  };
}

function git(root, args) {
  const result = spawnSync("git", args, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  });
  if (result.error || result.status !== 0) throw new Error(`git ${args[0]} unavailable`);
  return result.stdout.trimEnd();
}

function trackedFiles(root) {
  return new Map(
    git(root, ["ls-files", "--stage", "-z"])
      .split("\0")
      .filter(Boolean)
      .map((line) => {
        const match = /^(\d+) [0-9a-f]+ 0\t(.+)$/.exec(line);
        requireThat(match, "unsupported index entry");
        return [match[2], match[1]];
      }),
  );
}

function regularFile(root, file, tracked) {
  requireThat(
    ["100644", "100755"].includes(tracked.get(file)),
    `untracked or non-regular file: ${file}`,
  );
  let cursor = root;
  for (const segment of file.split("/")) {
    cursor = path.join(cursor, segment);
    requireThat(!fs.lstatSync(cursor).isSymbolicLink(), `symlink in path: ${file}`);
  }
  requireThat(
    fs.statSync(cursor).isFile() && fs.realpathSync(cursor) === path.join(root, file),
    `non-canonical file: ${file}`,
  );
  return cursor;
}

function buildGroups(root, paths, tracked, changedConfig = []) {
  root = fs.realpathSync(root);
  const groups = new Map();
  for (const file of paths) {
    regularFile(root, file, tracked);
    const pkg = file.split("/").slice(0, 2).join("/");
    requireThat(
      !changedConfig.some((p) => /^(vite|vitest)\.config\./.test(p) || p.startsWith(`${pkg}/`)),
      `test configuration changed for ${pkg}`,
    );
    const manifest = JSON.parse(
      fs.readFileSync(regularFile(root, `${pkg}/package.json`, tracked), "utf8"),
    );
    const expected = SPECIAL_SCRIPTS[pkg] || ORDINARY_SCRIPT;
    requireThat(manifest.scripts?.test === expected, `unsupported test script: ${pkg}`);
    const selector = file.slice(pkg.length + 1);
    requireThat(!selector.startsWith("-"), "test selector cannot be an option");
    // Vitest positional filters are case-insensitive substrings, not exact paths.
    const matches = [...tracked.keys()].filter(
      (p) =>
        p.startsWith(`${pkg}/`) &&
        /\.(test|spec)\.[^/]+$/i.test(p) &&
        p
          .slice(pkg.length + 1)
          .toLowerCase()
          .includes(selector.toLowerCase()),
    );
    requireThat(matches.length === 1 && matches[0] === file, `ambiguous test selector: ${file}`);
    if (!groups.has(pkg)) {
      const argv = ["vp", "test", "run"];
      if (!Object.hasOwn(SPECIAL_SCRIPTS, pkg))
        argv.push("--config", "../../vite.config.ts", "--dir", ".");
      if (pkg === "apps/web") argv.push("--project", "unit");
      groups.set(pkg, { check_id: `tests:${pkg}`, cwd: pkg, argv, files: [] });
    }
    const group = groups.get(pkg);
    group.files.push(file);
    group.argv.push(selector);
  }
  return [...groups.values()].sort((a, b) => a.cwd.localeCompare(b.cwd));
}

function inspectCandidate(root, request, defaultSha) {
  const actual = git(root, ["rev-parse", "HEAD"]);
  requireThat(actual === request.candidate_sha_requested, "candidate checkout mismatch");
  const identity = {
    candidate_sha_checked_out: actual,
    candidate_tree: git(root, ["rev-parse", "HEAD^{tree}"]),
    contains_default_head: "unknown",
    candidate_changes_precheck: "unknown",
  };
  const ancestor = spawnSync("git", ["merge-base", "--is-ancestor", defaultSha, "HEAD"], {
    cwd: root,
  });
  if (ancestor.status === 0 || ancestor.status === 1)
    identity.contains_default_head = ancestor.status === 0;
  try {
    const changed = git(root, [
      "diff",
      "--name-only",
      `${defaultSha}...HEAD`,
      "--",
      WORKFLOW,
      HELPER,
    ]).split("\n");
    identity.candidate_changes_precheck = changed.some(Boolean);
  } catch {
    /* Missing ancestry is unknown, never a clean comparison. */
  }
  const tracked = trackedFiles(root);
  let changedConfig = [];
  if (request.test_paths.length) {
    // Config edits need a new binding; do not infer runner semantics from a changed config.
    const changed = git(root, ["diff", "--name-only", defaultSha, "HEAD"]).split("\n");
    changedConfig = changed.filter((p) => /(^|\/)(vite|vitest)\.config\.[^/]+$/.test(p));
  }
  const groups = buildGroups(root, request.test_paths, tracked, changedConfig);
  return { ...identity, groups };
}

function testArgv(group, report) {
  // Vite+ fc287d7 cli/resolver.rs forwards test args unchanged to bundled Vitest.
  // Vitest 03630a5 cli-config.ts supports repeated reporters and outputFile.json;
  // reporters/json.ts writes absolute names and per-assertion statuses from this run.
  return [
    ...group.argv,
    "--passWithNoTests=false",
    "--reporter=default",
    "--reporter=json",
    `--outputFile.json=${report}`,
  ];
}

function assessReport(report, expected, cwd, exitCode) {
  if (exitCode === null)
    return { result: "unknown", selection_mismatch: true, reason: "test process did not complete" };
  let mismatch = true;
  let reason = "missing or invalid JSON report";
  let executed = false;
  try {
    requireThat(Array.isArray(report.testResults) && report.testResults.length > 0, reason);
    const names = report.testResults
      .map((row) => fs.realpathSync(path.resolve(cwd, row.name)))
      .sort();
    mismatch = JSON.stringify(names) !== JSON.stringify([...expected].sort());
    reason = mismatch
      ? "reported files differ from requested files"
      : "no executed assertions in one or more files";
    executed = report.testResults.every(
      (row) =>
        Array.isArray(row.assertionResults) &&
        row.assertionResults.some((a) => a.status === "passed" || a.status === "failed"),
    );
    if (!mismatch && executed) {
      const failed =
        report.success !== true ||
        report.testResults.some(
          (row) =>
            row.status !== "passed" || row.assertionResults.some((a) => a.status === "failed"),
        );
      return {
        result: exitCode !== 0 || failed ? "fail" : "pass",
        selection_mismatch: false,
        reason:
          exitCode !== 0 || failed
            ? "test invocation failed"
            : "exact files with executed assertions",
      };
    }
  } catch {
    /* Invalid reports cannot establish selection or execution. */
  }
  return {
    result: exitCode !== null && exitCode !== 0 ? "fail" : "unknown",
    selection_mismatch: mismatch,
    reason,
  };
}

function verifyRunner(root) {
  const fromRoot = createRequire(path.join(root, "package.json"));
  const vpPath = fromRoot.resolve("vite-plus/package.json");
  const vp = JSON.parse(fs.readFileSync(vpPath, "utf8"));
  const vitestPath = createRequire(vpPath).resolve("vitest/package.json");
  const vitest = JSON.parse(fs.readFileSync(vitestPath, "utf8"));
  requireThat(
    vp.version === "1.0.0" && vitest.version === "5.0.1",
    "unsupported installed Vite+/Vitest version",
  );
  return { vite_plus: vp.version, vitest: vitest.version };
}

function baseline(env) {
  return {
    request: "unknown",
    request_id: /^[a-z0-9][a-z0-9-]{7,63}$/.test(env.INPUT_REQUEST_ID || "")
      ? env.INPUT_REQUEST_ID
      : "invalid-request",
    profile: ["static", "tests", "static-and-tests"].includes(env.INPUT_PROFILE)
      ? env.INPUT_PROFILE
      : "unknown",
    test_paths: [],
    run_id: env.GITHUB_RUN_ID || null,
    run_attempt: env.GITHUB_RUN_ATTEMPT || null,
    run_url: `${env.GITHUB_SERVER_URL || "https://github.com"}/${env.GITHUB_REPOSITORY || "unknown"}/actions/runs/${env.GITHUB_RUN_ID || "unknown"}`,
    workflow_ref: env.GITHUB_WORKFLOW_REF || null,
    workflow_sha: env.GITHUB_WORKFLOW_SHA || null,
    workflow_path: WORKFLOW,
    candidate_sha_requested: /^[0-9a-f]{40}$/.test(env.INPUT_CANDIDATE_SHA || "")
      ? env.INPUT_CANDIDATE_SHA
      : null,
    candidate_sha_checked_out: null,
    candidate_tree: null,
    default_head_sha: env.GITHUB_SHA || null,
    contains_default_head: "unknown",
    candidate_changes_precheck: "unknown",
    precheck_change_behavior: "unknown",
    helper_sha256: crypto.createHash("sha256").update(fs.readFileSync(__filename)).digest("hex"),
    runner_image: env.ImageOS || null,
    runner_image_version: env.ImageVersion || null,
    setup_seconds: null,
    checks: [],
    not_covered: NOT_COVERED,
    advisory:
      "Candidate code shares this runner and can modify temporary evidence; this receipt is not a security attestation.",
  };
}

function expectedChecks(plan) {
  return [
    ...(plan.profile === "tests"
      ? []
      : Object.entries(STATIC).map(([check_id, command]) => ({
          check_id,
          argv: ["bash", "-euo", "pipefail", "-c", command],
        }))),
    ...plan.groups,
  ];
}

function finalize(base, plan, rows, setupOK) {
  if (!plan || plan.request !== "valid") return { ...base, checks: [], result: "unknown" };
  const checks = expectedChecks(plan).map((expected) => {
    const matches = rows.filter((row) => row.check_id === expected.check_id);
    return matches.length === 1
      ? matches[0]
      : {
          check_id: expected.check_id,
          argv: expected.argv,
          result: setupOK ? "unknown" : "unavailable",
          seconds: null,
          reason: matches.length ? "duplicate result" : "check did not finish",
        };
  });
  const result = !setupOK
    ? "unavailable"
    : checks.some((c) => c.result === "fail")
      ? "fail"
      : checks.length && checks.every((c) => c.result === "pass")
        ? "pass"
        : "unknown";
  return {
    ...base,
    ...plan,
    setup_seconds: rows.find((r) => Number.isFinite(r.setup_seconds))?.setup_seconds ?? null,
    checks,
    result,
  };
}

function main(command, env = process.env) {
  const dir = path.join(env.RUNNER_TEMP, "focused-precheck");
  fs.mkdirSync(dir, { recursive: true });
  const file = (name) => path.join(dir, name);
  const write = (name, value) =>
    fs.writeFileSync(file(name), JSON.stringify(value, null, 2) + "\n");
  const read = (name) => JSON.parse(fs.readFileSync(file(name), "utf8"));
  const output = (key, value) => {
    if (env.GITHUB_OUTPUT) fs.appendFileSync(env.GITHUB_OUTPUT, `${key}=${value}\n`);
  };
  const root = env.GITHUB_WORKSPACE || process.cwd();
  const append = (row, plan) =>
    fs.appendFileSync(
      file("results.jsonl"),
      JSON.stringify({
        ...row,
        setup_seconds: Math.max(0, (Date.now() - plan.setup_started_at) / 1000 - row.seconds),
      }) + "\n",
    );
  if (command === "validate") {
    const base = baseline(env);
    write("receipt.json", base);
    output(
      "artifact",
      `focused-precheck-${base.request_id}-a${/^\d+$/.test(env.GITHUB_RUN_ATTEMPT || "") ? env.GITHUB_RUN_ATTEMPT : "unknown"}`,
    );
    try {
      const request = validateRequest(env);
      write("request.json", request);
      output("candidate_sha", request.candidate_sha_requested);
      return 0;
    } catch (error) {
      write("receipt.json", { ...base, error: error.message });
      return 1;
    }
  }
  if (command === "plan") {
    try {
      const request = read("request.json");
      const plan = {
        ...request,
        ...inspectCandidate(root, request, env.GITHUB_SHA),
        request: "valid",
        setup_started_at: Date.now(),
      };
      write("plan.json", plan);
      output("static", plan.profile !== "tests");
      output("tests", plan.groups.length > 0);
      output(
        "server",
        plan.groups.some((group) => group.cwd === "apps/server"),
      );
      return 0;
    } catch (error) {
      write("receipt.json", { ...read("receipt.json"), error: error.message });
      return 1;
    }
  }
  if (command === "run-check") {
    const id = process.argv[3];
    requireThat(Object.hasOwn(STATIC, id), "unsupported static check");
    const plan = read("plan.json");
    requireThat(plan.request === "valid" && plan.profile !== "tests", "check is not in plan");
    const argv = ["bash", "-euo", "pipefail", "-c", STATIC[id]];
    const start = Date.now();
    const child = spawnSync(argv[0], argv.slice(1), { cwd: root, stdio: "inherit" });
    const result =
      child.error || child.status === null ? "unknown" : child.status === 0 ? "pass" : "fail";
    append({ check_id: id, argv, result, seconds: (Date.now() - start) / 1000 }, plan);
    return result === "pass" ? 0 : 1;
  }
  if (command === "run-tests") {
    const plan = read("plan.json");
    requireThat(plan.request === "valid" && plan.groups.length > 0, "empty test plan");
    let runner;
    try {
      runner = verifyRunner(root);
    } catch (error) {
      for (const group of plan.groups)
        append(
          {
            check_id: group.check_id,
            argv: group.argv,
            result: "unknown",
            seconds: 0,
            reason: error.message,
          },
          plan,
        );
      return 1;
    }
    let passed = true;
    for (const [index, group] of plan.groups.entries()) {
      const reportPath = file(`tests-${index}.json`);
      fs.rmSync(reportPath, { force: true });
      const argv = testArgv(group, reportPath);
      const cwd = path.join(root, group.cwd);
      const start = Date.now();
      const child = spawnSync(argv[0], argv.slice(1), { cwd, stdio: "inherit" });
      let report;
      try {
        report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
      } catch {
        /* Missing evidence stays unknown. */
      }
      const result = assessReport(
        report,
        group.files.map((p) => path.join(fs.realpathSync(root), p)),
        cwd,
        child.status,
      );
      append(
        {
          check_id: group.check_id,
          argv,
          cwd: group.cwd,
          ...result,
          runner,
          seconds: (Date.now() - start) / 1000,
        },
        plan,
      );
      passed &&= result.result === "pass";
    }
    return passed ? 0 : 1;
  }
  if (command === "finish") {
    let base = baseline(env),
      plan,
      rows = [];
    try {
      base = read("receipt.json");
    } catch {
      /* Bootstrap failure still gets an unknown receipt. */
    }
    try {
      plan = read("plan.json");
    } catch {
      /* Invalid requests never acquire check results. */
    }
    try {
      rows = fs
        .readFileSync(file("results.jsonl"), "utf8")
        .trim()
        .split("\n")
        .filter(Boolean)
        .map(JSON.parse);
    } catch {
      /* Partial writes are not complete results. */
    }
    const receipt = finalize(base, plan, rows, env.PRECHECK_SETUP_OK === "true");
    delete receipt.groups;
    delete receipt.setup_started_at;
    write("receipt.json", receipt);
    if (env.GITHUB_STEP_SUMMARY)
      fs.appendFileSync(
        env.GITHUB_STEP_SUMMARY,
        `### Focused precheck: ${receipt.result}\n\n${receipt.not_covered}\n\n\`\`\`json\n${JSON.stringify(receipt, null, 2)}\n\`\`\`\n`,
      );
    return receipt.result === "pass" ? 0 : 1;
  }
  throw new Error("unsupported helper command");
}

module.exports = {
  validateRequest,
  trackedFiles,
  regularFile,
  buildGroups,
  inspectCandidate,
  testArgv,
  assessReport,
  verifyRunner,
  baseline,
  expectedChecks,
  finalize,
  main,
};
if (require.main === module) {
  try {
    process.exitCode = main(process.argv[2]);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
