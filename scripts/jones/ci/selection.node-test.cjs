const assert = require("node:assert/strict");
const { test } = require("node:test");
const { readWorkspace } = require("../../../.github/scripts/ci-changes.cjs");
const { changedPaths, packageBatch, select } = require("./linux-changes.cjs");
const { selectPackages } = require("./run-packages.cjs");

const packages = readWorkspace();
const event = {
  repository: { full_name: "Jones-Systems/Jones-Code", default_branch: "main" },
  pull_request: {
    number: 12,
    base: { sha: "a".repeat(40), ref: "main" },
    head: { sha: "b".repeat(40) },
  },
};

test("docs-only changes skip package work; all unknown/global/manifest changes select the full graph", () => {
  const docs = select(["docs/operations/development.md", "README.md"], packages);
  for (const job of [
    "typecheck",
    "build",
    "test_web",
    "test_server",
    "test_desktop",
    "test_mobile",
    "test_client_runtime",
    "rust",
    "release_smoke",
  ])
    assert.equal(docs[job], false, job);
  assert.equal(docs.test_packages, "[]");
  const full = select(null, packages);
  for (const file of [
    "unrecognized/file.ts",
    "package.json",
    "pnpm-lock.yaml",
    "vite.config.ts",
    "apps/web/package.json",
    "packages/shared/src/HostProcess.ts",
    "apps/mobile/deps/example.tgz",
    ".github/workflows/ci.yml",
    "scripts/jones/ci/linux-changes.cjs",
    "scripts/jones/server-shard-weights/select.ts",
    "scripts/jones/ci-test-progress-reporter.ts",
  ]) {
    assert.deepEqual(select([file], packages), full, file);
  }
});

test("package selection follows imported dependency and test-only edges", () => {
  const web = select(["apps/web/src/main.tsx"], packages);
  assert.equal(web.test_web, true);
  assert.equal(web.test_server, false);
  assert.equal(web.test_desktop, false);
  assert.equal(web.build, true);
  assert.ok(JSON.parse(web.test_packages).includes("@t3tools/scripts"));
  const shared = select(["packages/shared/src/example.ts"], packages);
  for (const job of [
    "test_web",
    "test_server",
    "test_desktop",
    "test_mobile",
    "test_client_runtime",
  ])
    assert.equal(shared[job], true, job);
  const rust = select(["native/resource-monitor/src/lib.rs"], packages);
  assert.equal(rust.rust, true);
  assert.ok(JSON.parse(rust.test_packages).includes("@t3tools/scripts"));
});

test("the split and package batch partition every previously enrolled workspace test", () => {
  const batch = packageBatch(packages);
  const split = [
    "t3",
    "@t3tools/web",
    "@t3tools/desktop",
    "@t3tools/mobile",
    "@t3tools/client-runtime",
  ];
  const all = packages
    .filter((pkg) => pkg.hasTest && pkg.name !== "@t3tools/monorepo")
    .map((pkg) => pkg.name);
  assert.equal(new Set([...batch, ...split]).size, all.length);
  assert.deepEqual([...batch, ...split].sort(), all.sort());
  assert.deepEqual(JSON.parse(select(null, packages).test_packages), batch);
});

test("exact PR diffs include both rename sides without line-oriented filename loss", () => {
  const calls = [];
  const result = changedPaths("pull_request", event, (command, args) => {
    calls.push([command, args]);
    return command === "gh"
      ? JSON.stringify(event.pull_request)
      : "apps/web/src/deleted.ts\0docs/moved file.md\0";
  });
  assert.deepEqual(result.files, ["apps/web/src/deleted.ts", "docs/moved file.md"]);
  assert.deepEqual(calls[1], [
    "git",
    [
      "diff",
      "--no-renames",
      "--name-only",
      "-z",
      event.pull_request.base.sha,
      event.pull_request.head.sha,
      "--",
    ],
  ]);
  assert.equal(select(result.files, packages).test_web, true);
});

test("push, manual, and missing-event runs always select full Linux qualification", () => {
  for (const name of ["push", "workflow_dispatch", undefined]) {
    assert.equal(
      changedPaths(name, event, () => {
        throw new Error("must not inspect a narrow diff");
      }).files,
      null,
    );
  }
});

test("stack membership, non-main bases, stale metadata and API failures cannot narrow qualification", () => {
  const cases = [
    { metadata: { ...event.pull_request, stack: { base: { ref: "main" } } } },
    {
      event: {
        ...event,
        pull_request: {
          ...event.pull_request,
          base: { ...event.pull_request.base, ref: "lower-layer" },
        },
      },
      metadata: { ...event.pull_request, base: { ...event.pull_request.base, ref: "lower-layer" } },
    },
    { metadata: { ...event.pull_request, head: { sha: "c".repeat(40) } } },
    { metadata: {} },
    { error: true },
  ];
  for (const scenario of cases) {
    const result = changedPaths("pull_request", scenario.event ?? event, (command) => {
      assert.equal(command, "gh");
      if (scenario.error) throw new Error("API unavailable");
      return JSON.stringify(scenario.metadata);
    });
    assert.equal(result.files, null);
  }
});

test("unavailable commits, malformed events and truncated or ambiguous diffs run full", () => {
  assert.equal(
    changedPaths("pull_request", {}, () => {
      throw new Error("unexpected execution");
    }).files,
    null,
  );
  for (const paths of [null, "apps/web/src/a.ts", "docs/line\nbreak.md\0", "../escape\0"]) {
    const result = changedPaths("pull_request", event, (command) => {
      if (command === "gh") return JSON.stringify(event.pull_request);
      if (paths === null) throw new Error("missing commit");
      return paths;
    });
    assert.equal(result.files, null);
  }
});

test("package execution accepts known selected names, and missing/malformed outputs run the full batch", () => {
  const available = packageBatch(packages);
  assert.deepEqual(selectPackages("[]", available), []);
  assert.deepEqual(selectPackages(JSON.stringify([available[0]]), available), [available[0]]);
  for (const input of [undefined, "", "broken", "null", '["--unsafe"]', '["t3"]'])
    assert.deepEqual(selectPackages(input, available), available);
});
