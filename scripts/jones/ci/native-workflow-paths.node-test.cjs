const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs/promises");
const path = require("node:path");
const os = require("node:os");
const { execFileSync } = require("node:child_process");
const { parseDiff, selectNative, changedPaths } = require("./native-workflow-paths.cjs");

const diff = (...fields) => Buffer.from(fields.join("\0") + "\0");
const select = (bytes) => selectNative({ eventName: "pull_request", labelled: true, diff: bytes });

test("the labelled selector preserves every old root and covers the exercised provider, plan and composer families", () => {
  for (const file of [
    ".github/workflows/jones-native-workflows.yml",
    "apps/desktop/scripts/ui-evidence/runner.mjs",
    "apps/web/src/components/Sidebar.tsx",
    "apps/web/src/components/sidebar/row.tsx",
    "apps/web/src/jones/workstreams/row.tsx",
    "apps/web/src/state/store.ts",
    "apps/web/src/lib/api.ts",
    "apps/web/src/lib/api/transport.ts",
    "apps/server/src/jones/workstreams/service.ts",
    "apps/server/src/orchestration-v2/EffectWorker.ts",
    "apps/server/src/auth/service.ts",
    "apps/server/src/ws.ts",
    "apps/server/src/ws/handler.ts",
    "apps/server/src/persistence/rows.ts",
    "scripts/jones/e2e/acp-stub/agent.mjs",
    "packages/client-runtime/src/operations.ts",
    "packages/contracts/src/rpc.ts",
    "packages/shared/src/value.ts",
    "scripts/lib/fixture.ts",
    "native/browser-secret/src/lib.rs",
    "apps/server/package.json",
    "apps/server/vite.config.ts",
    "apps/web/package.json",
    "apps/web/vite.config.ts",
    "package.json",
    "pnpm-lock.yaml",
    "pnpm-workspace.yaml",
    "vite.config.ts",
    "patches/provider.patch",
    "apps/server/src/jones/provider/claude/retire.ts",
    "apps/server/src/jones/threadConfiguration/store.ts",
    "apps/server/src/provider/registry.ts",
    "packages/provider-codex/src/server/adapter.ts",
    "packages/provider-claude/src/server/adapter.ts",
    "packages/effect-codex-app-server/src/client.ts",
    "apps/web/src/jones/threadConfiguration/send.ts",
    "apps/web/src/jones/pendingSends/controller.ts",
    "apps/web/src/jones/threadMonitor/actions.ts",
    "apps/web/src/jones/threadMonitor/planFollowUp.ts",
    "apps/web/src/jones/threadMonitor/MonitorController.tsx",
    "apps/web/src/components/ChatView.tsx",
    "apps/web/src/components/ChatView.browser.test.tsx",
    "apps/web/src/components/chat/Composer.tsx",
    "apps/web/src/composerDraftStore.ts",
    "apps/web/src/composerDraftStore.test.ts",
    "scripts/jones/ci/native-workflow-paths.cjs",
    "scripts/jones/ci/native-workflow-evidence.mjs",
    "scripts/jones/ci/native-workflow-paths.node-test.cjs",
  ])
    assert.equal(select(diff("M", file)), true, file);
  for (const file of [
    "docs/native.md",
    "README.md",
    "apps/marketing/src/index.tsx",
    "apps/server/src/jones/unrelated/service.ts",
    "apps/web/src/jones/unrelated/view.tsx",
    "native/resource-monitor/src/lib.rs",
    "scripts/unrelated.mjs",
  ])
    assert.equal(select(diff("M", file)), false, file);
});

test("NUL records retain both rename sides, deletion, spaces and newlines without an API path cap", () => {
  const renamedOut = diff("R100", "apps/web/src/components/chat/old file.tsx", "docs/old file.tsx");
  const renamedIn = diff(
    "R098",
    "docs/new\nfile.tsx",
    "apps/server/src/jones/provider/new\nfile.ts",
  );
  assert.deepEqual(parseDiff(renamedOut), [
    "apps/web/src/components/chat/old file.tsx",
    "docs/old file.tsx",
  ]);
  assert.equal(select(renamedOut), true);
  assert.equal(select(renamedIn), true);
  assert.equal(select(diff("D", "apps/server/src/jones/threadConfiguration/old file.ts")), true);
  const many = Array.from({ length: 120 }, (_, i) => ["M", `docs/change ${i}.md`]).flat();
  assert.equal(select(diff(...many, "D", "packages/provider-cursor/src/server/adapter.ts")), true);
});

test("unavailable, malformed and truncated diffs fail safe only after explicit label admission", () => {
  for (const bytes of [
    null,
    "not bytes",
    Buffer.from("M\0docs/a"),
    diff("bogus", "docs/a"),
    diff("R100", "docs/a"),
    diff("M", "../escape"),
    Buffer.from([77, 0, 255, 0]),
  ]) {
    assert.equal(select(bytes), true);
    assert.equal(selectNative({ eventName: "pull_request", labelled: false, diff: bytes }), false);
  }
  assert.equal(select(Buffer.alloc(0)), false);
  for (const eventName of ["schedule", "workflow_dispatch"])
    assert.equal(selectNative({ eventName, labelled: false, diff: null }), true);
  const calls = [];
  assert.equal(
    changedPaths("a".repeat(40), "b".repeat(40), (command, args) => {
      calls.push({ command, args });
      throw new Error("unreadable Git object");
    }),
    null,
  );
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].args, [
    "diff",
    "--name-status",
    "-z",
    "--find-renames",
    `${"a".repeat(40)}...${"b".repeat(40)}`,
    "--",
  ]);
  assert.equal(
    changedPaths("invalid", "b".repeat(40), () => assert.fail("invalid ref executed")),
    null,
  );
});

test("the actual YAML selector loads its helper from the validated candidate object without a checkout or dependencies", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "native-workflow-selection-"));
  try {
    const source = path.join(root, "source");
    const workspace = path.join(root, "objects-only");
    await fs.mkdir(source);
    await fs.mkdir(workspace);
    const gitEnv = {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_CONFIG_COUNT: "0",
      GIT_TERMINAL_PROMPT: "0",
      GIT_ASKPASS: "/bin/false",
    };
    for (const key of ["GIT_CONFIG_PARAMETERS", "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"])
      delete gitEnv[key];
    const git = (...args) =>
      execFileSync("git", args, { cwd: source, encoding: "utf8", env: gitEnv }).trim();
    git("init", "-q", "-b", "native-selector-fixture");
    const helper = "scripts/jones/ci/native-workflow-paths.cjs";
    await fs.mkdir(path.join(source, path.dirname(helper)), { recursive: true });
    await fs.copyFile(path.join(__dirname, "native-workflow-paths.cjs"), path.join(source, helper));
    await fs.writeFile(path.join(source, "README.md"), "fixture");
    const commit = () => {
      git("add", ".");
      git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-qm",
        "Synthetic candidate",
      );
      return git("rev-parse", "HEAD");
    };
    const base = commit();
    await fs.mkdir(path.join(source, "apps/server/src/jones/provider"), { recursive: true });
    await fs.writeFile(
      path.join(source, "apps/server/src/jones/provider/space file.ts"),
      "fixture",
    );
    const head = commit();
    execFileSync("git", ["init", "-q", "."], { cwd: workspace, env: gitEnv });
    execFileSync(
      "git",
      ["fetch", "--no-tags", source, `${base}:refs/heads/base`, `${head}:refs/heads/candidate`],
      { cwd: workspace, env: gitEnv, stdio: "pipe" },
    );
    // No checkout: the only available helper is the blob at HEAD_SHA.
    await assert.rejects(fs.lstat(path.join(workspace, helper)), { code: "ENOENT" });
    const yaml = await fs.readFile(
      path.join(__dirname, "../../../.github/workflows/jones-native-workflows.yml"),
      "utf8",
    );
    const step = yaml
      .split("      - name: Select native workflow paths\n")[1]
      .split("\n  # This dedicated")[0];
    const script = step
      .split("        run: |\n")[1]
      .split("\n")
      .map((line) => line.replace(/^          /, ""))
      .join("\n");
    assert.ok(script.includes('git show "$HEAD_SHA:scripts/jones/ci/native-workflow-paths.cjs"'));
    assert.ok(!script.includes("setup-vp") && !script.includes("git checkout"));
    const invoke = async (candidate, event = "pull_request", labelled = "true") => {
      const output = path.join(root, `output-${candidate}-${event}-${labelled}`);
      execFileSync("bash", ["-e", "-c", script], {
        cwd: workspace,
        env: {
          ...gitEnv,
          RUNNER_TEMP: root,
          GITHUB_WORKSPACE: workspace,
          GITHUB_OUTPUT: output,
          BASE_SHA: base,
          HEAD_SHA: candidate,
          EVENT_NAME: event,
          NATIVE_LABELLED: labelled,
        },
        stdio: "pipe",
      });
      return fs.readFile(output, "utf8");
    };
    assert.equal(await invoke(head), "changed=true\n");
    assert.equal(await invoke(base), "changed=false\n");
    assert.equal(await invoke(head, "pull_request", "false"), "changed=false\n");
    assert.equal(await invoke(head, "workflow_dispatch"), "changed=true\n");
    assert.equal(await invoke("0".repeat(40)), "changed=true\n");
    git("rm", "-q", helper);
    const missingHelper = commit();
    execFileSync("git", ["fetch", "--no-tags", source, missingHelper], {
      cwd: workspace,
      env: gitEnv,
      stdio: "pipe",
    });
    assert.equal(await invoke(missingHelper), "changed=true\n");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
