import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeTest from "node:test";

const repository = NodePath.resolve(import.meta.dirname, "../../..");
const script = await NodeFSP.readFile(new URL("./anonymous-checkout.sh", import.meta.url), "utf8");
const sparse = "/*\n!/.repos/";
const workflowNames = ["ci", "jones-update-qualification", "native-enrollment-candidate"];
const steps = [];
for (const workflow of workflowNames) {
  const text = await NodeFSP.readFile(
    NodePath.join(repository, `.github/workflows/${workflow}.yml`),
    "utf8",
  );
  for (const step of text.split(/^      - /m).slice(1)) {
    if (!step.includes("JONES_CHECKOUT_DEPTH:")) continue;
    const run = step.match(/        run: \|\n((?:          [^\n]*\n)+)/);
    NodeAssert.ok(run);
    steps.push({
      workflow,
      step,
      run:
        run[1]
          .split("\n")
          .filter(Boolean)
          .map((line) => line.slice(10))
          .join("\n") + "\n",
    });
  }
}

NodeTest.test("all 22 bootstraps preserve merge/head, depth, history and sparse selection", () => {
  NodeAssert.equal(steps.length, 22);
  const expectedDepths = [2, 0, ...Array(15).fill(1), 0, 0, 0, 0, 1];
  for (const [index, item] of steps.entries()) {
    NodeAssert.equal(item.run, script);
    NodeAssert.match(item.step, /shell: bash/);
    NodeAssert.match(item.step, /GITHUB_REPOSITORY: Jones-Systems\/Jones-Code/);
    NodeAssert.match(item.step, new RegExp(`JONES_CHECKOUT_DEPTH: "${expectedDepths[index]}"`));
    NodeAssert.ok(
      item.step.includes(
        `JONES_CHECKOUT_SHA: ${index === 20 ? "${{ github.event.pull_request.head.sha || github.sha }}" : "${{ github.sha }}"}`,
      ),
    );
    if (index === 9 || index === 11) {
      NodeAssert.match(
        item.step,
        /JONES_CHECKOUT_SPARSE: \|\n            \/package.json\n            \/scripts\/jones\/server-shard-weights\//,
      );
    } else if (index === 16) {
      NodeAssert.match(item.step, /JONES_CHECKOUT_CONE: "true"/);
      NodeAssert.match(item.step, /JONES_CHECKOUT_SPARSE: \|\n            scripts\/jones\/ci/);
    } else {
      NodeAssert.match(item.step, /JONES_CHECKOUT_CONE: "false"/);
      NodeAssert.match(
        item.step,
        /JONES_CHECKOUT_SPARSE: \|\n            \/\*\n            !\/.repos\//,
      );
    }
  }
  for (const [workflow, count] of [
    ["ci", 17],
    ["jones-update-qualification", 4],
    ["native-enrollment-candidate", 1],
  ])
    NodeAssert.equal(steps.filter((item) => item.workflow === workflow).length, count);
});

const gitPath = NodeChildProcess.execFileSync("which", ["git"], { encoding: "utf8" }).trim();
async function fixture(t) {
  const scratch = NodePath.join(repository, "node_modules/.cache/jones/anonymous-checkout-tests");
  await NodeFSP.mkdir(scratch, { recursive: true });
  const root = await NodeFSP.mkdtemp(NodePath.join(scratch, "run-"));
  t.after(() => NodeFSP.rm(root, { recursive: true, force: true }));
  const source = NodePath.join(root, "source");
  const home = NodePath.join(root, "home");
  const bin = NodePath.join(root, "bin");
  const workspace = NodePath.join(root, "workspace");
  await Promise.all([source, home, bin, workspace].map((p) => NodeFSP.mkdir(p)));
  const env = {
    ...process.env,
    HOME: home,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_COUNT: "0",
    GIT_AUTHOR_NAME: "Fixture",
    GIT_AUTHOR_EMAIL: "fixture@example.invalid",
    GIT_COMMITTER_NAME: "Fixture",
    GIT_COMMITTER_EMAIL: "fixture@example.invalid",
  };
  delete env.GIT_CONFIG_PARAMETERS;
  const git = (...args) =>
    NodeChildProcess.execFileSync(gitPath, ["-C", source, ...args], {
      env,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  git("init", "--initial-branch=main");
  for (const [name, value] of Object.entries({
    "root.txt": "root",
    "package.json": "{}",
    "scripts/jones/ci/check.cjs": "module.exports={};",
    "scripts/jones/server-shard-weights/weights.json": "{}",
  })) {
    await NodeFSP.mkdir(NodePath.dirname(NodePath.join(source, name)), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(source, name), value);
  }
  git("add", ".");
  git("commit", "-m", "base");
  const base = git("rev-parse", "HEAD");
  for (const name of ["distilled", "floci"]) {
    git(
      "update-index",
      "--add",
      "--cacheinfo",
      `160000,${base},.repos/alchemy-effect/submodules/${name}`,
    );
    await NodeFSP.mkdir(NodePath.join(source, `.repos/alchemy-effect/submodules/${name}`), {
      recursive: true,
    });
  }
  git("commit", "-m", "unmapped gitlinks");
  const head = git("rev-parse", "HEAD");
  git("tag", "fixture-tag");
  await NodeFSP.writeFile(NodePath.join(source, "merge.txt"), "merge source");
  git("add", "merge.txt");
  git("commit", "-m", "merge-event candidate");
  const merge = git("rev-parse", "HEAD");
  const originalTree = git("write-tree");
  const remote = NodePath.join(root, "remote.git");
  NodeChildProcess.execFileSync(gitPath, ["clone", "--bare", source, remote], {
    env,
    stdio: "ignore",
  });
  const calls = NodePath.join(root, "calls.jsonl");
  const helperMarker = NodePath.join(root, "helper-ran");
  await NodeFSP.writeFile(
    NodePath.join(home, ".gitconfig"),
    `[credential]\n helper = !touch '${helperMarker}'\n`,
  );
  const wrapper = `#!${process.execPath}
import NodeFSP from 'node:fs';
import NodeChildProcess from 'node:child_process';
const args=process.argv.slice(2);
if(process.env.GIT_CONFIG_GLOBAL!=='/dev/null'||process.env.GIT_CONFIG_NOSYSTEM!=='1'||process.env.GIT_CONFIG_COUNT!=='0'||process.env.GIT_TERMINAL_PROMPT!=='0')process.exit(90);
NodeFSP.appendFileSync(${JSON.stringify(calls)},JSON.stringify(args)+'\\n');
if(args[0]==='remote'&&args[1]==='add'){
 if(args[3]!=='https://github.com/Jones-Systems/Jones-Code.git')process.exit(91);
 args[3]=${JSON.stringify(remote)};
}
if(process.env.FIXTURE_FAILURE==='fetch'&&args.includes('fetch'))process.exit(41);
if(process.env.FIXTURE_FAILURE==='checkout'&&args[0]==='checkout')process.exit(42);
const result=NodeChildProcess.spawnSync(${JSON.stringify(gitPath)},args,{stdio:'inherit'});
process.exit(result.status??99);
`;
  await NodeFSP.writeFile(NodePath.join(bin, "git"), wrapper, { mode: 0o700 });
  return {
    root,
    source,
    home,
    workspace,
    calls,
    helperMarker,
    git,
    base,
    head,
    merge,
    originalTree,
    env: {
      ...env,
      PATH: `${bin}:${process.env.PATH}`,
      GITHUB_WORKSPACE: workspace,
      GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
      JONES_CHECKOUT_CONE: "false",
      JONES_CHECKOUT_DEPTH: "1",
      JONES_CHECKOUT_SPARSE: sparse,
    },
  };
}

for (const scenario of [
  "shallow merge",
  "depth two",
  "full history",
  "preview head",
  "cone",
  "weights",
  "fetch failure",
  "checkout failure",
]) {
  NodeTest.test(`anonymous checkout with unmapped gitlinks: ${scenario}`, async (t) => {
    const f = await fixture(t);
    const depth = scenario === "full history" ? "0" : scenario === "depth two" ? "2" : "1";
    const sha = scenario === "preview head" ? f.head : f.merge;
    const failure =
      scenario === "fetch failure" ? "fetch" : scenario === "checkout failure" ? "checkout" : "";
    const result = NodeChildProcess.spawnSync("bash", ["-c", script], {
      cwd: f.workspace,
      env: {
        ...f.env,
        JONES_CHECKOUT_SHA: sha,
        JONES_CHECKOUT_DEPTH: depth,
        JONES_CHECKOUT_CONE: scenario === "cone" ? "true" : "false",
        JONES_CHECKOUT_SPARSE:
          scenario === "cone"
            ? "scripts/jones/ci"
            : scenario === "weights"
              ? "/package.json\n/scripts/jones/server-shard-weights/"
              : sparse,
        FIXTURE_FAILURE: failure,
        GIT_CONFIG_GLOBAL: NodePath.join(f.home, ".gitconfig"),
        GIT_CONFIG_COUNT: "1",
        GIT_CONFIG_KEY_0: "credential.helper",
        GIT_CONFIG_VALUE_0: "!exit 89",
      },
      encoding: "utf8",
      maxBuffer: 256 * 1024,
      timeout: 20000,
    });
    NodeAssert.equal(result.signal, null);
    NodeAssert.equal(
      result.status,
      failure === "fetch" ? 41 : failure === "checkout" ? 42 : 0,
      result.stderr,
    );
    const calls = (await NodeFSP.readFile(f.calls, "utf8")).trim().split("\n").map(JSON.parse);
    NodeAssert.equal(calls.filter((args) => args.includes("fetch")).length, 1);
    NodeAssert.equal(
      calls.filter((args) => args[0] === "checkout").length,
      failure === "fetch" ? 0 : 1,
    );
    const local = (...args) =>
      NodeChildProcess.spawnSync(gitPath, ["-C", f.workspace, ...args], {
        env: f.env,
        encoding: "utf8",
      });
    // Only config key names and file existence are observed, never credential values.
    const credentials = local(
      "config",
      "--local",
      "--name-only",
      "--get-regexp",
      "^(credential\\.|http\\..*extraheader)",
    );
    NodeAssert.equal(credentials.status, 1);
    NodeAssert.equal(credentials.stdout, "");
    for (const filename of [
      f.helperMarker,
      NodePath.join(f.workspace, ".git-credentials"),
      NodePath.join(f.home, ".git-credentials"),
      NodePath.join(f.workspace, ".git/.credentials"),
    ])
      await NodeAssert.rejects(NodeFSP.stat(filename), { code: "ENOENT" });
    NodeAssert.equal(f.git("write-tree"), f.originalTree);
    NodeAssert.equal(f.git("status", "--porcelain"), "");
    if (failure) return;
    NodeAssert.equal(local("rev-parse", "HEAD").stdout.trim(), sha);
    NodeAssert.equal(
      local("rev-parse", "HEAD^{tree}").stdout.trim(),
      f.git("rev-parse", `${sha}^{tree}`),
    );
    NodeAssert.match(
      local("ls-files", "--stage").stdout,
      /160000 [a-f0-9]{40} 0\t.repos\/alchemy-effect\/submodules\/distilled/,
    );
    NodeAssert.match(
      local("ls-files", "--stage").stdout,
      /160000 [a-f0-9]{40} 0\t.repos\/alchemy-effect\/submodules\/floci/,
    );
    await NodeAssert.rejects(NodeFSP.stat(NodePath.join(f.workspace, ".repos")), {
      code: "ENOENT",
    });
    NodeAssert.equal(
      local("rev-list", "--count", "HEAD").stdout.trim(),
      depth === "0" ? "3" : depth,
    );
    if (depth === "0") {
      NodeAssert.equal(local("rev-parse", "origin/main").stdout.trim(), f.merge);
      NodeAssert.equal(local("rev-parse", "fixture-tag").stdout.trim(), f.head);
    }
    if (scenario === "cone")
      await NodeFSP.stat(NodePath.join(f.workspace, "scripts/jones/ci/check.cjs"));
    if (scenario === "weights") {
      await NodeFSP.stat(
        NodePath.join(f.workspace, "scripts/jones/server-shard-weights/weights.json"),
      );
      await NodeAssert.rejects(
        NodeFSP.stat(NodePath.join(f.workspace, "scripts/jones/ci/check.cjs")),
        {
          code: "ENOENT",
        },
      );
    }
  });
}

NodeTest.test(
  "refuses a foreign repository, invalid ref and occupied workspace before Git",
  async (t) => {
    for (const scenario of ["foreign", "ref", "occupied"]) {
      const f = await fixture(t);
      const marker = NodePath.join(f.workspace, "owned-by-another-task");
      if (scenario === "occupied") await NodeFSP.writeFile(marker, "preserve");
      const result = NodeChildProcess.spawnSync("bash", ["-c", script], {
        cwd: f.workspace,
        env: {
          ...f.env,
          GITHUB_REPOSITORY: scenario === "foreign" ? "Other/Repository" : f.env.GITHUB_REPOSITORY,
          JONES_CHECKOUT_SHA: scenario === "ref" ? "main; exit 0" : f.merge,
        },
        encoding: "utf8",
        timeout: 20000,
      });
      NodeAssert.notEqual(result.status, 0);
      await NodeAssert.rejects(NodeFSP.stat(f.calls), { code: "ENOENT" });
      await NodeAssert.rejects(NodeFSP.stat(NodePath.join(f.workspace, ".git")), {
        code: "ENOENT",
      });
      if (scenario === "occupied")
        NodeAssert.equal(await NodeFSP.readFile(marker, "utf8"), "preserve");
    }
  },
);
