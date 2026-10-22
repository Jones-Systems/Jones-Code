import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import { sandboxEnvironment, bwrapArguments, stageAcpAgent, stageRun } from "./sandbox.mjs";
import { parseOptions, queueCaseConfiguration, harness } from "../ui-evidence.mjs";
import { resolveQueueCase } from "./scenarios/queue-edit-cancel.mjs";

NodeTest.test(
  "containment has private namespaces, a cleared environment and narrow mounts",
  async () => {
    const argv = await bwrapArguments({
      stage: "/owned/stage",
      runtime: { directory: "/owned/electron" },
      artifacts: "/owned/artifacts",
    });
    for (const flag of [
      "--unshare-user",
      "--unshare-pid",
      "--unshare-ipc",
      "--unshare-net",
      "--unshare-uts",
      "--die-with-parent",
      "--new-session",
      "--clearenv",
    ])
      NodeAssert.ok(argv.includes(flag));
    NodeAssert.equal(
      argv.filter((value) => value === "--setenv").length,
      Object.keys(sandboxEnvironment()).length,
    );
    NodeAssert.ok(
      !argv.some(
        (value) =>
          value.includes("/home/") ||
          value.includes(".git") ||
          value.includes(".env") ||
          value.includes(".t3") ||
          value.includes(".config"),
      ),
    );
    NodeAssert.ok(!argv.includes("--no-sandbox"));
    NodeAssert.deepEqual(argv.slice(-3), ["/usr/bin/node", "/harness/runner.mjs", "--inner"]);
    const env = sandboxEnvironment();
    NodeAssert.equal(env.HOME, "/scratch/home");
    NodeAssert.equal(env.T3CODE_HOME, "/scratch/t3home");
    NodeAssert.equal(env.T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD, "false");
    for (const key of [
      "DISPLAY",
      "DBUS_SESSION_BUS_ADDRESS",
      "SSH_AUTH_SOCK",
      "T3CODE_DESKTOP_DEV",
      "T3CODE_PACK_EXE",
    ])
      NodeAssert.equal(env[key], undefined);
  },
);
NodeTest.test("CLI rejects malformed options before any work", () => {
  NodeAssert.throws(() => parseOptions(["run", "--theme", "unknown"]));
  NodeAssert.throws(() => parseOptions(["run", "--size", "200x800"]));
  NodeAssert.throws(() => parseOptions(["run", "--scale", "NaN"]));
  NodeAssert.throws(() => parseOptions(["run", "--out"]));
  NodeAssert.throws(() => parseOptions(["run", "--source", "/one", "--source", "/two"]));
  NodeAssert.equal(parseOptions(["run"]).theme, "system");
});

async function acpStageFixture(t) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-acp-stage-"));
  t.after(() => NodeFSP.rm(root, { recursive: true, force: false }));
  const harness = NodePath.join(root, "harness");
  const directory = NodePath.join(harness, "scripts/jones/e2e/acp-stub");
  await NodeFSP.mkdir(directory, { recursive: true });
  return { root, harness, directory, destination: NodePath.join(root, "staged-agent.mjs") };
}

NodeTest.test("old harness revisions do not require an ACP fixture", async (t) => {
  const fixture = await acpStageFixture(t);
  NodeAssert.equal(await stageAcpAgent(fixture.harness, fixture.destination), false);
  await NodeAssert.rejects(NodeFSP.lstat(fixture.destination), { code: "ENOENT" });
});

NodeTest.test(
  "ACP staging copies only the exact agent without overwriting an existing file",
  async (t) => {
    const fixture = await acpStageFixture(t);
    const bytes = "process.stdout.write('owned ACP fixture');\n";
    await NodeFSP.writeFile(NodePath.join(fixture.directory, "agent.mjs"), bytes);
    await NodeFSP.writeFile(NodePath.join(fixture.directory, "unrelated.mjs"), "not staged");
    NodeAssert.equal(await stageAcpAgent(fixture.harness, fixture.destination), true);
    NodeAssert.equal(await NodeFSP.readFile(fixture.destination, "utf8"), bytes);
    await NodeAssert.rejects(stageAcpAgent(fixture.harness, fixture.destination), {
      code: "EEXIST",
    });
  },
);

NodeTest.test("ACP staging refuses file symlinks and parent-directory escapes", async (t) => {
  const fixture = await acpStageFixture(t);
  const outside = NodePath.join(fixture.root, "outside");
  await NodeFSP.mkdir(outside);
  await NodeFSP.writeFile(NodePath.join(outside, "agent.mjs"), "not a harness agent");
  const source = NodePath.join(fixture.directory, "agent.mjs");
  await NodeFSP.symlink(NodePath.join(outside, "agent.mjs"), source);
  await NodeAssert.rejects(
    stageAcpAgent(fixture.harness, fixture.destination),
    /contained regular file/,
  );
  await NodeFSP.unlink(source);
  await NodeFSP.rmdir(fixture.directory);
  await NodeFSP.symlink(outside, fixture.directory);
  await NodeAssert.rejects(
    stageAcpAgent(fixture.harness, fixture.destination),
    /contained regular file/,
  );
  await NodeAssert.rejects(NodeFSP.lstat(fixture.destination), { code: "ENOENT" });
});

NodeTest.test("queue CLI accepts only three cases on the canonical real scenario", async () => {
  const scenario = NodePath.join(
    await NodeFSP.realpath(harness),
    "apps/desktop/scripts/ui-evidence/scenarios/queue-edit-cancel.mjs",
  );
  for (const queueCase of ["workflow", "gate-only", "no-edit-negative"]) {
    const options = parseOptions(["run", "--scenario", scenario, "--queue-case", queueCase]);
    NodeAssert.deepEqual(await queueCaseConfiguration(options), { queueCase });
  }
  NodeAssert.deepEqual(await queueCaseConfiguration(parseOptions(["run"])), {});
  NodeAssert.throws(() => parseOptions(["run", "--queue-case", "unknown"]), /Queue case/);
  for (const command of ["doctor", "setup", "cleanup"])
    NodeAssert.throws(() => parseOptions([command, "--queue-case", "workflow"]), /Invalid option/);
  for (const path of [
    undefined,
    "queue-edit-cancel",
    scenario.replace("queue-edit-cancel", "sidebar-pin-restart"),
    scenario.replace("/scenarios/", "/scenarios/../scenarios/"),
  ])
    await NodeAssert.rejects(
      queueCaseConfiguration({ command: "run", scenario: path, "queue-case": "gate-only" }),
      /canonical real/,
    );
});

NodeTest.test(
  "queue CLI refuses aliases and wrong scenarios before allocating scratch",
  async (t) => {
    const fixture = await acpStageFixture(t);
    const scenario = NodePath.join(
      await NodeFSP.realpath(harness),
      "apps/desktop/scripts/ui-evidence/scenarios/queue-edit-cancel.mjs",
    );
    const alias = NodePath.join(fixture.root, "queue-alias.mjs");
    await NodeFSP.symlink(scenario, alias);
    const state = NodePath.join(fixture.root, "state");
    for (const selected of [alias, scenario.replace("queue-edit-cancel", "sidebar-pin-restart")]) {
      const child = NodeChildProcess.spawnSync(
        process.execPath,
        [
          NodePath.join(harness, "apps/desktop/scripts/ui-evidence.mjs"),
          "run",
          "--scenario",
          selected,
          "--queue-case",
          "gate-only",
        ],
        {
          env: {
            HOME: fixture.root,
            XDG_STATE_HOME: state,
            TMPDIR: fixture.root,
            PATH: NodePath.dirname(process.execPath),
          },
          encoding: "utf8",
          timeout: 3000,
        },
      );
      NodeAssert.equal(child.status, 2);
      NodeAssert.equal(child.signal, null);
      const receipt = JSON.parse(child.stdout);
      NodeAssert.equal(receipt.outcome, "rejected");
      NodeAssert.match(receipt.error, /canonical real/);
      await NodeAssert.rejects(NodeFSP.access(state), { code: "ENOENT" });
    }
  },
);

NodeTest.test(
  "closed queue configuration survives real staging and selects the matching case",
  async (t) => {
    const fixture = await acpStageFixture(t);
    const source = NodePath.join(fixture.root, "source");
    for (const directory of ["apps/desktop/dist-electron", "apps/server/dist"])
      await NodeFSP.mkdir(NodePath.join(source, directory), { recursive: true });
    for (const file of ["package.json", "apps/desktop/package.json", "apps/server/package.json"])
      await NodeFSP.writeFile(NodePath.join(source, file), "{}\n");
    const scenario = NodePath.join(
      await NodeFSP.realpath(harness),
      "apps/desktop/scripts/ui-evidence/scenarios/queue-edit-cancel.mjs",
    );
    for (const queueCase of ["workflow", "gate-only", "no-edit-negative"]) {
      const configuration = await queueCaseConfiguration(
        parseOptions(["run", "--scenario", scenario, "--queue-case", queueCase]),
      );
      const stage = await stageRun({
        run: { root: NodePath.join(fixture.root, `run-${queueCase}`) },
        source,
        harness,
        scenario,
        closure: { packages: [], roots: [] },
        configuration,
      });
      const staged = JSON.parse(
        await NodeFSP.readFile(NodePath.join(stage, "harness/config.json"), "utf8"),
      );
      NodeAssert.deepEqual(staged, { queueCase });
      NodeAssert.equal(resolveQueueCase(staged), queueCase);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(stage, "harness/scenario.mjs"), "utf8"),
        await NodeFSP.readFile(scenario, "utf8"),
      );
    }
  },
);
