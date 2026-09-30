import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import { sandboxEnvironment, bwrapArguments } from "./sandbox.mjs";
import { parseOptions } from "../ui-evidence.mjs";

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
