import * as NodeAssert from "node:assert/strict";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeVM from "node:vm";
import { childEnvironment, privateBootLauncher } from "./jones-thread-monitor-mac-smoke.mjs";
NodeTest.test(
  "native smoke child receives only private storage and an explicit environment allowlist",
  () => {
    const root = "/private/runner/jones-monitor-owned";
    const env = childEnvironment(root);
    NodeAssert.deepEqual(
      Object.keys(env).sort(),
      [
        "AGENT_CLI_CREDENTIAL_STORE",
        "HOME",
        "LANG",
        "OTEL_SDK_DISABLED",
        "PATH",
        "T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD",
        "T3CODE_DESKTOP_USER_DATA_DIR",
        "T3CODE_DEV_SEED",
        "T3CODE_DISABLE_AUTO_UPDATE",
        "T3CODE_HOME",
        "T3CODE_TELEMETRY_ENABLED",
        "TMPDIR",
        "TZ",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
      ].sort(),
    );
    for (const key of [
      "HOME",
      "TMPDIR",
      "XDG_CONFIG_HOME",
      "XDG_CACHE_HOME",
      "XDG_DATA_HOME",
      "T3CODE_HOME",
      "T3CODE_DESKTOP_USER_DATA_DIR",
    ]) {
      const relative = NodePath.relative(root, env[key]);
      NodeAssert.ok(relative && !relative.startsWith("..") && !NodePath.isAbsolute(relative), key);
    }
    NodeAssert.equal(env.T3CODE_DEV_SEED, "0");
    NodeAssert.equal(env.T3CODE_DISABLE_AUTO_UPDATE, "true");
    NodeAssert.equal(env.T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD, "false");
    NodeAssert.equal(env.AGENT_CLI_CREDENTIAL_STORE, "memory");
    NodeAssert.equal(env.GH_TOKEN, undefined);
    NodeAssert.equal(env.GITHUB_TOKEN, undefined);
    NodeAssert.equal(env.NODE_OPTIONS, undefined);
    NodeAssert.equal(env.SSH_AUTH_SOCK, undefined);
  },
);
NodeTest.test(
  "native smoke rejects relative state roots and keeps root configurations disjoint",
  () => {
    NodeAssert.throws(() => childEnvironment("relative-home"), /must be absolute/);
    const first = childEnvironment("/private/runner/smoke-a");
    const second = childEnvironment("/private/runner/smoke-b");
    for (const key of ["HOME", "TMPDIR", "T3CODE_HOME", "T3CODE_DESKTOP_USER_DATA_DIR"]) {
      NodeAssert.notEqual(first[key], second[key]);
    }
  },
);

NodeTest.test(
  "private Electron home and application path are set before compiled boot executes",
  () => {
    const boot = "/isolated/repo/apps/desktop/dist-electron/boot.cjs";
    const calls = [];
    NodeVM.runInNewContext(privateBootLauncher(boot), {
      process: { env: { HOME: "/isolated/smoke/home" } },
      require: (id) => {
        if (id === "electron")
          return {
            app: {
              setPath: (...args) => calls.push(["setPath", ...args]),
              setAppPath: (...args) => calls.push(["setAppPath", ...args]),
            },
          };
        calls.push(["boot", id]);
      },
    });
    NodeAssert.deepEqual(calls, [
      ["setPath", "home", "/isolated/smoke/home"],
      ["setAppPath", "/isolated/repo/apps/desktop/dist-electron"],
      ["boot", boot],
    ]);
    NodeAssert.throws(() => privateBootLauncher("relative/boot.cjs"), /must be absolute/);
  },
);
