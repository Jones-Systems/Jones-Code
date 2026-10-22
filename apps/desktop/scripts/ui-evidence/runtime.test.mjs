import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  checksumFor,
  runtimeExternalDependencies,
  captureApplicationProcesses,
  assertApplicationStopped,
  closeApplicationGeneration,
  assertRestartIdentity,
} from "./runtime.mjs";

NodeTest.test("official checksum accepts binary and text filename markers", () => {
  const digest = "ad71063e77da29a647f872b090478f9fa9ccffd67de7b2ae6315409cf1f13436";
  for (const marker of [" ", "*"])
    NodeAssert.equal(
      checksumFor(
        `${digest} ${marker}electron-v44.4.2-linux-x64.zip\n`,
        "electron-v44.4.2-linux-x64.zip",
      ),
      digest,
    );
  NodeAssert.throws(() =>
    checksumFor(`${digest} *different.zip`, "electron-v44.4.2-linux-x64.zip"),
  );
  NodeAssert.throws(() => checksumFor("invalid *electron.zip", "electron.zip"));
});

NodeTest.test(
  "runtime closure uses nightly canonical prefix and exact-boundary policies",
  async () => {
    const source = NodePath.resolve(
      NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
      "../../../..",
    );
    NodeAssert.deepEqual(
      await runtimeExternalDependencies(source, "server", {
        "node-pty": "1",
        "@cursor/sdk": "1",
        zod: "1",
        "zod-to-json-schema": "1",
        "@connectrpc/connect": "1",
        react: "1",
      }),
      ["node-pty", "@cursor/sdk", "zod", "@connectrpc/connect"],
    );
    NodeAssert.deepEqual(
      await runtimeExternalDependencies(source, "desktop", {
        "playwright-core": "1",
        "@napi-rs/keyring": "1",
        electron: "1",
        react: "1",
      }),
      ["playwright-core", "@napi-rs/keyring"],
    );
    await NodeAssert.rejects(
      runtimeExternalDependencies(source, "unknown", {}),
      /Unknown runtime app/,
    );
  },
);

function processFiles(entries) {
  return async (path) => {
    if (!Object.hasOwn(entries, path))
      throw Object.assign(new Error("No such process"), { code: "ENOENT" });
    return entries[path];
  };
}
function procStat(pid, parent, ticks) {
  return `${pid} (synthetic process) S ${parent} ${Array(17).fill("0").join(" ")} ${ticks} 0`;
}
const processIdentity = {
  electron: { pid: 100, parentPid: 1, startTicks: "10", state: "S" },
  server: { pid: 101, parentPid: 100, startTicks: "11", state: "S" },
};

NodeTest.test(
  "restart captures only the backend directly owned by the captured Electron process",
  async () => {
    const entries = {
      "/proc/100/stat": procStat(100, 1, "10"),
      "/proc/100/task/100/children": "101 102",
      "/proc/101/stat": procStat(101, 100, "11"),
      "/proc/101/cmdline": "/electron/electron\0/app/apps/server/dist/bin.mjs\0",
      "/proc/102/cmdline": "/electron/electron\0--type=renderer\0",
    };
    const app = { process: () => ({ pid: 100 }) };
    NodeAssert.deepEqual(
      await captureApplicationProcesses(app, processFiles(entries)),
      processIdentity,
    );
    await NodeAssert.rejects(
      captureApplicationProcesses(
        app,
        processFiles({
          ...entries,
          "/proc/101/stat": procStat(101, 99, "11"),
        }),
      ),
      /Backend parent identity changed/,
    );
    await NodeAssert.rejects(
      captureApplicationProcesses(
        app,
        processFiles({
          ...entries,
          "/proc/101/cmdline": "unrelated\0",
        }),
      ),
      /Expected one captured app-owned backend/,
    );
  },
);

NodeTest.test(
  "graceful generation close proves both old processes absent and closes only once",
  async () => {
    let closes = 0;
    const child = { exitCode: null, signalCode: null };
    const generation = {
      processes: processIdentity,
      app: {
        process: () => {
          if (closes > 0) throw new Error("Application dispatcher was disposed by close");
          return child;
        },
        close: async () => {
          closes++;
          child.exitCode = 0;
        },
      },
    };
    await closeApplicationGeneration(generation, processFiles({}));
    await closeApplicationGeneration(generation, processFiles({}));
    NodeAssert.equal(closes, 1);
    NodeAssert.equal(generation.closed, true);
    NodeAssert.deepEqual(generation.exit, { code: 0, signal: null });
  },
);

NodeTest.test(
  "a surviving backend or abnormal Electron exit cannot qualify as a restart",
  async () => {
    await NodeAssert.rejects(
      assertApplicationStopped(
        processIdentity,
        processFiles({
          "/proc/101/stat": procStat(101, 100, "11"),
        }),
      ),
      /Old server process still exists/,
    );
    let closes = 0;
    const generation = {
      app: {
        process: () => ({ exitCode: null, signalCode: "SIGKILL" }),
        close: async () => {
          closes++;
        },
      },
    };
    await NodeAssert.rejects(closeApplicationGeneration(generation), /did not exit cleanly/);
    await NodeAssert.rejects(closeApplicationGeneration(generation), /unresolved outcome/);
    NodeAssert.equal(closes, 1);
    NodeAssert.notEqual(generation.closed, true);
  },
);

NodeTest.test("restart identity requires new PIDs and the same profile and database inode", () => {
  const before = {
    processes: processIdentity,
    paths: { userData: "/scratch/xdg-config/app", t3home: "/scratch/t3home" },
    database: { path: "/scratch/t3home/userdata/statev2.sqlite", device: 1, inode: 2 },
  };
  const after = {
    ...before,
    processes: {
      electron: { ...processIdentity.electron, pid: 200 },
      server: { ...processIdentity.server, pid: 201 },
    },
  };
  assertRestartIdentity(before, after);
  for (const role of ["electron", "server"]) {
    NodeAssert.throws(
      () =>
        assertRestartIdentity(before, {
          ...after,
          processes: { ...after.processes, [role]: before.processes[role] },
        }),
      /Restart reused/,
    );
  }
  NodeAssert.throws(
    () => assertRestartIdentity(before, { ...after, paths: { userData: "/different" } }),
    /changed private application paths/,
  );
  NodeAssert.throws(
    () => assertRestartIdentity(before, { ...after, database: { ...after.database, inode: 3 } }),
    /replaced the synthetic database/,
  );
});
