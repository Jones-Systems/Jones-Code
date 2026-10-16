import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  gateFixtureExecutable,
  recoverFixtureExecutable,
  runLegacyAdmissionNativeFixture,
  withNativeAdmissionScratch,
} from "./legacy-admission-native-fixture.mjs";

// oxlint-disable-next-line t3code/no-global-process-runtime -- These standalone native tests need the actual host platform without loading the application runtime.
const nativePlatform = process.platform;
const unix = { skip: nativePlatform === "win32" };

async function executable(root, mode = 0o751) {
  const filename = NodePath.join(root, "console-fixture");
  await NodeFSP.writeFile(filename, "fixture executable bytes\n", { flag: "wx", mode });
  await NodeFSP.chmod(filename, mode);
  return filename;
}

NodeTest.test(
  "native proof uses actual Darwin admission and declares its custody limit",
  async () => {
    const result = await runLegacyAdmissionNativeFixture();
    NodeAssert.equal(result.activationAllowed, false);
    NodeAssert.equal(result.productionCustodyQualified, false);
    if (nativePlatform !== "darwin") {
      NodeAssert.equal(result.status, "unsupported");
      NodeAssert.equal(result.reason, "darwin-required");
      NodeAssert.equal(result.scenarios, undefined);
      return;
    }
    NodeAssert.equal(result.status, "passed");
    NodeAssert.equal(result.packagedArtifactQualified, false);
    NodeAssert.deepEqual(
      result.scenarios.map((entry) => [entry.scenario, entry.writeObserved, entry.custodyRefused]),
      [
        ["resume", true, false],
        ["drain", false, false],
        ["missing-custody", false, true],
      ],
    );
    for (const entry of result.scenarios) {
      NodeAssert.equal(entry.freshLaunchDenied, true);
      NodeAssert.equal(entry.capturedChildReaped, true);
      NodeAssert.equal(entry.originalModeRestored, true);
    }
  },
);

NodeTest.test(
  "durable original mode survives abrupt gate-owner exit and fresh-process recovery",
  unix,
  async () => {
    await withNativeAdmissionScratch(async (root) => {
      const file = await executable(root);
      const module = new URL("./legacy-admission-native-fixture.mjs", import.meta.url).href;
      const gate = NodeChildProcess.spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `const {gateFixtureExecutable}=await import(process.argv[1]); await gateFixtureExecutable(process.argv[2]); process.exit(29);`,
          module,
          file,
        ],
        { cwd: root, env: { PATH: process.env.PATH }, timeout: 10_000, encoding: "utf8" },
      );
      NodeAssert.equal(gate.error, undefined);
      NodeAssert.equal(gate.status, 29);
      NodeAssert.equal((await NodeFSP.stat(file)).mode & 0o777, 0o640);
      const recover = NodeChildProcess.spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `const {recoverFixtureExecutable}=await import(process.argv[1]); console.log(JSON.stringify(await recoverFixtureExecutable(process.argv[2])));`,
          module,
          file,
        ],
        { cwd: root, env: { PATH: process.env.PATH }, timeout: 10_000, encoding: "utf8" },
      );
      NodeAssert.equal(recover.error, undefined);
      NodeAssert.equal(recover.status, 0, recover.stderr);
      NodeAssert.equal(JSON.parse(recover.stdout).originalMode, 0o751);
      NodeAssert.equal((await NodeFSP.stat(file)).mode & 0o777, 0o751);
      NodeAssert.equal((await recoverFixtureExecutable(file)).status, "restored");
    });
  },
);

for (const fault of ["inode", "content", "mode", "symlink", "hardlink"]) {
  NodeTest.test(
    `recovery refuses ${fault} substitution without changing the replacement`,
    unix,
    async () => {
      await withNativeAdmissionScratch(async (root) => {
        const file = await executable(root);
        await gateFixtureExecutable(file);
        if (fault === "inode") {
          await NodeFSP.rename(file, `${file}.retained`);
          await NodeFSP.writeFile(file, "fixture executable bytes\n", { flag: "wx", mode: 0o640 });
        } else if (fault === "content") {
          await NodeFSP.writeFile(file, "different fixture bytes\n");
        } else if (fault === "mode") {
          await NodeFSP.chmod(file, 0o600);
        } else if (fault === "symlink") {
          await NodeFSP.rename(file, `${file}.retained`);
          await NodeFSP.symlink(`${file}.retained`, file);
        } else {
          await NodeFSP.link(file, `${file}.alias`);
        }
        const before = await NodeFSP.lstat(file, { bigint: true });
        await NodeAssert.rejects(recoverFixtureExecutable(file));
        const after = await NodeFSP.lstat(file, { bigint: true });
        NodeAssert.equal(after.ino, before.ino);
        NodeAssert.equal(after.mode, before.mode);
      });
    },
  );
}

NodeTest.test(
  "journal is exclusive and recovery preserves the original nondefault mode",
  unix,
  async () => {
    await withNativeAdmissionScratch(async (root) => {
      const file = await executable(root, 0o510);
      await gateFixtureExecutable(file);
      await NodeAssert.rejects(gateFixtureExecutable(file));
      await recoverFixtureExecutable(file);
      NodeAssert.equal((await NodeFSP.stat(file)).mode & 0o777, 0o510);
    });
  },
);

NodeTest.test(
  "scratch success, failure and concurrent owners remove only their own roots",
  async () => {
    const paths = [];
    await Promise.all(
      [0, 1].map((number) =>
        withNativeAdmissionScratch(async (root) => {
          paths.push(root);
          await NodeFSP.writeFile(NodePath.join(root, "owned"), String(number), { flag: "wx" });
          await new Promise((resolve) => setImmediate(resolve));
          NodeAssert.equal(
            await NodeFSP.readFile(NodePath.join(root, "owned"), "utf8"),
            String(number),
          );
        }),
      ),
    );
    NodeAssert.equal(new Set(paths).size, 2);
    for (const root of paths) await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
    let failedRoot;
    const original = new Error("deliberate fixture failure");
    await NodeAssert.rejects(
      withNativeAdmissionScratch(async (root) => {
        failedRoot = root;
        throw original;
      }),
      (error) => error === original,
    );
    await NodeAssert.rejects(NodeFSP.lstat(failedRoot), { code: "ENOENT" });
  },
);

NodeTest.test(
  "cancellation after suspended admission drains the captured child and cleans scratch",
  { skip: nativePlatform !== "darwin" },
  async () => {
    await withNativeAdmissionScratch(async (parent) => {
      const cancellation = new AbortController();
      let captured = false;
      const original = new Error("cancel at native admission barrier");
      await NodeAssert.rejects(
        runLegacyAdmissionNativeFixture({
          temporaryDirectory: parent,
          signal: cancellation.signal,
          onSuspended: () => {
            captured = true;
            cancellation.abort(original);
          },
        }),
        (error) => error === original,
      );
      NodeAssert.equal(captured, true);
      NodeAssert.deepEqual(await NodeFSP.readdir(parent), []);
    });
  },
);

NodeTest.test("cleanup failure retains the original failure as the primary cause", async () => {
  const original = new Error("original operation failed");
  await NodeAssert.rejects(
    withNativeAdmissionScratch(async (root) => {
      await NodeFSP.rmdir(root);
      throw original;
    }),
    (error) => {
      NodeAssert.ok(error instanceof AggregateError);
      NodeAssert.equal(error.cause, original);
      NodeAssert.equal(error.errors[0], original);
      NodeAssert.equal(error.errors[1].code, "ENOENT");
      return true;
    },
  );
  await NodeAssert.rejects(
    withNativeAdmissionScratch(async (root) => {
      await NodeFSP.rmdir(root);
    }),
    { code: "ENOENT" },
  );
});

NodeTest.test("already cancelled invocation creates no fixture", async () => {
  const cancellation = new AbortController();
  const original = new Error("already cancelled");
  cancellation.abort(original);
  await NodeAssert.rejects(
    runLegacyAdmissionNativeFixture({ signal: cancellation.signal }),
    (error) => error === original,
  );
});
