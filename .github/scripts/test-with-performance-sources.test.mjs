import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import { withOwnedSourceParent } from "./test-with-performance-sources.mjs";

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));

async function invocation(use) {
  const outer = await NodeFSP.mkdtemp(NodePath.join(directory, ".source-lifecycle-test-"));
  const workspace = NodePath.join(outer, "candidate");
  await NodeFSP.mkdir(workspace);
  try {
    await use(workspace);
  } finally {
    const entries = await NodeFSP.readdir(outer);
    NodeAssert.deepEqual(entries, ["candidate"], `Unproved cleanup retained at ${outer}`);
    await NodeFSP.rmdir(workspace);
    await NodeFSP.rmdir(outer);
  }
}

NodeTest.test(
  "source parent disappears after success and preserves a failing child's status",
  async () => {
    await invocation(async (workspace) => {
      for (const exitCode of [0, 7]) {
        let captured;
        const result = withOwnedSourceParent(
          workspace,
          new AbortController().signal,
          async ({ root, run }) => {
            captured = root;
            await run(process.execPath, ["-e", `process.exit(${exitCode})`], workspace, {});
          },
        );
        if (exitCode) await NodeAssert.rejects(result, { exitCode });
        else await result;
        NodeAssert.equal(NodeFS.existsSync(captured), false);
      }
    });
  },
);

NodeTest.test("concurrent source parents cannot clean each other's inputs", async () => {
  await invocation(async (workspace) => {
    let firstRoot;
    await withOwnedSourceParent(workspace, new AbortController().signal, async ({ root }) => {
      firstRoot = root;
      await NodeFSP.writeFile(NodePath.join(root, "input"), "first");
      let secondRoot;
      await withOwnedSourceParent(
        workspace,
        new AbortController().signal,
        async ({ root: second }) => {
          secondRoot = second;
          NodeAssert.notEqual(second, firstRoot);
          NodeAssert.equal(
            await NodeFSP.readFile(NodePath.join(firstRoot, "input"), "utf8"),
            "first",
          );
        },
      );
      NodeAssert.equal(NodeFS.existsSync(secondRoot), false);
      NodeAssert.equal(NodeFS.existsSync(firstRoot), true);
    });
    NodeAssert.equal(NodeFS.existsSync(firstRoot), false);
  });
});

NodeTest.test("handled cancellation closes the owned child before source cleanup", async () => {
  await invocation(async (workspace) => {
    const cancellation = new AbortController();
    const abort = () => cancellation.abort();
    let captured;
    process.once("SIGTERM", abort);
    try {
      await NodeAssert.rejects(
        withOwnedSourceParent(workspace, cancellation.signal, async ({ root, run }) => {
          captured = root;
          await run(
            process.execPath,
            ["-e", "process.kill(process.ppid, 'SIGTERM'); setInterval(() => {}, 1000)"],
            workspace,
            {},
          );
        }),
        { exitCode: 1 },
      );
    } finally {
      process.removeListener("SIGTERM", abort);
    }
    NodeAssert.equal(cancellation.signal.aborted, true);
    NodeAssert.equal(NodeFS.existsSync(captured), false);
  });
});

NodeTest.test("cleanup failure preserves the original consumer failure", async () => {
  await invocation(async (workspace) => {
    const original = Object.assign(new Error("consumer failure"), { exitCode: 9 });
    await NodeAssert.rejects(
      withOwnedSourceParent(workspace, new AbortController().signal, async ({ root }) => {
        await NodeFSP.rmdir(root);
        throw original;
      }),
      (error) => error === original,
    );
  });
});

NodeTest.test(
  "failed source consumer preserves separately owned retained fixture scratch",
  async () => {
    await invocation(async (workspace) => {
      const fixtureScratch = NodePath.join(workspace, "retained-fixture");
      await NodeFSP.mkdir(fixtureScratch);
      const marker = NodePath.join(fixtureScratch, "unknown-close-evidence");
      await NodeFSP.writeFile(marker, "retain");
      try {
        await NodeAssert.rejects(
          withOwnedSourceParent(workspace, new AbortController().signal, async ({ root, run }) => {
            await run(process.execPath, ["-e", "process.exit(7)"], workspace, {
              TMPDIR: fixtureScratch,
              JONES_PERFORMANCE_SOURCE_PARENT: root,
            });
          }),
          { exitCode: 7 },
        );
        NodeAssert.equal(await NodeFSP.readFile(marker, "utf8"), "retain");
      } finally {
        await NodeFSP.unlink(marker);
        await NodeFSP.rmdir(fixtureScratch);
      }
    });
  },
);
