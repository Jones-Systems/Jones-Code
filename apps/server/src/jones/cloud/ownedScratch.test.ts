// @effect-diagnostics nodeBuiltinImport:off
import { it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { bundleFileSystem } from "./qualifiedRuntime.ts";
import { OwnedScratchCleanupError, ownedScratchRemover, withOwnedScratch } from "./ownedScratch.ts";

const prefix = ".jones-extract-";
const notEmpty = () =>
  Object.assign(new Error("ENOTEMPTY: directory not empty, rmdir"), { code: "ENOTEMPTY" });
const failingRemove = async () => {
  throw notEmpty();
};

async function fixture(body: (parent: string) => Promise<void>): Promise<void> {
  const parent = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-owned-scratch-"));
  let failed = false;
  try {
    await body(parent);
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    try {
      await NodeFSP.rm(parent, { recursive: true, force: true });
      await NodeAssert.rejects(NodeFSP.lstat(parent), { code: "ENOENT" });
    } catch (cleanup) {
      // The primary failure stays visible; cleanup only fails the test when nothing else did.
      if (!failed) throw cleanup;
    }
  }
}

it("removes exactly the scratch it created and never sweeps earlier leftovers", async () => {
  await fixture(async (parent) => {
    const earlier = NodePath.join(parent, `${prefix}earlier`);
    await NodeFSP.mkdir(earlier);
    await NodeFSP.writeFile(NodePath.join(earlier, "payload"), "left by an earlier attempt");
    let scratchPath = "";
    const result = await withOwnedScratch(parent, prefix, async (scratch) => {
      scratchPath = scratch;
      NodeAssert.equal(NodePath.dirname(scratch), parent);
      await NodeFSP.mkdir(NodePath.join(scratch, "nested"));
      await NodeFSP.writeFile(NodePath.join(scratch, "nested", "file"), "x");
      return "staged";
    });
    NodeAssert.deepEqual(result, { value: "staged" });
    await NodeAssert.rejects(NodeFSP.lstat(scratchPath), { code: "ENOENT" });
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(earlier, "payload"), "utf8"),
      "left by an earlier attempt",
    );
  });
});

it("keeps a durable result when removal fails afterwards and reports the leftover path", async () => {
  await fixture(async (parent) => {
    const result = await withOwnedScratch(parent, prefix, async () => "durable", failingRemove);
    NodeAssert.equal(result.value, "durable");
    NodeAssert.ok(result.leftover);
    NodeAssert.equal(NodePath.dirname(result.leftover.path), parent);
    NodeAssert.ok(NodePath.basename(result.leftover.path).startsWith(prefix));
    NodeAssert.equal((result.leftover.cause as NodeJS.ErrnoException).code, "ENOTEMPTY");
    // The injected remover removed nothing, so the reported path is really there.
    NodeAssert.ok((await NodeFSP.lstat(result.leftover.path)).isDirectory());
  });
});

it("rethrows the original failure unchanged when removal succeeds", async () => {
  await fixture(async (parent) => {
    const original = new Error("stage refused");
    let scratchPath = "";
    await NodeAssert.rejects(
      withOwnedScratch(parent, prefix, async (scratch) => {
        scratchPath = scratch;
        throw original;
      }),
      (error) => error === original,
    );
    await NodeAssert.rejects(NodeFSP.lstat(scratchPath), { code: "ENOENT" });
  });
});

it("keeps the stage failure as the primary error when removal also fails", async () => {
  await fixture(async (parent) => {
    const original = Object.assign(new Error("occupied-cache: version already staged"), {
      reason: "occupied-cache",
    });
    const error = await withOwnedScratch(
      parent,
      prefix,
      async () => {
        throw original;
      },
      failingRemove,
    ).then(
      () => NodeAssert.fail("the stage failure must surface"),
      (caught: unknown) => caught,
    );
    NodeAssert.ok(error instanceof OwnedScratchCleanupError);
    NodeAssert.equal(error.message, original.message);
    NodeAssert.equal(error.cause, original);
    NodeAssert.equal(error.errors[0], original);
    NodeAssert.equal((error.errors[1] as NodeJS.ErrnoException).code, "ENOTEMPTY");
    NodeAssert.equal(NodePath.dirname(error.leftover.path), parent);
  });
});

it("removes through the raw filesystem when running under Electron", async () => {
  const calls: Array<readonly [string, unknown]> = [];
  const loaded: string[] = [];
  const remove = ownedScratchRemover(() =>
    bundleFileSystem({ electron: "44.4.2" }, (id) => {
      loaded.push(id);
      return {
        promises: {
          rm: async (path: string, options: unknown) => {
            calls.push([path, options]);
          },
        },
      };
    }),
  );
  await remove("/owned/scratch");
  NodeAssert.deepEqual(loaded, ["original-fs"]);
  NodeAssert.deepEqual(calls, [["/owned/scratch", { recursive: true, force: true }]]);
});

it("uses ordinary Node removal outside Electron without loading Electron modules", async () => {
  await fixture(async (parent) => {
    const scratch = NodePath.join(parent, `${prefix}plain`);
    await NodeFSP.mkdir(NodePath.join(scratch, "deep"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(scratch, "deep", "file"), "x");
    await ownedScratchRemover(() =>
      bundleFileSystem({}, () => {
        throw new Error("must not load");
      }),
    )(scratch);
    await NodeAssert.rejects(NodeFSP.lstat(scratch), { code: "ENOENT" });
  });
});
