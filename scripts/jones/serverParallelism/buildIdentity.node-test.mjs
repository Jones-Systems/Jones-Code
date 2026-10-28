import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { serverComputeBuildIdentity, computeBuildPlaceholder } from "./buildIdentity.ts";

test("one complete-input digest stamps every chunk and changes with sibling code or lock bytes", async () => {
  const root = await mkdtemp(join(tmpdir(), "jones-build-id-"));
  try {
    for (const file of ["package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml", "vite.config.ts",
      "apps/server/package.json", "apps/server/vite.config.ts", "patches/@ff-labs__fff-node@0.11.0.patch"]) {
      await mkdir(dirname(join(root, file)), { recursive: true }); await writeFile(join(root, file), file);
    }
    const build = async (sibling) => {
      const bundle = Object.fromEntries(["bin.mjs", "worker.mjs"].map((name) => [name,
        { type: "chunk", code: `// jones-server-compute-build:${computeBuildPlaceholder}\n${name === "worker.mjs" ? sibling : "entry"}` }]));
      const hook = serverComputeBuildIdentity(root, { platform: "synthetic-test" }).generateBundle;
      await hook.call({ getModuleIds: () => ["entry", "sibling"],
        getModuleInfo: (id) => ({ isExternal: false, code: id === "sibling" ? sibling : "entry" }) }, {}, bundle);
      const ids = Object.values(bundle).map((chunk) => /jones-server-compute-build:([a-f0-9]{64})/.exec(chunk.code)[1]);
      assert.equal(new Set(ids).size, 1); assert.notEqual(ids[0], computeBuildPlaceholder);
      return ids[0];
    };
    const first = await build("worker A");
    assert.equal(first, await build("worker A"));
    assert.notEqual(first, await build("worker B"));
    await writeFile(join(root, "pnpm-lock.yaml"), "different actual external dependency resolution");
    assert.notEqual(first, await build("worker A"));
  } finally { await rm(root, { recursive: true }); }
});
