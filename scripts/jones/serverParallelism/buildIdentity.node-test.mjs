import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { serverComputeBuildIdentity, computeBuildPlaceholder } from "./buildIdentity.ts";

NodeTest.test(
  "one complete-input digest stamps every chunk and changes with sibling code or lock bytes",
  async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-build-id-"));
    try {
      for (const file of [
        "package.json",
        "pnpm-lock.yaml",
        "pnpm-workspace.yaml",
        "vite.config.ts",
        "apps/server/package.json",
        "apps/server/vite.config.ts",
        "patches/@ff-labs__fff-node@0.11.0.patch",
      ]) {
        await NodeFSP.mkdir(NodePath.dirname(NodePath.join(root, file)), { recursive: true });
        await NodeFSP.writeFile(NodePath.join(root, file), file);
      }
      const build = async (sibling) => {
        const bundle = Object.fromEntries(
          ["bin.mjs", "worker.mjs"].map((name) => [
            name,
            {
              type: "chunk",
              code: `// jones-server-compute-build:${computeBuildPlaceholder}\n${name === "worker.mjs" ? sibling : "entry"}`,
            },
          ]),
        );
        const hook = serverComputeBuildIdentity(root, {
          platform: "synthetic-test",
        }).generateBundle;
        await hook.call(
          {
            getModuleIds: () => ["entry", "sibling"],
            getModuleInfo: (id) => ({
              code: id === "sibling" ? sibling : "entry",
            }),
          },
          {},
          bundle,
        );
        const ids = Object.values(bundle).map(
          (chunk) => /jones-server-compute-build:([a-f0-9]{64})/.exec(chunk.code)[1],
        );
        NodeAssert.equal(new Set(ids).size, 1);
        NodeAssert.notEqual(ids[0], computeBuildPlaceholder);
        return ids[0];
      };
      const first = await build("worker A");
      NodeAssert.equal(first, await build("worker A"));
      NodeAssert.notEqual(first, await build("worker B"));
      await NodeFSP.writeFile(
        NodePath.join(root, "pnpm-lock.yaml"),
        "different actual external dependency resolution",
      );
      NodeAssert.notEqual(first, await build("worker A"));
    } finally {
      await NodeFSP.rm(root, { recursive: true });
    }
  },
);
