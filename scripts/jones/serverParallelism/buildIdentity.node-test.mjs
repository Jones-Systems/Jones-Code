import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
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

// Loads apps/server/vite.config.ts under one pack mode and runs its real identity plugin over a
// synthetic bundle made of the mode's configured entries, so the Jones hooks are proven per mode.
const packModeProbe = `
const pack = (await import(process.argv[1])).default.pack;
const plugin = pack.plugins.find((entry) => entry.name === "jones-server-compute-build-identity");
const bundle = { "entry.mjs": { type: "chunk", code: pack.banner.js + "body" } };
await plugin.generateBundle.call(
  { getModuleIds: () => pack.entry, getModuleInfo: (id) => ({ code: "module " + id }) },
  {},
  bundle,
);
process.stdout.write(
  JSON.stringify({
    entry: pack.entry,
    outDir: pack.outDir,
    exe: Object.keys(pack.exe ?? {}),
    banner: pack.banner.js,
    define: pack.define.__JONES_SERVER_COMPUTE_BUILD_ID__,
    stamped: bundle["entry.mjs"].code,
  }),
);
`;

NodeTest.test(
  "the server pack config keeps the Jones identity hooks in each upstream pack mode",
  async () => {
    const serverRoot = NodeURL.fileURLToPath(new URL("../../../apps/server/", import.meta.url));
    const configUrl = NodeURL.pathToFileURL(NodePath.join(serverRoot, "vite.config.ts")).href;
    const assetDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-pack-modes-"));
    try {
      const inherited = Object.fromEntries(
        Object.entries(process.env).filter(([name]) => !name.startsWith("T3CODE_")),
      );
      const probe = (mode) =>
        JSON.parse(
          NodeChildProcess.execFileSync(
            process.execPath,
            ["--input-type=module", "-e", packModeProbe, configUrl],
            {
              cwd: serverRoot,
              encoding: "utf8",
              // A target skips the exact-Node-version gate, which only guards real executable builds.
              env: {
                ...inherited,
                T3CODE_PACK_EXE: mode,
                T3CODE_PACK_EXE_TARGET: "linux-x64",
                T3CODE_CLI_ASSET_DIR: assetDir,
              },
            },
          ),
        );
      const expected = {
        1: { entry: ["src/bin.ts"], outDir: "dist-exe", sea: true },
        bootstrap: { entry: ["src/bin.ts"], outDir: "dist-exe", sea: false },
        bundle: { entry: ["src/binMain.ts"], outDir: assetDir, sea: false },
      };
      const stamps = {};
      for (const [mode, want] of Object.entries(expected)) {
        const result = probe(mode);
        NodeAssert.deepEqual(result.entry, want.entry, mode);
        NodeAssert.equal(result.outDir, want.outDir, mode);
        NodeAssert.equal(result.exe.includes("seaConfig"), want.sea, mode);
        NodeAssert.equal(result.define, JSON.stringify(computeBuildPlaceholder), mode);
        NodeAssert.ok(
          result.banner.startsWith("#!/usr/bin/env node\n") &&
            result.banner.includes(`// jones-server-compute-build:${computeBuildPlaceholder}\n`),
          mode,
        );
        const stamp = /jones-server-compute-build:([a-f0-9]{64})/.exec(result.stamped)?.[1];
        NodeAssert.ok(stamp !== undefined && stamp !== computeBuildPlaceholder, mode);
        NodeAssert.ok(!result.stamped.includes(computeBuildPlaceholder), mode);
        stamps[mode] = stamp;
      }
      // The embedded main bundle and the bootstrap are different code, so they bind different identities.
      NodeAssert.notEqual(stamps.bundle, stamps.bootstrap);
      NodeAssert.equal(probe("bundle").stamped, probe("bundle").stamped);
    } finally {
      await NodeFSP.rm(assetDir, { recursive: true });
    }
  },
);
