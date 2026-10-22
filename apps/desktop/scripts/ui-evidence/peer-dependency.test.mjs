import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeModule from "node:module";
import { dependencyClosure } from "./runtime.mjs";
import { stageRun } from "./sandbox.mjs";

async function fixture(fn) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-peer-closure-"));
  const source = NodePath.join(root, "source");
  async function write(relative, text) {
    const file = NodePath.join(source, relative);
    await NodeFSP.mkdir(NodePath.dirname(file), { recursive: true });
    await NodeFSP.writeFile(file, text);
  }
  async function link(from, to) {
    await NodeFSP.mkdir(NodePath.dirname(from), { recursive: true });
    await NodeFSP.symlink(NodePath.relative(NodePath.dirname(from), to), from);
  }
  async function pkg(label, name, metadata, code = "module.exports = 42;") {
    const store = NodePath.join(source, "node_modules/.pnpm", label, "node_modules");
    const directory = NodePath.join(store, name);
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(directory, "package.json"),
      JSON.stringify({ name, version: "1.0.0", main: "index.cjs", ...metadata }),
    );
    await NodeFSP.writeFile(NodePath.join(directory, "index.cjs"), code);
    return { directory, store };
  }
  try {
    await write("package.json", "{}");
    await write("apps/desktop/package.json", JSON.stringify({ dependencies: {} }));
    await write("apps/server/package.json", JSON.stringify({ dependencies: { consumer: "1" } }));
    await write("apps/desktop/dist-electron/boot.cjs", "");
    await write("apps/server/dist/server.cjs", "");
    await write(
      "scripts/lib/desktop-external-packages.ts",
      "export const selectDesktopRuntimeExternalDependencies = (value) => value;",
    );
    await write(
      "scripts/lib/cli-external-packages.ts",
      "export const selectCliRuntimeExternalDependencies = (value) => value;",
    );
    await write("apps/desktop/scripts/ui-evidence/scenario.mjs", "export default async () => {};");
    const playwright = await pkg("playwright", "playwright-core", {});
    await link(
      NodePath.join(source, "apps/desktop/node_modules/playwright-core"),
      playwright.directory,
    );
    await fn({ root, source, pkg, link });
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

NodeTest.test("staged transitive required peers resolve through their consuming package", () =>
  fixture(async ({ root, source, pkg, link }) => {
    const protobuf = await pkg(
      "protobuf",
      "@bufbuild/protobuf",
      {},
      "module.exports = 'peer resolved';",
    );
    const adapter = await pkg(
      "adapter",
      "@connectrpc/connect",
      { peerDependencies: { "@bufbuild/protobuf": "1" } },
      "module.exports = require('@bufbuild/protobuf');",
    );
    const consumer = await pkg(
      "consumer",
      "consumer",
      { dependencies: { "@connectrpc/connect": "1", "@bufbuild/protobuf": "1" } },
      "module.exports = require('@connectrpc/connect');",
    );
    await link(NodePath.join(source, "apps/server/node_modules/consumer"), consumer.directory);
    await link(NodePath.join(consumer.store, "@connectrpc/connect"), adapter.directory);
    await link(NodePath.join(consumer.store, "@bufbuild/protobuf"), protobuf.directory);
    await link(NodePath.join(adapter.store, "@bufbuild/protobuf"), protobuf.directory);
    const closure = await dependencyClosure(source);
    const stage = await stageRun({
      run: { root: NodePath.join(root, "run") },
      source,
      harness: source,
      scenario: NodePath.join(source, "apps/desktop/scripts/ui-evidence/scenario.mjs"),
      closure,
      configuration: {},
    });
    const adapterIndex = closure.packages.findIndex((entry) => entry.root === adapter.directory);
    const protobufIndex = closure.packages.findIndex((entry) => entry.root === protobuf.directory);
    const peerLink = NodePath.join(
      stage,
      "deps",
      String(adapterIndex),
      "node_modules/@bufbuild/protobuf",
    );
    NodeAssert.equal(
      await NodeFSP.realpath(peerLink),
      NodePath.join(stage, "deps", String(protobufIndex)),
    );
    const require = NodeModule.createRequire(NodePath.join(stage, "app/apps/server/package.json"));
    NodeAssert.equal(require("consumer"), "peer resolved");
  }),
);

NodeTest.test(
  "required missing peers reject closure even when optional peer metadata overlaps a dependency",
  () =>
    fixture(async ({ source, pkg, link }) => {
      const consumer = await pkg("consumer", "consumer", {
        peerDependencies: { "missing-required-peer": "1" },
      });
      await link(NodePath.join(source, "apps/server/node_modules/consumer"), consumer.directory);
      await NodeAssert.rejects(
        dependencyClosure(source),
        /Missing runtime package: missing-required-peer/,
      );
      await NodeFSP.writeFile(
        NodePath.join(consumer.directory, "package.json"),
        JSON.stringify({
          name: "consumer",
          dependencies: { "missing-required-peer": "1" },
          peerDependencies: { "missing-required-peer": "1" },
          peerDependenciesMeta: { "missing-required-peer": { optional: true } },
        }),
      );
      await NodeAssert.rejects(
        dependencyClosure(source),
        /Missing runtime package: missing-required-peer/,
      );
    }),
);

NodeTest.test("optional absent peers are skipped and installed peers join the closure", () =>
  fixture(async ({ source, pkg, link }) => {
    const installed = await pkg("installed", "installed-peer", {});
    const consumer = await pkg("consumer", "consumer", {
      peerDependencies: { "missing-peer": "1", "installed-peer": "1" },
      peerDependenciesMeta: {
        "missing-peer": { optional: true },
        "installed-peer": { optional: true },
      },
    });
    await link(NodePath.join(source, "apps/server/node_modules/consumer"), consumer.directory);
    await link(NodePath.join(consumer.store, "installed-peer"), installed.directory);
    const closure = await dependencyClosure(source);
    NodeAssert.ok(closure.packages.some((entry) => entry.root === installed.directory));
    NodeAssert.equal(
      closure.packages.some((entry) => entry.name === "missing-peer"),
      false,
    );
  }),
);

NodeTest.test("optional peers do not hide filesystem resolution errors", () =>
  fixture(async ({ source, pkg, link }) => {
    const consumer = await pkg("consumer", "consumer", {
      peerDependencies: { "broken-peer": "1" },
      peerDependenciesMeta: { "broken-peer": { optional: true } },
    });
    await link(NodePath.join(source, "apps/server/node_modules/consumer"), consumer.directory);
    await NodeFSP.symlink("broken-peer", NodePath.join(consumer.store, "broken-peer"));
    await NodeAssert.rejects(dependencyClosure(source), { code: "ELOOP" });
  }),
);
