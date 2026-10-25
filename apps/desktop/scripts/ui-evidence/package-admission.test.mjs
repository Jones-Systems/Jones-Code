import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeModule from "node:module";
import {
  createPackageResolver,
  dependencyClosure,
  resolvedPackageDependencies,
} from "./runtime.mjs";
import { stageRun, bwrapArguments } from "./sandbox.mjs";

async function fixture(fn) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jcue-source-bound-"));
  const source = NodePath.join(root, ".t3/worktrees/task");
  const write = async (path, value) => {
    await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
    await NodeFSP.writeFile(path, typeof value === "string" ? value : JSON.stringify(value));
  };
  const pkg = async (name, metadata = {}, path) => {
    path ??= NodePath.join(
      source,
      "node_modules/.pnpm",
      name.replaceAll("/", "+") + "@1",
      "node_modules",
      name,
    );
    await write(NodePath.join(path, "package.json"), {
      name,
      version: "1",
      main: "index.cjs",
      ...metadata,
    });
    await write(NodePath.join(path, "index.cjs"), "module.exports = 42;");
    return path;
  };
  const link = async (destination, target) => {
    await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
    await NodeFSP.symlink(NodePath.relative(NodePath.dirname(destination), target), destination);
  };
  try {
    await write(NodePath.join(source, "package.json"), { type: "module" });
    for (const app of ["desktop", "server"]) {
      await write(NodePath.join(source, "apps", app, "package.json"), { dependencies: {} });
      await NodeFSP.mkdir(NodePath.join(source, "apps", app, "node_modules"), { recursive: true });
    }
    for (const policy of ["desktop", "cli"]) {
      const select = policy === "desktop" ? "Desktop" : "Cli";
      await write(
        NodePath.join(source, "scripts/lib", policy + "-external-packages.ts"),
        `export const select${select}RuntimeExternalDependencies = (dependencies) => dependencies;`,
      );
    }
    await link(NodePath.join(source, "node_modules/playwright-core"), await pkg("playwright-core"));
    await fn({ root, source, write, pkg, link });
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

NodeTest.test(
  "source-bound closure stages aliases, transitive packages and already-present required peers under a .t3 source",
  () =>
    fixture(async ({ root, source, write, pkg, link }) => {
      const peer = await pkg("@peer/lib");
      const consumer = await pkg("consumer", {
        peerDependencies: { "@peer/lib": "1" },
        optionalDependencies: { absent: "1" },
      });
      await write(NodePath.join(consumer, "index.cjs"), "module.exports = require('@peer/lib');");
      await link(NodePath.join(source, "node_modules/@peer/lib"), peer);
      await link(NodePath.join(source, "apps/desktop/node_modules/alias"), consumer);
      await write(NodePath.join(source, "apps/desktop/package.json"), {
        dependencies: { "@peer/lib": "1", alias: "1" },
      });
      const alias = NodePath.join(root, "source-link");
      await link(alias, source);
      const closure = await dependencyClosure(alias);
      NodeAssert.equal(closure.source, source);
      NodeAssert.equal(closure.packages.filter((p) => p.root === peer).length, 1);
      NodeAssert.equal(
        closure.packages.find((p) => p.root === consumer).dependencies[0].root,
        peer,
      );
      for (const path of [
        "apps/desktop/dist-electron/boot.cjs",
        "apps/server/dist/bin.mjs",
        "apps/desktop/scripts/ui-evidence/runner.mjs",
        "scenario.mjs",
      ])
        await write(NodePath.join(source, path), "export default () => {};");
      const run = { root: NodePath.join(root, "run") };
      const stage = await stageRun({
        run,
        source: alias,
        harness: source,
        scenario: NodePath.join(source, "scenario.mjs"),
        closure,
        configuration: {},
      });
      const require = NodeModule.createRequire(
        NodePath.join(stage, "app/apps/desktop/package.json"),
      );
      NodeAssert.equal(require("alias"), 42);
      const args = await bwrapArguments({
        stage,
        runtime: { directory: "/synthetic/electron" },
        artifacts: NodePath.join(root, "artifacts"),
      });
      NodeAssert.ok(!args.includes(source));
      NodeAssert.ok(!args.includes(NodePath.join(source, "node_modules")));
    }),
);

NodeTest.test(
  "initial and recursive dependencies reject sibling and similarly prefixed storage, and redirected anchors",
  () =>
    fixture(async ({ root, source, pkg, link, write }) => {
      const consumer = await pkg("consumer", { dependencies: { unsafe: "1" } });
      await link(NodePath.join(source, "apps/desktop/node_modules/consumer"), consumer);
      await write(NodePath.join(source, "apps/desktop/package.json"), {
        dependencies: { consumer: "1" },
      });
      for (const sibling of ["sibling", "task-extra"]) {
        const external = await pkg(
          "unsafe",
          {},
          NodePath.join(root, ".t3/worktrees", sibling, "node_modules/unsafe"),
        );
        const alias = NodePath.join(source, "node_modules/unsafe");
        await link(alias, external);
        await NodeAssert.rejects(dependencyClosure(source), /escaped source/);
        await write(NodePath.join(source, "apps/server/package.json"), {
          dependencies: { unsafe: "1" },
        });
        await NodeAssert.rejects(dependencyClosure(source), /escaped source/);
        await write(NodePath.join(source, "apps/server/package.json"), { dependencies: {} });
        await NodeFSP.unlink(alias);
      }
      const anchor = NodePath.join(source, "apps/server/node_modules");
      await NodeFSP.rmdir(anchor);
      await link(anchor, NodePath.join(root, ".t3/worktrees/sibling/node_modules"));
      await NodeAssert.rejects(createPackageResolver(source), /anchor escaped source/);
    }),
);

NodeTest.test(
  "protected suffixes and aliases to visited packages are rejected before metadata or deduplication",
  () =>
    fixture(async ({ source, pkg, link, write }) => {
      const good = await pkg("good");
      await link(NodePath.join(source, "node_modules/good"), good);
      await write(NodePath.join(source, "apps/desktop/package.json"), {
        dependencies: { good: "1" },
      });
      for (const protectedName of [
        ".t3",
        ".git",
        ".ssh",
        ".codex",
        ".claude",
        ".env",
        ".env.test",
      ]) {
        const unsafe = NodePath.join(source, "node_modules", protectedName, "node_modules", "bad");
        await link(unsafe, good);
        const resolver = await createPackageResolver(source);
        await NodeAssert.rejects(resolver.admit(unsafe), /Protected/);
        const target = await pkg(
          "bad",
          {},
          NodePath.join(source, "node_modules", protectedName, "physical/node_modules/bad"),
        );
        const alias = NodePath.join(source, "node_modules/alias");
        await link(alias, target);
        await write(NodePath.join(source, "apps/server/package.json"), {
          dependencies: { alias: "1" },
        });
        await NodeAssert.rejects(dependencyClosure(source), /Protected/);
        await NodeFSP.unlink(alias);
      }
      await write(NodePath.join(source, "apps/server/package.json"), {
        dependencies: { "good/../good": "1" },
      });
      await NodeAssert.rejects(dependencyClosure(source), /Invalid runtime package name/);
    }),
);

NodeTest.test("fabricated manifests do not admit containers or package subdirectories", () =>
  fixture(async ({ source, pkg, write }) => {
    const good = await pkg("good");
    const resolver = await createPackageResolver(source);
    for (const path of [
      source,
      NodePath.join(source, "node_modules"),
      NodePath.join(source, "node_modules/.pnpm"),
      NodePath.join(source, "node_modules/.pnpm/locator"),
      NodePath.join(source, "node_modules/@scope"),
      NodePath.join(good, "subdir"),
    ]) {
      await write(NodePath.join(path, "package.json"), { name: "fake" });
      await NodeAssert.rejects(resolver.admit(path));
    }
    for (const name of [
      "../escape",
      "/absolute",
      "bad\\name",
      "@scope",
      "@scope/name/extra",
      ".pnpm",
      "a/../../good",
    ])
      await NodeAssert.rejects(resolver.resolve(name, source), /Invalid runtime package name/);
  }),
);

NodeTest.test(
  "only genuinely missing optional dependencies are optional; manifests and installed targets stay closed",
  () =>
    fixture(async ({ root, source, pkg, link, write }) => {
      const consumer = await pkg("consumer");
      const resolver = await createPackageResolver(source);
      NodeAssert.deepEqual(
        await resolvedPackageDependencies(
          consumer,
          {
            optionalDependencies: { absent: "1" },
            peerDependencies: { missingPeer: "1" },
            peerDependenciesMeta: { missingPeer: { optional: true } },
          },
          resolver,
        ),
        [],
      );
      await NodeAssert.rejects(
        resolvedPackageDependencies(consumer, { peerDependencies: { required: "1" } }, resolver),
        /Missing runtime package/,
      );
      const unsafe = NodePath.join(source, "node_modules/unsafe");
      await link(unsafe, NodePath.join(root, "missing"));
      await NodeAssert.rejects(
        resolvedPackageDependencies(consumer, { optionalDependencies: { unsafe: "1" } }, resolver),
      );
      await NodeFSP.unlink(unsafe);
      const nestedStorage = NodePath.join(consumer, "node_modules");
      await link(nestedStorage, NodePath.join(root, "missing-store"));
      await NodeAssert.rejects(
        resolvedPackageDependencies(consumer, { optionalDependencies: { absent: "1" } }, resolver),
      );
      await NodeFSP.unlink(nestedStorage);
      await NodeFSP.mkdir(NodePath.join(root, "external-store"));
      await link(nestedStorage, NodePath.join(root, "external-store"));
      await NodeAssert.rejects(
        resolvedPackageDependencies(consumer, { optionalDependencies: { absent: "1" } }, resolver),
        /escaped source/,
      );
      await NodeFSP.unlink(nestedStorage);
      await write(NodePath.join(root, "manifest.json"), { name: "unsafe" });
      await NodeFSP.unlink(NodePath.join(consumer, "package.json"));
      await link(NodePath.join(consumer, "package.json"), NodePath.join(root, "manifest.json"));
      await NodeAssert.rejects(resolver.metadata(consumer), /manifest escaped/);
    }),
);

NodeTest.test("staging rejects escaping package content while retaining internal links", () =>
  fixture(async ({ root, source, pkg, link, write }) => {
    const consumer = await pkg("consumer");
    await link(NodePath.join(source, "apps/desktop/node_modules/consumer"), consumer);
    await write(NodePath.join(source, "apps/desktop/package.json"), {
      dependencies: { consumer: "1" },
    });
    for (const path of [
      "apps/desktop/dist-electron/boot.cjs",
      "apps/server/dist/bin.mjs",
      "apps/desktop/scripts/ui-evidence/runner.mjs",
      "scenario.mjs",
    ])
      await write(NodePath.join(source, path), "export default () => {};");
    await write(NodePath.join(consumer, "internal.txt"), "safe");
    await link(NodePath.join(consumer, "internal-link"), NodePath.join(consumer, "internal.txt"));
    const closure = await dependencyClosure(source);
    const input = {
      source,
      harness: source,
      scenario: NodePath.join(source, "scenario.mjs"),
      closure,
      configuration: {},
    };
    await stageRun({ ...input, run: { root: NodePath.join(root, "safe-run") } });
    await write(NodePath.join(root, "outside.txt"), "synthetic");
    await link(NodePath.join(consumer, "escape"), NodePath.join(root, "outside.txt"));
    await NodeAssert.rejects(
      stageRun({ ...input, run: { root: NodePath.join(root, "unsafe-run") } }),
      /symlink escaped/,
    );
  }),
);
