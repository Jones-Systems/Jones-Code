import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const inside = (root, path) => path === root || path.startsWith(`${root}${NodePath.sep}`);
const sha = (path) =>
  NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex");
export function installHistoricalResolutionGuard({
  receivingRoot,
  historicalRoots,
  maxRecords,
  maxFileBytes,
  sourceInputs,
}) {
  NodeAssert.ok(Number.isSafeInteger(maxRecords) && maxRecords > 0);
  NodeAssert.ok(Number.isSafeInteger(maxFileBytes) && maxFileBytes > 0);
  const roots = [receivingRoot, ...historicalRoots];
  for (const root of roots) NodeAssert.equal(NodeFS.realpathSync(root), root);
  NodeAssert.equal(new Set(roots).size, roots.length);
  for (const root of roots)
    NodeAssert.ok(!roots.some((other) => root !== other && inside(other, root)));
  const owner = (path) => roots.find((root) => inside(root, path));
  const bridges = ["fixtures-historical-worker.mjs", "migration-restore-worker.mjs"].map((file) =>
    NodePath.join(receivingRoot, "scripts/jones/performance", file),
  );
  const inputBindings = new Map();
  NodeAssert.deepEqual(sourceInputs.map(({ root }) => root).sort(), [...roots].sort());
  for (const input of sourceInputs) {
    const path = NodePath.join(input.root, "pnpm-lock.yaml");
    const info = NodeFS.lstatSync(path);
    NodeAssert.ok(info.isFile() && info.size <= maxFileBytes);
    NodeAssert.equal(sha(path), input.lockSha256, "frozen lock identity differs");
    const text = NodeFS.readFileSync(path, "utf8");
    // Only read package keys, not YAML values or arbitrary executable configuration.
    const section = text.split(/^packages:\s*$/m)[1]?.split(/^\S/m)[0];
    NodeAssert.ok(section, "frozen lock package inventory unavailable");
    const keys = new Set(
      [...section.matchAll(/^  (.+):\s*$/gm)].map(([, key]) => {
        if (key.startsWith("'")) return key.slice(1, -1).replaceAll("''", "'");
        if (key.startsWith('"')) return JSON.parse(key);
        return key;
      }),
    );
    inputBindings.set(input.root, { ...input, keys });
  }
  const records = new Map();
  function capture(path, root) {
    if (records.has(path)) return;
    NodeAssert.ok(records.size < maxRecords, "resolution record bound exceeded");
    const info = NodeFS.lstatSync(path);
    NodeAssert.ok(
      info.isFile() && info.size <= maxFileBytes,
      "resolved module file bound exceeded",
    );
    let directory = NodePath.dirname(path),
      packageIdentity;
    while (inside(root, directory)) {
      const manifest = NodePath.join(directory, "package.json");
      if (NodeFS.existsSync(manifest)) {
        const real = NodeFS.realpathSync(manifest),
          info = NodeFS.lstatSync(real);
        NodeAssert.ok(inside(root, real) && info.isFile() && info.size <= maxFileBytes);
        const parsed = JSON.parse(NodeFS.readFileSync(real, "utf8"));
        packageIdentity = {
          path: real,
          name: parsed.name ?? null,
          version: parsed.version ?? null,
          sha256: sha(real),
        };
        const input = inputBindings.get(root);
        if (real.includes(`${NodePath.sep}node_modules${NodePath.sep}`)) {
          const key = `${parsed.name}@${parsed.version}`;
          NodeAssert.ok(
            input.keys.has(key) || [...input.keys].some((entry) => entry.startsWith(`${key}(`)),
            "resolved package version differs from frozen lock",
          );
        } else if (input.inputs) {
          NodeAssert.equal(
            packageIdentity.sha256,
            input.inputs[NodePath.relative(root, real)],
            "workspace manifest or patch input identity differs",
          );
        }
        break;
      }
      directory = NodePath.dirname(directory);
    }
    records.set(path, { path, owner: root, sha256: sha(path), packageIdentity });
  }
  const hooks = NodeModule.registerHooks({
    resolve(specifier, context, nextResolve) {
      const result = nextResolve(specifier, context);
      if (result.url.startsWith("node:")) return result;
      NodeAssert.ok(result.url.startsWith("file:"), "non-file dependency resolution refused");
      const resolved = NodeFS.realpathSync(NodeURL.fileURLToPath(result.url));
      const targetRoot = owner(resolved);
      NodeAssert.ok(targetRoot, "dependency resolved outside all bound checkouts");
      const importer = context.parentURL?.startsWith("file:")
        ? NodeFS.realpathSync(NodeURL.fileURLToPath(context.parentURL))
        : undefined;
      const importerRoot = importer && owner(importer);
      const explicitSourceBridge =
        bridges.includes(importer) &&
        historicalRoots.includes(targetRoot) &&
        specifier.startsWith("file:") &&
        !inside(NodePath.join(targetRoot, "node_modules"), resolved);
      NodeAssert.ok(
        importerRoot === targetRoot || explicitSourceBridge,
        "dependency resolution escaped its source owner",
      );
      capture(resolved, targetRoot);
      return result;
    },
    load(url, context, nextLoad) {
      if (url.startsWith("file:")) {
        const path = NodeFS.realpathSync(NodeURL.fileURLToPath(url)),
          root = owner(path);
        NodeAssert.ok(root, "module load escaped source owner");
        capture(path, root);
      }
      return nextLoad(url, context);
    },
  });
  return {
    receipt() {
      for (const input of sourceInputs)
        NodeAssert.equal(
          sha(NodePath.join(input.root, "pnpm-lock.yaml")),
          input.lockSha256,
          "frozen lock changed during imports",
        );
      for (const record of records.values()) {
        NodeAssert.equal(sha(record.path), record.sha256, "resolved module changed");
        if (record.packageIdentity)
          NodeAssert.equal(
            sha(record.packageIdentity.path),
            record.packageIdentity.sha256,
            "resolved package identity changed",
          );
      }
      return {
        schema: "jones-historical-resolution/v1",
        roots,
        inputBindings: sourceInputs,
        records: [...records.values()],
        packagedBuild: "unqualified",
      };
    },
    close: () => hooks.deregister(),
  };
}
