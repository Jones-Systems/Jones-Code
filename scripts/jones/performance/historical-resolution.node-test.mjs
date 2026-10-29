import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

const helper = NodeURL.fileURLToPath(new URL("./historical-resolution.mjs", import.meta.url));
function exercise(t, mode, worker = "migration-restore-worker.mjs") {
  const base = NodeFS.mkdtempSync(NodePath.join(process.env.TMPDIR ?? NodeOS.tmpdir(), "hr-"));
  const identity = NodeFS.statSync(base);
  let child;
  t.after(() => {
    if (child?.error || child?.status === null) {
      t.diagnostic(`retained synthetic resolution root; child closure unproved: ${base}`);
      return;
    }
    const current = NodeFS.lstatSync(base);
    NodeAssert.equal(current.dev, identity.dev);
    NodeAssert.equal(current.ino, identity.ino);
    NodeAssert.equal(current.uid, identity.uid);
    NodeFS.rmSync(base, { recursive: true });
  });
  const receiving = NodePath.join(base, "receiving"),
    history = NodePath.join(base, "history");
  for (const root of [receiving, history]) {
    NodeFS.mkdirSync(NodePath.join(root, "apps/server"), { recursive: true });
    NodeFS.writeFileSync(
      NodePath.join(root, "apps/server/package.json"),
      '{"name":"server","type":"module"}',
    );
    NodeFS.mkdirSync(NodePath.join(root, "node_modules/owned"), { recursive: true });
    NodeFS.writeFileSync(NodePath.join(root, "package.json"), '{"name":"source","type":"module"}');
    NodeFS.writeFileSync(
      NodePath.join(root, "node_modules/owned/package.json"),
      '{"name":"owned","version":"1.0.0","main":"index.cjs"}',
    );
    NodeFS.writeFileSync(
      NodePath.join(root, "node_modules/owned/index.cjs"),
      'module.exports = "private";',
    );
  }
  for (const root of [receiving, history]) {
    const target = NodePath.join(root, "node_modules/.pnpm/owned@1.0.0/node_modules/owned");
    NodeFS.mkdirSync(NodePath.dirname(target), { recursive: true });
    NodeFS.renameSync(NodePath.join(root, "node_modules/owned"), target);
    NodeFS.symlinkSync(
      ".pnpm/owned@1.0.0/node_modules/owned",
      NodePath.join(root, "node_modules/owned"),
    );
  }
  for (const root of [receiving, history])
    NodeFS.writeFileSync(
      NodePath.join(root, "pnpm-lock.yaml"),
      "lockfileVersion: '9.0'\npackages:\n  owned@1.0.0:\n    resolution: {}\nsnapshots: {}\n",
    );
  const sourceInputs = [receiving, history].map((root) => ({
    root,
    lockSha256: NodeCrypto.createHash("sha256")
      .update(NodeFS.readFileSync(NodePath.join(root, "pnpm-lock.yaml")))
      .digest("hex"),
  }));
  if (mode === "mismatch")
    NodeFS.writeFileSync(
      NodePath.join(history, "node_modules/owned/package.json"),
      '{"name":"owned","version":"9.0.0","main":"index.cjs"}',
    );
  if (["sibling", "dependency-sibling"].includes(mode)) {
    NodeFS.rmSync(NodePath.join(history, "node_modules/owned"), { recursive: true });
    NodeFS.symlinkSync(
      NodePath.join(receiving, "node_modules/owned"),
      NodePath.join(history, "node_modules/owned"),
    );
  }
  if (["parent", "dependency-parent"].includes(mode)) {
    NodeFS.renameSync(NodePath.join(history, "node_modules"), NodePath.join(base, "node_modules"));
  }
  if (mode.includes("nested"))
    NodeFS.renameSync(
      NodePath.join(history, "node_modules"),
      NodePath.join(history, "apps/server/node_modules"),
    );
  const createRequireOnly = mode === "require-sibling";
  if (createRequireOnly) {
    NodeFS.rmSync(NodePath.join(history, "node_modules/owned"), { recursive: true });
    NodeFS.symlinkSync(
      NodePath.join(receiving, "node_modules/owned"),
      NodePath.join(history, "node_modules/owned"),
    );
  }
  NodeFS.writeFileSync(
    NodePath.join(history, "entry.mjs"),
    `${createRequireOnly ? "" : 'import value from "owned";'}
import { createRequire } from "node:module";
export default createRequire(import.meta.url)("owned");`,
  );
  const bridge = NodePath.join(receiving, `scripts/jones/performance/${worker}`);
  NodeFS.mkdirSync(NodePath.dirname(bridge), { recursive: true });
  NodeFS.writeFileSync(
    bridge,
    mode.startsWith("dependency")
      ? `import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
export async function load(guard) {
 const require = createRequire(${JSON.stringify(`${history}/apps/server/package.json`)});
 const url = ${mode.startsWith("dependency-unbound") ? "undefined" : `guard.dependencyURL?.(${JSON.stringify(history)}, "owned", ${mode === "dependency-wrong-caller" ? JSON.stringify(NodeURL.pathToFileURL(NodePath.join(receiving, "driver.mjs")).href) : "import.meta.url"})`} ?? pathToFileURL(require.resolve("owned")).href;
 return import(url);
}`
      : `export const loaded = await import(${JSON.stringify(`file://${history}/entry.mjs`)});`,
  );
  const driver = NodePath.join(receiving, "driver.mjs");
  NodeFS.writeFileSync(
    driver,
    `import { installHistoricalResolutionGuard } from ${JSON.stringify(`file://${helper}`)};
import * as NodeFS from "node:fs";
const guard = installHistoricalResolutionGuard({ receivingRoot: ${JSON.stringify(receiving)}, historicalRoots: [${JSON.stringify(history)}], sourceInputs: ${JSON.stringify(sourceInputs)}, maxRecords: 100, maxFileBytes: 65536 });
try {
 const worker = await import(${JSON.stringify(`./scripts/jones/performance/${worker}`)});
 ${mode.startsWith("dependency") ? "await worker.load(guard);" : ""}
 ${mode === "changed" ? `NodeFS.appendFileSync(${JSON.stringify(NodePath.join(history, "node_modules/owned/index.cjs"))}, "// changed");` : ""}
 console.log(JSON.stringify(guard.receipt()));
} finally { guard.close(); }`,
  );
  child = NodeChildProcess.spawnSync(process.execPath, [driver], {
    encoding: "utf8",
    env: { PATH: "/usr/bin:/bin" },
    timeout: 5000,
    maxBuffer: 65536,
  });
  return child;
}
NodeTest.test(
  "ESM and createRequire observe each package's private provenance without build attestation",
  (t) => {
    const child = exercise(t, "private");
    NodeAssert.equal(child.status, 0, child.stderr);
    const receipt = JSON.parse(child.stdout);
    NodeAssert.equal(receipt.packagedBuild, "unqualified");
    const owned = receipt.records.find(({ packageIdentity }) => packageIdentity?.name === "owned");
    NodeAssert.equal(owned.packageIdentity.version, "1.0.0");
    NodeAssert.match(owned.sha256, /^[a-f0-9]{64}$/);
  },
);
NodeTest.test(
  "parent/global fallback, ESM sibling links and createRequire sibling links refuse before evaluation",
  (t) => {
    for (const mode of ["parent", "sibling", "require-sibling"]) {
      const child = exercise(t, mode);
      NodeAssert.notEqual(child.status, 0, mode);
      NodeAssert.match(child.stderr, /outside all bound checkouts|escaped its source owner/, mode);
    }
  },
);
NodeTest.test(
  "resolved file or package replacement invalidates the observed-import receipt",
  (t) => {
    const child = exercise(t, "changed");
    NodeAssert.notEqual(child.status, 0);
    NodeAssert.match(child.stderr, /resolved module changed/);
  },
);

NodeTest.test("resolved package version must match the immutable lock inventory", (t) => {
  const child = exercise(t, "mismatch");
  NodeAssert.notEqual(child.status, 0);
  NodeAssert.match(child.stderr, /package version differs from frozen lock/);
});

NodeTest.test("both receiving workers import source-local require.resolve dependency URLs", (t) => {
  for (const worker of ["fixtures-historical-worker.mjs", "migration-restore-worker.mjs"]) {
    const child = exercise(t, "dependency", worker);
    NodeAssert.equal(child.status, 0, child.stderr);
    const receipt = JSON.parse(child.stdout);
    NodeAssert.equal(receipt.dependencyBridges.length, 1);
    NodeAssert.equal(receipt.dependencyBridges[0].specifier, "owned");
    NodeAssert.ok(receipt.dependencyBridges[0].sourceRoot.endsWith("/history"));
    NodeAssert.ok(
      receipt.records.some(
        (record) => record.owner.endsWith("/history") && record.packageIdentity?.name === "owned",
      ),
    );
  }
});

NodeTest.test(
  "explicit dependency bridges refuse sibling/parent fallback, unbound URLs and wrong callers",
  (t) => {
    for (const mode of [
      "dependency-sibling",
      "dependency-parent",
      "dependency-unbound",
      "dependency-wrong-caller",
      "dependency-unbound-nested",
    ]) {
      const child = exercise(t, mode);
      NodeAssert.notEqual(child.status, 0, mode);
      NodeAssert.match(
        child.stderr,
        /outside all bound checkouts|escaped its source owner|not a bound worker/,
        mode,
      );
    }
  },
);
