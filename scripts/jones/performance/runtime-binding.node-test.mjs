import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { readRuntimeBinding } from "./runtime-binding.mjs";
import { syntheticSourceParent } from "./sources.mjs";

NodeTest.test(
  "historical sources require an explicit parent instead of a host-specific fallback",
  () => {
    NodeAssert.throws(() => syntheticSourceParent({}), /source parent/);
  },
);

NodeTest.test(
  "runtime binding requires an explicit executable identity and rejects changed bytes",
  () => {
    NodeAssert.throws(() => readRuntimeBinding({}), /explicit runtime binding/);
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-runtime-binding-"));
    try {
      const executablePath = NodePath.join(NodeFS.realpathSync(root), "node");
      NodeFS.writeFileSync(executablePath, "synthetic executable", { mode: 0o700 });
      const binding = {
        schema: "jones-performance-runtime-binding/v1",
        runtimeRoot: NodeFS.realpathSync(root),
        executablePath,
        nodeVersion: "26.8.2",
        executableSha256: NodeCrypto.createHash("sha256")
          .update("synthetic executable")
          .digest("hex"),
      };
      const file = NodePath.join(root, "binding.json");
      NodeFS.writeFileSync(file, JSON.stringify(binding));
      NodeAssert.deepEqual(readRuntimeBinding({ JONES_RUNTIME_BINDING: file }), binding);
      NodeFS.appendFileSync(executablePath, "changed");
      NodeAssert.throws(() => readRuntimeBinding({ JONES_RUNTIME_BINDING: file }));
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);
