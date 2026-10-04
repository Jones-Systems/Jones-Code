import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import * as Test from "node:test";
import { readRuntimeBinding } from "./runtime-binding.mjs";
import { syntheticSourceParent } from "./sources.mjs";

Test.test("historical sources require an explicit parent instead of a host-specific fallback", () => {
  Assert.throws(() => syntheticSourceParent({}), /source parent/);
});

Test.test("runtime binding requires an explicit executable identity and rejects changed bytes", () => {
  Assert.throws(() => readRuntimeBinding({}), /explicit runtime binding/);
  const root = FS.mkdtempSync(Path.join(OS.tmpdir(), "jones-runtime-binding-"));
  try {
    const executablePath = Path.join(FS.realpathSync(root), "node");
    FS.writeFileSync(executablePath, "synthetic executable", { mode: 0o700 });
    const binding = { schema: "jones-performance-runtime-binding/v1", runtimeRoot: FS.realpathSync(root), executablePath, nodeVersion: "26.8.2", executableSha256: Crypto.createHash("sha256").update("synthetic executable").digest("hex") };
    const file = Path.join(root, "binding.json");
    FS.writeFileSync(file, JSON.stringify(binding));
    Assert.deepEqual(readRuntimeBinding({ JONES_RUNTIME_BINDING: file }), binding);
    FS.appendFileSync(executablePath, "changed");
    Assert.throws(() => readRuntimeBinding({ JONES_RUNTIME_BINDING: file }));
  } finally { FS.rmSync(root, { recursive: true, force: true }); }
});
