import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import { executableIdentity, stampedIdentity } from "./commandIdentity.mjs";

NodeTest.test("SEA manifests match production entry framing and reject a different executable", () => {
  const bytes = Buffer.from("synthetic executable A");
  const fileDigest = NodeCrypto.createHash("sha256").update(bytes).digest("hex");
  const production = NodeCrypto.createHash("sha256")
    .update(JSON.stringify({ path: "entry", sha256: fileDigest }) + "\n")
    .digest("hex");
  NodeAssert.equal(executableIdentity(bytes), production);
  NodeAssert.notEqual(executableIdentity(bytes), fileDigest);
  NodeAssert.notEqual(executableIdentity(Buffer.from("synthetic executable B")), production);
});
NodeTest.test("bundle and Electron entries extract the stamped build without rehashing", () => {
  const id = "a".repeat(64);
  for (const entry of ["bundle", "electron"]) {
    NodeAssert.equal(stampedIdentity(Buffer.from(`// jones-server-compute-build:${id}\n${entry}`)), id);
  }
  NodeAssert.equal(stampedIdentity(Buffer.from("unstamped")), undefined);
});

NodeTest.test("actual manifest CLI binds the exact SEA bytes; wrong bytes and missing bundle stamps fail closed", async () => {
  const { mkdtemp, writeFile, readFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const { fileURLToPath } = await import("node:url");
  const execute = promisify(execFile);
  const root = await mkdtemp(join(tmpdir(), "jones-sp-manifest-test-"));
  try {
    const executable = join(root, "synthetic-sea-bytes"),
      a = join(root, "a.json"),
      b = join(root, "b.json");
    const cli = fileURLToPath(new URL("./artifact-command.mjs", import.meta.url));
    await writeFile(executable, "executable A");
    await execute(process.execPath, [cli, "sea", executable, "-", a]);
    const manifest = JSON.parse(await readFile(a, "utf8"));
    NodeAssert.equal(manifest.build, executableIdentity(Buffer.from("executable A")));
    await writeFile(executable, "executable B");
    await execute(process.execPath, [cli, "sea", executable, "-", b]);
    NodeAssert.notEqual(JSON.parse(await readFile(b, "utf8")).build, manifest.build);
    await NodeAssert.rejects(
      execute(process.execPath, [
        cli,
        "bundle",
        process.execPath,
        executable,
        join(root, "invalid.json"),
      ]),
    );
    await NodeAssert.rejects(readFile(join(root, "invalid.json")), { code: "ENOENT" });
    for (const form of ["bundle", "electron"]) {
      const stamp = "b".repeat(64),
        entry = join(root, `${form}.mjs`),
        output = join(root, `${form}.json`);
      await writeFile(entry, `// jones-server-compute-build:${stamp}\n`);
      await execute(process.execPath, [cli, form, process.execPath, entry, output]);
      NodeAssert.equal(JSON.parse(await readFile(output, "utf8")).build, stamp);
    }
  } finally {
    await rm(root, { recursive: true });
  }
});
