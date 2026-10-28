import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { executableIdentity, stampedIdentity } from "./commandIdentity.mjs";

test("SEA manifests match production entry framing and reject a different executable", () => {
  const bytes = Buffer.from("synthetic executable A");
  const fileDigest = createHash("sha256").update(bytes).digest("hex");
  const production = createHash("sha256")
    .update(JSON.stringify({ path: "entry", sha256: fileDigest }) + "\n")
    .digest("hex");
  assert.equal(executableIdentity(bytes), production);
  assert.notEqual(executableIdentity(bytes), fileDigest);
  assert.notEqual(executableIdentity(Buffer.from("synthetic executable B")), production);
});
test("bundle and Electron entries extract the stamped build without rehashing", () => {
  const id = "a".repeat(64);
  for (const entry of ["bundle", "electron"]) {
    assert.equal(stampedIdentity(Buffer.from(`// jones-server-compute-build:${id}\n${entry}`)), id);
  }
  assert.equal(stampedIdentity(Buffer.from("unstamped")), undefined);
});

test("actual manifest CLI binds the exact SEA bytes; wrong bytes and missing bundle stamps fail closed", async () => {
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
    assert.equal(manifest.build, executableIdentity(Buffer.from("executable A")));
    await writeFile(executable, "executable B");
    await execute(process.execPath, [cli, "sea", executable, "-", b]);
    assert.notEqual(JSON.parse(await readFile(b, "utf8")).build, manifest.build);
    await assert.rejects(
      execute(process.execPath, [
        cli,
        "bundle",
        process.execPath,
        executable,
        join(root, "invalid.json"),
      ]),
    );
    await assert.rejects(readFile(join(root, "invalid.json")), { code: "ENOENT" });
    for (const form of ["bundle", "electron"]) {
      const stamp = "b".repeat(64),
        entry = join(root, `${form}.mjs`),
        output = join(root, `${form}.json`);
      await writeFile(entry, `// jones-server-compute-build:${stamp}\n`);
      await execute(process.execPath, [cli, form, process.execPath, entry, output]);
      assert.equal(JSON.parse(await readFile(output, "utf8")).build, stamp);
    }
  } finally {
    await rm(root, { recursive: true });
  }
});
