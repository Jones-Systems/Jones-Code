// @effect-diagnostics nodeBuiltinImport:off -- Runs the pinned Electron executable as Node over a fixture-only scratch tree.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";

// Server-side staging removes its extracted Darwin scratch from inside the Electron binary
// running as Node. Electron patches `fs` so an `*.asar` file reads as a directory, which is what
// made recursive removal of an owned scratch fail with ENOTEMPTY. This runs the real runtime
// over a real minimal ASAR to keep that premise, and the `original-fs` removal, verified.

const isCi = ["1", "true"].includes((process.env.CI ?? "").toLowerCase());

function locateElectron(): string | undefined {
  try {
    const electron = NodeModule.createRequire(import.meta.url)("electron") as unknown;
    return typeof electron === "string" && NodeFS.existsSync(electron) ? electron : undefined;
  } catch {
    return undefined;
  }
}
const electron = locateElectron();

/** The smallest archive Electron accepts: one file, no unpacked entries. */
function minimalAsar(): Buffer {
  const payload = Buffer.from('{"name":"fixture"}');
  const header = Buffer.from(
    JSON.stringify({ files: { "package.json": { offset: "0", size: payload.length } } }),
  );
  const padded = Math.ceil(header.length / 4) * 4;
  const prefix = Buffer.alloc(16);
  prefix.writeUInt32LE(4, 0);
  prefix.writeUInt32LE(8 + padded, 4);
  prefix.writeUInt32LE(4 + padded, 8);
  prefix.writeUInt32LE(header.length, 12);
  return Buffer.concat([prefix, header, Buffer.alloc(padded - header.length), payload]);
}

async function writeScratch(scratch: string): Promise<string> {
  const resources = NodePath.join(scratch, "Jones Code.app", "Contents", "Resources");
  await NodeFSP.mkdir(resources, { recursive: true });
  const asar = NodePath.join(resources, "app.asar");
  await NodeFSP.writeFile(asar, minimalAsar());
  await NodeFSP.writeFile(NodePath.join(resources, "other.txt"), "ordinary sibling");
  return asar;
}

// Reads the patched and raw views of one archive, then removes a scratch tree both ways.
const probe = String.raw`
const patched = require('node:fs');
const raw = require('original-fs');
const fixture = JSON.parse(process.env.JONES_ASAR_FIXTURE);
(async () => {
  const report = {
    electron: process.versions.electron,
    patchedAsarIsDirectory: patched.statSync(fixture.asar).isDirectory(),
    rawAsarIsFile: raw.statSync(fixture.asar).isFile(),
  };
  try {
    await patched.promises.rm(fixture.patchedScratch, { recursive: true, force: true });
    report.patchedRm = { ok: true };
  } catch (error) {
    report.patchedRm = { ok: false, code: error.code };
  }
  await raw.promises.rm(fixture.rawScratch, { recursive: true, force: true });
  report.rawScratchRemoved = !raw.existsSync(fixture.rawScratch);
  console.log(JSON.stringify(report));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
`;

// Local runs without a provisioned Electron skip; CI provisions it and must never skip silently.
it.skipIf(electron === undefined && !isCi)(
  "removes an owned scratch holding app.asar with original-fs under Electron run-as-node",
  async () => {
    if (electron === undefined)
      throw new Error(
        "Electron is not provisioned. Run `vp run --filter @t3tools/desktop ensure:electron` before this test.",
      );
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-asar-scratch-"));
    let failed = false;
    try {
      const patchedScratch = NodePath.join(root, "patched");
      const rawScratch = NodePath.join(root, "raw");
      const asar = await writeScratch(patchedScratch);
      await writeScratch(rawScratch);
      const result = NodeChildProcess.spawnSync(electron, ["-e", probe], {
        env: {
          ...process.env,
          ELECTRON_RUN_AS_NODE: "1",
          JONES_ASAR_FIXTURE: JSON.stringify({ asar, patchedScratch, rawScratch }),
        },
        encoding: "utf8",
        timeout: 30_000,
        maxBuffer: 1024 * 1024,
      });
      expect(result.status, result.stderr).toBe(0);
      const report = JSON.parse(result.stdout.trim().split("\n").at(-1) ?? "") as {
        readonly patchedAsarIsDirectory: boolean;
        readonly rawAsarIsFile: boolean;
        readonly patchedRm: { readonly ok: boolean; readonly code?: string };
        readonly rawScratchRemoved: boolean;
      };
      expect(report.patchedAsarIsDirectory).toBe(true);
      expect(report.rawAsarIsFile).toBe(true);
      expect(report.rawScratchRemoved).toBe(true);
      await expect(NodeFSP.lstat(rawScratch)).rejects.toMatchObject({ code: "ENOENT" });
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      try {
        await NodeFSP.rm(root, { recursive: true, force: true });
        await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
      } catch (cleanup) {
        // Keep the primary failure; cleanup only fails the test when nothing else did.
        if (!failed) throw cleanup;
      }
    }
  },
);
