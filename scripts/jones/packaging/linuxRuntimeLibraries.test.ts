// @effect-diagnostics nodeBuiltinImport:off
import { it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { extractQualifiedLinuxArchive } from "../../../apps/server/src/jones/cloud/qualifiedArchive.ts";
import { qualifiedPayloadDigest } from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";
import { linuxLibatomicLayout } from "./linuxRuntimeLibraries.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

it("stages bundled Linux libraries through the installed updater's existing payload contract", async () => {
  const scratch = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-library-layout-"));
  try {
    const runtime = NodePath.join(scratch, "runtime");
    const destination = NodePath.join(scratch, "extracted");
    const library = linuxLibatomicLayout.file;
    await NodeFSP.mkdir(NodePath.join(runtime, linuxLibatomicLayout.directory), { recursive: true });
    await NodeFSP.mkdir(NodePath.join(runtime, "client"));
    await NodeFSP.mkdir(NodePath.join(runtime, "resource-monitor"));
    await NodeFSP.mkdir(destination);
    await NodeFSP.writeFile(NodePath.join(runtime, "t3"), "executable", { mode: 0o700 });
    await NodeFSP.writeFile(NodePath.join(runtime, "client/index.html"), "client");
    await NodeFSP.writeFile(NodePath.join(runtime, library), "libatomic fixture");
    await NodeFSP.writeFile(NodePath.join(runtime, `${library}.LICENSE`), "license fixture");

    // These are the roots accepted by already-installed updaters. Do not expand
    // that consumer contract to accommodate a new package layout.
    NodeAssert.deepEqual((await NodeFSP.readdir(runtime)).sort(), [
      "client",
      "node_modules",
      "resource-monitor",
      "t3",
    ]);
    const archive = NodePath.join(scratch, "runtime.tar.gz");
    await execFile("tar", ["--format=ustar", "-czf", archive, "-C", scratch, "runtime"], {
      env: { COPYFILE_DISABLE: "1" },
    });
    await extractQualifiedLinuxArchive(archive, destination);
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(destination, library), "utf8"),
      "libatomic fixture",
    );
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(destination, `${library}.LICENSE`), "utf8"),
      "license fixture",
    );
    NodeAssert.match(await qualifiedPayloadDigest(destination), /^[a-f0-9]{64}$/);
  } finally {
    await NodeFSP.rm(scratch, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(scratch), { code: "ENOENT" });
  }
});
