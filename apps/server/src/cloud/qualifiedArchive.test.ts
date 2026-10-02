// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { extractQualifiedLinuxArchive } from "./qualifiedArchive.ts";

type Entry = { name: string; kind?: string; data?: string; link?: string };
function archive(entries: Entry[]) {
  const buffers: Buffer[] = [];
  for (const entry of entries) {
    const bytes = Buffer.from(entry.data ?? "");
    const header = Buffer.alloc(512);
    header.write(entry.name, 0, 100);
    header.write("0000700\0", 100);
    header.write(`${bytes.length.toString(8).padStart(11, "0")}\0`, 124);
    header.fill(32, 148, 156);
    header.write(entry.kind ?? "0", 156);
    header.write(entry.link ?? "", 157, 100);
    header.write("ustar\0", 257);
    const sum = header.reduce((total, byte) => total + byte, 0);
    header.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148);
    buffers.push(header, bytes, Buffer.alloc((512 - (bytes.length % 512)) % 512));
  }
  return NodeZlib.gzipSync(Buffer.concat([...buffers, Buffer.alloc(1024)]));
}
async function extract<A>(
  entries: Entry[],
  body: (destination: string, run: () => Promise<void>) => Promise<A>,
): Promise<A> {
  const scratch = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-archive-test-"));
  try {
    const destination = NodePath.join(scratch, "payload");
    await NodeFSP.mkdir(destination);
    const file = NodePath.join(scratch, "runtime.tar.gz");
    await NodeFSP.writeFile(file, archive(entries));
    return await body(destination, () => extractQualifiedLinuxArchive(file, destination));
  } finally {
    await NodeFSP.rm(scratch, { recursive: true, force: true });
  }
}

it("extracts a bounded Linux runtime with contained relative symlinks", async () => {
  await extract(
    [
      { name: "runtime/", kind: "5" },
      { name: "runtime/t3", data: "executable" },
      { name: "runtime/client/index.html", data: "fixture" },
      { name: "runtime/client/current", kind: "2", link: "index.html" },
    ],
    async (directory, run) => {
      await run();
      assert.equal(await NodeFSP.readFile(NodePath.join(directory, "t3"), "utf8"), "executable");
      assert.equal(
        await NodeFSP.readFile(NodePath.join(directory, "client", "current"), "utf8"),
        "fixture",
      );
    },
  );
});

it("rejects traversal, multiple roots, special files and escaping symlinks", async () => {
  for (const malicious of [
    { name: "runtime/../escape", data: "bad" },
    { name: "/runtime/t3", data: "bad" },
    { name: "other/t3", data: "bad" },
    { name: "runtime/client/device", kind: "3" },
    { name: "runtime/client/link", kind: "2", link: "../../outside" },
    { name: "runtime/unknown", data: "bad" },
  ])
    await extract([{ name: "runtime/", kind: "5" }, malicious], async (_directory, run) => {
      await NodeAssert.rejects(run());
    });
});

it("rejects files written through previously extracted symlink directories", async () => {
  await extract(
    [
      { name: "runtime/", kind: "5" },
      { name: "runtime/client/", kind: "5" },
      { name: "runtime/node_modules/link", kind: "2", link: "../client" },
      { name: "runtime/node_modules/link/file", data: "bad" },
    ],
    async (directory, run) => {
      await NodeAssert.rejects(run(), /writes through a symlink/);
      await NodeAssert.rejects(NodeFSP.access(NodePath.join(directory, "client", "file")));
    },
  );
});
