import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  assertLoopbackEndpoint,
  loadScenario,
  pngDimensions,
  validateCaptureName,
} from "./scenario-api.mjs";
import { sha256 } from "./provenance.mjs";

NodeTest.test(
  "scenario loader records exact content and rejects missing default function",
  async () => {
    const parent = NodePath.resolve(
      NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
      "../../../../.t3/ui-evidence-test-scratch",
    );
    await NodeFSP.mkdir(parent, { recursive: true });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "scenario-"));
    try {
      const file = NodePath.join(root, "scenario.mjs");
      const bytes = "export default async () => 42;\n";
      await NodeFSP.writeFile(file, bytes);
      const loaded = await loadScenario(file);
      NodeAssert.equal(loaded.identity.sha256, sha256(bytes));
      NodeAssert.equal(await loaded.run(), 42);
      const invalid = NodePath.join(root, "invalid.mjs");
      await NodeFSP.writeFile(invalid, "export const other = 1;\n");
      await NodeAssert.rejects(loadScenario(invalid), /default async function/);
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);
NodeTest.test("endpoints and capture paths stay bounded", () => {
  NodeAssert.equal(assertLoopbackEndpoint("http://127.0.0.1:1234").port, "1234");
  for (const endpoint of [
    "https://example.com",
    "http://127.0.0.1:1?token=secret",
    "http://user:pass@localhost",
  ])
    NodeAssert.throws(() => assertLoopbackEndpoint(endpoint));
  NodeAssert.equal(validateCaptureName("after-reload"), "after-reload");
  for (const name of ["../secret", "/absolute", "", "a.png"])
    NodeAssert.throws(() => validateCaptureName(name));
});
NodeTest.test("PNG metadata uses encoded physical dimensions", () => {
  const bytes = Buffer.alloc(24);
  Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]).copy(bytes);
  bytes.write("IHDR", 12);
  bytes.writeUInt32BE(1280, 16);
  bytes.writeUInt32BE(800, 20);
  NodeAssert.deepEqual(pngDimensions(bytes), { width: 1280, height: 800 });
  NodeAssert.throws(() => pngDimensions(Buffer.from("not a PNG")));
});
