import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { checksumFor, runtimeExternalDependencies } from "./runtime.mjs";

NodeTest.test("official checksum accepts binary and text filename markers", () => {
  const digest = "ad71063e77da29a647f872b090478f9fa9ccffd67de7b2ae6315409cf1f13436";
  for (const marker of [" ", "*"])
    NodeAssert.equal(
      checksumFor(
        `${digest} ${marker}electron-v44.4.2-linux-x64.zip\n`,
        "electron-v44.4.2-linux-x64.zip",
      ),
      digest,
    );
  NodeAssert.throws(() =>
    checksumFor(`${digest} *different.zip`, "electron-v44.4.2-linux-x64.zip"),
  );
  NodeAssert.throws(() => checksumFor("invalid *electron.zip", "electron.zip"));
});

NodeTest.test("runtime closure uses nightly canonical prefix and exact-boundary policies", async () => {
  const source = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "../../../..");
  NodeAssert.deepEqual(await runtimeExternalDependencies(source, "server", {
    "node-pty": "1", "@cursor/sdk": "1", "zod": "1", "zod-to-json-schema": "1",
    "@connectrpc/connect": "1", "react": "1",
  }), ["node-pty", "@cursor/sdk", "zod", "@connectrpc/connect"]);
  NodeAssert.deepEqual(await runtimeExternalDependencies(source, "desktop", {
    "playwright-core": "1", "@napi-rs/keyring": "1", "electron": "1", "react": "1",
  }), ["playwright-core", "@napi-rs/keyring"]);
  await NodeAssert.rejects(runtimeExternalDependencies(source, "unknown", {}), /Unknown runtime app/);
});
