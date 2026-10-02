import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import { checksumFor } from "./runtime.mjs";

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
