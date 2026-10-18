import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export function readRuntimeBinding(environment = process.env) {
  const path = environment.JONES_RUNTIME_BINDING;
  NodeAssert.ok(
    typeof path === "string" && NodePath.isAbsolute(path),
    "explicit runtime binding required",
  );
  const info = NodeFS.lstatSync(path);
  NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 4096);
  const binding = JSON.parse(NodeFS.readFileSync(path, "utf8"));
  NodeAssert.equal(binding.schema, "jones-performance-runtime-binding/v1");
  for (const key of ["runtimeRoot", "executablePath"]) {
    NodeAssert.equal(NodePath.resolve(binding[key]), binding[key]);
    NodeAssert.equal(NodeFS.realpathSync(binding[key]), binding[key]);
  }
  NodeAssert.ok(NodeFS.statSync(binding.runtimeRoot).isDirectory());
  NodeAssert.match(binding.nodeVersion, /^\d+\.\d+\.\d+$/);
  NodeAssert.match(binding.executableSha256, /^[a-f0-9]{64}$/);
  const executable = NodeFS.lstatSync(binding.executablePath);
  NodeAssert.ok(executable.isFile() && executable.size <= 256 * 1024 * 1024);
  NodeFS.accessSync(binding.executablePath, NodeFS.constants.X_OK);
  NodeAssert.equal(
    NodeCrypto.createHash("sha256")
      .update(NodeFS.readFileSync(binding.executablePath))
      .digest("hex"),
    binding.executableSha256,
  );
  return Object.freeze(binding);
}
