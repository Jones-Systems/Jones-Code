import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as Path from "node:path";

export function readRuntimeBinding(environment = process.env) {
  const path = environment.JONES_RUNTIME_BINDING;
  Assert.ok(typeof path === "string" && Path.isAbsolute(path), "explicit runtime binding required");
  const info = FS.lstatSync(path);
  Assert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 4096);
  const binding = JSON.parse(FS.readFileSync(path, "utf8"));
  Assert.equal(binding.schema, "jones-performance-runtime-binding/v1");
  for (const key of ["runtimeRoot", "executablePath"]) {
    Assert.equal(Path.resolve(binding[key]), binding[key]);
    Assert.equal(FS.realpathSync(binding[key]), binding[key]);
  }
  Assert.ok(FS.statSync(binding.runtimeRoot).isDirectory());
  Assert.match(binding.nodeVersion, /^\d+\.\d+\.\d+$/);
  Assert.match(binding.executableSha256, /^[a-f0-9]{64}$/);
  const executable = FS.lstatSync(binding.executablePath);
  Assert.ok(executable.isFile() && executable.size <= 256 * 1024 * 1024);
  FS.accessSync(binding.executablePath, FS.constants.X_OK);
  Assert.equal(Crypto.createHash("sha256").update(FS.readFileSync(binding.executablePath)).digest("hex"), binding.executableSha256);
  return Object.freeze(binding);
}
