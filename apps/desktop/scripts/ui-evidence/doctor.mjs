import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  exec,
  electronVersion,
  runtimeDirectory,
  validateRuntime,
  dependencyClosure,
} from "./runtime.mjs";

const libraryPackages = {
  "libnss3.so": "libnss3",
  "libatk-1.0.so.0": "libatk1.0-0t64",
  "libatk-bridge-2.0.so.0": "libatk-bridge2.0-0t64",
  "libcups.so.2": "libcups2t64",
  "libdrm.so.2": "libdrm2",
  "libXcomposite.so.1": "libxcomposite1",
  "libXdamage.so.1": "libxdamage1",
  "libXrandr.so.2": "libxrandr2",
  "libgbm.so.1": "libgbm1",
  "libasound.so.2": "libasound2t64",
  "libgtk-3.so.0": "libgtk-3-0t64",
  "libX11.so.6": "libx11-6",
  "libxcb.so.1": "libxcb1",
  "libxkbcommon.so.0": "libxkbcommon0",
};
export async function doctor(source) {
  const checks = [];
  const aptPackages = new Set();
  async function check(name, fn, packages = []) {
    try {
      const detail = await fn();
      checks.push({ name, status: "passed", detail });
      return detail;
    } catch (error) {
      checks.push({ name, status: "failed", error: error.message });
      for (const pkg of packages) aptPackages.add(pkg);
      return null;
    }
  }
  await check("platform", () => {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone evidence script has no Effect runtime.
    if (process.platform !== "linux" || process.arch !== "x64")
      throw new Error("Linux x64 required");
    return "linux-x64";
  });
  await check(
    "node",
    async () => {
      const version = (await exec("/usr/bin/node", ["--version"])).stdout.trim();
      if (Number(version.slice(1).split(".")[0]) < 24) throw new Error("Node >=24 required");
      return version;
    },
    ["nodejs"],
  );
  await check("Xvfb", () => NodeFSP.access("/usr/bin/Xvfb"), ["xvfb"]);
  await check("unzip", () => NodeFSP.access("/usr/bin/unzip"), ["unzip"]);
  const bwrap = await check("bwrap", () => NodeFSP.access("/usr/bin/bwrap"), ["bubblewrap"]);
  if (bwrap !== null)
    await check("nested-user-namespace", async () => {
      const aliases = [];
      for (const name of ["bin", "sbin", "lib", "lib64"]) {
        try {
          aliases.push("--symlink", await NodeFSP.realpath(`/${name}`), `/${name}`);
        } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
      }
      await exec("/usr/bin/bwrap", [
        "--unshare-user",
        "--unshare-pid",
        "--unshare-net",
        "--die-with-parent",
        "--ro-bind",
        "/usr",
        "/usr",
        ...aliases,
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--",
        "/usr/bin/unshare",
        "--user",
        "--map-root-user",
        "/usr/bin/true",
      ]);
      return true;
    });
  await check(
    "fonts",
    async () => {
      const output = (await exec("/usr/bin/fc-list", [])).stdout;
      if (!output.trim()) throw new Error("No fonts available");
      return { available: true };
    },
    ["fontconfig", "fonts-dejavu-core"],
  );
  const version = await check("electron-source-pin", () => electronVersion(source));
  const runtime =
    version &&
    (await check("electron-cache", () => validateRuntime(runtimeDirectory(version), version)));
  if (runtime)
    await check("electron-libraries", async () => {
      const output = (await exec("/usr/bin/ldd", [runtime.executable])).stdout;
      const missing = [...output.matchAll(/^\s*(\S+)\s+=>\s+not found/gm)].map((match) => match[1]);
      for (const name of missing) if (libraryPackages[name]) aptPackages.add(libraryPackages[name]);
      if (missing.length) throw new Error(`Missing libraries: ${missing.join(", ")}`);
      return { missing: [] };
    });
  for (const relative of [
    "apps/desktop/dist-electron/boot.cjs",
    "apps/server/dist/bin.mjs",
    "apps/server/dist/client/index.html",
  ])
    await check(relative, () => NodeFSP.access(NodePath.join(source, relative)));
  await check("external-closure", async () => {
    const closure = await dependencyClosure(source);
    return { packages: closure.packages.length };
  });
  return {
    outcome: checks.every((check) => check.status === "passed") ? "complete" : "rejected",
    source,
    checks,
    aptPackages: [...aptPackages].sort(),
    runtime: runtime || null,
  };
}
