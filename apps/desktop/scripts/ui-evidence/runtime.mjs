import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodeStreamPromises from "node:stream/promises";
import * as NodeStream from "node:stream";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import { allocateRun, removeRun } from "./lifecycle.mjs";

export const exec = NodeUtil.promisify(NodeChildProcess.execFile);
export async function sha256(file) {
  const digest = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(file)) digest.update(chunk);
  return digest.digest("hex");
}
export async function electronVersion(source) {
  const pkg = JSON.parse(
    await NodeFSP.readFile(NodePath.join(source, "apps/desktop/package.json"), "utf8"),
  );
  const version = pkg.dependencies?.electron;
  if (!/^\d+\.\d+\.\d+$/.test(version || ""))
    throw new Error("Electron version is not an exact source pin");
  const lock = await NodeFSP.readFile(NodePath.join(source, "pnpm-lock.yaml"), "utf8");
  if (!lock.includes(`electron@${version}`))
    throw new Error("Electron package and lockfile disagree");
  return version;
}
export function runtimeDirectory(version) {
  return NodePath.join(
    process.env.XDG_CACHE_HOME || NodePath.join(NodeOS.homedir(), ".cache"),
    "jones-code-ui-evidence/electron",
    version,
  );
}
export async function validateRuntime(directory, version) {
  const record = JSON.parse(
    await NodeFSP.readFile(NodePath.join(directory, "verified.json"), "utf8"),
  );
  if (
    record.version !== version ||
    record.platform !== "linux-x64" ||
    !/^[a-f0-9]{64}$/.test(record.archiveSha256 || "")
  )
    throw new Error("Runtime verification record mismatch");
  if ((await sha256(NodePath.join(directory, "electron.zip"))) !== record.archiveSha256)
    throw new Error("Runtime archive digest mismatch");
  for (const [file, digest] of Object.entries(record.files || {})) {
    if (
      !file ||
      NodePath.isAbsolute(file) ||
      file.split("/").includes("..") ||
      !/^[a-f0-9]{64}$/.test(digest)
    )
      throw new Error("Invalid runtime tree digest");
    if ((await sha256(NodePath.join(directory, file))) !== digest)
      throw new Error(`Runtime file digest mismatch: ${file}`);
  }
  if (!record.files?.electron || !record.files?.["resources/default_app.asar"])
    throw new Error("Runtime tree digest is incomplete");
  return {
    version,
    directory,
    executable: NodePath.join(directory, "electron"),
    archiveSha256: record.archiveSha256,
  };
}
export function checksumFor(sums, name) {
  const entry = sums
    .split("\n")
    .map((line) => line.trim().split(/\s+/))
    .find((parts) => parts[1]?.replace(/^\*/, "") === name);
  if (!/^[a-f0-9]{64}$/.test(entry?.[0] || "")) throw new Error("Official checksum entry missing");
  return entry[0];
}
async function download(url, destination, signal) {
  let current = url;
  for (let redirects = 0; redirects < 6; redirects++) {
    const target = new URL(current);
    if (
      target.protocol !== "https:" ||
      ![
        "github.com",
        "release-assets.githubusercontent.com",
        "objects.githubusercontent.com",
      ].includes(target.hostname)
    )
      throw new Error("Runtime download left official HTTPS release hosts");
    const response = await fetch(current, {
      redirect: "manual",
      signal: AbortSignal.any([signal, AbortSignal.timeout(120000)]),
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      current = new URL(response.headers.get("location"), current).href;
      continue;
    }
    if (!response.ok || !response.body)
      throw new Error(`Runtime download failed: HTTP ${response.status}`);
    await NodeStreamPromises.pipeline(
      NodeStream.Readable.fromWeb(response.body),
      NodeFS.createWriteStream(destination, { mode: 0o600, flags: "wx" }),
    );
    return;
  }
  throw new Error("Too many runtime download redirects");
}
async function treeDigests(root, prefix = "") {
  const files = {};
  for (const entry of await NodeFSP.readdir(NodePath.join(root, prefix), { withFileTypes: true })) {
    const name = NodePath.posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`Runtime archive contained a symlink: ${name}`);
    if (entry.isDirectory()) Object.assign(files, await treeDigests(root, name));
    else if (entry.isFile()) files[name] = await sha256(NodePath.join(root, name));
    else throw new Error("Runtime archive contained a special file");
  }
  return files;
}
export async function setupRuntime(source) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone evidence script has no Effect runtime.
  if (process.platform !== "linux" || process.arch !== "x64")
    throw new Error("Only Linux x64 runtime setup is supported");
  await NodeFSP.access("/usr/bin/unzip");
  const version = await electronVersion(source);
  const directory = runtimeDirectory(version);
  try {
    await NodeFSP.stat(directory);
    return { outcome: "no_change", runtime: await validateRuntime(directory, version) };
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const run = await allocateRun();
  let cancellation;
  const controller = new AbortController();
  const cancel = (signal) => {
    cancellation = signal;
    controller.abort();
  };
  const interrupt = () => cancel("SIGINT");
  const terminate = () => cancel("SIGTERM");
  process.on("SIGINT", interrupt);
  process.on("SIGTERM", terminate);
  try {
    const base = `https://github.com/electron/electron/releases/download/v${version}`;
    const zip = NodePath.join(run.root, "electron.zip");
    await download(
      `${base}/SHASUMS256.txt`,
      NodePath.join(run.root, "SHASUMS256.txt"),
      controller.signal,
    );
    const name = `electron-v${version}-linux-x64.zip`;
    const sums = await NodeFSP.readFile(NodePath.join(run.root, "SHASUMS256.txt"), "utf8");
    const expected = checksumFor(sums, name);
    await download(`${base}/${name}`, zip, controller.signal);
    if (cancellation) throw new Error(`Cancelled: ${cancellation}`);
    if ((await sha256(zip)) !== expected) throw new Error("Official runtime checksum mismatch");
    const listing = (await exec("/usr/bin/unzip", ["-Z1", zip])).stdout.trim().split("\n");
    if (
      listing.some(
        (name) =>
          NodePath.isAbsolute(name) || name.split("/").includes("..") || name.includes("\\"),
      )
    )
      throw new Error("Unsafe runtime archive path");
    const staged = NodePath.join(run.root, "runtime");
    await NodeFSP.mkdir(staged, { mode: 0o700 });
    await exec("/usr/bin/unzip", ["-q", zip, "-d", staged], { signal: controller.signal });
    const files = await treeDigests(staged);
    await NodeFSP.rename(zip, NodePath.join(staged, "electron.zip"));
    await NodeFSP.writeFile(
      NodePath.join(staged, "verified.json"),
      JSON.stringify({ version, platform: "linux-x64", archiveSha256: expected, files }),
      { mode: 0o600 },
    );
    await NodeFSP.mkdir(NodePath.dirname(directory), { recursive: true, mode: 0o700 });
    if (cancellation) throw new Error(`Cancelled: ${cancellation}`);
    await NodeFSP.rename(staged, directory);
    return { outcome: "complete", runtime: await validateRuntime(directory, version) };
  } finally {
    await removeRun(run);
    process.off("SIGINT", interrupt);
    process.off("SIGTERM", terminate);
  }
}

export async function packageAt(name, from) {
  let current = from;
  while (true) {
    const candidate = NodePath.join(current, "node_modules", name);
    try {
      return await NodeFSP.realpath(candidate);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const parent = NodePath.dirname(current);
    if (parent === current) throw new Error(`Missing runtime package: ${name}`);
    current = parent;
  }
}
export async function runtimeExternalDependencies(source, app, dependencies) {
  if (!["desktop", "server"].includes(app)) throw new Error("Unknown runtime app");
  const desktop = app === "desktop";
  const file = NodePath.join(
    source,
    "scripts/lib",
    desktop ? "desktop-external-packages.ts" : "cli-external-packages.ts",
  );
  // Node >=24 loads these pure source policies, including exact package-boundary rules.
  const policy = await import(NodeURL.pathToFileURL(file).href);
  const select = desktop
    ? policy.selectDesktopRuntimeExternalDependencies
    : policy.selectCliRuntimeExternalDependencies;
  if (typeof select !== "function")
    throw new Error("Cannot resolve canonical runtime external policy");
  return Object.keys(select(dependencies));
}
export async function dependencyClosure(source) {
  const roots = [];
  const links = new Map();
  const packages = new Map();
  for (const app of ["desktop", "server"]) {
    const dir = NodePath.join(source, "apps", app);
    const pkg = JSON.parse(await NodeFSP.readFile(NodePath.join(dir, "package.json"), "utf8"));
    const names = await runtimeExternalDependencies(source, app, pkg.dependencies || {});
    for (const name of names) {
      const root = await packageAt(name, dir);
      roots.push({ name, root, destination: `/app/apps/${app}/node_modules/${name}` });
    }
  }
  // Playwright drives Electron from the harness, without importing the Electron package helper.
  roots.push({
    name: "playwright-core",
    root: await packageAt("playwright-core", NodePath.join(source, "apps/desktop")),
    destination: "/harness/node_modules/playwright-core",
  });
  async function visit(root) {
    if (packages.has(root)) return;
    if (
      !root.includes("/node_modules/") ||
      root.split("/").some((part) => [".t3", ".git", ".ssh", ".codex", ".claude"].includes(part))
    )
      throw new Error("Runtime dependency escaped package storage");
    const pkg = JSON.parse(await NodeFSP.readFile(NodePath.join(root, "package.json"), "utf8"));
    packages.set(root, { name: pkg.name, version: pkg.version });
    for (const name of new Set([
      ...Object.keys(pkg.dependencies || {}),
      ...Object.keys(pkg.optionalDependencies || {}),
    ])) {
      let dependency;
      try {
        dependency = await packageAt(name, root);
      } catch (error) {
        if (pkg.optionalDependencies?.[name]) continue;
        throw error;
      }
      let lexical = root;
      while (true) {
        const candidate = NodePath.join(lexical, "node_modules", name);
        try {
          await NodeFSP.lstat(candidate);
          links.set(candidate, dependency);
          break;
        } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
        const parent = NodePath.dirname(lexical);
        if (parent === lexical) throw new Error("Dependency link resolution failed");
        lexical = parent;
      }
      await visit(dependency);
    }
  }
  for (const root of roots) await visit(root.root);
  return {
    roots,
    packages: [...packages].map(([root, identity]) => ({ root, ...identity })),
    links: [...links].map(([destination, root]) => ({ destination, root })),
  };
}
