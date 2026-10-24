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

const protectedSegments = new Set([".t3", ".git", ".ssh", ".codex", ".claude"]);
function below(base, target) {
  const relative = NodePath.relative(base, target);
  return (
    relative !== "" &&
    !NodePath.isAbsolute(relative) &&
    relative !== ".." &&
    !relative.startsWith(`..${NodePath.sep}`)
  );
}
export function validatePackageName(name) {
  if (
    typeof name !== "string" ||
    !/^(?:@[a-zA-Z0-9_][a-zA-Z0-9._-]*\/)?[a-zA-Z0-9_][a-zA-Z0-9._-]*$/.test(name)
  )
    throw new Error("Invalid runtime package name");
  return name;
}
export async function createPackageResolver(source) {
  source = await NodeFSP.realpath(source);
  const anchors = ["node_modules", "apps/desktop/node_modules", "apps/server/node_modules"].map(
    (path) => NodePath.join(source, path),
  );
  const suffixSafe = (path) => {
    if (!below(source, path)) throw new Error("Runtime dependency escaped source");
    const parts = NodePath.relative(source, path).split(NodePath.sep);
    if (
      parts.some(
        (part) => protectedSegments.has(part) || part === ".env" || part.startsWith(".env."),
      )
    )
      throw new Error("Protected runtime dependency path");
  };
  for (const anchor of anchors) {
    try {
      await NodeFSP.lstat(anchor);
    } catch (error) {
      if (error.code === "ENOENT") continue;
      throw error;
    }
    const physical = await NodeFSP.realpath(anchor);
    if (!anchors.includes(physical) || !(await NodeFSP.stat(physical)).isDirectory())
      throw new Error("Runtime storage anchor escaped source");
  }
  const packagePath = (path) => {
    suffixSafe(path);
    if (!anchors.some((anchor) => below(anchor, path)))
      throw new Error("Runtime dependency escaped package storage");
    const parts = NodePath.relative(source, path).split(NodePath.sep);
    const tail = parts.slice(parts.lastIndexOf("node_modules") + 1);
    if (tail.length !== (tail[0]?.startsWith("@") ? 2 : 1))
      throw new Error("Runtime dependency is not a package root");
    validatePackageName(tail.join("/"));
  };
  const admit = async (path) => {
    packagePath(path);
    const root = await NodeFSP.realpath(path);
    packagePath(root);
    if (!(await NodeFSP.stat(root)).isDirectory())
      throw new Error("Runtime package is not a directory");
    return root;
  };
  const metadata = async (root) => {
    if ((await admit(root)) !== root) throw new Error("Runtime package identity changed");
    const manifest = await NodeFSP.realpath(NodePath.join(root, "package.json"));
    if (!below(root, manifest)) throw new Error("Runtime package manifest escaped package");
    suffixSafe(manifest);
    if (!(await NodeFSP.stat(manifest)).isFile())
      throw new Error("Runtime package manifest is not a file");
    return JSON.parse(await NodeFSP.readFile(manifest, "utf8"));
  };
  const resolve = async (name, from) => {
    validatePackageName(name);
    let current = await NodeFSP.realpath(from);
    if (current !== source) suffixSafe(current);
    while (true) {
      const storage = NodePath.join(current, "node_modules");
      const destination = NodePath.join(storage, name);
      let present = true;
      try {
        await NodeFSP.lstat(storage);
      } catch (error) {
        if (error.code === "ENOENT") present = false;
        else throw error;
      }
      if (present) {
        const physicalStorage = await NodeFSP.realpath(storage);
        suffixSafe(storage);
        suffixSafe(physicalStorage);
        if (!anchors.some((anchor) => physicalStorage === anchor || below(anchor, physicalStorage)))
          throw new Error("Runtime lookup storage escaped source");
        try {
          await NodeFSP.lstat(destination);
        } catch (error) {
          if (error.code === "ENOENT") present = false;
          else throw error;
        }
      }
      if (present) {
        // Validate every alias before deduplication, including installed optional dependencies.
        const root = await admit(destination);
        await metadata(root);
        return { name, root, destination };
      }
      if (current === source) break;
      current = NodePath.dirname(current);
      if (current !== source && !below(source, current))
        throw new Error("Runtime lookup escaped source");
    }
    throw Object.assign(new Error(`Missing runtime package: ${name}`), {
      code: "RUNTIME_PACKAGE_MISSING",
    });
  };
  return { source, resolve, admit, metadata };
}

// Required and optional peer semantics match PR507 at 238b185a9c05fccce5aedbcc485205f62b61a99f.
export async function resolvedPackageDependencies(root, metadata, resolver) {
  const dependencies = [];
  for (const name of new Set([
    ...Object.keys(metadata.dependencies || {}),
    ...Object.keys(metadata.optionalDependencies || {}),
    ...Object.keys(metadata.peerDependencies || {}),
  ])) {
    const optional =
      Object.hasOwn(metadata.optionalDependencies || {}, name) ||
      (!Object.hasOwn(metadata.dependencies || {}, name) &&
        metadata.peerDependenciesMeta?.[name]?.optional === true);
    try {
      dependencies.push(await resolver.resolve(name, root));
    } catch (error) {
      if (optional && error.code === "RUNTIME_PACKAGE_MISSING") continue;
      throw error;
    }
  }
  return dependencies;
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
  const resolver = await createPackageResolver(source);
  source = resolver.source;
  const roots = [],
    links = new Map(),
    packages = new Map();
  for (const app of ["desktop", "server"]) {
    const dir = NodePath.join(source, "apps", app);
    const pkg = JSON.parse(await NodeFSP.readFile(NodePath.join(dir, "package.json"), "utf8"));
    for (const name of await runtimeExternalDependencies(source, app, pkg.dependencies || {})) {
      const edge = await resolver.resolve(name, dir);
      roots.push({ ...edge, destination: `/app/apps/${app}/node_modules/${name}` });
    }
  }
  roots.push({
    ...(await resolver.resolve("playwright-core", NodePath.join(source, "apps/desktop"))),
    destination: "/harness/node_modules/playwright-core",
  });
  async function visit(root) {
    const pkg = await resolver.metadata(root);
    if (packages.has(root)) return;
    const dependencies = await resolvedPackageDependencies(root, pkg, resolver);
    packages.set(root, { name: pkg.name, version: pkg.version, dependencies });
    for (const edge of dependencies) {
      links.set(edge.destination, edge.root);
      await visit(edge.root);
    }
  }
  for (const root of roots) await visit(root.root);
  return {
    source,
    roots,
    packages: [...packages].map(([root, identity]) => ({ root, ...identity })),
    links: [...links].map(([destination, root]) => ({ destination, root })),
  };
}

export async function readProcessIdentity(pid, readFile = NodeFSP.readFile) {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error("Invalid captured process PID");
  const stat = await readFile(`/proc/${pid}/stat`, "utf8");
  const fields = stat
    .slice(stat.lastIndexOf(")") + 2)
    .trim()
    .split(/\s+/);
  if (!/^\d+$/.test(fields[19] || "")) throw new Error("Missing process start identity");
  return { pid, parentPid: Number(fields[1]), startTicks: fields[19], state: fields[0] };
}

export async function captureApplicationProcesses(app, readFile = NodeFSP.readFile) {
  const electron = await readProcessIdentity(app.process().pid, readFile);
  // Only inspect direct children of the captured app inside the runner's private PID namespace.
  const children = await readFile(`/proc/${electron.pid}/task/${electron.pid}/children`, "utf8");
  const servers = [];
  for (const value of children.trim().split(/\s+/).filter(Boolean)) {
    const pid = Number(value);
    try {
      const argv = (await readFile(`/proc/${pid}/cmdline`, "utf8")).split("\0");
      if (!argv.includes("/app/apps/server/dist/bin.mjs")) continue;
      const server = await readProcessIdentity(pid, readFile);
      if (server.parentPid !== electron.pid) throw new Error("Backend parent identity changed");
      servers.push(server);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  if (servers.length !== 1) throw new Error("Expected one captured app-owned backend");
  return { electron, server: servers[0] };
}

export async function assertApplicationStopped(processes, readFile = NodeFSP.readFile) {
  for (const [role, identity] of Object.entries(processes)) {
    try {
      const current = await readProcessIdentity(identity.pid, readFile);
      if (current.startTicks === identity.startTicks)
        throw new Error(`Old ${role} process still exists after app close`);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
}

export async function closeApplicationGeneration(generation, readFile = NodeFSP.readFile) {
  if (generation.closeAttempted) {
    if (!generation.closed) throw new Error("Previous app close has unresolved outcome");
    return;
  }
  const child = generation.app.process();
  generation.closeAttempted = true;
  await generation.app.close();
  generation.exit = { code: child.exitCode, signal: child.signalCode };
  if (child.exitCode !== 0 || child.signalCode !== null)
    throw new Error("Electron did not exit cleanly");
  if (generation.processes) await assertApplicationStopped(generation.processes, readFile);
  generation.closed = true;
}

export function assertRestartIdentity(before, after) {
  for (const role of ["electron", "server"]) {
    if (before.processes[role].pid === after.processes[role].pid)
      throw new Error(`Restart reused the ${role} PID`);
  }
  if (JSON.stringify(before.paths) !== JSON.stringify(after.paths))
    throw new Error("Restart changed private application paths");
  if (JSON.stringify(before.database) !== JSON.stringify(after.database))
    throw new Error("Restart replaced the synthetic database file");
}
