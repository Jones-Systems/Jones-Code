import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { packageAt } from "./runtime.mjs";

const writableNames = ["home", "xdg-config", "xdg-data", "xdg-cache", "t3home", "workspace", "tmp"];
export function sandboxEnvironment() {
  return {
    PATH: "/usr/bin:/bin",
    PWD: "/app",
    HOME: "/scratch/home",
    XDG_CONFIG_HOME: "/scratch/xdg-config",
    XDG_DATA_HOME: "/scratch/xdg-data",
    XDG_CACHE_HOME: "/scratch/xdg-cache",
    T3CODE_HOME: "/scratch/t3home",
    T3CODE_PORT: "43773",
    TMPDIR: "/scratch/tmp",
    LANG: "C.UTF-8",
    TZ: "UTC",
    T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
    T3CODE_DISABLE_AUTO_UPDATE: "true",
    OTEL_SDK_DISABLED: "true",
  };
}
async function copyClosedTree(source, destination) {
  const base = await NodeFSP.realpath(source);
  async function inspect(directory) {
    for (const entry of await NodeFSP.readdir(directory, { withFileTypes: true })) {
      const file = NodePath.join(directory, entry.name);
      if (
        [".git", ".t3"].includes(entry.name) ||
        entry.name === ".env" ||
        entry.name.startsWith(".env.")
      )
        throw new Error("Protected file appeared in staged closure");
      if (entry.isSymbolicLink()) {
        const target = await NodeFSP.realpath(file);
        if (target !== base && !target.startsWith(`${base}/`))
          throw new Error(`Staged symlink escaped its package: ${file}`);
      } else if (entry.isDirectory()) await inspect(file);
      else if (!entry.isFile()) throw new Error("Special file in staged closure");
    }
  }
  await inspect(base);
  await NodeFSP.cp(base, destination, {
    recursive: true,
    dereference: true,
    errorOnExist: true,
    force: false,
  });
}
export async function stageRun({ run, source, harness, scenario, closure, configuration }) {
  const stage = NodePath.join(run.root, "stage");
  for (const name of ["app", "harness", "deps", ...writableNames.map((name) => `writable/${name}`)])
    await NodeFSP.mkdir(NodePath.join(stage, name), { recursive: true, mode: 0o700 });
  for (const relative of ["apps/desktop/dist-electron", "apps/server/dist"])
    await copyClosedTree(NodePath.join(source, relative), NodePath.join(stage, "app", relative));
  for (const relative of [
    "package.json",
    "apps/desktop/package.json",
    "apps/server/package.json",
  ]) {
    const destination = NodePath.join(stage, "app", relative);
    await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
    await NodeFSP.writeFile(destination, await NodeFSP.readFile(NodePath.join(source, relative)), {
      mode: 0o600,
      flag: "wx",
    });
  }
  for (const entry of await NodeFSP.readdir(
    NodePath.join(harness, "apps/desktop/scripts/ui-evidence"),
    {
      withFileTypes: true,
    },
  )) {
    if (entry.isFile() && entry.name.endsWith(".mjs") && !entry.name.endsWith(".test.mjs"))
      await NodeFSP.cp(
        NodePath.join(harness, "apps/desktop/scripts/ui-evidence", entry.name),
        NodePath.join(stage, "harness", entry.name),
      );
  }
  await NodeFSP.cp(scenario, NodePath.join(stage, "harness/scenario.mjs"));
  await NodeFSP.writeFile(
    NodePath.join(stage, "harness/config.json"),
    JSON.stringify(configuration),
    {
      mode: 0o600,
      flag: "wx",
    },
  );
  const mapped = new Map(closure.packages.map((pkg, index) => [pkg.root, `/deps/${index}`]));
  for (const pkg of closure.packages) {
    const target = mapped.get(pkg.root);
    await copyClosedTree(pkg.root, NodePath.join(stage, target));
    const metadata = JSON.parse(
      await NodeFSP.readFile(NodePath.join(pkg.root, "package.json"), "utf8"),
    );
    for (const name of new Set([
      ...Object.keys(metadata.dependencies || {}),
      ...Object.keys(metadata.optionalDependencies || {}),
    ])) {
      let resolved;
      try {
        resolved = await packageAt(name, pkg.root);
      } catch (error) {
        if (metadata.optionalDependencies?.[name]) continue;
        throw error;
      }
      if (!mapped.has(resolved)) throw new Error("Incomplete dependency closure");
      const destination = NodePath.join(stage, target, "node_modules", name);
      await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
      await NodeFSP.symlink(mapped.get(resolved), destination);
    }
  }
  for (const root of closure.roots) {
    const destination = NodePath.join(stage, root.destination.slice(1));
    await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
    await NodeFSP.symlink(mapped.get(root.root), destination);
  }
  // Synthetic git metadata contains no copied repository history or credentials.
  const gitDir = NodePath.join(stage, "writable/workspace/.git");
  await NodeFSP.mkdir(NodePath.join(gitDir, "objects"), { recursive: true });
  await NodeFSP.mkdir(NodePath.join(gitDir, "refs/heads"), { recursive: true });
  await NodeFSP.writeFile(NodePath.join(gitDir, "HEAD"), "ref: refs/heads/main\n");
  await NodeFSP.writeFile(
    NodePath.join(gitDir, "config"),
    "[core]\n\trepositoryformatversion = 0\n\tbare = false\n",
  );
  return stage;
}
export async function bwrapArguments({ stage, runtime, artifacts }) {
  const args = [
    "--unshare-user",
    "--unshare-pid",
    "--unshare-ipc",
    "--unshare-net",
    "--unshare-uts",
    "--unshare-cgroup-try",
    "--die-with-parent",
    "--new-session",
    "--clearenv",
    "--ro-bind",
    "/usr",
    "/usr",
  ];
  for (const name of ["bin", "sbin", "lib", "lib64"]) {
    try {
      const target = await NodeFSP.realpath(`/${name}`);
      args.push("--symlink", target, `/${name}`);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  args.push("--dir", "/etc");
  for (const name of [
    "ld.so.cache",
    "ld.so.conf",
    "ld.so.conf.d",
    "fonts",
    "passwd",
    "group",
    "nsswitch.conf",
    "localtime",
    "machine-id",
  ]) {
    try {
      await NodeFSP.lstat(`/etc/${name}`);
      args.push("--ro-bind", `/etc/${name}`, `/etc/${name}`);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  args.push("--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--tmpfs", "/dev/shm");
  for (const name of ["app", "harness", "deps"])
    args.push("--ro-bind", NodePath.join(stage, name), `/${name}`);
  args.push(
    "--ro-bind",
    runtime.directory,
    "/electron",
    "--bind",
    artifacts,
    "/artifacts",
    "--dir",
    "/scratch",
  );
  for (const name of writableNames)
    args.push("--bind", NodePath.join(stage, "writable", name), `/scratch/${name}`);
  for (const [key, value] of Object.entries(sandboxEnvironment()))
    args.push("--setenv", key, value);
  args.push("--chdir", "/app", "/usr/bin/node", "/harness/runner.mjs", "--inner");
  return args;
}
