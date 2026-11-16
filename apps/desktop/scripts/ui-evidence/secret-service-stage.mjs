import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { sha256 } from "./runtime.mjs";

const publicPaths = {
  daemon: "/usr/bin/gnome-keyring-daemon",
  bus: "/usr/bin/dbus-daemon",
  client: "/usr/bin/gdbus",
};
const daemonPath = "/harness/support/native/bin/gnome-keyring-daemon";

async function inspect(path, fs, hash) {
  const info = await fs.lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || (await fs.realpath(path)) !== path)
    throw new Error("Secret Service input must be a canonical regular file");
  if (!(info.mode & 0o111)) throw new Error("Secret Service input must be executable");
  return { path, sha256: await hash(path) };
}

export async function prepareSecretService(mode, { fs = NodeFSP, hash = sha256 } = {}) {
  if (mode === undefined) return null;
  if (mode !== "system") throw new Error("Secret Service must be system");
  const inputs = {};
  for (const [name, path] of Object.entries(publicPaths))
    inputs[name] = await inspect(path, fs, hash);
  return {
    schema: "jones-system-secret-service-stage/v1",
    inputs,
    configuration: {
      managedSecretService: {
        schema: "jones-private-secret-service/v1",
        daemon: { path: daemonPath, sha256: inputs.daemon.sha256 },
        bus: inputs.bus,
        client: inputs.client,
        // The sandbox already mounts apt-managed /usr and loader configuration read-only.
        libraries: [],
        libraryDirectories: [],
      },
    },
  };
}

export async function stageSecretService(prepared, stage, { fs = NodeFSP, hash = sha256 } = {}) {
  if (prepared === null) return;
  const current = await prepareSecretService("system", { fs, hash });
  if (JSON.stringify(current) !== JSON.stringify(prepared))
    throw new Error("Secret Service inputs changed before staging");
  const support = NodePath.join(stage, "harness/support");
  if ((await fs.realpath(support)) !== support || !(await fs.lstat(support)).isDirectory())
    throw new Error("Secret Service stage must be a canonical owned directory");
  const native = NodePath.join(support, "native");
  await fs.mkdir(native, { mode: 0o700 });
  await fs.mkdir(NodePath.join(native, "bin"), { mode: 0o700 });
  const destination = NodePath.join(stage, daemonPath.slice(1));
  await fs.copyFile(publicPaths.daemon, destination, NodeFS.constants.COPYFILE_EXCL);
  await fs.chmod(destination, 0o700);
  if ((await hash(destination)) !== prepared.inputs.daemon.sha256)
    throw new Error("Secret Service daemon changed during staging");
  const after = await prepareSecretService("system", { fs, hash });
  if (JSON.stringify(after) !== JSON.stringify(prepared))
    throw new Error("Secret Service inputs changed during staging");
}
