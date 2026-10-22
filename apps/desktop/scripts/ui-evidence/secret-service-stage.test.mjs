import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { sha256 } from "./runtime.mjs";
import { prepareSecretService, stageSecretService } from "./secret-service-stage.mjs";
import { verifySecretServiceInputs } from "./support/secret-service.mjs";
import { stageRun } from "./sandbox.mjs";
import { harness, parseOptions } from "../ui-evidence.mjs";

async function fixture(t) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-secret-stage-")),
  );
  t.after(() => NodeFSP.rm(root, { recursive: true }));
  const publicRoot = NodePath.join(root, "public");
  const stage = NodePath.join(root, "stage");
  await NodeFSP.mkdir(publicRoot);
  await NodeFSP.mkdir(NodePath.join(stage, "harness/support"), { recursive: true });
  for (const name of ["gnome-keyring-daemon", "dbus-daemon", "gdbus"])
    await NodeFSP.writeFile(NodePath.join(publicRoot, name), `synthetic ${name}`, { mode: 0o700 });
  const mapped = (path) =>
    path.startsWith("/usr/bin/") ? NodePath.join(publicRoot, NodePath.basename(path)) : path;
  const fs = {
    ...NodeFSP,
    lstat: (path) => NodeFSP.lstat(mapped(path)),
    realpath: async (path) => {
      const resolved = await NodeFSP.realpath(mapped(path));
      return resolved === mapped(path) ? path : resolved;
    },
    copyFile: (from, to, flags) => NodeFSP.copyFile(mapped(from), to, flags),
  };
  return { root, publicRoot, stage, fs, hash: (path) => sha256(mapped(path)) };
}

NodeTest.test("absent opt-in reads and stages no native inputs", async () => {
  const fs = new Proxy(
    {},
    {
      get: () => {
        throw new Error("unexpected filesystem access");
      },
    },
  );
  NodeAssert.equal(await prepareSecretService(undefined, { fs }), null);
  await stageSecretService(null, "/unused", { fs });
  await NodeAssert.rejects(prepareSecretService("ambient", { fs }), /must be system/);
  NodeAssert.equal(parseOptions(["run"])["secret-service"], undefined);
  NodeAssert.equal(parseOptions(["run", "--secret-service", "system"])["secret-service"], "system");
  NodeAssert.throws(() => parseOptions(["run", "--secret-service", "ambient"]), /must be system/);
  for (const command of ["setup", "doctor", "cleanup"])
    NodeAssert.throws(
      () => parseOptions([command, "--secret-service", "system"]),
      /Invalid option/,
    );
});

NodeTest.test("public inputs must exist and cannot be symlink aliases", async (t) => {
  const f = await fixture(t);
  const client = NodePath.join(f.publicRoot, "gdbus");
  await NodeFSP.unlink(client);
  await NodeAssert.rejects(prepareSecretService("system", f), { code: "ENOENT" });
  await NodeFSP.symlink(NodePath.join(f.publicRoot, "dbus-daemon"), client);
  await NodeAssert.rejects(prepareSecretService("system", f), /canonical regular/);
  await NodeFSP.unlink(client);
  await NodeFSP.mkdir(client);
  await NodeAssert.rejects(prepareSecretService("system", f), /canonical regular/);
});

NodeTest.test("changed public input rejects staging before creating a daemon", async (t) => {
  const f = await fixture(t);
  const prepared = await prepareSecretService("system", f);
  await NodeFSP.appendFile(NodePath.join(f.publicRoot, "dbus-daemon"), "changed");
  await NodeAssert.rejects(stageSecretService(prepared, f.stage, f), /changed before staging/);
  await NodeAssert.rejects(NodeFSP.access(NodePath.join(f.stage, "harness/support/native")), {
    code: "ENOENT",
  });
});

NodeTest.test("public parent aliases and non-executable inputs are refused", async (t) => {
  const f = await fixture(t);
  const daemon = NodePath.join(f.publicRoot, "gnome-keyring-daemon");
  await NodeFSP.chmod(daemon, 0o600);
  await NodeAssert.rejects(prepareSecretService("system", f), /must be executable/);
  await NodeFSP.chmod(daemon, 0o700);
  const relocated = NodePath.join(f.root, "relocated-public");
  await NodeFSP.rename(f.publicRoot, relocated);
  await NodeFSP.symlink(relocated, f.publicRoot);
  await NodeAssert.rejects(prepareSecretService("system", f), /canonical regular/);
});

NodeTest.test("daemon copying is exclusive and detects changed copy bytes", async (t) => {
  const f = await fixture(t);
  const prepared = await prepareSecretService("system", f);
  const altered = {
    ...f,
    fs: {
      ...f.fs,
      copyFile: async (from, to, flags) => {
        await f.fs.copyFile(from, to, flags);
        await NodeFSP.appendFile(to, "changed");
      },
    },
  };
  await NodeAssert.rejects(
    stageSecretService(prepared, f.stage, altered),
    /changed during staging/,
  );
  const daemon = NodePath.join(f.stage, "harness/support/native/bin/gnome-keyring-daemon");
  const bytes = await NodeFSP.readFile(daemon);
  await NodeAssert.rejects(stageSecretService(prepared, f.stage, f), { code: "EEXIST" });
  NodeAssert.deepEqual(await NodeFSP.readFile(daemon), bytes);
});

NodeTest.test(
  "public tool changes during the copy cannot qualify the staged configuration",
  async (t) => {
    const f = await fixture(t);
    const prepared = await prepareSecretService("system", f);
    const changed = {
      ...f,
      fs: {
        ...f.fs,
        copyFile: async (from, to, flags) => {
          await f.fs.copyFile(from, to, flags);
          await NodeFSP.appendFile(NodePath.join(f.publicRoot, "gdbus"), "changed");
        },
      },
    };
    await NodeAssert.rejects(
      stageSecretService(prepared, f.stage, changed),
      /inputs changed during staging/,
    );
  },
);

NodeTest.test(
  "real staging propagates opt-in and produces inner-verifiable public inputs",
  async (t) => {
    const f = await fixture(t);
    const source = NodePath.join(f.root, "source");
    for (const directory of ["apps/desktop/dist-electron", "apps/server/dist"])
      await NodeFSP.mkdir(NodePath.join(source, directory), { recursive: true });
    for (const file of ["package.json", "apps/desktop/package.json", "apps/server/package.json"])
      await NodeFSP.writeFile(NodePath.join(source, file), "{}");
    const prepared = await prepareSecretService("system", f);
    for (const selected of [null, prepared]) {
      const configuration = { size: { width: 1280, height: 800 }, ...selected?.configuration };
      const stage = await stageRun({
        run: { root: NodePath.join(f.root, selected ? "opt-in" : "default") },
        source,
        harness,
        scenario: NodePath.join(
          harness,
          "apps/desktop/scripts/ui-evidence/scenarios/two-servers-one-client.mjs",
        ),
        closure: { packages: [], roots: [] },
        configuration,
      });
      await stageSecretService(selected, stage, f);
      const actual = JSON.parse(
        await NodeFSP.readFile(NodePath.join(stage, "harness/config.json"), "utf8"),
      );
      NodeAssert.deepEqual(actual, configuration);
      if (!selected) {
        await NodeAssert.rejects(NodeFSP.access(NodePath.join(stage, "harness/support/native")), {
          code: "ENOENT",
        });
        continue;
      }
      const map = (path) =>
        path.startsWith("/harness/")
          ? NodePath.join(stage, path.slice(1))
          : NodePath.join(f.publicRoot, NodePath.basename(path));
      const inputs = await verifySecretServiceInputs(actual.managedSecretService, {
        lstat: (path) => NodeFSP.lstat(map(path)),
        realpath: async (path) =>
          (await NodeFSP.realpath(map(path))) === map(path) ? path : "alias",
        readFile: (path) => NodeFSP.readFile(map(path)),
      });
      NodeAssert.equal(inputs.length, 3);
      NodeAssert.deepEqual(actual.managedSecretService.libraries, []);
      NodeAssert.deepEqual(actual.managedSecretService.libraryDirectories, []);
      NodeAssert.equal(
        await sha256(map(actual.managedSecretService.daemon.path)),
        prepared.inputs.daemon.sha256,
      );
    }
  },
);
