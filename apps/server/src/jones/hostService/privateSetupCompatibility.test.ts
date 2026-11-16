// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodeProcess from "node:process";
import { qualifiedPayloadDigest } from "../cloud/qualifiedRuntime.ts";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import {
  archivePrivateSetupProvenance,
  assertPrivateSetupRuntime,
  inspectPrivateSetupRuntime,
  isUnattestedPrivateSetupArtifact,
  projectAuthenticatedPrivateSetupPayload,
} from "./privateSetupCompatibility.ts";

const executable = "#!/bin/sh\nexit 0\n";
const artifact = {
  repository: "Jones-Systems/Jones-Code",
  source: "d3e6f8e843a0477542380a4ec0d9ae02a1084053",
  tree: "250db17dd136cef960e9bb8e054acadb41fba03d",
  version: "0.0.45-preview.20261008.37988903951.1",
  platform: "linux",
  architecture: "x64",
  artifact: "synthetic-authenticated-linux-x64.tar.gz",
  sha256: "a".repeat(64),
};
const entrySha256 = NodeCrypto.createHash("sha256").update(executable).digest("hex");
const provenance = {
  schema: 1,
  repository: artifact.repository,
  source: artifact.source,
  version: artifact.version,
  platform: artifact.platform,
  architecture: artifact.architecture,
  artifact: artifact.artifact,
  sha256: artifact.sha256,
  entrySha256,
};
async function fixture(
  body: (f: {
    root: string;
    directory: string;
    path: string;
    archive: string;
    uid: number;
    inspect: () => ReturnType<typeof inspectPrivateSetupRuntime>;
  }) => Promise<void>,
) {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-private-setup-test-")),
  );
  try {
    const directory = NodePath.join(root, "runtime");
    const path = NodePath.join(directory, ".jones-provenance.json");
    const uid = NodeOS.userInfo().uid;
    await NodeFSP.mkdir(directory, { mode: 0o700 });
    for (const name of ["client", "node_modules", "resource-monitor"])
      await NodeFSP.mkdir(NodePath.join(directory, name), { mode: 0o700 });
    await NodeFSP.writeFile(NodePath.join(directory, "t3"), executable, { mode: 0o700 });
    await NodeFSP.writeFile(NodePath.join(directory, ".install-complete"), artifact.version, {
      mode: 0o600,
    });
    await NodeFSP.writeFile(path, JSON.stringify(provenance), { mode: 0o600 });
    await body({
      root,
      directory,
      path,
      uid,
      archive: NodePath.join(root, "archives", "private-preimage"),
      inspect: () => inspectPrivateSetupRuntime({ directory, uid, artifact, entrySha256 }),
    });
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

it.each([
  { field: "source", value: "b".repeat(40) },
  { field: "tree", value: "b".repeat(40) },
  { field: "platform", value: "darwin" },
  { field: "architecture", value: "arm64" },
])("does not infer pre-publisher compatibility from a different $field", ({ field, value }) =>
  fixture(async (f) => {
    const candidate = { ...artifact, [field]: value };
    expect(isUnattestedPrivateSetupArtifact(artifact)).toBe(true);
    expect(isUnattestedPrivateSetupArtifact(candidate)).toBe(false);
    const before = await NodeFSP.readFile(f.path);
    await expect(
      inspectPrivateSetupRuntime({
        directory: f.directory,
        uid: f.uid,
        artifact: candidate,
        entrySha256,
      }),
    ).rejects.toThrow();
    expect(await NodeFSP.readFile(f.path)).toEqual(before);
    await expect(NodeFSP.lstat(f.archive)).rejects.toMatchObject({ code: "ENOENT" });
  }),
);

it("preserves provenance bytes and inode by exclusive same-filesystem archive rename", () =>
  fixture(async (f) => {
    const preimage = await f.inspect();
    const original = await NodeFSP.readFile(f.path);
    const before = await NodeFSP.lstat(f.path);
    await archivePrivateSetupProvenance(preimage, f.archive, f.uid);
    await expect(NodeFSP.lstat(f.path)).rejects.toMatchObject({ code: "ENOENT" });
    const archived = NodePath.join(f.archive, ".jones-provenance.json");
    const after = await NodeFSP.lstat(archived);
    expect(await NodeFSP.readFile(archived)).toEqual(original);
    expect([after.dev, after.ino, after.uid, after.mode]).toEqual([
      before.dev,
      before.ino,
      before.uid,
      before.mode,
    ]);
    expect((await NodeFSP.lstat(f.archive)).mode & 0o777).toBe(0o700);
    await assertPrivateSetupRuntime(preimage, f.uid, f.archive);
    await expect(archivePrivateSetupProvenance(preimage, f.archive, f.uid)).rejects.toThrow();
    expect(await NodeFSP.readFile(archived)).toEqual(original);
  }));

it.each(["extra-key", "missing-key", "source", "sha256", "entrySha256", "schema"])(
  "refuses private provenance with mismatched %s without altering evidence",
  (field) =>
    fixture(async (f) => {
      const value: Record<string, unknown> = { ...provenance };
      if (field === "extra-key") value.unexpected = true;
      else if (field === "missing-key") delete value.platform;
      else value[field] = field === "schema" ? 9 : "0".repeat(field === "source" ? 40 : 64);
      await NodeFSP.writeFile(f.path, JSON.stringify(value));
      const before = await NodeFSP.readFile(f.path);
      await expect(f.inspect()).rejects.toThrow();
      expect(await NodeFSP.readFile(f.path)).toEqual(before);
      await expect(NodeFSP.lstat(f.archive)).rejects.toMatchObject({ code: "ENOENT" });
    }),
);
it("refuses an occupied archive without replacing its evidence or moving provenance", () =>
  fixture(async (f) => {
    const preimage = await f.inspect();
    await NodeFSP.mkdir(f.archive, { recursive: true, mode: 0o700 });
    const occupied = NodePath.join(f.archive, ".jones-provenance.json");
    await NodeFSP.writeFile(occupied, "existing trial evidence", { mode: 0o600 });
    await expect(archivePrivateSetupProvenance(preimage, f.archive, f.uid)).rejects.toThrow();
    expect(await NodeFSP.readFile(f.path, "utf8")).toBe(preimage.text);
    expect(await NodeFSP.readFile(occupied, "utf8")).toBe("existing trial evidence");
  }));

it.each(["symlink", "hardlink", "public-mode", "oversized", "wrong-owner", "unexpected-root"])(
  "refuses unsafe private evidence %s without moving or deleting it",
  (kind) =>
    fixture(async (f) => {
      if (kind === "symlink") {
        const target = NodePath.join(f.root, "provenance-target.json");
        await NodeFSP.rename(f.path, target);
        await NodeFSP.symlink(target, f.path);
      } else if (kind === "hardlink")
        await NodeFSP.link(f.path, NodePath.join(f.root, "provenance-link.json"));
      else if (kind === "public-mode") await NodeFSP.chmod(f.path, 0o644);
      else if (kind === "oversized") await NodeFSP.writeFile(f.path, " ".repeat(65537));
      else if (kind === "unexpected-root")
        await NodeFSP.writeFile(NodePath.join(f.directory, "unexpected"), "held");
      const before = await NodeFSP.lstat(f.path);
      const bytes = await NodeFSP.readFile(f.path);
      const inspection =
        kind === "wrong-owner"
          ? inspectPrivateSetupRuntime({
              directory: f.directory,
              uid: f.uid + 1,
              artifact,
              entrySha256,
            })
          : f.inspect();
      await expect(inspection).rejects.toThrow();
      const after = await NodeFSP.lstat(f.path);
      expect([after.dev, after.ino, after.mode, after.nlink]).toEqual([
        before.dev,
        before.ino,
        before.mode,
        before.nlink,
      ]);
      expect(await NodeFSP.readFile(f.path)).toEqual(bytes);
      await expect(NodeFSP.lstat(f.archive)).rejects.toMatchObject({ code: "ENOENT" });
    }),
);

it.each(["provenance", "payload", "root-entry"])(
  "refuses %s changed after inspection before archiving",
  (kind) =>
    fixture(async (f) => {
      const preimage = await f.inspect();
      if (kind === "provenance") await NodeFSP.appendFile(f.path, "\n");
      else if (kind === "payload")
        await NodeFSP.appendFile(NodePath.join(f.directory, "t3"), "# changed\n");
      else await NodeFSP.writeFile(NodePath.join(f.directory, "late-entry"), "held");
      const before = await NodeFSP.readFile(f.path);
      await expect(archivePrivateSetupProvenance(preimage, f.archive, f.uid)).rejects.toThrow();
      expect(await NodeFSP.readFile(f.path)).toEqual(before);
      await expect(NodeFSP.lstat(f.archive)).rejects.toMatchObject({ code: "ENOENT" });
    }),
);

async function pairedPayload(f: { root: string; directory: string }) {
  const payloadDirectory = NodePath.join(f.root, "authenticated-payload");
  await NodeFSP.mkdir(payloadDirectory, { mode: 0o700 });
  for (const name of ["client", "node_modules", "resource-monitor"]) {
    await NodeFSP.mkdir(NodePath.join(payloadDirectory, name), { mode: 0o700 });
    await NodeFSP.chmod(NodePath.join(payloadDirectory, name), 0o755);
  }
  for (const [name, bytes, executable] of [
    ["t3", "#!/bin/sh\nexit 0\n", true],
    ["node_modules/helper", "helper bytes", true],
    ["client/index.html", "page bytes", false],
  ] as const) {
    if (name !== "t3") {
      await NodeFSP.writeFile(NodePath.join(f.directory, name), bytes);
      await NodeFSP.chmod(NodePath.join(f.directory, name), executable ? 0o700 : 0o600);
    }
    await NodeFSP.writeFile(NodePath.join(payloadDirectory, name), bytes);
    await NodeFSP.chmod(NodePath.join(payloadDirectory, name), executable ? 0o711 : 0o600);
  }
  await NodeFSP.symlink("../t3", NodePath.join(f.directory, "client/entry"));
  await NodeFSP.symlink("../t3", NodePath.join(payloadDirectory, "client/entry"));
  return payloadDirectory;
}

it.each([0o077, 0o022])("projects only authenticated scratch under caller umask %s", (umask) =>
  fixture(async (f) => {
    const beforeUmask = NodeProcess.umask(umask);
    try {
      const payloadDirectory = await pairedPayload(f);
      const preimage = await f.inspect();
      const original = await NodeFSP.lstat(f.path);
      expect(await qualifiedPayloadDigest(payloadDirectory)).not.toBe(preimage.payloadSha256);
      await projectAuthenticatedPrivateSetupPayload({
        preimage,
        payloadDirectory,
        uid: f.uid,
        artifact,
      });
      expect(await qualifiedPayloadDigest(payloadDirectory)).toBe(preimage.payloadSha256);
      expect((await NodeFSP.stat(NodePath.join(payloadDirectory, "t3"))).mode & 0o777).toBe(0o700);
      expect(
        (await NodeFSP.stat(NodePath.join(payloadDirectory, "client/index.html"))).mode & 0o777,
      ).toBe(0o600);
      expect(await NodeFSP.readlink(NodePath.join(payloadDirectory, "client/entry"))).toBe("../t3");
      const projected = await NodeFSP.lstat(NodePath.join(payloadDirectory, "t3"));
      await projectAuthenticatedPrivateSetupPayload({
        preimage,
        payloadDirectory,
        uid: f.uid,
        artifact,
      });
      expect((await NodeFSP.lstat(NodePath.join(payloadDirectory, "t3"))).ino).toBe(projected.ino);
      expect(await NodeFSP.readFile(f.path, "utf8")).toBe(preimage.text);
      const after = await NodeFSP.lstat(f.path);
      expect([after.dev, after.ino, after.uid, after.mode]).toEqual([
        original.dev,
        original.ino,
        original.uid,
        original.mode,
      ]);
      await assertPrivateSetupRuntime(preimage, f.uid);
      await expect(NodeFSP.lstat(f.archive)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      NodeProcess.umask(beforeUmask);
    }
  }),
);

it.each([
  "content",
  "length",
  "path",
  "type",
  "link",
  "owner-execute",
  "nonexecutable",
  "partial-execute",
  "partial-read",
])(
  "refuses unsupported private projection %s before any scratch chmod or provenance movement",
  (kind) =>
    fixture(async (f) => {
      const payloadDirectory = await pairedPayload(f);
      if (kind === "content")
        await NodeFSP.writeFile(NodePath.join(payloadDirectory, "client/index.html"), "PAGE BYTES");
      if (kind === "length")
        await NodeFSP.appendFile(NodePath.join(payloadDirectory, "client/index.html"), "changed");
      if (kind === "path")
        await NodeFSP.writeFile(NodePath.join(payloadDirectory, "client/extra"), "extra");
      if (kind === "type") {
        await NodeFSP.unlink(NodePath.join(payloadDirectory, "node_modules/helper"));
        await NodeFSP.mkdir(NodePath.join(payloadDirectory, "node_modules/helper"));
      }
      if (kind === "link") {
        await NodeFSP.unlink(NodePath.join(payloadDirectory, "client/entry"));
        await NodeFSP.symlink(
          "../node_modules/helper",
          NodePath.join(payloadDirectory, "client/entry"),
        );
      }
      if (kind === "owner-execute")
        await NodeFSP.chmod(NodePath.join(f.directory, "node_modules/helper"), 0o600);
      if (kind === "nonexecutable")
        await NodeFSP.chmod(NodePath.join(f.directory, "client/index.html"), 0o700);
      if (kind === "partial-execute") await NodeFSP.chmod(NodePath.join(f.directory, "t3"), 0o710);
      if (kind === "partial-read") await NodeFSP.chmod(NodePath.join(f.directory, "t3"), 0o740);
      const preimage = await f.inspect();
      const original = await NodeFSP.lstat(f.path);
      const rawDigest = await qualifiedPayloadDigest(payloadDirectory);
      await expect(
        projectAuthenticatedPrivateSetupPayload({
          preimage,
          payloadDirectory,
          uid: f.uid,
          artifact,
        }),
      ).rejects.toMatchObject({ reason: "occupied-cache" });
      expect(await qualifiedPayloadDigest(payloadDirectory)).toBe(rawDigest);
      expect((await NodeFSP.stat(NodePath.join(payloadDirectory, "t3"))).mode & 0o777).toBe(0o711);
      expect(await NodeFSP.readFile(f.path, "utf8")).toBe(preimage.text);
      const after = await NodeFSP.lstat(f.path);
      expect([after.dev, after.ino, after.uid, after.mode]).toEqual([
        original.dev,
        original.ino,
        original.uid,
        original.mode,
      ]);
      await assertPrivateSetupRuntime(preimage, f.uid);
      await expect(NodeFSP.lstat(f.archive)).rejects.toMatchObject({ code: "ENOENT" });
    }),
);
