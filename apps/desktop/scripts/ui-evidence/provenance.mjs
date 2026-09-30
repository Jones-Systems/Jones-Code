import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const execute = NodeUtil.promisify(NodeChildProcess.execFile);
export const sha256 = (data) => NodeCrypto.createHash("sha256").update(data).digest("hex");
const oidPattern = /^[a-f0-9]{40}$/;
const protectedPath = (name) =>
  name
    .split("/")
    .some(
      (part) => part === ".git" || part === ".t3" || part === ".env" || part.startsWith(".env."),
    );
async function git(root, args) {
  return (
    await execute("git", ["-C", root, ...args], { encoding: "buffer", maxBuffer: 64 * 1024 * 1024 })
  ).stdout;
}
async function hashTree(root) {
  const entries = [];
  let earliest = Infinity;
  let latest = -Infinity;
  async function visit(relative) {
    const full = NodePath.join(root, relative);
    const info = await NodeFSP.lstat(full);
    earliest = Math.min(earliest, info.mtimeMs);
    latest = Math.max(latest, info.mtimeMs);
    if (info.isSymbolicLink()) {
      entries.push([relative, "symlink", sha256(await NodeFSP.readlink(full))]);
    } else if (info.isDirectory()) {
      for (const name of (await NodeFSP.readdir(full)).sort())
        await visit(NodePath.join(relative, name));
    } else if (info.isFile()) {
      entries.push([relative, "file", info.mode & 0o777, sha256(await NodeFSP.readFile(full))]);
    } else throw new Error(`Unsupported build entry: ${relative}`);
  }
  await visit("");
  if (!entries.length) throw new Error(`Empty build directory: ${root}`);
  return {
    path: root,
    sha256: sha256(JSON.stringify(entries)),
    files: entries.length,
    mtime: { earliest: new Date(earliest).toISOString(), latest: new Date(latest).toISOString() },
  };
}
async function repository(root) {
  const head = (await git(root, ["rev-parse", "--verify", "HEAD"])).toString().trim();
  if (!oidPattern.test(head)) throw new Error("Invalid source HEAD");
  const status = await git(root, ["status", "--porcelain=v1", "-z", "--untracked-files=all"]);
  return { path: root, head, clean: status.length === 0, statusSha256: sha256(status) };
}
export async function collectProvenance({
  source,
  harness,
  comparison,
  artifacts,
  buildPaths,
  buildReceipt,
}) {
  source = NodePath.resolve(source);
  harness = NodePath.resolve(harness);
  const sourceInfo = await repository(source);
  const harnessInfo = await repository(harness);
  if (comparison != null) {
    if (!oidPattern.test(comparison)) throw new Error("Comparison must be a full commit OID");
    const resolved = (await git(source, ["rev-parse", "--verify", `${comparison}^{commit}`]))
      .toString()
      .trim();
    if (resolved !== comparison)
      throw new Error("Comparison does not identify the supplied commit");
  }
  const changed = (await git(source, ["diff", "--name-only", "-z", "HEAD"]))
    .toString()
    .split("\0")
    .filter(Boolean);
  if (changed.some(protectedPath))
    throw new Error("Protected source path is modified; refusing patch capture");
  const patch = await git(source, ["diff", "--binary", "HEAD"]);
  const untrackedNames = (await git(source, ["ls-files", "--others", "--exclude-standard", "-z"]))
    .toString()
    .split("\0")
    .filter(Boolean)
    .sort();
  const untracked = [];
  for (const name of untrackedNames) {
    const full = NodePath.resolve(source, name);
    if (
      protectedPath(name) ||
      full === NodePath.resolve(artifacts) ||
      full.startsWith(`${NodePath.resolve(artifacts)}${NodePath.sep}`)
    )
      continue;
    const info = await NodeFSP.lstat(full);
    if (info.isSymbolicLink())
      untracked.push([name, "symlink", sha256(await NodeFSP.readlink(full))]);
    else if (info.isFile())
      untracked.push([name, "file", info.mode & 0o777, sha256(await NodeFSP.readFile(full))]);
    else throw new Error(`Unsupported untracked source entry: ${name}`);
  }
  const paths = buildPaths ?? {
    desktop: NodePath.join(source, "apps/desktop/dist-electron"),
    server: NodePath.join(source, "apps/server/dist"),
    web: NodePath.join(source, "apps/web/dist"),
  };
  const build = {};
  for (const [name, directory] of Object.entries(paths)) build[name] = await hashTree(directory);
  const bootPath = NodePath.join(paths.desktop, "boot.cjs");
  build.boot = { path: bootPath, sha256: sha256(await NodeFSP.readFile(bootPath)) };
  build.sourceCorrespondence = {
    status: "unproved",
    reason:
      "Build tree hashes and mtimes identify outputs; no qualified source-to-build receipt was supplied.",
  };
  // Source drift must reject the record rather than combine identities from different trees.
  const current = await repository(source);
  if (
    current.head !== sourceInfo.head ||
    current.statusSha256 !== sourceInfo.statusSha256 ||
    sha256(await git(source, ["diff", "--binary", "HEAD"])) !== sha256(patch)
  )
    throw new Error("Source changed during provenance collection");
  for (const entry of untracked) {
    const [name, kind] = entry;
    const full = NodePath.join(source, name);
    const expected = entry.at(-1);
    if (
      sha256(kind === "symlink" ? await NodeFSP.readlink(full) : await NodeFSP.readFile(full)) !==
      expected
    )
      throw new Error("Untracked source changed during provenance collection");
  }
  if (buildReceipt) {
    const bytes = await NodeFSP.readFile(buildReceipt);
    const receipt = JSON.parse(bytes);
    const expectedSource = {
      head: sourceInfo.head,
      statusSha256: sourceInfo.statusSha256,
      patchSha256: sha256(patch),
      untrackedSha256: sha256(JSON.stringify(untracked)),
    };
    if (
      receipt.schema !== "jones-code-ui-evidence-build/v1" ||
      !receipt.source ||
      !receipt.build ||
      typeof receipt.invocation !== "string" ||
      !receipt.invocation.trim()
    )
      throw new Error("Invalid build receipt");
    for (const [name, value] of Object.entries(expectedSource))
      if (receipt.source[name] !== value) throw new Error(`Build receipt source mismatch: ${name}`);
    for (const name of ["desktop", "server", "web", "boot"])
      if (receipt.build[name] !== build[name]?.sha256)
        throw new Error(`Build receipt output mismatch: ${name}`);
    if (
      ![receipt.startedAt, receipt.finishedAt].every(
        (value) => typeof value === "string" && Number.isFinite(Date.parse(value)),
      ) ||
      Date.parse(receipt.finishedAt) < Date.parse(receipt.startedAt)
    )
      throw new Error("Invalid build receipt times");
    build.sourceCorrespondence = {
      status: "receipt-matched",
      receipt: {
        path: NodePath.resolve(buildReceipt),
        sha256: sha256(bytes),
        invocation: receipt.invocation,
        startedAt: receipt.startedAt,
        finishedAt: receipt.finishedAt,
      },
      basis:
        "Operator build receipt matches recorded source and output hashes; this is not a runtime embedded commit identity.",
    };
  }
  const currentHarness = await repository(harness);
  if (
    currentHarness.head !== harnessInfo.head ||
    currentHarness.statusSha256 !== harnessInfo.statusSha256
  )
    throw new Error("Harness changed during provenance collection");
  await NodeFSP.writeFile(NodePath.join(artifacts, "source.patch"), patch, {
    flag: "wx",
    mode: 0o600,
  });
  return {
    harness: harnessInfo,
    source: {
      ...sourceInfo,
      dirty: !sourceInfo.clean,
      comparison: comparison ?? null,
      comparisonKind: comparison ? "commit-reference-only" : "same-source-behavior-sequence",
      patch: { file: "source.patch", sha256: sha256(patch) },
      untracked: {
        count: untracked.length,
        sha256: sha256(JSON.stringify(untracked)),
        excluded: ["protected dotenv/git/T3 paths", "artifact output"],
      },
    },
    build,
  };
}
