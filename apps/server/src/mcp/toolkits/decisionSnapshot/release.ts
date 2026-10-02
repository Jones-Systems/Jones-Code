// @effect-diagnostics nodeBuiltinImport:off - Custody validation requires no-follow open flags and inode identities unavailable through FileSystem.
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";

export const RELEASE_CUSTODY = "immutable-by-api" as const;
const sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const relative = Schema.String.check(
  Schema.isMaxLength(512),
  Schema.isPattern(/^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/),
  Schema.makeFilter(
    (value) =>
      value
        .split("/")
        .every(
          (part) =>
            part !== "." &&
            part !== ".." &&
            ![
              ".ssh",
              "state",
              "userdata",
              "logs",
              "sessions",
              "history",
              "node_modules",
              "__pycache__",
            ].includes(part.toLowerCase()),
        ) &&
      !/(?:^|\/)(?:auth\.json|\.credentials\.json|\.claude\.json|\.env(?:\..*)?|settings\.json)$|\.(?:db|sqlite3?|pem|key|pyc)$|-(?:wal|shm)$/i.test(
        value,
      ),
  ),
);
const ReleaseManifest = Schema.Struct({
  schema: Schema.Literal("codex.decision-snapshot-release/v1"),
  release_id: sha256,
  entrypoint: Schema.Literal("tools/decision-snapshot.py"),
  custody: Schema.Literal(RELEASE_CUSTODY),
  external_runtime: Schema.Struct({
    python: Schema.Literal("/usr/bin/python3"),
    stdlib: Schema.Literal(true),
    account_native_bindings: Schema.Literal("tools/decision-snapshot-sources.json"),
  }),
  files: Schema.Array(
    Schema.Struct({
      path: relative,
      size_bytes: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4_194_304 })),
      sha256,
      provenance: Schema.Union([
        Schema.Struct({
          kind: Schema.Literal("source"),
          repository: Schema.Literal("Jones-Systems/Codex-V3"),
          commit: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
          path: relative,
        }),
        Schema.Struct({
          kind: Schema.Literal("fixture"),
          fixture_id: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_.-]{1,128}$/)),
          path: relative,
        }),
      ]),
    }),
  ).check(Schema.isMinLength(1), Schema.isMaxLength(512)),
});
export interface CollectorBinding {
  readonly releaseDirectory: string;
  readonly releaseId: string;
  readonly manifestSha256: string;
  readonly custody: typeof RELEASE_CUSTODY;
  readonly allowFixtureProvenance?: boolean;
}
export interface VerifiedRelease {
  readonly entrypoint: string;
  readonly fingerprint: string;
}
class ReleaseMismatch extends Error {}

async function readBounded(path: string, maximum: number): Promise<Buffer> {
  const file = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > maximum) throw new ReleaseMismatch();
    const buffer = Buffer.alloc(maximum + 1);
    let count = 0;
    while (count <= maximum) {
      const result = await file.read(buffer, count, buffer.length - count, count);
      if (result.bytesRead === 0) break;
      count += result.bytesRead;
    }
    if (count > maximum) throw new ReleaseMismatch();
    return buffer.subarray(0, count);
  } finally {
    await file.close();
  }
}

export async function verifyRelease(binding: CollectorBinding): Promise<VerifiedRelease> {
  if (
    binding.custody !== RELEASE_CUSTODY ||
    !NodePath.isAbsolute(binding.releaseDirectory) ||
    !/^[a-f0-9]{64}$/.test(binding.releaseId) ||
    !/^[a-f0-9]{64}$/.test(binding.manifestSha256)
  )
    throw new ReleaseMismatch();
  const root = binding.releaseDirectory;
  if ((await NodeFSP.realpath(root)) !== root || NodePath.basename(root) !== binding.releaseId)
    throw new ReleaseMismatch();
  const descriptor = "decision-snapshot-release.json";
  const manifestPath = NodePath.join(root, descriptor);
  const metadata = await NodeFSP.lstat(manifestPath);
  if (!metadata.isFile() || metadata.nlink !== 1 || metadata.size > 262_144)
    throw new ReleaseMismatch();
  const bytes = await readBounded(manifestPath, 262_144);
  if (NodeCrypto.createHash("sha256").update(bytes).digest("hex") !== binding.manifestSha256)
    throw new ReleaseMismatch();
  const manifest = Schema.decodeUnknownSync(Schema.fromJsonString(ReleaseManifest))(
    bytes.toString("utf8"),
    { onExcessProperty: "error" },
  );
  if (
    manifest.release_id !== binding.releaseId ||
    (binding.allowFixtureProvenance !== true &&
      manifest.files.some((file) => file.provenance.kind === "fixture"))
  )
    throw new ReleaseMismatch();
  const files = new Map(manifest.files.map((item) => [item.path, item]));
  if (
    files.size !== manifest.files.length ||
    files.has(descriptor) ||
    !files.has(manifest.entrypoint)
  )
    throw new ReleaseMismatch();
  let total = 0;
  const identities: Array<string> = [];
  const seen = new Set<string>();
  const walk = async (directory: string, prefix: string): Promise<void> => {
    const directoryStat = await NodeFSP.lstat(directory);
    if (!directoryStat.isDirectory() || directoryStat.isSymbolicLink()) throw new ReleaseMismatch();
    identities.push(
      `${prefix}:${directoryStat.dev}:${directoryStat.ino}:${directoryStat.mode}:${directoryStat.mtimeMs}:${directoryStat.ctimeMs}`,
    );
    for (const entry of (await NodeFSP.readdir(directory)).sort()) {
      const path = prefix ? `${prefix}/${entry}` : entry;
      const target = NodePath.join(directory, entry);
      const stat = await NodeFSP.lstat(target);
      if (stat.isSymbolicLink()) throw new ReleaseMismatch();
      if (stat.isDirectory()) {
        if (![...files.keys()].some((file) => file.startsWith(`${path}/`)))
          throw new ReleaseMismatch();
        await walk(target, path);
        continue;
      }
      if (!stat.isFile() || stat.nlink !== 1 || seen.size >= 513) throw new ReleaseMismatch();
      identities.push(
        `${path}:${stat.dev}:${stat.ino}:${stat.mode}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`,
      );
      if (path === descriptor) {
        seen.add(path);
        continue;
      }
      const expected = files.get(path);
      if (!expected || stat.size !== expected.size_bytes || stat.size > 4_194_304)
        throw new ReleaseMismatch();
      total += stat.size;
      if (total > 33_554_432) throw new ReleaseMismatch();
      const content = await readBounded(target, expected.size_bytes);
      if (
        content.length !== expected.size_bytes ||
        NodeCrypto.createHash("sha256").update(content).digest("hex") !== expected.sha256
      )
        throw new ReleaseMismatch();
      seen.add(path);
    }
  };
  await walk(root, "");
  if (seen.size !== files.size + 1 || [...files.keys()].some((file) => !seen.has(file)))
    throw new ReleaseMismatch();
  return {
    entrypoint: NodePath.join(root, manifest.entrypoint),
    fingerprint: NodeCrypto.createHash("sha256").update(identities.join("\n")).digest("hex"),
  };
}
