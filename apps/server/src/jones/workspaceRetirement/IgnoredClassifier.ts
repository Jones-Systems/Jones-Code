// @effect-diagnostics nodeBuiltinImport:off -- Bounded metadata/proof adapter; no removal or owner authentication.
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import {
  canonical,
  exists,
  git,
  inside,
  openAbsolute,
  readBounded,
  sensitive,
} from "./Filesystem.ts";
import { contentManifest, LIMITS, metadata, verify } from "./OutputAttestation.ts";

export interface Coverage {
  exactRoots?: readonly string[];
  sensitivePaths?: readonly string[];
  classes?: readonly string[];
  keepMode?: "any-local-files" | "uncommitted-changes" | "tracked-changes";
}
export interface Preservation {
  source: string;
  destination: string;
  destinationIdentity: { dev: number; ino: number };
  manifestSha256: string;
  sourceManifestSha256?: string;
  mode: "copy" | "move";
  retainedOwner: string;
  recordPath: string;
}
export interface IgnoredResult {
  path: string;
  outcome: "disposable" | "hold" | "unknown";
  reason: string | null;
  basis?: string;
  manifestSha256?: string;
  paths?: readonly string[];
  count?: number;
}
export async function verifyPreservation(
  record: Preservation,
  targets: readonly string[],
  common: string,
  temporaryRoots: readonly string[],
) {
  try {
    const durable = JSON.parse((await readBounded(record.recordPath, 65536)).toString());
    if (
      canonical(durable) !== canonical(record) ||
      !record.retainedOwner ||
      [...targets, common, ...temporaryRoots].some(
        (root) => inside(record.destination, root) || inside(record.recordPath, root),
      )
    )
      return false;
    const dir = await openAbsolute(record.destination, true);
    try {
      const s = await dir.stat();
      if (s.dev !== record.destinationIdentity.dev || s.ino !== record.destinationIdentity.ino)
        return false;
    } finally {
      await dir.close();
    }
    const dest = await contentManifest(record.destination);
    if (
      dest.observation.verification !== "content" ||
      dest.observation.content_manifest_sha256 !== record.manifestSha256
    )
      return false;
    if (record.mode === "copy") {
      const src = await contentManifest(record.source);
      return (
        src.observation.verification === "content" &&
        src.observation.content_manifest_sha256 === record.sourceManifestSha256 &&
        record.sourceManifestSha256 === record.manifestSha256
      );
    }
    return record.mode === "move" && !(await exists(record.source));
  } catch {
    return false;
  }
}
async function sharedStore(
  root: string,
  nested: readonly string[],
  registered: readonly { path: string; common: string }[],
) {
  for (const relative of nested) {
    const path = Path.join(root, relative);
    if (registered.some((r) => inside(r.path, root) || inside(r.common, root))) return true;
    try {
      const stat = await FS.lstat(path);
      // Resolve only native pointer metadata. Running Git here would also parse
      // an ignored repository's config/includes before returning its store.
      if (stat.isSymbolicLink()) {
        const target = Path.resolve(Path.dirname(path), await FS.readlink(path));
        if (registered.some((r) => r.common === target)) return true;
        continue;
      }
      let admin = path;
      if (stat.isFile()) {
        const pointer = (await readBounded(path, 65536))
          .toString()
          .match(/^gitdir: ([^\r\n]+)\r?\n?$/);
        if (!pointer) continue;
        admin = Path.resolve(Path.dirname(path), pointer[1]!);
      }
      const directory = await openAbsolute(admin, true);
      await directory.close();
      const commonFile = Path.join(admin, "commondir");
      const common = (await exists(commonFile))
        ? Path.resolve(admin, (await readBounded(commonFile, 65536)).toString().trim())
        : admin;
      if (registered.some((r) => r.common === common)) return true;
      if (
        (await exists(Path.join(common, "worktrees"))) &&
        (await FS.readdir(Path.join(common, "worktrees"))).length > 0
      )
        return true;
      if (await exists(Path.join(common, "objects/info/alternates"))) return true;
    } catch {
      /* An unresolvable nested store remains protected as a nested repository. */
    }
  }
  return false;
}
export async function classifyIgnored(input: {
  worktree: string;
  root: string;
  coverage?: Coverage | undefined;
  sidecar?: string | undefined;
  preservation?: Preservation | undefined;
  targets?: readonly string[];
  common: string;
  temporaryRoots?: readonly string[];
  registered?: readonly { path: string; common: string }[] | undefined;
}): Promise<IgnoredResult> {
  const { root, worktree } = input,
    coverage = input.coverage ?? {};
  const hold = (reason: string, extra: Partial<IgnoredResult> = {}): IgnoredResult => ({
    path: root,
    outcome: "hold",
    reason,
    ...extra,
  });
  const allow = (basis: string, manifestSha256?: string): IgnoredResult => ({
    path: root,
    outcome: "disposable",
    reason: null,
    basis,
    ...(manifestSha256 ? { manifestSha256 } : {}),
  });
  try {
    const info = await FS.lstat(root);
    let nested: string[] = [],
      sensitivePaths: string[] = [],
      entries: string[] = [],
      unobservedSensitiveDirectory = info.isDirectory() && sensitive(Path.basename(root));
    if (root.split(Path.sep).some(sensitive)) sensitivePaths.push(root);
    if (info.isDirectory()) {
      const dir = await openAbsolute(root, true);
      try {
        const m = await metadata(dir, performance.now() + 60000, {
          entries: LIMITS.entries,
          bytes: LIMITS.bytes,
        });
        nested = m.nested;
        sensitivePaths.push(...m.sensitivePaths.map((p) => Path.join(root, p)));
        entries = m.entries.map((e) => e.relpath);
        unobservedSensitiveDirectory ||= m.entries.some(
          (e) => e.type === "directory" && sensitive(Path.basename(e.relpath)),
        );
        if (m.reasons.some((r) => r !== "nested_repository" && r !== "sensitive_ignored"))
          return hold("attestation_unverifiable");
      } finally {
        await dir.close();
      }
    } else if (!info.isFile() && !info.isSymbolicLink()) return hold("attestation_unverifiable");
    if (Path.basename(root) === ".git") nested.push("");
    if (input.registered?.some((r) => inside(r.path, root) || inside(r.common, root)))
      return hold("shared_common_store");
    if (nested.length)
      return hold(
        (await sharedStore(root, nested, input.registered ?? []))
          ? "shared_common_store"
          : "nested_repository",
      );
    if (
      unobservedSensitiveDirectory ||
      sensitivePaths.some((path) => !coverage.sensitivePaths?.includes(path))
    )
      return hold("sensitive_ignored", {
        paths: sensitivePaths.slice(0, 20),
        count: sensitivePaths.length,
      });
    if (coverage.keepMode === "any-local-files") return hold("ignored");
    const exact = coverage.exactRoots?.includes(root) === true;
    if (input.sidecar) {
      const result = await verify(input.sidecar, worktree);
      if (result.verification === "content" && result.root === root)
        return allow("attested_content", result.content_manifest_sha256);
      return exact
        ? allow("covered, including additions")
        : hold(result.reason ?? "attestation_unverifiable");
    }
    if (input.preservation) {
      if (
        input.preservation.source === root &&
        (await verifyPreservation(
          input.preservation,
          input.targets ?? [worktree],
          input.common,
          input.temporaryRoots ?? [],
        ))
      )
        return allow("preserved", input.preservation.manifestSha256);
      return exact ? allow("covered, including additions") : hold("preservation_unverified");
    }
    if (exact) return allow("covered, including additions");
    if (Path.basename(root) === "__pycache__" || root.endsWith(".pyc"))
      return hold("unattested_ignored");
    const classes = coverage.classes ?? [];
    if (
      classes.includes("declared_cache") &&
      info.isDirectory() &&
      !["node_modules", "vendor", ".venv"].includes(Path.basename(root)) &&
      !entries.some((p) => Path.basename(p) === "pyvenv.cfg") &&
      (await exists(Path.join(root, "CACHEDIR.TAG")))
    ) {
      if (
        (await readBounded(Path.join(root, "CACHEDIR.TAG"), 4096))
          .toString()
          .startsWith("Signature: 8a477f597d28d172789f06886806bc55")
      )
        return allow("covered, including additions");
    }
    if (
      classes.includes("format_recognized") &&
      Path.basename(root) === ".DS_Store" &&
      info.isFile()
    ) {
      const file = await openAbsolute(root);
      try {
        const bytes = Buffer.alloc(8);
        await file.read(bytes, 0, 8, 0);
        if (bytes.subarray(4, 8).toString() === "Bud1")
          return allow("covered, including additions");
      } finally {
        await file.close();
      }
    }
    if (
      classes.includes("dependency_reproduction") &&
      ["node_modules", "vendor", ".venv"].includes(Path.basename(root))
    ) {
      const parent = Path.relative(worktree, Path.dirname(root));
      const tracked = new Set(
        (await git(worktree, ["ls-tree", "-r", "--name-only", "HEAD"])).toString().split("\n"),
      );
      const pairs = [
        ["package.json", "pnpm-lock.yaml"],
        ["package.json", "bun.lock"],
        ["package.json", "package-lock.json"],
        ["package.json", "yarn.lock"],
        ["pyproject.toml", "uv.lock"],
        ["composer.json", "composer.lock"],
        ["Gemfile", "Gemfile.lock"],
      ];
      if (pairs.some((pair) => pair.every((p) => tracked.has(parent ? parent + "/" + p : p))))
        return allow("covered, including additions");
    }
    return hold("unattested_ignored", {
      paths: [root, ...entries.slice(0, 19).map((path) => Path.join(root, path))],
      count: 1 + entries.length,
    });
  } catch (cause) {
    return {
      path: root,
      outcome: "unknown",
      reason:
        cause instanceof Error && cause.message === "bound_exceeded"
          ? "bound_exceeded"
          : "observation_failed",
    };
  }
}
