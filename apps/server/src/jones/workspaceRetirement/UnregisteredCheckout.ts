// @effect-diagnostics nodeBuiltinImport:off -- Bounded lstat/readdir classification and a hashed read of leftover files; deletion runs in ReadOnlyRemoval's no-follow helper.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import { exists, git, inside } from "./Filesystem.ts";
import { classifyIgnored, type Coverage } from "./IgnoredClassifier.ts";
import { makeRootsWritable, removeTree } from "./ReadOnlyRemoval.ts";
import type { InterruptedRetirement, RetirementReceipt } from "./Receipts.ts";

export interface ResidueOutcome {
  readonly state: "retired" | "partial";
  readonly reason: string;
  /** Exact leftover paths that kept the checkout held, relative to the checkout. */
  readonly paths?: readonly string[];
  readonly fact?: string;
}

const MAX_ENTRIES = 200_000;
const MAX_HASHED_BYTES = 2 * 1024 ** 3;
const MAX_REPORTED = 20;

/**
 * The branch, containing, preservation and preserve-branch refs a retirement recorded still resolve
 * to their recorded heads. Nothing is read from the removed admin directory.
 */
export async function receiptRefsRead(projectRoot: string, receipt: RetirementReceipt) {
  for (const ref of receipt.preservationRefs)
    if ((await git(projectRoot, ["rev-parse", ref.ref])).toString().trim() !== ref.head)
      return false;
  if (
    receipt.preserve &&
    (
      await git(projectRoot, [
        "rev-parse",
        "--verify",
        `refs/heads/${receipt.preserve.branch}^{commit}`,
      ])
    )
      .toString()
      .trim() !== receipt.preserve.sha
  )
    return false;
  if (
    receipt.qualification.branch &&
    (await git(projectRoot, ["rev-parse", receipt.qualification.branch])).toString().trim() !==
      receipt.qualification.head
  )
    return false;
  for (const ref of receipt.qualification.containingRefs ?? [])
    await git(projectRoot, ["merge-base", "--is-ancestor", receipt.qualification.head!, ref]);
  return true;
}

async function blobId(path: string, symlink: boolean, algorithm: "sha1" | "sha256") {
  const before = await NodeFSP.lstat(path, { bigint: true });
  const hash = NodeCrypto.createHash(algorithm);
  if (symlink) {
    const target = Buffer.from(await NodeFSP.readlink(path, "buffer"));
    hash.update(`blob ${target.length}\0`).update(target);
  } else {
    hash.update(`blob ${before.size}\0`);
    const file = await NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
    try {
      for await (const chunk of file.createReadStream({ autoClose: false }))
        hash.update(chunk as Buffer);
    } finally {
      await file.close();
    }
    const after = await NodeFSP.lstat(path, { bigint: true });
    if (after.ino !== before.ino || after.size !== before.size || after.mtimeNs !== before.mtimeNs)
      throw new Error("observation_failed");
  }
  return hash.digest("hex");
}

/** HEAD's files by path, with directory prefixes, so leftovers can be matched byte-for-byte. */
async function trackedTree(projectRoot: string, head: string) {
  const files = new Map<string, { mode: string; sha: string }>();
  const directories = new Set<string>();
  const listing = (await git(projectRoot, ["ls-tree", "-r", "-z", "--full-tree", head]))
    .toString()
    .split("\0")
    .filter(Boolean);
  for (const line of listing) {
    const tab = line.indexOf("\t");
    const [mode, , sha] = line.slice(0, tab).split(" ");
    const path = line.slice(tab + 1);
    files.set(path, { mode: mode!, sha: sha! });
    for (let at = path.lastIndexOf("/"); at > 0; at = path.lastIndexOf("/", at - 1))
      directories.add(path.slice(0, at));
  }
  return { files, directories };
}

/**
 * Finishes the removal of a directory Git already deregistered, but only when the leftover is
 * covered ignored roots, tracked files equal to the recorded HEAD (or to the receipt's preserve
 * commit), the `.git` link or empty directories. Any other path holds the checkout and is named.
 * The caller proved the interrupted executor is gone and holds the owner's Retire for this receipt.
 */
export async function finishUnregisteredCheckout(
  projectRoot: string,
  stale: InterruptedRetirement,
): Promise<ResidueOutcome> {
  const receipt = stale.receipt,
    bound = receipt.binding,
    path = bound.path;
  const held = (reason: string, extra: Partial<ResidueOutcome> = {}): ResidueOutcome => ({
    state: "partial",
    reason,
    ...extra,
  });
  let deleting = false;
  try {
    if (await exists(bound.git_dir)) return held("interrupted_residue_admin_present");
    const stat = await NodeFSP.lstat(path, { bigint: true });
    if (!stat.isDirectory() || stat.dev !== BigInt(bound.dev) || stat.ino !== BigInt(bound.ino))
      return held("interrupted_checkout_replaced");
    const head = receipt.qualification.head;
    if (!head || !(await receiptRefsRead(projectRoot, receipt)))
      return held("interrupted_removal_refs_unverified");
    const algorithm = head.length === 64 ? "sha256" : "sha1";
    // A preserve commit carries the checkout's edits and untracked files, so leftovers equal to it are recreatable.
    const tracked = await trackedTree(projectRoot, receipt.preserve?.sha ?? head);
    const roots = new Set(
      stale.journal.filter((entry) => entry.kind === "ignored").map((entry) => entry.path),
    );
    const unrecognized: string[] = [];
    const ignoredPresent: string[] = [];
    let seen = 0,
      hashed = 0n;
    const walk = async (directory: string): Promise<void> => {
      const entries = await NodeFSP.readdir(directory, { withFileTypes: true });
      seen += entries.length;
      if (seen > MAX_ENTRIES) throw new Error("bound_exceeded");
      for (const entry of entries) {
        const absolute = NodePath.join(directory, entry.name);
        const relative = NodePath.relative(path, absolute).split(NodePath.sep).join("/");
        const flag = (reason = relative) => unrecognized.push(reason);
        if (entry.name.includes("�") || entry.name.includes("\n")) flag();
        else if (roots.has(absolute)) ignoredPresent.push(absolute);
        else if (entry.isSymbolicLink() || entry.isFile()) {
          const expected = tracked.files.get(relative);
          if (relative === ".git" && entry.isFile()) {
            const link = await NodeFSP.readFile(absolute, "utf8");
            if (link !== `gitdir: ${bound.git_dir}\n`) flag();
          } else if (
            !expected ||
            (entry.isSymbolicLink() ? expected.mode !== "120000" : !expected.mode.startsWith("100"))
          )
            flag();
          else {
            const size = (await NodeFSP.lstat(absolute, { bigint: true })).size;
            hashed += size;
            if (hashed > BigInt(MAX_HASHED_BYTES)) throw new Error("bound_exceeded");
            if ((await blobId(absolute, entry.isSymbolicLink(), algorithm)) !== expected.sha)
              flag();
          }
        } else if (entry.isDirectory()) {
          if (tracked.directories.has(relative)) await walk(absolute);
          else if ((await NodeFSP.readdir(absolute)).length) flag();
        } else flag();
      }
    };
    await walk(path);
    for (const root of ignoredPresent) {
      const result = await classifyIgnored({
        worktree: path,
        root,
        common: bound.git_common_dir,
        coverage: {
          keepMode: (receipt.coverage as Coverage | null)?.keepMode ?? "uncommitted-changes",
          exactRoots: [root],
        },
      });
      if (result.outcome !== "disposable")
        unrecognized.push(
          NodePath.relative(path, root).split(NodePath.sep).join("/") +
            ` (${result.reason ?? result.outcome})`,
        );
    }
    if (unrecognized.length)
      return held("interrupted_residue_unrecognized_content", {
        paths: unrecognized.slice(0, MAX_REPORTED),
        fact: `${unrecognized.length} leftover path(s) are not regenerable ignored roots or tracked files that match the recorded HEAD.`,
      });
    // The leftover is verified; only now are directories made writable and the exact path removed.
    const target = { path, dev: bound.dev, ino: bound.ino };
    deleting = true;
    await makeRootsWritable(
      target,
      ignoredPresent.filter((root) => inside(root, path)),
    );
    await removeTree(target);
    if ((await exists(path)) || !(await receiptRefsRead(projectRoot, receipt)))
      return held("interrupted_residue_delete_failed");
    return { state: "retired", reason: "interrupted_residue_removed" };
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    return held(
      deleting ? "interrupted_residue_delete_failed" : "interrupted_residue_unclassifiable",
      { fact: message.split(/\r?\n/)[0]!.slice(0, 512) },
    );
  }
}
