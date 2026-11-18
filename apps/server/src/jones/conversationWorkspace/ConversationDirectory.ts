// @effect-diagnostics nodeBuiltinImport:off -- Exact Jones-owned directory lifecycle uses no-follow identities and rmdir, never recursive removal.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";

export interface DirectoryIdentity {
  readonly path: string;
  readonly device: string;
  readonly inode: string;
}
const within = (parent: string, target: string) => {
  const relative = NodePath.relative(parent, target);
  return (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative))
  );
};
const lstatOrNull = (target: string) =>
  NodeFSP.lstat(target).catch((cause: unknown) => {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null;
    throw cause;
  });
// Mirrors Git discovery: a gitfile or a `.git` directory holding HEAD marks a
// checkout. Git ignores an empty `.git` directory, such as a stray one in $HOME.
async function isGitCheckoutMarker(directory: string): Promise<boolean> {
  const marker = await lstatOrNull(NodePath.join(directory, ".git"));
  if (marker === null) return false;
  if (!marker.isDirectory()) return true;
  return (await lstatOrNull(NodePath.join(directory, ".git", "HEAD"))) !== null;
}
export async function createConversationDirectory(
  stateDirectory: string,
  excluded: readonly string[],
): Promise<DirectoryIdentity> {
  const state = await NodeFSP.realpath(stateDirectory);
  // Production state cannot live in scratch. Synthetic tests explicitly supply
  // a state root outside the system temporary directory too.
  const root = NodePath.join(state, "conversation-workspaces");
  // Canonicalize concurrently: each serial await waits a full event-loop turn,
  // which took tens of milliseconds per path on busy servers.
  const canonicalRoots = await Promise.all(
    [...new Set([...excluded, NodeOS.tmpdir()])].map((candidate) =>
      NodeFSP.realpath(candidate).catch((cause: unknown) => {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT")
          return NodePath.resolve(candidate);
        throw cause;
      }),
    ),
  );
  if (canonicalRoots.some((canonical) => within(canonical, root)))
    throw new Error("Conversation directory overlaps an excluded root");
  const ancestors = [state];
  for (let ancestor = state; NodePath.dirname(ancestor) !== ancestor;)
    ancestors.push((ancestor = NodePath.dirname(ancestor)));
  if ((await Promise.all(ancestors.map(isGitCheckoutMarker))).some(Boolean))
    throw new Error("Conversation state lies inside a Git checkout");
  await NodeFSP.mkdir(root, { recursive: true, mode: 0o700 });
  const rootStat = await NodeFSP.lstat(root);
  if (
    !rootStat.isDirectory() ||
    rootStat.isSymbolicLink() ||
    (await NodeFSP.realpath(root)) !== root
  )
    throw new Error("Invalid conversation root");
  const directory = NodePath.join(root, NodeCrypto.randomUUID());
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  const stat = await NodeFSP.lstat(directory, { bigint: true });
  return { path: directory, device: String(stat.dev), inode: String(stat.ino) };
}
export async function inspectConversationDirectory(
  identity: DirectoryIdentity,
): Promise<"empty" | "conversation_dir_nonempty" | "absent"> {
  const stat = await NodeFSP.lstat(identity.path, { bigint: true }).catch((cause: unknown) => {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null;
    throw cause;
  });
  if (stat === null) return "absent";
  if (
    (await NodeFSP.realpath(identity.path)) !== identity.path ||
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    String(stat.dev) !== identity.device ||
    String(stat.ino) !== identity.inode
  )
    throw new Error("Conversation directory identity changed");
  const entries = await NodeFSP.opendir(identity.path);
  try {
    return (await entries.read()) === null ? "empty" : "conversation_dir_nonempty";
  } finally {
    await entries.close();
  }
}
export async function disposeConversationDirectory(
  identity: DirectoryIdentity,
): Promise<"disposed" | "conversation_dir_nonempty"> {
  const state = await inspectConversationDirectory(identity);
  if (state === "conversation_dir_nonempty") return state;
  if (state === "empty") {
    try {
      await NodeFSP.rmdir(identity.path);
    } catch (cause) {
      if (
        cause instanceof Error &&
        "code" in cause &&
        (cause.code === "ENOTEMPTY" || cause.code === "EEXIST")
      )
        return "conversation_dir_nonempty";
      throw cause;
    }
  }
  return "disposed";
}
export async function resumeConversationDirectory(
  identity: DirectoryIdentity,
): Promise<DirectoryIdentity> {
  if ((await inspectConversationDirectory(identity)) !== "absent") return identity;
  const parent = NodePath.dirname(identity.path);
  if ((await NodeFSP.realpath(parent)) !== parent)
    throw new Error("Conversation directory parent changed");
  await NodeFSP.mkdir(identity.path, { mode: 0o700 });
  const stat = await NodeFSP.lstat(identity.path, { bigint: true });
  return { path: identity.path, device: String(stat.dev), inode: String(stat.ino) };
}

export function ownsConversationPath(stateDirectory: string, cwd: string | null): boolean {
  return (
    cwd !== null &&
    within(NodePath.resolve(stateDirectory, "conversation-workspaces"), NodePath.resolve(cwd))
  );
}
