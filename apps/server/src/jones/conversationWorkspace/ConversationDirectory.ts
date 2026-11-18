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
export const placementRefusals = {
  excluded_root: "Conversation directory overlaps an excluded root",
  git_checkout: "Conversation state lies inside a Git checkout",
  system_temp: "Conversation state lies inside the system temporary directory",
  invalid_root: "Invalid conversation root",
  reserved_root_registered:
    "A project or thread workspace is inside the reserved conversation root",
} as const;

export class ConversationPlacementError extends Error {
  readonly rule: keyof typeof placementRefusals;

  constructor(rule: keyof typeof placementRefusals) {
    super(placementRefusals[rule]);
    this.rule = rule;
  }
}

const isMissing = (cause: unknown) =>
  cause instanceof Error && "code" in cause && cause.code === "ENOENT";

const lstatIfPresent = (path: string) =>
  NodeFSP.lstat(path).catch((cause: unknown) => {
    if (isMissing(cause)) return null;
    throw cause;
  });

async function realLocation(path: string): Promise<string> {
  try {
    return await NodeFSP.realpath(path);
  } catch (cause) {
    if (!isMissing(cause) || (await lstatIfPresent(path)) !== null) throw cause;
    const parent = NodePath.dirname(path);
    if (parent === path) throw cause;
    return NodePath.join(await realLocation(parent), NodePath.basename(path));
  }
}

async function validateConversationRoot(root: string): Promise<void> {
  const stat = await lstatIfPresent(root);
  if (
    stat !== null &&
    (!stat.isDirectory() || stat.isSymbolicLink() || (await NodeFSP.realpath(root)) !== root)
  ) {
    throw new ConversationPlacementError("invalid_root");
  }
}

export async function checkConversationDirectoryPlacement(
  stateDirectory: string,
  excluded: readonly string[],
  registered: readonly string[] = [],
): Promise<string> {
  const state = await NodeFSP.realpath(stateDirectory);
  const root = NodePath.join(state, "conversation-workspaces");
  await validateConversationRoot(root);
  if (within(await realLocation(NodeOS.tmpdir()), root)) {
    throw new ConversationPlacementError("system_temp");
  }
  for (const candidate of excluded) {
    const location = await realLocation(NodePath.resolve(candidate));
    if (within(location, root) || within(root, location)) {
      throw new ConversationPlacementError("excluded_root");
    }
  }
  for (const candidate of registered) {
    if (within(root, await realLocation(NodePath.resolve(candidate)))) {
      throw new ConversationPlacementError("reserved_root_registered");
    }
  }
  for (let ancestor = root; ; ancestor = NodePath.dirname(ancestor)) {
    if ((await lstatIfPresent(NodePath.join(ancestor, ".git"))) !== null) {
      throw new ConversationPlacementError("git_checkout");
    }
    if (NodePath.dirname(ancestor) === ancestor) break;
  }
  return root;
}

export async function createConversationDirectory(
  stateDirectory: string,
  excluded: readonly string[],
  registered: readonly string[] = [],
): Promise<DirectoryIdentity> {
  const root = await checkConversationDirectoryPlacement(stateDirectory, excluded, registered);
  await NodeFSP.mkdir(root, { recursive: true, mode: 0o700 });
  await validateConversationRoot(root);
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
