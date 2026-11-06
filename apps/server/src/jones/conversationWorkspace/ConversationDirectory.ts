// @effect-diagnostics nodeBuiltinImport:off -- Exact Jones-owned directory lifecycle uses no-follow identities and rmdir, never recursive removal.
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";

export interface DirectoryIdentity { readonly path: string; readonly device: string; readonly inode: string; }
const within = (parent: string, target: string) => { const relative = path.relative(parent, target); return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative)); };
export async function createConversationDirectory(stateDirectory: string, excluded: readonly string[]): Promise<DirectoryIdentity> {
  const state = await fs.realpath(stateDirectory);
  // Production state cannot live in scratch. Synthetic tests explicitly supply
  // a state root outside the system temporary directory too.
  const root = path.join(state, "conversation-workspaces");
  for (const candidate of [...excluded, tmpdir()]) {
    const canonical = await fs.realpath(candidate).catch((cause: unknown) => { if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return path.resolve(candidate); throw cause; });
    if (within(canonical, root)) throw new Error("Conversation directory overlaps an excluded root");
  }
  for (let ancestor = state; ; ancestor = path.dirname(ancestor)) {
    const git = await fs.lstat(path.join(ancestor, ".git")).catch((cause: unknown) => { if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null; throw cause; });
    if (git !== null) throw new Error("Conversation state lies inside a Git checkout");
    if (path.dirname(ancestor) === ancestor) break;
  }
  await fs.mkdir(root, { recursive: true, mode: 0o700 });
  const rootStat = await fs.lstat(root);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink() || await fs.realpath(root) !== root) throw new Error("Invalid conversation root");
  // @effect-diagnostics-next-line cryptoRandomUUID:off -- Node-only exclusive directory allocation; the persisted filesystem identity is the binding.
  const directory = path.join(root, randomUUID());
  await fs.mkdir(directory, { mode: 0o700 });
  const stat = await fs.lstat(directory, { bigint: true });
  return { path: directory, device: String(stat.dev), inode: String(stat.ino) };
}
export async function inspectConversationDirectory(identity: DirectoryIdentity): Promise<"empty" | "conversation_dir_nonempty" | "absent"> {
  const stat = await fs.lstat(identity.path, { bigint: true }).catch((cause: unknown) => {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null;
    throw cause;
  });
  if (stat === null) return "absent";
  if (await fs.realpath(identity.path) !== identity.path || !stat.isDirectory() || stat.isSymbolicLink() || String(stat.dev) !== identity.device || String(stat.ino) !== identity.inode) throw new Error("Conversation directory identity changed");
  const entries = await fs.opendir(identity.path);
  try { return await entries.read() === null ? "empty" : "conversation_dir_nonempty"; } finally { await entries.close(); }
}
export async function disposeConversationDirectory(identity: DirectoryIdentity): Promise<"disposed" | "conversation_dir_nonempty"> {
  const state = await inspectConversationDirectory(identity);
  if (state === "conversation_dir_nonempty") return state;
  if (state === "empty") {
    try { await fs.rmdir(identity.path); }
    catch (cause) {
      if (cause instanceof Error && "code" in cause && (cause.code === "ENOTEMPTY" || cause.code === "EEXIST")) return "conversation_dir_nonempty";
      throw cause;
    }
  }
  return "disposed";
}
export async function resumeConversationDirectory(identity: DirectoryIdentity): Promise<DirectoryIdentity> {
  if (await inspectConversationDirectory(identity) !== "absent") return identity;
  const parent = path.dirname(identity.path);
  if (await fs.realpath(parent) !== parent) throw new Error("Conversation directory parent changed");
  await fs.mkdir(identity.path, { mode: 0o700 });
  const stat = await fs.lstat(identity.path, { bigint: true });
  return { path: identity.path, device: String(stat.dev), inode: String(stat.ino) };
}

export function ownsConversationPath(stateDirectory: string, cwd: string | null): boolean {
  return cwd !== null && within(path.resolve(stateDirectory, "conversation-workspaces"), path.resolve(cwd));
}
