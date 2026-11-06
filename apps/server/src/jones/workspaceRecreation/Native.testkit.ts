// @effect-diagnostics nodeBuiltinImport:off -- Small real Git fixtures use unique roots and await all Git children before exact cleanup.
import * as Assert from "node:assert/strict";
import * as FSP from "node:fs/promises";
import * as Path from "node:path";
import * as OS from "node:os";
import { binding, incarnation } from "../workspaceRetirement/OutputAttestation.ts";
import { canonical, exists, git, sha256 } from "../workspaceRetirement/Filesystem.ts";
import { missingSource, readBreadcrumb, recreate, RecreationHold, type RecreationInput, type RecreationSource } from "./Native.ts";

const g = async (cwd: string, args: string[]) => (await git(cwd, args)).toString().trim();
const commit = (cwd: string, message: string) => g(cwd, ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-qm", message]);
export const scratchReceipts: { root: string; device: number; inode: number; state: "removed" }[] = [];
export async function withNativeFixture<A>(action: (f: { root: string; repo: string; checkout: string; source: RecreationSource; input: RecreationInput }) => Promise<A>) {
  const root = await FSP.realpath(await FSP.mkdtemp(Path.join(OS.tmpdir(), "jones-swl-j4-")));
  const identity = await FSP.lstat(root);
  try {
    const repo = Path.join(root, "repo"), checkout = Path.join(root, "checkout");
    await FSP.mkdir(repo);
    await g(repo, ["init", "-b", "main", "--quiet"]);
    await FSP.writeFile(Path.join(repo, "tracked"), "retained source\n");
    await g(repo, ["add", "tracked"]);
    await commit(repo, "base");
    await g(repo, ["worktree", "add", "-b", "task", checkout]);
    const bound = await binding(checkout);
    const source: RecreationSource = { id: "receipt-j4", incarnation: incarnation(bound), binding: bound,
      head: await g(checkout, ["rev-parse", "HEAD"]), branch: "refs/heads/task" };
    return await action({ root, repo, checkout, source, input: { cwd: repo, source, owner: "turn-j4", alternatePath: Path.join(root, "alternate") } });
  } finally {
    const current = await FSP.lstat(root);
    Assert.equal(current.dev, identity.dev); Assert.equal(current.ino, identity.ino); Assert.equal(current.isSymbolicLink(), false);
    await FSP.rm(root, { recursive: true, force: true });
    Assert.equal(await exists(root), false, `fixture cleanup: ${root}`);
    scratchReceipts.push({ root, device: identity.dev, inode: identity.ino, state: "removed" });
  }
}
const fixture = withNativeFixture;
const held = (reason: RecreationHold["reason"]) => (cause: unknown) => cause instanceof RecreationHold && cause.reason === reason;
export const nativeCases: Record<string, () => Promise<void>> = {
  "terminal receipt recreates a new incarnation and replays only the observed breadcrumb": () => fixture(async ({ repo, checkout, source, input }) => {
    await g(repo, ["worktree", "remove", checkout]);
    const result = await recreate(input);
    Assert.equal(result.path, checkout); Assert.equal(result.head, source.head); Assert.equal(result.branch, source.branch);
    Assert.equal(await FSP.readFile(Path.join(checkout, "tracked"), "utf8"), "retained source\n");
    Assert.notEqual(incarnation(await binding(checkout)), source.incarnation);
    Assert.deepEqual(await readBreadcrumb((await binding(checkout)).git_dir), result);
    Assert.deepEqual(await recreate(input), result);
    Assert.deepEqual(await recreate({ ...input, owner: "later-turn", alternatePath: Path.join(Path.dirname(checkout), "another-path") }), result);
    Assert.equal(await exists(Path.join(source.binding.git_common_dir, "refs/heads/task.lock")), false);
  }),
  "manual deletion preserves reflog-only commits before removing exactly one missing registration": () => fixture(async ({ repo, checkout, input, root }) => {
    await commit(checkout, "reflog only");
    const orphan = await g(checkout, ["rev-parse", "HEAD"]);
    // Only the disposable fixture is reset to create a reflog-only tip; production never resets.
    await g(checkout, ["reset", "--hard", "HEAD~"]);
    for (const name of ["other-a", "other-b"]) {
      const path = Path.join(root, name);
      await g(repo, ["worktree", "add", "--detach", path, "HEAD"]);
      await FSP.rm(path, { recursive: true });
    }
    await FSP.rm(checkout, { recursive: true });
    const source = await missingSource(repo, checkout);
    const before = (await g(repo, ["worktree", "list", "--porcelain", "-z"])).split("\0\0").filter((b) => !b.startsWith(`worktree ${checkout}\0`));
    const result = await recreate({ ...input, source });
    const after = (await g(repo, ["worktree", "list", "--porcelain", "-z"])).split("\0\0").filter((b) => !b.startsWith(`worktree ${checkout}\0`));
    Assert.deepEqual(after, before);
    Assert.equal(result.head, source.head);
    const refs = await g(repo, ["for-each-ref", `--contains=${orphan}`, "--format=%(refname)", "refs/jones/workspace-recreation/"]);
    Assert.ok(refs.length > 0, "reflog-only tip remains reachable outside the removed admin directory");
  }),
  "occupied saved path is preserved and recreation uses a new managed path": () => fixture(async ({ repo, checkout, input }) => {
    await g(repo, ["worktree", "remove", checkout]);
    await FSP.mkdir(checkout); await FSP.writeFile(Path.join(checkout, "owner-data"), "untouched");
    const result = await recreate(input);
    Assert.equal(result.path, input.alternatePath);
    Assert.equal(await FSP.readFile(Path.join(checkout, "owner-data"), "utf8"), "untouched");
  }),
  "branch in another checkout holds without changing its HEAD or the missing path": () => fixture(async ({ repo, checkout, input, root, source }) => {
    await g(repo, ["worktree", "remove", checkout]);
    const other = Path.join(root, "in-use"); await g(repo, ["worktree", "add", other, "task"]);
    await Assert.rejects(recreate(input), held("branch_in_use"));
    Assert.equal(await g(other, ["rev-parse", "HEAD"]), source.head); Assert.equal(await exists(checkout), false);
  }),
  "changed branch holds unchanged; explicit detached request recreates receipt HEAD": () => fixture(async ({ repo, checkout, source, input }) => {
    await g(repo, ["worktree", "remove", checkout]);
    await commit(repo, "new main tip"); const changed = await g(repo, ["rev-parse", "HEAD"]);
    await g(repo, ["update-ref", "refs/heads/task", changed, source.head]);
    await Assert.rejects(recreate(input), held("branch_changed"));
    Assert.equal(await g(repo, ["rev-parse", "task"]), changed); Assert.equal(await exists(checkout), false);
    Assert.equal(await exists(Path.join(source.binding.git_common_dir, "refs/heads/task.lock")), false);
    const result = await recreate({ ...input, detached: true });
    Assert.equal(result.branch, null); Assert.equal(await g(checkout, ["rev-parse", "HEAD"]), source.head);
    Assert.equal(await g(repo, ["rev-parse", "task"]), changed);
  }),
  "missing saved branch detaches if its commit exists and unknown object never creates a checkout": () => fixture(async ({ repo, checkout, input, source }) => {
    await g(repo, ["worktree", "remove", checkout]);
    await g(repo, ["update-ref", "-d", "refs/heads/task", source.head]);
    await Assert.rejects(recreate({ ...input, source: { ...source, head: "1".repeat(40) } }), held("unknown"));
    Assert.equal(await exists(checkout), false);
    const result = await recreate(input); Assert.equal(result.branch, null); Assert.equal(result.head, source.head);
  }),
  "native guarded checkout keeps its typed refusal without changing registration": () => fixture(async ({ repo, checkout, input }) => {
    await FSP.rm(checkout, { recursive: true });
    const before = await g(repo, ["worktree", "list", "--porcelain", "-z"]);
    await Assert.rejects(recreate({ ...input, nativeGuarded: true }), held("native_recreation_unsupported"));
    Assert.equal(await g(repo, ["worktree", "list", "--porcelain", "-z"]), before);
  }),
  "locked stale registration is held and every unrelated registration survives": () => fixture(async ({ repo, checkout, input }) => {
    await g(repo, ["worktree", "lock", checkout]); await FSP.rm(checkout, { recursive: true });
    const before = await g(repo, ["worktree", "list", "--porcelain", "-z"]);
    await Assert.rejects(recreate(input), held("stale_registration"));
    Assert.equal(await g(repo, ["worktree", "list", "--porcelain", "-z"]), before);
  }),
  "an interrupted recreation intent fences later turns instead of creating another checkout": () => fixture(async ({ repo, checkout, source, input }) => {
    await g(repo, ["worktree", "remove", checkout]);
    const directory = Path.join(source.binding.git_common_dir, "workspace-lifecycle/recreations");
    await FSP.mkdir(directory, { recursive: true });
    await FSP.writeFile(Path.join(directory, sha256(source.id) + ".json"), canonical({ state: "intent", source, owner: "original-turn", result: { path: checkout } }));
    await Assert.rejects(recreate({ ...input, owner: "later-turn" }), held("unknown"));
    Assert.equal(await exists(checkout), false); Assert.equal(await exists(input.alternatePath), false);
    Assert.equal(await exists(Path.join(source.binding.git_common_dir, "refs/heads/task.lock")), false);
  }),
  "manual deletion captures and reapplies sparse worktree configuration before checkout": () => fixture(async ({ repo, checkout, input }) => {
    await FSP.mkdir(Path.join(checkout, "included")); await FSP.mkdir(Path.join(checkout, "excluded"));
    await FSP.writeFile(Path.join(checkout, "included/file"), "retained\n");
    await FSP.writeFile(Path.join(checkout, "excluded/file"), "retained but sparse\n");
    await g(checkout, ["add", "."]); await commit(checkout, "sparse source");
    await g(checkout, ["sparse-checkout", "set", "included"]);
    await FSP.rm(checkout, { recursive: true });
    const source = await missingSource(repo, checkout);
    Assert.equal(source.capturedConfig?.["core.sparsecheckout"], "true");
    const result = await recreate({ ...input, source });
    Assert.equal(result.head, source.head);
    Assert.equal(await FSP.readFile(Path.join(checkout, "included/file"), "utf8"), "retained\n");
    Assert.equal(await exists(Path.join(checkout, "excluded/file")), false);
    Assert.equal(await g(repo, ["rev-parse", "task"]), source.head);
  }),
};
