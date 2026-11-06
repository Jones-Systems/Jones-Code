// @effect-diagnostics nodeBuiltinImport:off -- Small real Git fixtures use unique roots and await all Git children before exact cleanup.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { binding, incarnation } from "../workspaceRetirement/OutputAttestation.ts";
import { canonical, exists, git, sha256 } from "../workspaceRetirement/Filesystem.ts";
import {
  missingSource,
  readBreadcrumb,
  recreate,
  RecreationHold,
  knownRecreation,
  acknowledgeInvalidation,
  type RecreationInput,
  type RecreationSource,
} from "./Native.ts";

const g = async (cwd: string, args: string[]) => (await git(cwd, args)).toString().trim();
const commit = (cwd: string, message: string) =>
  g(cwd, [
    "-c",
    "user.name=Fixture",
    "-c",
    "user.email=fixture@example.invalid",
    "commit",
    "--allow-empty",
    "-qm",
    message,
  ]);
export async function partialSource(repo: string, checkout: string) {
  await NodeFSP.writeFile(NodePath.join(checkout, ".gitattributes"), "tracked filter=fixture-failure\n");
  await g(checkout, ["add", ".gitattributes"]);
  await commit(checkout, "required population filter");
  await g(repo, ["config", "filter.fixture-failure.smudge", "false"]);
  await g(repo, ["config", "filter.fixture-failure.required", "true"]);
  await NodeFSP.rm(checkout, { recursive: true });
  return missingSource(repo, checkout);
}
export const scratchReceipts: { root: string; device: number; inode: number; state: "removed" }[] =
  [];
export interface NativeFixture {
  readonly root: string;
  readonly repo: string;
  readonly checkout: string;
  readonly source: RecreationSource;
  readonly input: RecreationInput;
}
const fixtureOwners = new WeakMap<NativeFixture, { device: number; inode: number }>();
async function cleanupFixture(root: string, identity: { device: number; inode: number }) {
  const current = await NodeFSP.lstat(root);
  NodeAssert.equal(current.dev, identity.device);
  NodeAssert.equal(current.ino, identity.inode);
  NodeAssert.equal(current.isSymbolicLink(), false);
  await NodeFSP.rm(root, { recursive: true, force: true });
  NodeAssert.equal(await exists(root), false, `fixture cleanup: ${root}`);
  scratchReceipts.push({ root, ...identity, state: "removed" });
}
export async function acquireNativeFixture(): Promise<NativeFixture> {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-swl-j4-")),
  );
  const stat = await NodeFSP.lstat(root);
  const identity = { device: stat.dev, inode: stat.ino };
  try {
    const repo = NodePath.join(root, "repo"),
      checkout = NodePath.join(root, "checkout");
    await NodeFSP.mkdir(repo);
    await g(repo, ["init", "-b", "main", "--quiet"]);
    await NodeFSP.writeFile(NodePath.join(repo, "tracked"), "retained source\n");
    await g(repo, ["add", "tracked"]);
    await commit(repo, "base");
    await g(repo, ["worktree", "add", "-b", "task", checkout]);
    const bound = await binding(checkout);
    const source: RecreationSource = {
      id: "receipt-j4",
      incarnation: incarnation(bound),
      binding: bound,
      head: await g(checkout, ["rev-parse", "HEAD"]),
      branch: "refs/heads/task",
    };
    const fixture = {
      root,
      repo,
      checkout,
      source,
      input: { cwd: repo, source, owner: "turn-j4", alternatePath: NodePath.join(root, "alternate") },
    };
    fixtureOwners.set(fixture, identity);
    return fixture;
  } catch (cause) {
    try {
      await cleanupFixture(root, identity);
    } catch (cleanup) {
      throw new AggregateError([cause, cleanup], "Native fixture acquisition/cleanup failed");
    }
    throw cause;
  }
}
export async function releaseNativeFixture(fixture: NativeFixture) {
  const identity = fixtureOwners.get(fixture);
  if (identity === undefined) throw new Error("Native fixture cleanup owner unavailable");
  await cleanupFixture(fixture.root, identity);
  fixtureOwners.delete(fixture);
}
export async function withNativeFixture<A>(action: (f: NativeFixture) => Promise<A>) {
  const fixture = await acquireNativeFixture();
  try {
    return await action(fixture);
  } finally {
    await releaseNativeFixture(fixture);
  }
}
const fixture = withNativeFixture;
const held = (reason: RecreationHold["reason"]) => (cause: unknown) =>
  cause instanceof RecreationHold && cause.reason === reason;
export const nativeCases: Record<string, () => Promise<void>> = {
  "acknowledged manual recreation allows the next descendant commit only in the same incarnation": () =>
    fixture(async ({ repo, checkout, input, root }) => {
      await NodeFSP.rm(checkout, { recursive: true });
      const source = await missingSource(repo, checkout);
      const result = await recreate({ ...input, source });
      const physical = incarnation(await binding(checkout));
      await acknowledgeInvalidation(result);
      await commit(checkout, "ordinary next commit");
      const head = await g(checkout, ["rev-parse", "HEAD"]);
      NodeAssert.notEqual(head, result.head);
      const before = await g(repo, ["worktree", "list", "--porcelain", "-z"]);
      NodeAssert.deepEqual(await knownRecreation(repo, checkout), result);
      NodeAssert.deepEqual(await knownRecreation(repo, checkout), result);
      NodeAssert.equal(incarnation(await binding(checkout)), physical);
      NodeAssert.equal(await g(checkout, ["rev-parse", "HEAD"]), head);
      NodeAssert.equal(await g(repo, ["worktree", "list", "--porcelain", "-z"]), before);
      NodeAssert.equal(await exists(input.alternatePath), false);
      await NodeAssert.rejects(recreate({ ...input, source }), held("unknown"));
      const moved = NodePath.join(root, "original-incarnation");
      await NodeFSP.rename(checkout, moved);
      await NodeFSP.mkdir(checkout);
      await NodeFSP.copyFile(NodePath.join(moved, ".git"), NodePath.join(checkout, ".git"));
      await NodeAssert.rejects(knownRecreation(repo, checkout), held("unknown"));
      NodeAssert.equal(await g(repo, ["rev-parse", "refs/heads/task"]), head);
      NodeAssert.equal(await exists(input.alternatePath), false);
    }),
  "an advanced HEAD without invalidation acknowledgement keeps strict creation readback": () =>
    fixture(async ({ repo, checkout, input }) => {
      await NodeFSP.rm(checkout, { recursive: true });
      const source = await missingSource(repo, checkout);
      const result = await recreate({ ...input, source });
      await commit(checkout, "unacknowledged next commit");
      const head = await g(checkout, ["rev-parse", "HEAD"]);
      NodeAssert.notEqual(head, result.head);
      await NodeAssert.rejects(knownRecreation(repo, checkout), held("unknown"));
      NodeAssert.equal(await g(checkout, ["rev-parse", "HEAD"]), head);
      NodeAssert.equal(await exists(input.alternatePath), false);
    }),
  "terminal receipt recreates a new incarnation and replays only the observed breadcrumb": () =>
    fixture(async ({ repo, checkout, source, input }) => {
      await g(repo, ["worktree", "remove", checkout]);
      const result = await recreate(input);
      NodeAssert.equal(result.path, checkout);
      NodeAssert.equal(result.head, source.head);
      NodeAssert.equal(result.branch, source.branch);
      NodeAssert.equal(await NodeFSP.readFile(NodePath.join(checkout, "tracked"), "utf8"), "retained source\n");
      NodeAssert.notEqual(incarnation(await binding(checkout)), source.incarnation);
      NodeAssert.deepEqual(await readBreadcrumb((await binding(checkout)).git_dir), result);
      NodeAssert.deepEqual(await knownRecreation(repo, checkout), result);
      NodeAssert.deepEqual(await recreate(input), result);
      NodeAssert.deepEqual(
        await recreate({
          ...input,
          owner: "later-turn",
          alternatePath: NodePath.join(NodePath.dirname(checkout), "another-path"),
        }),
        result,
      );
      NodeAssert.equal(
        await exists(NodePath.join(source.binding.git_common_dir, "refs/heads/task.lock")),
        false,
      );
    }),
  "a real post-add population failure keeps present-path retries behind the original intent": () =>
    fixture(async ({ repo, checkout, input }) => {
      NodeAssert.equal(await knownRecreation(repo, checkout), null);
      const source = await partialSource(repo, checkout);
      const attempt = { ...input, source };
      await NodeAssert.rejects(recreate(attempt), held("unknown"));
      NodeAssert.equal(await exists(checkout), true);
      NodeAssert.equal(await exists(NodePath.join(checkout, "tracked")), false);
      const pointer = NodePath.join(
        source.binding.git_common_dir,
        "workspace-lifecycle/recreations",
        `target-${sha256(checkout)}.json`,
      );
      const retained = await NodeFSP.readFile(pointer);
      await NodeAssert.rejects(knownRecreation(repo, checkout), held("unknown"));
      await NodeFSP.unlink(NodePath.join(checkout, ".git"));
      const before = await g(repo, ["worktree", "list", "--porcelain", "-z"]);
      await NodeAssert.rejects(knownRecreation(repo, checkout), held("unknown"));
      await NodeAssert.rejects(recreate(attempt), held("unknown"));
      NodeAssert.equal(await g(repo, ["worktree", "list", "--porcelain", "-z"]), before);
      NodeAssert.deepEqual(await NodeFSP.readFile(pointer), retained);
      NodeAssert.equal(
        await exists(NodePath.join(source.binding.git_common_dir, "refs/heads/task.lock")),
        false,
      );
      NodeAssert.equal(await exists(attempt.alternatePath), false);
    }),
  "manual deletion preserves reflog-only commits before removing exactly one missing registration":
    () =>
      fixture(async ({ repo, checkout, input, root }) => {
        await commit(checkout, "reflog only");
        const orphan = await g(checkout, ["rev-parse", "HEAD"]);
        // Only the disposable fixture is reset to create a reflog-only tip; production never resets.
        await g(checkout, ["reset", "--hard", "HEAD~"]);
        for (const name of ["other-a", "other-b"]) {
          const path = NodePath.join(root, name);
          await g(repo, ["worktree", "add", "--detach", path, "HEAD"]);
          await NodeFSP.rm(path, { recursive: true });
        }
        await NodeFSP.rm(checkout, { recursive: true });
        const source = await missingSource(repo, checkout);
        const before = (await g(repo, ["worktree", "list", "--porcelain", "-z"]))
          .split("\0\0")
          .filter((b) => !b.startsWith(`worktree ${checkout}\0`));
        const result = await recreate({ ...input, source });
        const after = (await g(repo, ["worktree", "list", "--porcelain", "-z"]))
          .split("\0\0")
          .filter((b) => !b.startsWith(`worktree ${checkout}\0`));
        NodeAssert.deepEqual(after, before);
        NodeAssert.equal(result.head, source.head);
        const refs = await g(repo, [
          "for-each-ref",
          `--contains=${orphan}`,
          "--format=%(refname)",
          "refs/jones/workspace-recreation/",
        ]);
        NodeAssert.ok(
          refs.length > 0,
          "reflog-only tip remains reachable outside the removed admin directory",
        );
      }),
  "occupied saved path is preserved and recreation uses a new managed path": () =>
    fixture(async ({ repo, checkout, input }) => {
      await g(repo, ["worktree", "remove", checkout]);
      await NodeFSP.mkdir(checkout);
      await NodeFSP.writeFile(NodePath.join(checkout, "owner-data"), "untouched");
      const result = await recreate(input);
      NodeAssert.equal(result.path, input.alternatePath);
      NodeAssert.equal(await NodeFSP.readFile(NodePath.join(checkout, "owner-data"), "utf8"), "untouched");
    }),
  "branch in another checkout holds without changing its HEAD or the missing path": () =>
    fixture(async ({ repo, checkout, input, root, source }) => {
      await g(repo, ["worktree", "remove", checkout]);
      const other = NodePath.join(root, "in-use");
      await g(repo, ["worktree", "add", other, "task"]);
      await NodeAssert.rejects(recreate(input), held("branch_in_use"));
      NodeAssert.equal(await g(other, ["rev-parse", "HEAD"]), source.head);
      NodeAssert.equal(await exists(checkout), false);
    }),
  "changed branch holds unchanged; explicit detached request recreates receipt HEAD": () =>
    fixture(async ({ repo, checkout, source, input }) => {
      await g(repo, ["worktree", "remove", checkout]);
      await commit(repo, "new main tip");
      const changed = await g(repo, ["rev-parse", "HEAD"]);
      await g(repo, ["update-ref", "refs/heads/task", changed, source.head]);
      await NodeAssert.rejects(recreate(input), held("branch_changed"));
      NodeAssert.equal(await g(repo, ["rev-parse", "task"]), changed);
      NodeAssert.equal(await exists(checkout), false);
      NodeAssert.equal(
        await exists(NodePath.join(source.binding.git_common_dir, "refs/heads/task.lock")),
        false,
      );
      const result = await recreate({ ...input, detached: true });
      NodeAssert.equal(result.branch, null);
      NodeAssert.equal(await g(checkout, ["rev-parse", "HEAD"]), source.head);
      NodeAssert.equal(await g(repo, ["rev-parse", "task"]), changed);
    }),
  "missing saved branch detaches if its commit exists and unknown object never creates a checkout":
    () =>
      fixture(async ({ repo, checkout, input, source }) => {
        await g(repo, ["worktree", "remove", checkout]);
        await g(repo, ["update-ref", "-d", "refs/heads/task", source.head]);
        await NodeAssert.rejects(
          recreate({ ...input, source: { ...source, head: "1".repeat(40) } }),
          held("unknown"),
        );
        NodeAssert.equal(await exists(checkout), false);
        const result = await recreate(input);
        NodeAssert.equal(result.branch, null);
        NodeAssert.equal(result.head, source.head);
      }),
  "native guarded checkout keeps its typed refusal without changing registration": () =>
    fixture(async ({ repo, checkout, input }) => {
      await NodeFSP.rm(checkout, { recursive: true });
      const before = await g(repo, ["worktree", "list", "--porcelain", "-z"]);
      await NodeAssert.rejects(
        recreate({ ...input, nativeGuarded: true }),
        held("native_recreation_unsupported"),
      );
      NodeAssert.equal(await g(repo, ["worktree", "list", "--porcelain", "-z"]), before);
    }),
  "locked stale registration is held and every unrelated registration survives": () =>
    fixture(async ({ repo, checkout, input }) => {
      await g(repo, ["worktree", "lock", checkout]);
      await NodeFSP.rm(checkout, { recursive: true });
      const before = await g(repo, ["worktree", "list", "--porcelain", "-z"]);
      await NodeAssert.rejects(recreate(input), held("stale_registration"));
      NodeAssert.equal(await g(repo, ["worktree", "list", "--porcelain", "-z"]), before);
    }),
  "an interrupted recreation intent fences later turns instead of creating another checkout": () =>
    fixture(async ({ repo, checkout, source, input }) => {
      await g(repo, ["worktree", "remove", checkout]);
      const directory = NodePath.join(source.binding.git_common_dir, "workspace-lifecycle/recreations");
      await NodeFSP.mkdir(directory, { recursive: true });
      await NodeFSP.writeFile(
        NodePath.join(directory, sha256(source.id) + ".json"),
        canonical({ state: "intent", source, owner: "original-turn", result: { path: checkout } }),
      );
      await NodeAssert.rejects(recreate({ ...input, owner: "later-turn" }), held("unknown"));
      NodeAssert.equal(await exists(checkout), false);
      NodeAssert.equal(await exists(input.alternatePath), false);
      NodeAssert.equal(
        await exists(NodePath.join(source.binding.git_common_dir, "refs/heads/task.lock")),
        false,
      );
    }),
  "manual deletion captures and reapplies sparse worktree configuration before checkout": () =>
    fixture(async ({ repo, checkout, input }) => {
      await NodeFSP.mkdir(NodePath.join(checkout, "included"));
      await NodeFSP.mkdir(NodePath.join(checkout, "excluded"));
      await NodeFSP.writeFile(NodePath.join(checkout, "included/file"), "retained\n");
      await NodeFSP.writeFile(NodePath.join(checkout, "excluded/file"), "retained but sparse\n");
      await g(checkout, ["add", "."]);
      await commit(checkout, "sparse source");
      await g(checkout, ["sparse-checkout", "set", "included"]);
      await NodeFSP.rm(checkout, { recursive: true });
      const source = await missingSource(repo, checkout);
      NodeAssert.equal(source.capturedConfig?.["core.sparsecheckout"], "true");
      const result = await recreate({ ...input, source });
      NodeAssert.equal(result.head, source.head);
      NodeAssert.equal(await NodeFSP.readFile(NodePath.join(checkout, "included/file"), "utf8"), "retained\n");
      NodeAssert.equal(await exists(NodePath.join(checkout, "excluded/file")), false);
      NodeAssert.equal(await g(repo, ["rev-parse", "task"]), source.head);
    }),
};
