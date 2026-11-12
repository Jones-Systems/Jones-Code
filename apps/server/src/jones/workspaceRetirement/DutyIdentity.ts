// @effect-diagnostics nodeBuiltinImport:off -- Real duty admission uses native metadata, independently of optional provenance capture.
import * as NodeFSP from "node:fs/promises";
import * as Context from "effect/Context";
import * as NodePath from "node:path";
import { canonical, git, inside, openAbsolute, sha256 } from "./Filesystem.ts";

export type DutyIdentity =
  | {
      status: "bound";
      incarnation: string;
      binding: {
        path: string;
        dev: string;
        ino: string;
        gitDirectory: string;
        gitDevice: string;
        gitInode: string;
        commonDirectory: string;
      };
    }
  | { status: "unbound"; reason: "native_worktree_identity_unavailable"; nextAction: string };

// No breadcrumb, output proof, recursive traversal, registry or optional metadata is read.
// Keep the requested lexical path: a replaced symlink must not claim a different checkout.
export async function dutyIdentity(worktree: string): Promise<DutyIdentity> {
  try {
    if (!NodePath.isAbsolute(worktree) || NodePath.normalize(worktree) !== worktree)
      throw new Error("noncanonical_path");
    const root = await openAbsolute(worktree, true);
    try {
      const before = await root.stat({ bigint: true });
      const gitDirectory = await NodeFSP.realpath(
        (await git(worktree, ["rev-parse", "--absolute-git-dir"], 1000)).toString().trim(),
      );
      const commonDirectory = await NodeFSP.realpath(
        (await git(worktree, ["rev-parse", "--path-format=absolute", "--git-common-dir"], 1000))
          .toString()
          .trim(),
      );
      const admin = await openAbsolute(gitDirectory, true);
      try {
        const native = await admin.stat({ bigint: true }),
          after = await NodeFSP.lstat(worktree, { bigint: true });
        if (!after.isDirectory() || before.dev !== after.dev || before.ino !== after.ino)
          throw new Error("identity_changed");
        const binding = {
          path: worktree,
          dev: String(before.dev),
          ino: String(before.ino),
          gitDirectory,
          gitDevice: String(native.dev),
          gitInode: String(native.ino),
          commonDirectory,
        };
        return { status: "bound", incarnation: sha256(canonical(binding)), binding };
      } finally {
        await admin.close();
      }
    } finally {
      await root.close();
    }
  } catch {
    // This is an explicit unresolved *real duty*, never an invented physical incarnation
    // or an optional-observability failure that prevents the user's ordinary operation.
    return {
      status: "unbound",
      reason: "native_worktree_identity_unavailable",
      nextAction: `Observe the native root and Git directory for ${worktree} through this duty's owner, or record its actual completion/drain; then reevaluate this target.`,
    };
  }
}

interface TerminalWorkspace {
  readonly requestedCwd: string;
  readonly cwd: string;
  readonly cwdDevice: string;
  readonly cwdInode: string;
  readonly worktree: string | null;
  readonly identity: DutyIdentity | null;
  readonly launchIdentity: string;
}

/** Native checkout membership, independent of optional provenance or client hints. */
export async function terminalWorkspace(
  cwd: string,
  hint?: string | null,
): Promise<TerminalWorkspace> {
  const physicalCwd = await NodeFSP.realpath(cwd);
  const directory = await openAbsolute(physicalCwd, true);
  try {
    const before = await directory.stat({ bigint: true });
    let root: string | null;
    try {
      const result = await git(physicalCwd, ["rev-parse", "--show-toplevel"], 1000);
      if (result.length > 65536) throw new Error("native_checkout_root_bound");
      root = await NodeFSP.realpath(result.toString().trim());
    } catch (cause) {
      // A known non-Git directory needs no checkout claim. An unavailable native
      // membership observation is not permission to bind a guessed client path.
      if (cause instanceof Error && cause.message.includes("not a git repository")) root = null;
      else throw cause;
    }
    const identity = root === null ? null : await dutyIdentity(root);
    if (root !== null && (identity?.status !== "bound" || !inside(physicalCwd, root)))
      throw new Error(
        "native_checkout_root_unavailable; reobserve this terminal's root and Git directory",
      );
    if (root !== null && hint != null && (await NodeFSP.realpath(hint)) !== root)
      throw new Error("terminal_worktree_hint_mismatch; use the server-observed checkout root");
    const result: TerminalWorkspace = {
      requestedCwd: cwd,
      cwd: physicalCwd,
      cwdDevice: String(before.dev),
      cwdInode: String(before.ino),
      worktree: root,
      identity,
      // Comparison data from this native observation, never a claim or removal token.
      launchIdentity: JSON.stringify([
        physicalCwd,
        String(before.dev),
        String(before.ino),
        identity?.status === "bound" ? identity.incarnation : null,
      ]),
    };
    await revalidateTerminalWorkspace(result);
    return result;
  } finally {
    await directory.close();
  }
}

export async function revalidateTerminalWorkspace(observed: TerminalWorkspace): Promise<void> {
  if ((await NodeFSP.realpath(observed.requestedCwd)) !== observed.cwd)
    throw new Error("terminal_cwd_identity_changed");
  const cwd = await openAbsolute(observed.cwd, true);
  try {
    const stat = await cwd.stat({ bigint: true });
    if (String(stat.dev) !== observed.cwdDevice || String(stat.ino) !== observed.cwdInode)
      throw new Error("terminal_cwd_identity_changed");
    if (observed.identity?.status === "bound") {
      const bound = observed.identity.binding;
      const root = await openAbsolute(bound.path, true);
      try {
        const current = await root.stat({ bigint: true });
        const admin = await openAbsolute(bound.gitDirectory, true);
        try {
          const gitDirectory = await admin.stat({ bigint: true });
          if (
            String(current.dev) !== bound.dev ||
            String(current.ino) !== bound.ino ||
            String(gitDirectory.dev) !== bound.gitDevice ||
            String(gitDirectory.ino) !== bound.gitInode
          )
            throw new Error("terminal_checkout_identity_changed");
        } finally {
          await admin.close();
        }
      } finally {
        await root.close();
      }
    }
  } finally {
    await cwd.close();
  }
}

export class TerminalWorkspaceObservation extends Context.Reference<{
  readonly observe: typeof terminalWorkspace;
  readonly revalidate: typeof revalidateTerminalWorkspace;
}>("jones/workspaceRetirement/TerminalWorkspaceObservation", {
  defaultValue: () => ({ observe: terminalWorkspace, revalidate: revalidateTerminalWorkspace }),
}) {}

// Internal observed facts, scoped only inside the matching native root lease.
export class TerminalWorkspaceAdmission extends Context.Reference<TerminalWorkspace | null>(
  "jones/workspaceRetirement/TerminalWorkspaceAdmission",
  { defaultValue: () => null },
) {}
