// @effect-diagnostics nodeBuiltinImport:off -- Real duty admission uses native metadata, independently of optional provenance capture.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { canonical, git, openAbsolute, sha256 } from "./Filesystem.ts";

export type DutyIdentity =
  | {status:"bound";incarnation:string;binding:{path:string;dev:string;ino:string;gitDirectory:string;gitDevice:string;gitInode:string;commonDirectory:string}}
  | {status:"unbound";reason:"native_worktree_identity_unavailable";nextAction:string};

// No breadcrumb, output proof, recursive traversal, registry or optional metadata is read.
// Keep the requested lexical path: a replaced symlink must not claim a different checkout.
export async function dutyIdentity(worktree:string):Promise<DutyIdentity> {
  try {
    if(!NodePath.isAbsolute(worktree) || NodePath.normalize(worktree)!==worktree) throw new Error("noncanonical_path");
    const root=await openAbsolute(worktree,true);
    try {
      const before=await root.stat({bigint:true});
      const gitDirectory=await NodeFSP.realpath((await git(worktree,["rev-parse","--absolute-git-dir"],1000)).toString().trim());
      const commonDirectory=await NodeFSP.realpath((await git(worktree,["rev-parse","--path-format=absolute","--git-common-dir"],1000)).toString().trim());
      const admin=await openAbsolute(gitDirectory,true);
      try {
        const native=await admin.stat({bigint:true}), after=await NodeFSP.lstat(worktree,{bigint:true});
        if(!after.isDirectory() || before.dev!==after.dev || before.ino!==after.ino) throw new Error("identity_changed");
        const binding={path:worktree,dev:String(before.dev),ino:String(before.ino),gitDirectory,gitDevice:String(native.dev),gitInode:String(native.ino),commonDirectory};
        return {status:"bound",incarnation:sha256(canonical(binding)),binding};
      } finally {await admin.close();}
    } finally {await root.close();}
  } catch {
    // This is an explicit unresolved *real duty*, never an invented physical incarnation
    // or an optional-observability failure that prevents the user's ordinary operation.
    return {status:"unbound",reason:"native_worktree_identity_unavailable",nextAction:`Observe the native root and Git directory for ${worktree} through this duty's owner, or record its actual completion/drain; then reevaluate this target.`};
  }
}
