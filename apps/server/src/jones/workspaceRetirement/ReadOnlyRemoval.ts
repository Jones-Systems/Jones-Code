// @effect-diagnostics nodeBuiltinImport:off -- fd-relative no-follow chmod and delete run in a bounded helper script, like Disposal's unlink.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import { inside } from "./Filesystem.ts";
import type { Binding } from "./OutputAttestation.ts";

/**
 * Read-only directory trees (a Go module cache is 0555 throughout) make `git worktree remove` stop
 * part-way. Everything here acts only on the exact target path through descriptors opened with
 * O_NOFOLLOW from `/`, never follows or opens a symlink, and stays on the target's device.
 */
const BOUNDS = { maxEntries: 1_000_000, maxDepth: 512 } as const;
const HELPER_TIMEOUT_MS = 10 * 60 * 1000;

// Both scripts share this prologue: it walks from `/` to the target directory, checking its identity.
const prologue = String.raw`
import os,sys,json,stat
v=json.loads(sys.argv[1]); b=v['binding']; MAXN=v['maxEntries']; MAXD=v['maxDepth']
count=0
def op(name,fd): return os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
def listing(fd):
 global count
 with os.scandir(fd) as it: out=[(e.name,e.is_dir(follow_symlinks=False)) for e in it]
 count+=len(out)
 if count>MAXN: raise RuntimeError('walk_bound')
 return out
fds=[]
def open_path(parts):
 fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY); fds.append(fd)
 for part in parts:
  fd=op(part,fd); fds.append(fd)
 return fd
`;
const writableScript =
  prologue +
  String.raw`
changed=0; skipped=[]
def writable(fd,dev,depth):
 global changed
 if depth>MAXD: raise RuntimeError('walk_bound')
 for name,isdir in listing(fd):
  if not isdir: continue
  cfd=op(name,fd)
  try:
   s=os.fstat(cfd)
   if s.st_dev!=dev: skipped.append(name); continue
   if (s.st_mode&0o700)!=0o700: os.fchmod(cfd,stat.S_IMODE(s.st_mode)|0o700); changed+=1
   writable(cfd,dev,depth+1)
  finally: os.close(cfd)
try:
 root=open_path(b['path'].strip('/').split('/'))
 rs=os.fstat(root)
 if rs.st_dev!=b['dev'] or rs.st_ino!=b['ino']: raise RuntimeError('identity_mismatch')
 for rel in v['roots']:
  parts=rel.split('/')
  if any(x in ('','.','..','.git') for x in parts): raise RuntimeError('identity_mismatch')
  fd=root; opened=[]
  try:
   for part in parts[:-1]:
    fd=op(part,fd); opened.append(fd)
   try: s=os.stat(parts[-1],dir_fd=fd,follow_symlinks=False)
   except FileNotFoundError: continue
   # A symlink, a file or a mount point is never entered; only directories on this device are made writable.
   if not stat.S_ISDIR(s.st_mode) or s.st_dev!=rs.st_dev: skipped.append(rel); continue
   cfd=op(parts[-1],fd); opened.append(cfd)
   cs=os.fstat(cfd)
   if cs.st_dev!=rs.st_dev or cs.st_ino!=s.st_ino: raise RuntimeError('disposal_path_changed')
   if (cs.st_mode&0o700)!=0o700: os.fchmod(cfd,stat.S_IMODE(cs.st_mode)|0o700); changed+=1
   writable(cfd,rs.st_dev,1)
  finally:
   for o in reversed(opened): os.close(o)
 print(json.dumps(dict(changed=changed,skipped=skipped[:50])))
finally:
 for fd in reversed(fds): os.close(fd)
`;
const removeScript =
  prologue +
  String.raw`
def purge(fd,dev,depth):
 if depth>MAXD: raise RuntimeError('walk_bound')
 for name,isdir in listing(fd):
  if isdir:
   cfd=op(name,fd)
   try:
    if os.fstat(cfd).st_dev!=dev: raise RuntimeError('crossed_device')
    purge(cfd,dev,depth+1)
   finally: os.close(cfd)
   os.rmdir(name,dir_fd=fd)
  else: os.unlink(name,dir_fd=fd)
try:
 parts=b['path'].strip('/').split('/')
 parent=open_path(parts[:-1])
 root=op(parts[-1],parent); fds.append(root)
 rs=os.fstat(root)
 if rs.st_dev!=b['dev'] or rs.st_ino!=b['ino']: raise RuntimeError('identity_mismatch')
 purge(root,rs.st_dev,1)
 os.rmdir(parts[-1],dir_fd=parent)
 print(json.dumps(dict(removed=count)))
finally:
 for fd in reversed(fds): os.close(fd)
`;

type Target = Pick<Binding, "path" | "dev" | "ino">;
function runHelper(script: string, payload: unknown): Promise<unknown> {
  return new Promise((resolve, reject) =>
    NodeChildProcess.execFile(
      "/usr/bin/python3",
      ["-I", "-S", "-B", "-c", script, JSON.stringify({ ...BOUNDS, ...(payload as object) })],
      { timeout: HELPER_TIMEOUT_MS, maxBuffer: 1024 * 1024 },
      (error, stdout, stderr) =>
        error
          ? reject(
              new Error(
                `read_only_helper_failed: ${String(stderr).trim().split(/\r?\n/).at(-1) ?? error.message}`,
              ),
            )
          : resolve(JSON.parse(String(stdout))),
    ),
  );
}

/** Roots strictly inside the target, with none nested in another; anything else is a caller error. */
function relativeRoots(target: Target, roots: readonly string[]) {
  const relative = roots.map((root) => {
    if (!inside(root, target.path) || root === target.path) throw new Error("identity_mismatch");
    const rel = NodePath.relative(target.path, root).split(NodePath.sep);
    if (rel.some((part) => part === "" || part === "." || part === ".." || part === ".git"))
      throw new Error("identity_mismatch");
    return rel.join("/");
  });
  return relative.filter(
    (rel) => !relative.some((other) => other !== rel && rel.startsWith(other + "/")),
  );
}

/**
 * Gives the owner read, write and search permission on every directory under the named ignored roots.
 * Files keep their modes: unlinking needs only a writable parent. Symlinks and other devices are
 * skipped, and nothing outside the roots is touched.
 */
export async function makeRootsWritable(target: Target, roots: readonly string[]) {
  const relative = relativeRoots(target, roots);
  if (!relative.length) return { changed: 0, skipped: [] as string[] };
  return (await runHelper(writableScript, { binding: target, roots: relative })) as {
    changed: number;
    skipped: string[];
  };
}

/** Deletes the exact target directory and everything under it without following a symlink. */
export async function removeTree(target: Target) {
  return (await runHelper(removeScript, { binding: target })) as { removed: number };
}

/**
 * Every spelling of a path that a record may have been written under: the requested one and the one
 * with its nearest existing ancestor canonicalized (a checkout that is gone cannot be realpath'd).
 */
export async function pathSpellings(path: string): Promise<readonly string[]> {
  let existing = path;
  const rest: string[] = [];
  for (;;) {
    try {
      const resolved = await NodeFSP.realpath(existing);
      return [...new Set([path, NodePath.join(resolved, ...rest)])];
    } catch {
      const parent = NodePath.dirname(existing);
      if (parent === existing) return [path];
      rest.unshift(NodePath.basename(existing));
      existing = parent;
    }
  }
}
