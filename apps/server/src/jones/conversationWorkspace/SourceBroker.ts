// @effect-diagnostics nodeBuiltinImport:off -- Bounded, killed-and-reaped Git object reader; no checkout or shell is used.
import { spawn } from "node:child_process";
import * as path from "node:path";

export interface SourceSnapshot {
  readonly commonDirectory: string;
  readonly commit: string;
  readonly baseRef: string;
}
export type SourceRequest =
  | { readonly operation: "read" | "list"; readonly path?: string }
  | { readonly operation: "search"; readonly query: string; readonly path?: string }
  | { readonly operation: "log" | "show" }
  | { readonly operation: "diff"; readonly from: string };
export const SOURCE_LIMITS = { bytes: 256 * 1024, timeoutMs: 3000, entries: 2000 } as const;

export class SourceReadError extends Error {
  readonly reason: "invalid_request" | "limit" | "git_failed";
  constructor(reason: "invalid_request" | "limit" | "git_failed") {
    super(`Conversation source read: ${reason}`);
    this.reason = reason;
  }
}

// All arguments come from the closed command builders below. Ambient Git routing,
// lazy fetch, replacements, external diff and text conversion are disabled.
export function objectGit(args: readonly string[], options: { commonDirectory?: string; cwd?: string; bytes?: number; timeoutMs?: number } = {}): Promise<string> {
  return new Promise((resolve, reject) => {
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")));
    const child = spawn("git", ["--no-pager", ...(options.commonDirectory ? [`--git-dir=${options.commonDirectory}`] : []), "--no-replace-objects", "-c", "core.fsmonitor=false", "-c", "gc.auto=0", "-c", "maintenance.auto=false", ...args], {
      ...(options.cwd ? { cwd: options.cwd } : {}),
      env: { ...env, GIT_OPTIONAL_LOCKS: "0", GIT_NO_LAZY_FETCH: "1", GIT_TERMINAL_PROMPT: "0", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const chunks: Buffer[] = [];
    let size = 0;
    let failure: SourceReadError | undefined;
    const stop = (reason: SourceReadError["reason"]) => { failure ??= new SourceReadError(reason); child.kill("SIGKILL"); };
    const timer = setTimeout(() => stop("limit"), options.timeoutMs ?? SOURCE_LIMITS.timeoutMs);
    child.stdout.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > (options.bytes ?? SOURCE_LIMITS.bytes)) stop("limit");
      else chunks.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => { size += chunk.length; if (size > (options.bytes ?? SOURCE_LIMITS.bytes)) stop("limit"); });
    child.on("error", () => { clearTimeout(timer); reject(new SourceReadError("git_failed")); });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (failure) reject(failure);
      else if (code !== 0 && !(args[0] === "grep" && code === 1)) reject(new SourceReadError("git_failed"));
      else resolve(Buffer.concat(chunks).toString("utf8"));
    });
  });
}
const commitPattern = /^[a-f0-9]{40}(?:[a-f0-9]{24})?$/;
const sourcePath = (value = "") => {
  if (value.includes("\0") || value.includes("\\") || value.startsWith("/") || value.split("/").some((part) => part === ".." || part === ".git") || value.length > 4096) throw new SourceReadError("invalid_request");
  return value;
};
export async function resolveSource(projectRoot: string, baseRef?: string): Promise<SourceSnapshot> {
  const commonDirectory = path.resolve(projectRoot, (await objectGit(["rev-parse", "--git-common-dir"], { cwd: projectRoot })).trim());
  let selected = baseRef;
  if (selected === undefined) {
    selected = (await objectGit(["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"], { commonDirectory }).catch(() => "")).trim() || (await objectGit(["symbolic-ref", "--quiet", "HEAD"], { commonDirectory })).trim();
  }
  if (!selected || selected.startsWith("-") || selected.includes("\0") || selected.length > 1024) throw new SourceReadError("invalid_request");
  const commit = (await objectGit(["rev-parse", "--verify", "--end-of-options", `${selected}^{commit}`], { commonDirectory })).trim();
  if (!commitPattern.test(commit)) throw new SourceReadError("git_failed");
  return { commonDirectory, commit, baseRef: selected };
}
export async function readSource(snapshot: SourceSnapshot, request: SourceRequest): Promise<string> {
  if (!commitPattern.test(snapshot.commit) || !path.isAbsolute(snapshot.commonDirectory)) throw new SourceReadError("invalid_request");
  const commit = snapshot.commit;
  let args: string[];
  switch (request.operation) {
    case "read": {
      const file = sourcePath(request.path);
      if (!file) throw new SourceReadError("invalid_request");
      // cat-file follows no filesystem symlinks: a symlink blob is returned as text.
      args = ["cat-file", "blob", `${commit}:${file}`]; break;
    }
    case "list": {
      const dir = sourcePath(request.path).replace(/\/$/, "");
      args = ["ls-tree", "-z", `${commit}${dir ? `:${dir}` : ""}`]; break;
    }
    case "search": {
      if (!request.query || request.query.length > 1024 || request.query.includes("\0")) throw new SourceReadError("invalid_request");
      args = ["grep", "--no-textconv", "-n", "-I", "-F", "-e", request.query, commit, "--", ...(request.path ? [`:(literal)${sourcePath(request.path)}`] : [])]; break;
    }
    case "log": args = ["log", "--no-show-signature", "--no-textconv", "--no-ext-diff", "-20", "--format=%H %s", commit, "--"]; break;
    case "show": args = ["show", "--no-show-signature", "--no-textconv", "--no-ext-diff", "--format=fuller", "--stat", commit, "--"]; break;
    case "diff":
      if (!commitPattern.test(request.from)) throw new SourceReadError("invalid_request");
      args = ["diff", "--no-textconv", "--no-ext-diff", "--no-renames", request.from, commit, "--"]; break;
  }
  const result = await objectGit(args, { commonDirectory: snapshot.commonDirectory });
  if (result.split(request.operation === "list" ? "\0" : "\n").length > SOURCE_LIMITS.entries) throw new SourceReadError("limit");
  return result;
}
