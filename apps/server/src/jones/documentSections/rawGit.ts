import * as HostProcess from "@t3tools/shared/HostProcess";
// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- This byte-preserving Promise boundary owns its exact Git child, deadline, bounded pipes and cancellation cleanup.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  DocumentSectionRefusal,
  decodeUtf8,
  MAX_DOCUMENT_BYTES,
  validateSelector,
} from "./pure.ts";
import type { DocumentSectionsSelector } from "../../../../../packages/contracts/src/jones/documentSections.ts";

export interface RawGitResult {
  readonly exitCode: number;
  readonly stdout: Uint8Array;
}
export type RawGitRunner = (
  root: string,
  args: readonly string[],
  stdoutLimit: number,
  signal: AbortSignal,
) => Promise<RawGitResult>;

export function boundedGitEnvironment(inherited: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const name of ["PATH", "SystemRoot", "WINDIR", "TMPDIR", "TMP", "TEMP"]) {
    if (inherited[name] !== undefined) env[name] = inherited[name];
  }
  const nullDevice = HostProcess.Platform.defaultValue() === "win32" ? "NUL" : "/dev/null";
  return {
    ...env,
    GIT_CONFIG_GLOBAL: nullDevice,
    GIT_CONFIG_SYSTEM: nullDevice,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_NO_REPLACE_OBJECTS: "1",
    GIT_NO_LAZY_FETCH: "1",
    GIT_OPTIONAL_LOCKS: "0",
    GIT_TERMINAL_PROMPT: "0",
    GIT_LITERAL_PATHSPECS: "1",
    GIT_ATTR_NOSYSTEM: "1",
    GIT_PROTOCOL_FROM_USER: "0",
    GIT_ALLOW_PROTOCOL: "",
    LC_ALL: "C",
  };
}

/** Fixed Git-only runner. Cancellation/overflow/deadline kill only its captured child. */
export const runRawGit: RawGitRunner = (root, args, stdoutLimit, signal) =>
  new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DocumentSectionRefusal("cancelled"));
      return;
    }
    const child = NodeChildProcess.spawn(
      "git",
      [
        "--no-pager",
        "--no-replace-objects",
        "--literal-pathspecs",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "protocol.allow=never",
        "-C",
        root,
        ...args,
      ],
      {
        shell: false,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
        env: boundedGitEnvironment(process.env),
      },
    );
    const chunks: Buffer[] = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let failure: DocumentSectionRefusal | undefined;
    let stopped = false;
    let settled = false;
    let cleanupTimer: ReturnType<typeof setTimeout> | undefined;
    const stop = (reason: DocumentSectionRefusal) => {
      if (settled || stopped) return;
      stopped = true;
      failure ??= reason;
      child.kill("SIGKILL");
      child.stdout.destroy();
      child.stderr.destroy();
      // A failed/unconfirmed reap is a refusal, never a claim of termination.
      cleanupTimer = setTimeout(() => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal.removeEventListener("abort", abort);
        reject(new DocumentSectionRefusal("git_cleanup_failed"));
      }, 1000);
    };
    const abort = () => stop(new DocumentSectionRefusal("cancelled"));
    signal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(() => stop(new DocumentSectionRefusal("git_timeout")), 10_000);
    child.stdout.on("data", (chunk: Buffer) => {
      stdoutBytes += chunk.byteLength;
      if (stdoutBytes > stdoutLimit) stop(new DocumentSectionRefusal("git_output_overflow"));
      else if (failure === undefined) chunks.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrBytes += chunk.byteLength;
      if (stderrBytes > 8192) stop(new DocumentSectionRefusal("git_output_overflow"));
    });
    child.stdout.on("error", () => stop(new DocumentSectionRefusal("git_failed")));
    child.stderr.on("error", () => stop(new DocumentSectionRefusal("git_failed")));
    child.on("error", () => {
      failure ??= new DocumentSectionRefusal("git_unavailable");
    });
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      if (cleanupTimer !== undefined) clearTimeout(cleanupTimer);
      if (failure !== undefined) reject(failure);
      else if (code === null) reject(new DocumentSectionRefusal("git_failed"));
      else resolve({ exitCode: code, stdout: Buffer.concat(chunks, stdoutBytes) });
    });
    // Abort may have arrived between the initial check and listener registration.
    if (signal.aborted) abort();
  });

export function githubIdentityFromOrigin(url: string): string | undefined {
  let owner: string | undefined;
  let repository: string | undefined;
  const scp = /^git@github\.com:([^/]+)\/([^/]+)$/i.exec(url);
  if (scp !== null) {
    owner = scp[1];
    repository = scp[2];
  } else {
    // Match the raw URL too: URL parsing normalizes dot paths and default ports.
    const match =
      /^(https:\/\/github\.com|ssh:\/\/git@github\.com(?::22)?)\/([^/]+)\/([^/]+)$/i.exec(url);
    if (match === null) return undefined;
    owner = match[2];
    repository = match[3];
  }
  if (repository?.endsWith(".git")) repository = repository.slice(0, -4);
  const identity = `github:${owner}/${repository}`;
  return /^github:[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?\/[A-Za-z0-9._-]{1,100}(?![\s\S])/.test(
    identity,
  )
    ? identity
    : undefined;
}

export async function readCommittedDocument(
  root: string,
  input: DocumentSectionsSelector,
  signal: AbortSignal,
  run: RawGitRunner = runRawGit,
): Promise<{ readonly blobOid: string; readonly bytes: Uint8Array }> {
  const selector = validateSelector(input);
  if (
    !NodePath.isAbsolute(root) ||
    !root.isWellFormed() ||
    root.includes("\0") ||
    root.includes("\n") ||
    root.includes("\r")
  )
    throw new DocumentSectionRefusal("repository_refused");
  let canonical: string;
  try {
    canonical = await NodeFSP.realpath(root);
  } catch {
    throw new DocumentSectionRefusal("repository_refused");
  }
  if (root !== canonical) throw new DocumentSectionRefusal("repository_refused");
  const git = async (args: readonly string[], limit = 64 * 1024): Promise<Uint8Array> => {
    if (signal.aborted) throw new DocumentSectionRefusal("cancelled");
    const result = await run(root, args, limit, signal);
    if (result.stdout.byteLength > limit) throw new DocumentSectionRefusal("git_output_overflow");
    if (result.exitCode !== 0) throw new DocumentSectionRefusal("git_failed");
    return result.stdout;
  };
  const line = (bytes: Uint8Array) => decodeUtf8(bytes).replace(/\n$/, "");
  const topLevel = line(await git(["rev-parse", "--show-toplevel"]));
  try {
    if (!NodePath.isAbsolute(topLevel) || (await NodeFSP.realpath(topLevel)) !== canonical)
      throw new DocumentSectionRefusal("repository_refused");
  } catch {
    throw new DocumentSectionRefusal("repository_refused");
  }
  if (line(await git(["rev-parse", "--show-object-format"])) !== "sha1")
    throw new DocumentSectionRefusal("unsupported_object_format");
  const origins = decodeUtf8(
    await git(["config", "--local", "--no-includes", "--null", "--get-all", "remote.origin.url"]),
  ).split("\0");
  if (origins.pop() !== "" || origins.length !== 1)
    throw new DocumentSectionRefusal("origin_refused");
  const identity = githubIdentityFromOrigin(origins[0]!);
  if (identity === undefined) throw new DocumentSectionRefusal("origin_refused");
  if (identity !== selector.repositoryIdentity)
    throw new DocumentSectionRefusal("identity_mismatch");
  if (
    line(await git(["rev-parse", "--verify", `${selector.commitOid}^{commit}`])) !==
    selector.commitOid
  )
    throw new DocumentSectionRefusal("commit_refused");
  const tree = decodeUtf8(
    await git(["ls-tree", "--full-tree", "-z", selector.commitOid, "--", selector.documentPath]),
  );
  const match = /^(100644|100755) blob ([a-f0-9]{40})\t([^\0]+)\0(?![\s\S])/.exec(tree);
  if (match === null || match[3] !== selector.documentPath)
    throw new DocumentSectionRefusal("path_refused");
  const blobOid = match[2]!;
  const size = line(await git(["cat-file", "-s", blobOid]));
  if (!/^(0|[1-9][0-9]{0,15})(?![\s\S])/.test(size))
    throw new DocumentSectionRefusal("blob_refused");
  if (BigInt(size) > BigInt(MAX_DOCUMENT_BYTES))
    throw new DocumentSectionRefusal("document_too_large");
  const bytes = await git(["cat-file", "blob", blobOid], MAX_DOCUMENT_BYTES);
  if (bytes.byteLength !== Number(size)) throw new DocumentSectionRefusal("blob_refused");
  const actualOid = NodeCrypto.createHash("sha1")
    .update(`blob ${bytes.byteLength}\0`)
    .update(bytes)
    .digest("hex");
  if (actualOid !== blobOid) throw new DocumentSectionRefusal("blob_refused");
  return { blobOid, bytes };
}
