// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// Socket transport for the root-owned writer observer. A service started with
// NoNewPrivileges=1 cannot elevate through sudo, so a root socket unit runs the
// same helper once per connection with its home fixed in the unit. The helper
// takes the caller's UID from the kernel's peer credentials. The client sends
// no request bytes and reads one protocol-1 result. Peer credentials identify
// the caller only: readiness still rests on the helper's complete observation,
// bound by the launcher client to its own state snapshot and allowed trial.
import type * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";

export const QUALIFIED_WRITER_OBSERVER_SOCKET = "/run/jones-code/writer-observer.sock";

export type SocketPathMetadata = Pick<
  NodeFS.Stats,
  "isDirectory" | "isSocket" | "isSymbolicLink" | "uid" | "mode" | "dev" | "ino"
>;

export type WriterObserverSocketInstallation =
  | { readonly status: "installed"; readonly identity: string }
  | { readonly status: "absent" | "untrusted" };

/**
 * A root unit listens on a root-owned path; anyone able to rename an ancestor
 * could substitute the listener, so every directory up to `/` must be real,
 * root-owned and not group- or other-writable. The socket itself belongs to
 * the service user alone. Only a missing socket is absent; any other doubt is
 * untrusted. The identity lets callers detect a replacement during a request.
 */
export async function writerObserverSocketInstallation(
  path: string,
  owner: number | undefined,
  metadata: (path: string) => Promise<SocketPathMetadata> = (entry) => NodeFSP.lstat(entry),
): Promise<WriterObserverSocketInstallation> {
  const untrusted = { status: "untrusted" } as const;
  if (owner === undefined || !NodePath.isAbsolute(path) || NodePath.normalize(path) !== path)
    return untrusted;
  const read = (entry: string) =>
    metadata(entry).catch((cause: unknown) =>
      (cause as NodeJS.ErrnoException)?.code === "ENOENT" ? ("absent" as const) : undefined,
    );
  const socket = await read(path);
  if (socket === "absent") return { status: "absent" };
  if (
    socket === undefined ||
    socket.isSymbolicLink() ||
    !socket.isSocket() ||
    socket.uid !== owner ||
    (socket.mode & 0o7777) !== 0o600
  )
    return untrusted;
  for (let directory = NodePath.dirname(path); ; directory = NodePath.dirname(directory)) {
    const stat = await read(directory);
    if (
      typeof stat !== "object" ||
      stat.isSymbolicLink() ||
      !stat.isDirectory() ||
      stat.uid !== 0 ||
      (stat.mode & 0o022) !== 0
    )
      return untrusted;
    if (directory === NodePath.dirname(directory))
      return { status: "installed", identity: `${socket.dev}:${socket.ino}` };
  }
}

export type WriterObserverSocketRead =
  | { readonly outcome: "closed"; readonly stdout: string }
  | { readonly outcome: "deadline" | "output-limit" | "error" };

/**
 * Connects, half-closes at once because the request is empty, and reads until
 * the helper closes its side. The deadline covers connection and reply. Every
 * outcome destroys the captured connection and detaches its listeners.
 */
export function readWriterObserverSocket(input: {
  readonly path: string;
  readonly deadlineMs: number;
  readonly outputLimit: number;
}): Promise<WriterObserverSocketRead> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let settled = false;
    const connection = NodeNet.createConnection({ path: input.path, allowHalfOpen: true });
    const settle = (result: WriterObserverSocketRead) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      connection.removeAllListeners();
      // A late error after destroy would otherwise have no listener.
      connection.on("error", () => undefined);
      connection.destroy();
      resolve(result);
    };
    const timer = setTimeout(() => settle({ outcome: "deadline" }), input.deadlineMs);
    connection.once("connect", () => connection.end());
    connection.on("data", (bytes: Buffer) => {
      size += bytes.length;
      if (size > input.outputLimit) settle({ outcome: "output-limit" });
      else chunks.push(bytes);
    });
    connection.once("end", () => {
      const bytes = Buffer.concat(chunks);
      const stdout = bytes.toString("utf8");
      settle(
        Buffer.from(stdout, "utf8").equals(bytes)
          ? { outcome: "closed", stdout }
          : { outcome: "error" },
      );
    });
    connection.once("error", () => settle({ outcome: "error" }));
    connection.once("close", () => settle({ outcome: "error" }));
  });
}

/**
 * A socket carries no exit status or stderr, so the helper's own terminal line
 * supplies the status the sudo route reads from its exit code. The full result
 * is then checked by the shared protocol parser.
 */
export function writerObserverSocketResult(stdout: string):
  | {
      readonly code: 0 | 2 | 3;
      readonly signal: null;
      readonly stdout: string;
      readonly stderr: "";
    }
  | undefined {
  const lines = stdout.split("\n");
  if (lines.pop() !== "") return undefined;
  const terminal = lines[lines.length - 1] ?? "";
  const code = /^complete \d+$/.test(terminal)
    ? 0
    : /^rejected [a-z-]+$/.test(terminal)
      ? 2
      : /^incomplete [a-z-]+$/.test(terminal)
        ? 3
        : undefined;
  return code === undefined ? undefined : { code, signal: null, stdout, stderr: "" };
}
