// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { parseWriterObserverResult } from "./qualifiedWriterObserver.ts";
import {
  QUALIFIED_WRITER_OBSERVER_SOCKET,
  readWriterObserverSocket,
  writerObserverSocketInstallation,
  writerObserverSocketResult,
  type SocketPathMetadata,
} from "./qualifiedWriterObserverSocket.ts";

const header = "jones-qualified-writer-observer 1";
const owner = 1000;

const entry = (
  kind: "directory" | "socket" | "file" | "symlink",
  uid: number,
  permissions: number,
  inode = 1,
): SocketPathMetadata => ({
  isDirectory: () => kind === "directory",
  isSocket: () => kind === "socket",
  isSymbolicLink: () => kind === "symlink",
  uid,
  mode: (kind === "directory" ? 0o040000 : kind === "socket" ? 0o140000 : 0o100000) | permissions,
  dev: 7,
  ino: inode,
});

/** Synthetic metadata for the fixed socket path; a string is the lstat error code. */
const chain = (overrides: Record<string, SocketPathMetadata | string> = {}) => {
  const entries: Record<string, SocketPathMetadata | string> = {
    "/": entry("directory", 0, 0o755),
    "/run": entry("directory", 0, 0o755),
    "/run/jones-code": entry("directory", 0, 0o755),
    [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", owner, 0o600, 9),
    ...overrides,
  };
  return async (path: string): Promise<SocketPathMetadata> => {
    const value = entries[path];
    if (value === undefined || typeof value === "string")
      throw Object.assign(new Error(`synthetic ${value ?? "ENOENT"}`), {
        code: value ?? "ENOENT",
      });
    return value;
  };
};

it("trusts the observer socket only through a root-owned chain to a private socket", async () => {
  const check = (metadata: (path: string) => Promise<SocketPathMetadata>, path?: string) =>
    writerObserverSocketInstallation(path ?? QUALIFIED_WRITER_OBSERVER_SOCKET, owner, metadata);
  assert.deepEqual(await check(chain()), { status: "installed", identity: "7:9" });
  // Only a missing socket is absent, even where its directory is also missing.
  assert.deepEqual(await check(chain({ [QUALIFIED_WRITER_OBSERVER_SOCKET]: "ENOENT" })), {
    status: "absent",
  });
  assert.deepEqual(
    await check(
      chain({ [QUALIFIED_WRITER_OBSERVER_SOCKET]: "ENOENT", "/run/jones-code": "ENOENT" }),
    ),
    { status: "absent" },
  );
  const untrusted: Record<string, SocketPathMetadata | string>[] = [
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: "EACCES" },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: "ENOTDIR" },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("file", owner, 0o600) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("symlink", owner, 0o600) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", 0, 0o600) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", owner + 1, 0o600) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", owner, 0o660) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", owner, 0o666) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", owner, 0o700) },
    { [QUALIFIED_WRITER_OBSERVER_SOCKET]: entry("socket", owner, 0o4600) },
    { "/run/jones-code": entry("directory", owner, 0o755) },
    { "/run/jones-code": entry("directory", 0, 0o775) },
    { "/run/jones-code": entry("directory", 0, 0o757) },
    { "/run/jones-code": entry("symlink", 0, 0o777) },
    { "/run/jones-code": entry("file", 0, 0o644) },
    { "/run/jones-code": "EACCES" },
    { "/run/jones-code": "ENOENT" },
    { "/run": entry("directory", 0, 0o1777) },
    { "/": entry("directory", owner, 0o755) },
  ];
  for (const overrides of untrusted)
    assert.deepEqual(
      await check(chain(overrides)),
      { status: "untrusted" },
      Object.keys(overrides)[0],
    );
  for (const path of [
    "run/jones-code/writer-observer.sock",
    "/run//jones-code/writer-observer.sock",
    "/run/other/../jones-code/writer-observer.sock",
  ])
    assert.deepEqual(await check(chain(), path), { status: "untrusted" }, path);
  assert.deepEqual(
    await writerObserverSocketInstallation(QUALIFIED_WRITER_OBSERVER_SOCKET, undefined, chain()),
    { status: "untrusted" },
  );
});

it("maps only the helper's own terminal line to its status and keeps the shared parser", () => {
  const complete = `${header}\nfile 0 10 20\nwriter 0 123 777 write\ncomplete 4\n`;
  assert.deepEqual(writerObserverSocketResult(complete), {
    code: 0,
    signal: null,
    stdout: complete,
    stderr: "",
  });
  assert.equal(writerObserverSocketResult(`${header}\nrejected caller\n`)?.code, 2);
  assert.equal(
    writerObserverSocketResult(`${header}\nfile 0 10 20\nincomplete deadline\n`)?.code,
    3,
  );
  for (const stdout of [
    "",
    "\n",
    `${header}\n`,
    `${header}\nfile 0 10 20\n`,
    `${header}\ncomplete 4`,
    `${header}\ncomplete 4\n\n`,
    `${header}\ncomplete 4\nfile 0 10 20\n`,
    `${header}\ncomplete four\n`,
    `${header}\nrejected Caller\n`,
    `${header}\nexited 0\n`,
    `${header}\r\ncomplete 4\r\n`,
  ])
    assert.equal(writerObserverSocketResult(stdout), undefined, JSON.stringify(stdout));
  // A recognised terminal line does not admit a malformed body.
  for (const stdout of [
    `${header}\nfile 0 10 20\nwriter 0 123 777 write\nwriter 0 123 777 write\ncomplete 4\n`,
    `${header}\nfile 0 10 20\nwriter 0 123 777 write\nincomplete deadline\n`,
    `jones-qualified-writer-observer 2\nfile 0 10 20\ncomplete 4\n`,
    `file 0 10 20\ncomplete 4\n`,
  ]) {
    const mapped = writerObserverSocketResult(stdout);
    assert.notEqual(mapped, undefined, stdout);
    assert.equal(parseWriterObserverResult(mapped!), undefined, stdout);
  }
});

async function scratch(body: (directory: string) => Promise<void>) {
  const directory = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-observer-socket-")),
  );
  try {
    await body(directory);
  } finally {
    await NodeFSP.rm(directory, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(directory), { code: "ENOENT" });
  }
}

/**
 * A stand-in listener that answers only after the client half-closes, so a
 * client that sent a request or kept its side open would reach its deadline.
 */
async function serve(
  directory: string,
  respond: (connection: NodeNet.Socket) => void,
  body: (input: {
    readonly path: string;
    readonly connections: { received: number; closed: Promise<void> }[];
  }) => Promise<void>,
) {
  const path = NodePath.join(directory, "o.sock");
  const connections: { received: number; closed: Promise<void> }[] = [];
  const accepted = new Set<NodeNet.Socket>();
  const closed: Promise<void>[] = [];
  const server = NodeNet.createServer({ allowHalfOpen: true }, (connection) => {
    accepted.add(connection);
    const observed = {
      received: 0,
      closed: new Promise<void>((resolve) => connection.once("close", () => resolve())),
    };
    connections.push(observed);
    closed.push(observed.closed);
    connection.on("error", () => undefined);
    connection.on("data", (bytes: Buffer) => {
      observed.received += bytes.length;
    });
    connection.once("end", () => respond(connection));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(path, () => resolve());
    });
    await body({ path, connections });
  } finally {
    // The fixture owns its half-open peer sockets, including a silent responder.
    for (const connection of accepted) connection.destroy();
    await Promise.all(closed);
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

it("sends no request, half-closes and reads one complete reply", async () => {
  await scratch(async (directory) => {
    const reply = `${header}\nfile 0 10 20\ncomplete 4\n`;
    await serve(
      directory,
      (connection) => connection.end(reply),
      async ({ path, connections }) => {
        assert.deepEqual(
          await readWriterObserverSocket({ path, deadlineMs: 5_000, outputLimit: 64 * 1024 }),
          { outcome: "closed", stdout: reply },
        );
        assert.equal(connections.length, 1);
        assert.equal(connections[0]!.received, 0);
        await connections[0]!.closed;
      },
    );
  });
});

it("rejects oversized and invalid replies and times out a silent peer", async () => {
  await scratch(async (directory) => {
    await serve(
      directory,
      (connection) => connection.end("x".repeat(65)),
      async ({ path, connections }) => {
        assert.deepEqual(
          await readWriterObserverSocket({ path, deadlineMs: 5_000, outputLimit: 64 }),
          {
            outcome: "output-limit",
          },
        );
        await connections[0]!.closed;
      },
    );
  });
  await scratch(async (directory) => {
    // The listener never answers; the deadline covers the whole exchange.
    await serve(
      directory,
      () => undefined,
      async ({ path, connections }) => {
        assert.deepEqual(
          await readWriterObserverSocket({ path, deadlineMs: 200, outputLimit: 64 }),
          {
            outcome: "deadline",
          },
        );
        assert.equal(connections[0]!.received, 0);
        // Its writable half is still owned by this fixture after the initial FIN.
      },
    );
  });
  await scratch(async (directory) => {
    await serve(
      directory,
      (connection) => connection.end(Buffer.from([0x63, 0xff, 0x0a])),
      async ({ path, connections }) => {
        assert.deepEqual(
          await readWriterObserverSocket({ path, deadlineMs: 5_000, outputLimit: 64 }),
          {
            outcome: "error",
          },
        );
        await connections[0]!.closed;
      },
    );
  });
  await scratch(async (directory) => {
    assert.deepEqual(
      await readWriterObserverSocket({
        path: NodePath.join(directory, "missing.sock"),
        deadlineMs: 5_000,
        outputLimit: 64,
      }),
      { outcome: "error" },
    );
  });
});
