import { HostProcessPlatform, HostProcessUserId } from "@t3tools/shared/hostProcess";
// @effect-diagnostics-next-line nodeBuiltinImport:off - The private-file check needs lstat, owner metadata, and O_NOFOLLOW/O_EXCL creation at the Node filesystem boundary.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - The synchronous Node adapter needs native path semantics outside an Effect runtime.
import * as NodePath from "node:path";
import { ConversationLibraryError } from "@t3tools/shared/conversationLibrary";
import { ConversationLibraryStore, type LibraryDatabase } from "./Store.ts";

function absent(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

function inspect(path: string, directory: boolean): boolean {
  try {
    const info = NodeFS.lstatSync(path);
    const platform = HostProcessPlatform.defaultValue();
    const ownerMismatch = () => {
      const userId = HostProcessUserId.defaultValue();
      return userId !== undefined && info.uid !== userId;
    };
    if (
      info.isSymbolicLink() ||
      (directory ? !info.isDirectory() : !info.isFile()) ||
      (platform !== "win32" && ((info.mode & 0o077) !== 0 || ownerMismatch()))
    ) {
      throw new ConversationLibraryError(
        "storage",
        "The library path is not a private, owner-controlled regular file or directory.",
      );
    }
    return true;
  } catch (error) {
    if (absent(error)) return false;
    throw error;
  }
}

async function database(path: string, readOnly: boolean): Promise<LibraryDatabase> {
  if (process.versions.bun) {
    // Bun's builtin has different option names and a null, rather than undefined, miss.
    const builtin = "bun:sqlite";
    const {
      Database,
    }: {
      Database: new (
        path: string,
        options: { readonly: boolean; strict: boolean },
      ) => {
        exec(sql: string): unknown;
        query(sql: string): {
          get(...values: (string | number | null)[]): Record<string, unknown> | null;
          all(...values: (string | number | null)[]): Record<string, unknown>[];
          run(...values: (string | number | null)[]): unknown;
        };
        close(throwOnError: boolean): void;
      };
    } = await import(builtin);
    const db = new Database(path, { readonly: readOnly, strict: true });
    return {
      exec: (sql) => db.exec(sql),
      close: () => db.close(true),
      prepare: (sql) => {
        const statement = db.query(sql);
        return {
          get: (...values) => statement.get(...values) ?? undefined,
          all: (...values) => statement.all(...values),
          run: (...values) => statement.run(...values),
        };
      },
    };
  }
  const { DatabaseSync } = await import("node:sqlite");
  return new DatabaseSync(path, {
    readOnly,
    enableForeignKeyConstraints: true,
    enableDoubleQuotedStringLiterals: false,
  });
}

/** The fixed path is independent of projects and never opens the coding state database. */
export async function openConversationLibrary(
  stateDir: string,
  write: boolean,
  now: number,
): Promise<ConversationLibraryStore> {
  const directory = NodePath.join(stateDir, "conversation-library");
  const path = NodePath.join(directory, "library.sqlite");
  let exists = inspect(directory, true);
  if (!exists && write) {
    try {
      NodeFS.mkdirSync(directory, { mode: 0o700 });
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
    }
    exists = inspect(directory, true);
  }
  let fileExists = exists && inspect(path, false);
  if (write && !fileExists) {
    try {
      NodeFS.closeSync(
        NodeFS.openSync(
          path,
          NodeFS.constants.O_CREAT |
            NodeFS.constants.O_EXCL |
            NodeFS.constants.O_WRONLY |
            NodeFS.constants.O_NOFOLLOW,
          0o600,
        ),
      );
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
    }
    fileExists = inspect(path, false);
  }
  if (exists) for (const suffix of ["-journal", "-wal", "-shm"]) inspect(path + suffix, false);
  const db = await database(fileExists ? path : ":memory:", fileExists && !write);
  try {
    // A missing read-only library is an empty in-memory view, not a disk mutation.
    return new ConversationLibraryStore(db, { initialize: write || !fileExists, clock: () => now });
  } catch (error) {
    db.close();
    throw error;
  }
}
