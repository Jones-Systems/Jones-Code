import {
  HOST_FILE_LIST_MAX_ENTRIES,
  HOST_FILE_READ_DEFAULT_LINES,
  HOST_FILE_READ_MAX_FILE_BYTES,
  HOST_FILE_READ_MAX_RESULT_BYTES,
  HOST_FILE_WRITE_MAX_BYTES,
  type HostFileEntry,
} from "@t3tools/contracts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

const HostFileErrorReason = Schema.Literals([
  "path_not_absolute",
  "not_found",
  "permission_denied",
  "unsupported_type",
  "too_large",
  "binary_file",
  "start_past_end",
  "line_too_long",
  "already_exists",
  "is_directory",
  "content_too_large",
  "operation_failed",
]);
type HostFileErrorReason = typeof HostFileErrorReason.Type;

const messages: Record<HostFileErrorReason, string> = {
  path_not_absolute: "The path must be absolute, or start with ~/.",
  not_found: "Nothing exists at that path.",
  permission_denied: "This environment's user may not access that path.",
  unsupported_type: "That path is neither a regular file nor a directory.",
  too_large: `The file is larger than the ${HOST_FILE_READ_MAX_FILE_BYTES}-byte read limit.`,
  binary_file: "The file is binary; only text files can be read.",
  start_past_end: "startLine is past the end of the file.",
  line_too_long: `A line in the requested range is longer than the ${HOST_FILE_READ_MAX_RESULT_BYTES}-byte result limit, so this file cannot be read by line.`,
  already_exists: "A file already exists there. Pass overwrite: true to replace it.",
  is_directory: "That path is a directory.",
  content_too_large: `The content is larger than the ${HOST_FILE_WRITE_MAX_BYTES}-byte write limit.`,
  operation_failed: "The file operation failed.",
};

export class HostFileError extends Schema.TaggedError<HostFileError>()("HostFileError", {
  reason: HostFileErrorReason,
  path: Schema.String,
  sizeBytes: Schema.optional(Schema.Number),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return messages[this.reason];
  }
}

export interface HostFileView {
  readonly kind: "file";
  readonly path: string;
  readonly content: string;
  readonly startLine: number;
  readonly totalLines: number;
  readonly nextStartLine: number | null;
  readonly sizeBytes: number;
  readonly modifiedAt: string | null;
}

export interface HostDirectoryView {
  readonly kind: "directory";
  readonly path: string;
  readonly entries: ReadonlyArray<HostFileEntry>;
  readonly truncated: boolean;
}

/**
 * Text files and directory listings anywhere this environment's user can
 * reach, by absolute path. It has no workspace boundary, unlike
 * `WorkspaceFileSystem`: callers decide who may use it.
 */
export class HostFiles extends Context.Service<
  HostFiles,
  {
    /** A window of a text file's lines, or a directory's entries. */
    readonly read: (input: {
      readonly path: string;
      readonly startLine?: number | undefined;
      readonly maxLines?: number | undefined;
    }) => Effect.Effect<HostFileView | HostDirectoryView, HostFileError>;
    /** Writes a text file, creating its parent directories. */
    readonly write: (input: {
      readonly path: string;
      readonly content: string;
      readonly overwrite?: boolean | undefined;
    }) => Effect.Effect<
      { readonly path: string; readonly sizeBytes: number; readonly created: boolean },
      HostFileError
    >;
  }
>()("jones/hostFiles/HostFiles") {}

const entryKind = (type: FileSystem.File.Type): HostFileEntry["kind"] =>
  type === "File"
    ? "file"
    : type === "Directory"
      ? "directory"
      : type === "SymbolicLink"
        ? "symlink"
        : "other";

const utf8 = new TextEncoder();

const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const failed = (target: string) => (cause: PlatformError.PlatformError) =>
    new HostFileError({
      reason:
        cause.reason._tag === "NotFound"
          ? "not_found"
          : cause.reason._tag === "PermissionDenied"
            ? "permission_denied"
            : cause.reason._tag === "AlreadyExists"
              ? "already_exists"
              : "operation_failed",
      path: target,
      cause,
    });

  const resolve = Effect.fn("HostFiles.resolve")(function* (input: string) {
    const expanded = expandHomePath(input.trim(), yield* HostProcess.HomeDirectory);
    if (!path.isAbsolute(expanded))
      return yield* new HostFileError({ reason: "path_not_absolute", path: input });
    return path.normalize(expanded);
  });

  const list = Effect.fn("HostFiles.list")(function* (target: string) {
    const all = yield* fileSystem.readDirectoryEntries(target).pipe(Effect.mapError(failed(target)));
    const entries = all
      .map((entry) => ({ name: entry.name, kind: entryKind(entry.type) }))
      .toSorted((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
    return {
      kind: "directory",
      path: target,
      entries: entries.slice(0, HOST_FILE_LIST_MAX_ENTRIES),
      truncated: entries.length > HOST_FILE_LIST_MAX_ENTRIES,
    } satisfies HostDirectoryView;
  });

  const read: HostFiles["Service"]["read"] = Effect.fn("HostFiles.read")(function* (input) {
    const target = yield* resolve(input.path);
    const info = yield* fileSystem.stat(target).pipe(Effect.mapError(failed(target)));
    if (info.type === "Directory") return yield* list(target);
    if (info.type !== "File")
      return yield* new HostFileError({ reason: "unsupported_type", path: target });
    const sizeBytes = Number(info.size);
    if (sizeBytes > HOST_FILE_READ_MAX_FILE_BYTES)
      return yield* new HostFileError({ reason: "too_large", path: target, sizeBytes });
    const bytes = yield* fileSystem.readFile(target).pipe(Effect.mapError(failed(target)));
    if (bytes.includes(0))
      return yield* new HostFileError({ reason: "binary_file", path: target, sizeBytes });

    const text = new TextDecoder("utf-8").decode(bytes);
    const lines = text.length === 0 ? [] : text.split("\n");
    // A final newline ends the last line; it does not start an empty one.
    if (lines.at(-1) === "") lines.pop();
    const startLine = input.startLine ?? 1;
    if (startLine > Math.max(lines.length, 1))
      return yield* new HostFileError({ reason: "start_past_end", path: target });

    const maxLines = input.maxLines ?? HOST_FILE_READ_DEFAULT_LINES;
    const window: Array<string> = [];
    let windowBytes = 0;
    for (let index = startLine - 1; index < lines.length && window.length < maxLines; index++) {
      const line = lines[index]!;
      const lineBytes = utf8.encode(line).length + 1;
      if (windowBytes + lineBytes > HOST_FILE_READ_MAX_RESULT_BYTES) {
        if (window.length === 0)
          return yield* new HostFileError({ reason: "line_too_long", path: target, sizeBytes });
        break;
      }
      window.push(line);
      windowBytes += lineBytes;
    }
    const end = startLine - 1 + window.length;
    return {
      kind: "file",
      path: target,
      content: window.join("\n"),
      startLine,
      totalLines: lines.length,
      nextStartLine: end < lines.length ? end + 1 : null,
      sizeBytes,
      modifiedAt: Option.match(info.mtime, {
        onNone: () => null,
        onSome: (date) => date.toISOString(),
      }),
    } satisfies HostFileView;
  });

  const write: HostFiles["Service"]["write"] = Effect.fn("HostFiles.write")(function* (input) {
    const target = yield* resolve(input.path);
    const sizeBytes = utf8.encode(input.content).length;
    if (sizeBytes > HOST_FILE_WRITE_MAX_BYTES)
      return yield* new HostFileError({ reason: "content_too_large", path: target, sizeBytes });
    const existing = yield* fileSystem.stat(target).pipe(
      Effect.map(Option.some),
      Effect.catchIf(
        (cause) => cause.reason._tag === "NotFound",
        () => Effect.succeed(Option.none<FileSystem.File.Info>()),
      ),
      Effect.mapError(failed(target)),
    );
    if (Option.isSome(existing)) {
      if (existing.value.type === "Directory")
        return yield* new HostFileError({ reason: "is_directory", path: target });
      if (existing.value.type !== "File")
        return yield* new HostFileError({ reason: "unsupported_type", path: target });
      if (input.overwrite !== true)
        return yield* new HostFileError({ reason: "already_exists", path: target });
    }
    yield* fileSystem
      .makeDirectory(path.dirname(target), { recursive: true })
      .pipe(Effect.mapError(failed(target)));
    // `wx` keeps a file created since the check from being replaced without overwrite.
    yield* fileSystem
      .writeFileString(target, input.content, { flag: Option.isSome(existing) ? "w" : "wx" })
      .pipe(Effect.mapError(failed(target)));
    return { path: target, sizeBytes, created: Option.isNone(existing) };
  });

  return HostFiles.of({ read, write });
});

export const layer = Layer.effect(HostFiles, make);
