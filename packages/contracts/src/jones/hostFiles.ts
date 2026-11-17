import * as Schema from "effect/Schema";
import { EnvironmentId, NonNegativeInt } from "../baseSchemas.ts";
import { OrchestratorMcpEnvironmentTarget } from "../orchestratorMcp.ts";

/** Longest file a line window is cut from; larger files are refused with their size. */
export const HOST_FILE_READ_MAX_FILE_BYTES = 8 * 1024 * 1024;
/** Text returned by one read, so a result stays well inside an MCP response. */
export const HOST_FILE_READ_MAX_RESULT_BYTES = 192 * 1024;
export const HOST_FILE_READ_DEFAULT_LINES = 2000;
export const HOST_FILE_READ_MAX_LINES = 5000;
export const HOST_FILE_LIST_MAX_ENTRIES = 1000;
export const HOST_FILE_WRITE_MAX_BYTES = 1024 * 1024;

const HostFilePath = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096));
const PositiveLine = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));

export const HostFileReadInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  path: HostFilePath.annotate({ description: "Absolute path, or ~/ for the home directory." }),
  startLine: Schema.optional(PositiveLine),
  maxLines: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: HOST_FILE_READ_MAX_LINES })),
  ),
});
export type HostFileReadInput = typeof HostFileReadInput.Type;

export const HostFileEntry = Schema.Struct({
  name: Schema.String,
  kind: Schema.Literals(["file", "directory", "symlink", "other"]),
});
export type HostFileEntry = typeof HostFileEntry.Type;

export const HostFileReadResult = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("file"),
    environmentId: EnvironmentId,
    path: Schema.String,
    content: Schema.String,
    startLine: PositiveLine,
    totalLines: NonNegativeInt,
    /** The line to continue from, or null when the file's end was returned. */
    nextStartLine: Schema.NullOr(PositiveLine),
    sizeBytes: NonNegativeInt,
    modifiedAt: Schema.NullOr(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("directory"),
    environmentId: EnvironmentId,
    path: Schema.String,
    entries: Schema.Array(HostFileEntry),
    /** True when the directory holds more entries than one listing returns. */
    truncated: Schema.Boolean,
  }),
]);
export type HostFileReadResult = typeof HostFileReadResult.Type;

export const HostFileWriteInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  path: HostFilePath.annotate({ description: "Absolute path, or ~/ for the home directory." }),
  content: Schema.String,
  overwrite: Schema.optional(Schema.Boolean),
});
export type HostFileWriteInput = typeof HostFileWriteInput.Type;

export const HostFileWriteResult = Schema.Struct({
  environmentId: EnvironmentId,
  path: Schema.String,
  sizeBytes: NonNegativeInt,
  created: Schema.Boolean,
});
export type HostFileWriteResult = typeof HostFileWriteResult.Type;
