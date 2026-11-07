// @effect-diagnostics nodeBuiltinImport:off
/** Native and canonical views share write state to avoid races on the same thread file. */
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import type { ThreadId } from "@t3tools/contracts";
import type * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import { makeAsyncRotatingFileSink } from "@t3tools/shared/logging";
import { errorTag } from "@t3tools/shared/observability";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import { toSafeThreadAttachmentSegment } from "../attachmentStore.ts";
import {
  SegmentBuffer,
  type LogBatch,
  type LogSegment,
  type PendingRecord,
} from "../jones/provider/diagnosticLogging/SegmentBuffer.ts";
import type * as ResourceAttribution from "../resourceTelemetry/ResourceAttribution.ts";

const MEBIBYTE = 1024 * 1024;
const DAY_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_MAX_BYTES = 10 * MEBIBYTE;
const DEFAULT_MAX_FILES = 10;
const DEFAULT_BATCH_WINDOW_MS = 1_000;
const DEFAULT_MAX_TOTAL_BYTES = 512 * MEBIBYTE;
const DEFAULT_MAX_AGE_MS = 14 * DAY_MS;
const DEFAULT_RETENTION_CHECK_INTERVAL_MS = 5 * 60 * 1_000;
const DEFAULT_MAX_BUFFERED_BYTES = MEBIBYTE;
const DEFAULT_MAX_BUFFERED_RECORDS = 512;
const MAX_RECORD_CHARACTERS = 64 * 1024;
const MAX_RECORD_FIELDS = 1_024;
const MAX_RECORD_DEPTH = 16;
const GLOBAL_THREAD_SEGMENT = "_global";
const LOG_SCOPE = "provider-observability";
const encodeUnknownJsonString = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const transientCanonicalEventTypes = new Set([
  "content.delta",
  "hook.progress",
  "item.updated",
  "task.progress",
  "thread.realtime.audio.delta",
  "tool.progress",
  "turn.proposed.delta",
]);
const transientNativeMethods = new Set([
  "item/agentMessage/delta",
  "item/commandExecution/outputDelta",
  "item/fileChange/outputDelta",
  "item/plan/delta",
  "item/reasoning/summaryTextDelta",
  "item/reasoning/textDelta",
  "thread/realtime/outputAudio/delta",
  "thread/realtime/transcript/delta",
  "turn/diff/updated",
]);
const transientAcpUpdates = new Set(["agent_message_chunk", "agent_thought_chunk"]);

export type { PendingRecord } from "../jones/provider/diagnosticLogging/SegmentBuffer.ts";

export type EventNdjsonStream = PendingRecord["stream"];

export interface EventNdjsonLogStore {
  readonly filePath: string;
  readonly logger: (stream: EventNdjsonStream) => ProviderEventLoggers.EventNdjsonLogger;
  readonly close: () => Effect.Effect<void>;
}

export interface EventNdjsonLogStoreOptions {
  readonly maxBytes?: number;
  readonly maxFiles?: number;
  readonly batchWindowMs?: number;
  readonly maxTotalBytes?: number;
  readonly maxAgeMs?: number;
  readonly retentionCheckIntervalMs?: number;
  /** Includes detached writes; one payload-bounded record larger than this cap runs alone. */
  readonly maxBufferedBytes?: number;
  /** Includes detached writes. Full capacity backpressures admission without dropping records. */
  readonly maxBufferedRecords?: number;
  readonly attribution?: ResourceAttribution.ResourceAttribution["Service"];
}

export class EventNdjsonLogConfigurationError extends Schema.TaggedError<EventNdjsonLogConfigurationError>()(
  "EventNdjsonLogConfigurationError",
  {
    filePath: Schema.String,
    option: Schema.String,
    value: Schema.Number,
    minimum: Schema.Number,
  },
) {
  override get message(): string {
    return `Provider event log option '${this.option}' must be an integer >= ${this.minimum}; received ${this.value} for '${this.filePath}'`;
  }
}

export class EventNdjsonLogDirectoryError extends Schema.TaggedError<EventNdjsonLogDirectoryError>()(
  "EventNdjsonLogDirectoryError",
  {
    directory: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to create provider event log directory '${this.directory}'`;
  }
}

export type EventNdjsonLogStoreError =
  | EventNdjsonLogConfigurationError
  | EventNdjsonLogDirectoryError;

interface ResolvedOptions {
  readonly maxBytes: number;
  readonly maxFiles: number;
  readonly batchWindowMs: number;
  readonly maxTotalBytes: number;
  readonly maxAgeMs: number;
  readonly retentionCheckIntervalMs: number;
  readonly maxBufferedBytes: number;
  readonly maxBufferedRecords: number;
  readonly attribution: ResourceAttribution.ResourceAttribution["Service"] | undefined;
}

type AsyncFileSink = Effect.Success<ReturnType<typeof makeAsyncRotatingFileSink>>;

interface AttributionSummary {
  readonly stream: EventNdjsonStream;
  readonly count: number;
  readonly logicalWriteBytes: number;
}

interface FileOperationFailure {
  readonly filePath: string;
  readonly cause: unknown;
}

interface RetentionResult {
  readonly failures: ReadonlyArray<FileOperationFailure>;
}

function logWarning(message: string, context: Record<string, unknown>): Effect.Effect<void> {
  return Effect.logWarning(message, context).pipe(Effect.annotateLogs({ scope: LOG_SCOPE }));
}

function resolveThreadSegment(raw: string | null | undefined): string {
  const normalized = typeof raw === "string" ? toSafeThreadAttachmentSegment(raw) : null;
  return normalized ?? GLOBAL_THREAD_SEGMENT;
}

function resolveStreamLabel(stream: EventNdjsonStream): string {
  return stream === "native" ? "NTIVE" : stream === "orchestration" ? "ORCH" : "CANON";
}

function providerLogPrefix(filePath: string): string {
  const basename = NodePath.basename(filePath);
  const extension = NodePath.extname(basename);
  return `${extension.length > 0 ? basename.slice(0, -extension.length) : basename}.`;
}

function providerLogPath(directory: string, prefix: string, threadSegment: string): string {
  return NodePath.join(directory, `${prefix}${threadSegment}.log`);
}

export function shouldPersistProviderEvent(stream: EventNdjsonStream, event: unknown): boolean {
  if (stream === "orchestration" || typeof event !== "object" || event === null) {
    return true;
  }
  try {
    const type = Reflect.get(event, "type");
    if (typeof type === "string" && transientCanonicalEventTypes.has(type)) {
      return false;
    }
    if (stream !== "native") return true;

    const nested = Reflect.get(event, "event");
    const envelope = typeof nested === "object" && nested !== null ? nested : event;
    // Decoded frames preserve raw-frame information without duplicating every token delta.
    if (Reflect.get(envelope, "stage") === "raw") return false;
    const decodedPayload = Reflect.get(envelope, "payload");
    const nativeEvent =
      Reflect.get(envelope, "stage") === "decoded" &&
      typeof decodedPayload === "object" &&
      decodedPayload !== null
        ? decodedPayload
        : envelope;
    const method = Reflect.get(nativeEvent, "method");
    if (
      typeof method === "string" &&
      (transientNativeMethods.has(method) ||
        method.startsWith("claude/stream_event/content_block_delta/"))
    ) {
      return false;
    }

    const nativeType = Reflect.get(nativeEvent, "type");
    if (nativeType === "system" && Reflect.get(nativeEvent, "subtype") === "thinking_tokens") {
      return false;
    }
    if (nativeType === "message.part.delta") return false;
    if (nativeType === "stream_event") {
      const streamEvent = Reflect.get(nativeEvent, "event");
      if (
        typeof streamEvent === "object" &&
        streamEvent !== null &&
        Reflect.get(streamEvent, "type") === "content_block_delta"
      ) {
        return false;
      }
    }

    const payload = Reflect.get(nativeEvent, "payload");
    if (typeof payload !== "object" || payload === null) return true;

    if (method === "session/update") {
      const update = Reflect.get(payload, "update");
      if (typeof update !== "object" || update === null) return true;
      const updateType = Reflect.get(update, "sessionUpdate");
      return typeof updateType !== "string" || !transientAcpUpdates.has(updateType);
    }

    if (nativeType === "message.part.updated") {
      const properties = Reflect.get(payload, "properties");
      if (typeof properties !== "object" || properties === null) return true;
      const part = Reflect.get(properties, "part");
      if (typeof part !== "object" || part === null) return true;
      const partType = Reflect.get(part, "type");
      if (partType === "text" || partType === "reasoning") return false;
      if (partType === "tool") {
        // Running snapshots repeat growing output.
        const state = Reflect.get(part, "state");
        if (typeof state === "object" && state !== null) {
          return Reflect.get(state, "status") !== "running";
        }
      }
      return true;
    }

    return true;
  } catch {
    return true;
  }
}

const summaryFields = [
  "provider",
  "protocol",
  "kind",
  "providerSessionId",
  "direction",
  "stage",
  "type",
  "subtype",
  "method",
  "id",
  "threadId",
  "turnId",
  "requestId",
  "session_id",
  "status",
  "is_error",
  "api_error_status",
  "terminal_reason",
  "stop_reason",
  "operation",
  "code",
  "willRetry",
  "message",
  "event",
  "payload",
  "params",
  "result",
  "thread",
  "turn",
  "error",
  "turns",
  "items",
  "content",
] as const;

function summarizeProviderEvent(event: unknown): unknown {
  let remainingFields = 128;
  let remainingCharacters = 8 * 1024;
  const summarize = (value: unknown, depth: number): unknown => {
    if (typeof value === "string") {
      if (value.length > Math.min(1_024, remainingCharacters)) {
        return { omittedCharacters: value.length };
      }
      remainingCharacters -= value.length;
      return value;
    }
    if (value === null || typeof value === "number" || typeof value === "boolean") return value;
    if (typeof value !== "object") return undefined;
    if (Array.isArray(value)) return { itemCount: value.length };
    const summary: Record<string, unknown> = { truncated: true };
    if (depth >= 6) return summary;
    for (const key of summaryFields) {
      if (remainingFields <= 0) break;
      const nested = Reflect.get(value, key);
      if (nested === undefined) continue;
      remainingFields -= 1;
      summary[key] = summarize(nested, depth + 1);
    }
    return summary;
  };
  try {
    return summarize(event, 0);
  } catch {
    return { truncated: true };
  }
}

export function boundProviderEventForLogging(event: unknown): unknown {
  let remainingCharacters = MAX_RECORD_CHARACTERS;
  let remainingFields = MAX_RECORD_FIELDS;
  const ancestors = new WeakSet<object>();
  const fits = (value: unknown, depth: number): boolean => {
    if (typeof value === "string") {
      remainingCharacters -= value.length;
      return remainingCharacters >= 0;
    }
    if (typeof value !== "object" || value === null) return true;
    if (depth > MAX_RECORD_DEPTH || ancestors.has(value)) return false;
    if (Array.isArray(value) && value.length > remainingFields) return false;
    ancestors.add(value);
    for (const key in value) {
      if (!Object.hasOwn(value, key)) continue;
      remainingFields -= 1;
      remainingCharacters -= key.length;
      if (
        remainingFields < 0 ||
        remainingCharacters < 0 ||
        !fits(Reflect.get(value, key), depth + 1)
      )
        return false;
    }
    ancestors.delete(value);
    return true;
  };
  try {
    if (fits(event, 0)) return event;
  } catch {}
  return summarizeProviderEvent(event);
}

export const writeBatchedMessages = Effect.fnUntraced(function* (
  sink: AsyncFileSink,
  records: ReadonlyArray<PendingRecord>,
  maxBytes: number,
  onWritten: (records: ReadonlyArray<PendingRecord>) => void,
) {
  let pendingRecords: Array<PendingRecord> = [];
  let pendingBytes = 0;

  const flush = Effect.gen(function* () {
    if (pendingRecords.length === 0) return;
    const writtenRecords = pendingRecords;
    yield* sink.write(writtenRecords.map((record) => record.line).join(""));
    onWritten(writtenRecords);
    pendingRecords = [];
    pendingBytes = 0;
  });

  for (const record of records) {
    if (pendingBytes > 0 && pendingBytes + record.bytes > maxBytes) {
      yield* flush;
    }
    pendingRecords.push(record);
    pendingBytes += record.bytes;
    if (pendingBytes >= maxBytes) {
      yield* flush;
    }
  }
  yield* flush;
});

async function isProviderLogFile(
  filePath: string,
  fileName: string,
  filePrefix: string,
): Promise<boolean> {
  if (!/\.log(?:\.\d+)?$/u.test(fileName)) return false;
  if (fileName.startsWith(filePrefix)) return true;

  const descriptor = await NodeFS.promises.open(filePath, "r");
  try {
    const header = Buffer.alloc(256);
    const { bytesRead } = await descriptor.read(header, 0, header.byteLength, 0);
    return /^\[[^\]\r\n]+\] (?:NTIVE|CANON|ORCH): /u.test(header.toString("utf8", 0, bytesRead));
  } finally {
    await descriptor.close();
  }
}

async function enforceRetention(input: {
  readonly directory: string;
  readonly maxTotalBytes: number;
  readonly maxAgeMs: number;
  readonly reserveRemoval: (filePath: string) => (() => void) | undefined;
  readonly filePrefix: string;
  readonly now: number;
}): Promise<RetentionResult> {
  const failures: Array<FileOperationFailure> = [];
  const files: Array<{ filePath: string; mtimeMs: number; size: number }> = [];

  let entries: ReadonlyArray<NodeFS.Dirent>;
  try {
    entries = await NodeFS.promises.readdir(input.directory, { withFileTypes: true });
  } catch (cause) {
    return { failures: [{ filePath: input.directory, cause }] };
  }

  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const filePath = NodePath.join(input.directory, entry.name);
    try {
      if (!(await isProviderLogFile(filePath, entry.name, input.filePrefix))) continue;
      const stat = await NodeFS.promises.stat(filePath);
      files.push({ filePath, mtimeMs: stat.mtimeMs, size: stat.size });
    } catch (cause) {
      failures.push({ filePath, cause });
    }
  }

  let totalBytes = files.reduce((total, file) => total + file.size, 0);
  const remove = async (file: (typeof files)[number]) => {
    // Reserve the whole rotating family against both current and newly admitted writers.
    const release = input.reserveRemoval(file.filePath);
    if (!release) return false;
    try {
      await NodeFS.promises.rm(file.filePath, { force: true });
      totalBytes -= file.size;
      return true;
    } catch (cause) {
      failures.push({ filePath: file.filePath, cause });
      return false;
    } finally {
      release();
    }
  };

  const retained: typeof files = [];
  for (const file of files) {
    if (input.now - file.mtimeMs <= input.maxAgeMs || !(await remove(file))) retained.push(file);
  }

  for (const file of retained.toSorted(
    (left, right) => left.mtimeMs - right.mtimeMs || left.filePath.localeCompare(right.filePath),
  )) {
    if (totalBytes <= input.maxTotalBytes) break;
    await remove(file);
  }

  return { failures };
}

function validateOption(input: {
  readonly filePath: string;
  readonly option: string;
  readonly value: number;
  readonly minimum: number;
}): EventNdjsonLogConfigurationError | undefined {
  if (Number.isInteger(input.value) && input.value >= input.minimum) return undefined;
  return new EventNdjsonLogConfigurationError(input);
}

function resolveOptions(
  filePath: string,
  options: EventNdjsonLogStoreOptions,
): Effect.Effect<ResolvedOptions, EventNdjsonLogConfigurationError> {
  const resolved = {
    maxBytes: options.maxBytes ?? DEFAULT_MAX_BYTES,
    maxFiles: options.maxFiles ?? DEFAULT_MAX_FILES,
    batchWindowMs: options.batchWindowMs ?? DEFAULT_BATCH_WINDOW_MS,
    maxTotalBytes: options.maxTotalBytes ?? DEFAULT_MAX_TOTAL_BYTES,
    maxAgeMs: options.maxAgeMs ?? DEFAULT_MAX_AGE_MS,
    retentionCheckIntervalMs:
      options.retentionCheckIntervalMs ?? DEFAULT_RETENTION_CHECK_INTERVAL_MS,
    maxBufferedBytes: options.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
    maxBufferedRecords: options.maxBufferedRecords ?? DEFAULT_MAX_BUFFERED_RECORDS,
    attribution: options.attribution,
  } satisfies ResolvedOptions;

  const validations = [
    ["maxBytes", resolved.maxBytes, 1],
    ["maxFiles", resolved.maxFiles, 1],
    ["batchWindowMs", resolved.batchWindowMs, 0],
    ["maxTotalBytes", resolved.maxTotalBytes, 1],
    ["maxAgeMs", resolved.maxAgeMs, 1],
    ["retentionCheckIntervalMs", resolved.retentionCheckIntervalMs, 1],
    ["maxBufferedBytes", resolved.maxBufferedBytes, 1],
    ["maxBufferedRecords", resolved.maxBufferedRecords, 1],
  ] as const;

  for (const [option, value, minimum] of validations) {
    const error = validateOption({ filePath, option, value, minimum });
    if (error) return Effect.fail(error);
  }
  return Effect.succeed(resolved);
}

const serializeEvent = Effect.fnUntraced(function* (event: unknown) {
  return yield* encodeUnknownJsonString(event).pipe(
    Effect.catch((error) =>
      logWarning("failed to serialize provider event log record", {
        errorTag: errorTag(error),
      }).pipe(Effect.as(undefined)),
    ),
  );
});

export const makeEventNdjsonLogStore = Effect.fnUntraced(function* (
  filePath: string,
  options: EventNdjsonLogStoreOptions = {},
): Effect.fn.Return<
  EventNdjsonLogStore,
  EventNdjsonLogStoreError,
  FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const resolved = yield* resolveOptions(filePath, options);
  const directory = NodePath.dirname(filePath);
  const filePrefix = providerLogPrefix(filePath);

  yield* fs
    .makeDirectory(directory, { recursive: true })
    .pipe(Effect.mapError((cause) => new EventNdjsonLogDirectoryError({ directory, cause })));
  const makeSink = (filePath: string) =>
    makeAsyncRotatingFileSink({
      filePath,
      maxBytes: resolved.maxBytes,
      maxFiles: resolved.maxFiles,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );

  const buffer = new SegmentBuffer(resolved);
  const sinks = new Map<LogSegment, AsyncFileSink>();
  const timerScope = yield* Scope.make();
  const workerScope = yield* Scope.make();
  const retentionScope = yield* Scope.make();
  let lastRetentionAt = yield* Clock.currentTimeMillis;
  let retentionRunning = false;

  // Retention has one I/O owner, separate from all segment workers and admissions.
  const retain = Effect.fnUntraced(function* () {
    const now = yield* Clock.currentTimeMillis;
    yield* Effect.sync(() => {
      for (const segment of buffer.retireIdle()) sinks.delete(segment);
    });
    const result = yield* Effect.promise(() =>
      enforceRetention({
        directory,
        maxTotalBytes: resolved.maxTotalBytes,
        maxAgeMs: resolved.maxAgeMs,
        reserveRemoval: (candidate) => buffer.reserveRemoval(candidate),
        filePrefix,
        now,
      }),
    );
    for (const failure of result.failures) {
      yield* logWarning("provider event log retention failed", {
        filePath: failure.filePath,
        errorTag: errorTag(failure.cause),
      });
    }
    lastRetentionAt = now;
  }, Effect.uninterruptible);
  yield* retain();

  const requestRetention = Effect.fnUntraced(function* () {
    const now = yield* Clock.currentTimeMillis;
    if (
      buffer.isClosed ||
      retentionRunning ||
      now - lastRetentionAt < resolved.retentionCheckIntervalMs
    )
      return;
    retentionRunning = true;
    yield* retain().pipe(
      Effect.ensuring(
        Effect.sync(() => {
          retentionRunning = false;
        }),
      ),
      Effect.forkIn(retentionScope, { startImmediately: true }),
    );
  }, Effect.uninterruptible);

  // These per-batch/per-record paths share the caller's provider span.
  const writeBatch = Effect.fnUntraced(
    function* (batch: LogBatch) {
      const startedAt = yield* Clock.currentTimeMillis;
      const attributionByStream = new Map<EventNdjsonStream, AttributionSummary>();
      const written = yield* Effect.gen(function* () {
        let sink = sinks.get(batch.segment);
        if (!sink) {
          sink = yield* makeSink(batch.segment.filePath);
          sinks.set(batch.segment, sink);
        }
        yield* writeBatchedMessages(sink, batch.records, resolved.maxBytes, (records) => {
          for (const record of records) {
            const previous = attributionByStream.get(record.stream);
            attributionByStream.set(record.stream, {
              stream: record.stream,
              count: (previous?.count ?? 0) + 1,
              logicalWriteBytes: (previous?.logicalWriteBytes ?? 0) + record.bytes,
            });
          }
        });
      }).pipe(Effect.exit);
      if (Exit.isFailure(written)) {
        sinks.delete(batch.segment);
        yield* logWarning("provider event log write or retention failed", {
          filePath: batch.segment.filePath,
          errorTag: errorTag(Cause.squash(written.cause)),
        });
      }

      if (resolved.attribution && attributionByStream.size > 0) {
        const completedAt = yield* Clock.currentTimeMillis;
        const durationMs = Math.max(0, completedAt - startedAt);
        const attributions = Array.from(attributionByStream.values());
        const totalBytes = attributions.reduce(
          (total, entry) => total + entry.logicalWriteBytes,
          0,
        );
        yield* Effect.forEach(
          attributions,
          (entry) =>
            resolved.attribution?.record({
              component: "provider-event-log",
              operation: `${entry.stream}.append`,
              logicalWriteBytes: entry.logicalWriteBytes,
              count: entry.count,
              durationMs:
                totalBytes === 0
                  ? 0
                  : Math.round(durationMs * (entry.logicalWriteBytes / totalBytes)),
            }) ?? Effect.void,
          { discard: true },
        );
      }
    },
    (effect, batch) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.sync(() => sinks.delete(batch.segment)).pipe(
            Effect.andThen(
              logWarning("provider event log batch failed", {
                filePath: batch.segment.filePath,
                errorTag: errorTag(Cause.squash(cause)),
              }),
            ),
          ),
        ),
        Effect.ensuring(Effect.sync(() => buffer.complete(batch))),
        Effect.uninterruptible,
      ),
  );

  const drainSegment = Effect.fnUntraced(function* (segment: LogSegment) {
    while (true) {
      const action = yield* Effect.sync(() => buffer.next(segment));
      if (action.kind === "done") return;
      if (action.kind === "wait") {
        yield* Deferred.await(action.changed);
      } else {
        yield* writeBatch(action.batch);
        yield* requestRetention();
      }
    }
  });

  const startWorkers = (segments: ReadonlyArray<LogSegment>) =>
    Effect.forEach(
      segments,
      (segment) =>
        Effect.forkIn(drainSegment(segment), workerScope, {
          startImmediately: true,
          uninterruptible: false,
        }),
      { discard: true },
    );

  const awaitWatermark = Effect.fnUntraced(function* (segment: LogSegment, watermark: number) {
    while (true) {
      const changed = yield* Effect.sync(() => buffer.awaitWatermark(segment, watermark));
      if (!changed) return;
      yield* Deferred.await(changed);
    }
  });

  const scheduleFlush = Effect.fnUntraced(function* () {
    yield* Effect.forkIn(
      Effect.sleep(resolved.batchWindowMs).pipe(
        Effect.andThen(
          Effect.sync(() => buffer.timerFired()).pipe(
            Effect.flatMap(startWorkers),
            Effect.uninterruptible,
          ),
        ),
      ),
      timerScope,
      { startImmediately: true, uninterruptible: false },
    );
  });

  const closeComplete = yield* Deferred.make<void>();
  let closeStarted = false;
  const close = Effect.fnUntraced(function* () {
    const closing = yield* Effect.sync(() => {
      if (closeStarted) return undefined;
      closeStarted = true;
      return buffer.closeAdmission();
    });
    if (!closing) return yield* Deferred.await(closeComplete);
    yield* Effect.gen(function* () {
      yield* Scope.close(timerScope, Exit.void);
      yield* startWorkers(closing.workers);
      yield* Effect.forEach(
        closing.watermarks,
        ({ segment, watermark }) => awaitWatermark(segment, watermark),
        { concurrency: "unbounded", discard: true },
      );
      yield* Scope.close(workerScope, Exit.void);
      yield* Scope.close(retentionScope, Exit.void);
      yield* retain();
    }).pipe(Effect.onExit((exit) => Deferred.done(closeComplete, exit)));
  }, Effect.uninterruptible);

  const loggerViews = new Map<EventNdjsonStream, ProviderEventLoggers.EventNdjsonLogger>();
  const logger = (stream: EventNdjsonStream): ProviderEventLoggers.EventNdjsonLogger => {
    const existing = loggerViews.get(stream);
    if (existing) return existing;

    const write = Effect.fnUntraced(function* (event: unknown, threadId: ThreadId | null) {
      if (!shouldPersistProviderEvent(stream, event)) return;
      let payload = yield* serializeEvent(boundProviderEventForLogging(event));
      if (payload === undefined) return;
      // Escaping can expand strings beyond their input size.
      if (Buffer.byteLength(payload) > MAX_RECORD_CHARACTERS) {
        payload = yield* serializeEvent(summarizeProviderEvent(event));
        if (payload === undefined) return;
      }

      const observedAt = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
      const line = `[${observedAt}] ${resolveStreamLabel(stream)}: ${payload}\n`;
      const bytes = Buffer.byteLength(line);
      const threadSegment = resolveThreadSegment(threadId);
      const record = { stream, threadSegment, line, bytes } satisfies PendingRecord;
      const segmentPath = providerLogPath(directory, filePrefix, threadSegment);
      yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          while (true) {
            const action = yield* Effect.sync(() => buffer.admit(record, segmentPath));
            if (action.kind === "closed") return;
            yield* startWorkers(action.workers);
            if (action.kind === "wait") {
              yield* restore(Deferred.await(action.changed));
              continue;
            }
            if (action.schedule) yield* scheduleFlush();
            if (action.flush) yield* restore(awaitWatermark(action.segment, action.watermark));
            return;
          }
        }),
      );
    });

    const view = {
      filePath,
      write,
      close: () => Effect.void,
    } satisfies ProviderEventLoggers.EventNdjsonLogger;
    loggerViews.set(stream, view);
    return view;
  };

  return { filePath, logger, close } satisfies EventNdjsonLogStore;
});
