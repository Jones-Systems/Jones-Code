// @effect-diagnostics nodeBuiltinImport:off - The native descriptor boundary requires O_NOFOLLOW, descriptor ownership and identity checks, and bounded reads from that same descriptor.
import {
  WorkQueueMetadata,
  WORK_QUEUE_METADATA_MAX_BYTES,
  type WorkQueueMetadataResult,
} from "@t3tools/contracts";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { workQueueMetadataConfig, type WorkQueueMetadataConfig } from "./config.ts";

export class WorkQueueMetadataService extends Context.Service<
  WorkQueueMetadataService,
  {
    readonly snapshot: Effect.Effect<WorkQueueMetadataResult>;
  }
>()("t3/jones/workQueueMetadata/WorkQueueMetadataService") {}

const decodeMetadata = Schema.decodeUnknownSync(WorkQueueMetadata);

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.entries(value)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, child]) => `${JSON.stringify(key)}:${canonical(child)}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

async function read(
  config: WorkQueueMetadataConfig,
  currentTimeMillis: () => number,
): Promise<WorkQueueMetadataResult> {
  if (config.status === "unconfigured") return { status: "unconfigured", reason: "not_configured" };
  if (config.status !== "configured")
    return { status: "unavailable", reason: "invalid_configuration" };
  let bytes: Uint8Array;
  try {
    // Reject symlinked ancestors as well as the final component; never follow an alternate source.
    if ((await NodeFSP.realpath(config.path)) !== NodePath.resolve(config.path))
      throw new Error("Invalid artifact");
    const before = await NodeFSP.lstat(config.path);
    const handle = await NodeFSP.open(
      config.path,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    try {
      const opened = await handle.stat();
      const uid = process.getuid?.();
      if (
        !opened.isFile() ||
        uid === undefined ||
        opened.uid !== uid ||
        (opened.mode & 0o077) !== 0 ||
        opened.nlink !== 1 ||
        opened.size > WORK_QUEUE_METADATA_MAX_BYTES ||
        opened.dev !== before.dev ||
        opened.ino !== before.ino
      )
        throw new Error("Invalid artifact");
      const buffer = Buffer.alloc(WORK_QUEUE_METADATA_MAX_BYTES + 1);
      let length = 0;
      while (length < buffer.length) {
        const result = await handle.read(buffer, length, buffer.length - length, length);
        if (result.bytesRead === 0) break;
        length += result.bytesRead;
      }
      const after = await handle.stat();
      const current = await NodeFSP.lstat(config.path);
      if (
        length > WORK_QUEUE_METADATA_MAX_BYTES ||
        length !== opened.size ||
        after.size !== opened.size ||
        after.mtimeMs !== opened.mtimeMs ||
        after.ctimeMs !== opened.ctimeMs ||
        current.dev !== opened.dev ||
        current.ino !== opened.ino ||
        current.size !== opened.size ||
        current.mtimeMs !== opened.mtimeMs ||
        current.ctimeMs !== opened.ctimeMs ||
        current.isSymbolicLink()
      )
        throw new Error("Invalid artifact");
      if ((await NodeFSP.realpath(config.path)) !== NodePath.resolve(config.path))
        throw new Error("Invalid artifact");
      bytes = buffer.subarray(0, length);
    } finally {
      await handle.close();
    }
  } catch {
    return { status: "unavailable", reason: "source_unavailable" };
  }
  let snapshot: WorkQueueMetadata;
  try {
    snapshot = decodeMetadata(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)), {
      onExcessProperty: "error",
    });
    const { snapshot_token, ...unsigned } = snapshot;
    if (
      NodeCrypto.createHash("sha256").update(canonical(unsigned), "utf8").digest("hex") !==
      snapshot_token
    )
      throw new Error("Invalid digest");
  } catch {
    return { status: "unavailable", reason: "invalid_artifact" };
  }
  if (
    Object.entries(config.source).some(
      ([key, value]) => snapshot.source[key as keyof typeof snapshot.source] !== value,
    )
  )
    return { status: "unavailable", reason: "source_mismatch" };
  const now = currentTimeMillis();
  if (snapshot.observed_at_ms > now) return { status: "unavailable", reason: "future_sample" };
  const expires_at_ms = snapshot.observed_at_ms + config.maxAgeMs;
  if (!Number.isSafeInteger(expires_at_ms))
    return { status: "unavailable", reason: "invalid_artifact" };
  return {
    status: now >= expires_at_ms ? "stale" : snapshot.coverage === "partial" ? "partial" : "ready",
    snapshot,
    expires_at_ms,
  };
}

export const layerWithConfig = (config: WorkQueueMetadataConfig) =>
  Layer.succeed(WorkQueueMetadataService, {
    snapshot: Effect.gen(function* () {
      const clock = yield* Clock.Clock;
      return yield* Effect.promise(() => read(config, () => clock.currentTimeMillisUnsafe()));
    }),
  });
export const layer = Layer.suspend(() => layerWithConfig(workQueueMetadataConfig(process.env)));
