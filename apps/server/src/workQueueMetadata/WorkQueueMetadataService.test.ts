// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - Synthetic native JSON fixtures exercise malformed shapes, exact digest bytes, descriptor permissions, symlink rejection, and scoped cleanup at the reader boundary.
import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { WorkQueueMetadata, WORK_QUEUE_METADATA_MAX_BYTES } from "@t3tools/contracts";
import * as Service from "./WorkQueueMetadataService.ts";
import { workQueueMetadataConfig, type WorkQueueMetadataConfig } from "./config.ts";

const decodeMetadata = Schema.decodeUnknownSync(WorkQueueMetadata);
const sampledNow = 100_000;
const source = {
  queue_id: "queue",
  host_id: "host",
  environment_ref: "environment",
  exporter_instance_id: "exporter",
};
function artifact(change: Record<string, unknown> = {}) {
  const value = {
    schema: "codex.t3-work-queue-metadata/v1",
    source,
    observed_at_ms: sampledNow - 10,
    coverage: "complete",
    items: [],
    authority_effect: "none",
    ...change,
  };
  const canonical = (input: unknown): string =>
    Array.isArray(input)
      ? `[${input.map(canonical).join(",")}]`
      : input !== null && typeof input === "object"
        ? `{${Object.entries(input)
            .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
            .map(([key, child]) => `${JSON.stringify(key)}:${canonical(child)}`)
            .join(",")}}`
        : JSON.stringify(input);
  return {
    ...value,
    snapshot_token: NodeCrypto.createHash("sha256").update(canonical(value)).digest("hex"),
  };
}
const read = (config: WorkQueueMetadataConfig) =>
  Effect.flatMap(Service.WorkQueueMetadataService, (service) => service.snapshot).pipe(
    Effect.provide(Service.layerWithConfig(config)),
  );
const fixture = <A, E, R>(
  run: (path: string, config: WorkQueueMetadataConfig) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "work-queue-metadata-test-")),
    ),
    (directory) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(sampledNow);
        const path = NodePath.join(directory, "snapshot.json");
        return yield* run(path, { status: "configured", path, source, maxAgeMs: 30_000 });
      }),
    (directory) => Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
  );

describe("configured queue metadata service", () => {
  it.effect.each([
    [1_500, "ready"],
    [500, "stale"],
  ] as const)(
    "classifies observation %i using the clock after asynchronous artifact reading",
    ([observedAt, status]) =>
      fixture((path) =>
        Effect.gen(function* () {
          const value = artifact({ observed_at_ms: observedAt });
          yield* Effect.promise(() =>
            NodeFSP.writeFile(path, JSON.stringify(value), { mode: 0o600 }),
          );
          let now = 1_000;
          const baseClock = yield* Clock.Clock;
          const clock: Clock.Clock = {
            ...baseClock,
            currentTimeMillisUnsafe: () => now,
            currentTimeMillis: Effect.sync(() => now),
          };
          const result = yield* read({ status: "configured", path, source, maxAgeMs: 1_000 }).pipe(
            Effect.provideService(Clock.Clock, clock),
            Effect.forkChild({ startImmediately: true }),
          );
          now = 2_000;
          expect(yield* Fiber.join(result)).toEqual({
            status,
            snapshot: value,
            expires_at_ms: observedAt + 1_000,
          });
        }),
      ),
  );
  it.effect(
    "validates the exact Python producer bytes and digest while reporting their sample as stale",
    () =>
      fixture((path) =>
        Effect.gen(function* () {
          const bytes = yield* Effect.promise(() =>
            NodeFSP.readFile(
              new URL(
                "../../../../packages/contracts/src/fixtures/work_queue_metadata_v1.json.fixture",
                import.meta.url,
              ),
            ),
          );
          const snapshot = decodeMetadata(JSON.parse(bytes.toString("utf8")));
          yield* Effect.promise(() => NodeFSP.writeFile(path, bytes, { mode: 0o600 }));
          expect(
            yield* read({ status: "configured", path, source: snapshot.source, maxAgeMs: 30_000 }),
          ).toEqual({
            status: "stale",
            snapshot,
            expires_at_ms: 30_100,
          });
        }),
      ),
  );
  it.effect("returns explicit unconfigured and invalid configuration outcomes", () =>
    Effect.gen(function* () {
      expect(yield* read(workQueueMetadataConfig({}))).toEqual({
        status: "unconfigured",
        reason: "not_configured",
      });
      for (const env of [
        { T3CODE_WORK_QUEUE_METADATA_PATH: "relative" },
        {
          T3CODE_WORK_QUEUE_METADATA_PATH: "/synthetic",
          T3CODE_WORK_QUEUE_METADATA_MAX_AGE_MS: "NaN",
        },
      ])
        expect(yield* read(workQueueMetadataConfig(env))).toEqual({
          status: "unavailable",
          reason: "invalid_configuration",
        });
    }),
  );
  it.effect(
    "distinguishes complete, partial and stale samples and preserves observation identity",
    () =>
      fixture((path, config) =>
        Effect.gen(function* () {
          for (const [change, status] of [
            [{}, "ready"],
            [{ coverage: "partial" }, "partial"],
            [{ observed_at_ms: sampledNow - 60_000 }, "stale"],
          ] as const) {
            const value = artifact(change);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(path, JSON.stringify(value), { mode: 0o600 }),
            );
            expect(yield* read(config)).toMatchObject({
              status,
              snapshot: value,
              expires_at_ms: value.observed_at_ms + 30_000,
            });
          }
        }),
      ),
  );
  it.effect("rejects wrong source, future sample, corrupted digest and private metadata", () =>
    fixture((path, config) =>
      Effect.gen(function* () {
        for (const [value, reason] of [
          [artifact({ source: { ...source, host_id: "other" } }), "source_mismatch"],
          [artifact({ observed_at_ms: sampledNow + 60_000 }), "future_sample"],
          [{ ...artifact(), snapshot_token: "0".repeat(64) }, "invalid_artifact"],
          [artifact({ prompt: "private text" }), "invalid_artifact"],
        ] as const) {
          yield* Effect.promise(() =>
            NodeFSP.writeFile(path, JSON.stringify(value), { mode: 0o600 }),
          );
          expect(yield* read(config)).toEqual({ status: "unavailable", reason });
        }
      }),
    ),
  );
  it.effect(
    "never treats missing, malformed, oversized, symlinked or permissive files as an empty queue",
    () =>
      fixture((path, config) =>
        Effect.gen(function* () {
          expect(yield* read(config)).toEqual({
            status: "unavailable",
            reason: "source_unavailable",
          });
          yield* Effect.promise(() => NodeFSP.writeFile(path, "not json", { mode: 0o600 }));
          expect(yield* read(config)).toEqual({
            status: "unavailable",
            reason: "invalid_artifact",
          });
          yield* Effect.promise(() => NodeFSP.writeFile(path, Buffer.from([0xff, 0xfe])));
          expect(yield* read(config)).toEqual({
            status: "unavailable",
            reason: "invalid_artifact",
          });
          yield* Effect.promise(() =>
            NodeFSP.writeFile(path, Buffer.alloc(WORK_QUEUE_METADATA_MAX_BYTES + 1)),
          );
          expect(yield* read(config)).toEqual({
            status: "unavailable",
            reason: "source_unavailable",
          });
          yield* Effect.promise(() => NodeFSP.writeFile(path, JSON.stringify(artifact())));
          yield* Effect.promise(() => NodeFSP.chmod(path, 0o644));
          expect(yield* read(config)).toEqual({
            status: "unavailable",
            reason: "source_unavailable",
          });
          yield* Effect.promise(() => NodeFSP.chmod(path, 0o600));
          const link = `${path}.link`;
          yield* Effect.promise(() => NodeFSP.symlink(path, link));
          expect(
            yield* read({ status: "configured", path: link, source, maxAgeMs: 30_000 }),
          ).toEqual({ status: "unavailable", reason: "source_unavailable" });
          const uid = process.getuid?.();
          if (uid !== undefined) {
            yield* Effect.acquireUseRelease(
              Effect.sync(() => vi.spyOn(process, "getuid").mockReturnValue(uid + 1)),
              () =>
                read(config).pipe(
                  Effect.tap((result) =>
                    Effect.sync(() => {
                      expect(result).toEqual({
                        status: "unavailable",
                        reason: "source_unavailable",
                      });
                    }),
                  ),
                ),
              (owner) => Effect.sync(() => owner.mockRestore()),
            );
          }
          const directory = `${path}.directory`;
          yield* Effect.promise(() => NodeFSP.mkdir(directory));
          const directoryLink = `${path}.directory-link`;
          yield* Effect.promise(() => NodeFSP.symlink(directory, directoryLink));
          yield* Effect.promise(() =>
            NodeFSP.writeFile(
              NodePath.join(directory, "snapshot.json"),
              JSON.stringify(artifact()),
              { mode: 0o600 },
            ),
          );
          expect(
            yield* read({
              status: "configured",
              path: NodePath.join(directoryLink, "snapshot.json"),
              source,
              maxAgeMs: 30_000,
            }),
          ).toEqual({ status: "unavailable", reason: "source_unavailable" });
          expect(
            yield* read({ status: "configured", path: directory, source, maxAgeMs: 30_000 }),
          ).toEqual({ status: "unavailable", reason: "source_unavailable" });
          const response = JSON.stringify(
            yield* read({
              status: "configured",
              path: `${path}.missing`,
              source,
              maxAgeMs: 30_000,
            }),
          );
          expect(response).not.toContain(path);
        }),
      ),
  );
});
