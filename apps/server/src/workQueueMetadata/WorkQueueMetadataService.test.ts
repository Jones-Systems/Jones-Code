import { describe, expect, it, vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { createHash } from "node:crypto";
import { chmod, mkdtemp, rm, symlink, writeFile, mkdir, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WorkQueueMetadata, WORK_QUEUE_METADATA_MAX_BYTES } from "@t3tools/contracts";
import * as Service from "./WorkQueueMetadataService.ts";
import { workQueueMetadataConfig, type WorkQueueMetadataConfig } from "./config.ts";

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
    observed_at_ms: Date.now() - 10,
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
  return { ...value, snapshot_token: createHash("sha256").update(canonical(value)).digest("hex") };
}
const read = (config: WorkQueueMetadataConfig) =>
  Effect.runPromise(
    Effect.flatMap(Service.WorkQueueMetadataService, (service) => service.snapshot).pipe(
      Effect.provide(Service.layerWithConfig(config)),
    ),
  );
async function fixture(run: (path: string, config: WorkQueueMetadataConfig) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), "work-queue-metadata-test-"));
  try {
    const path = join(directory, "snapshot.json");
    await run(path, { status: "configured", path, source, maxAgeMs: 30_000 });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

describe("configured queue metadata service", () => {
  it("validates the exact Python producer bytes and digest while reporting their sample as stale", async () =>
    fixture(async (path) => {
      const bytes = await readFile(
        new URL(
          "../../../../packages/contracts/src/fixtures/work_queue_metadata_v1.json",
          import.meta.url,
        ),
      );
      const snapshot = Schema.decodeUnknownSync(WorkQueueMetadata)(
        JSON.parse(bytes.toString("utf8")),
      );
      await writeFile(path, bytes, { mode: 0o600 });
      expect(
        await read({ status: "configured", path, source: snapshot.source, maxAgeMs: 30_000 }),
      ).toEqual({
        status: "stale",
        snapshot,
        expires_at_ms: 30_100,
      });
    }));
  it("returns explicit unconfigured and invalid configuration outcomes", async () => {
    expect(await read(workQueueMetadataConfig({}))).toEqual({
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
      expect(await read(workQueueMetadataConfig(env))).toEqual({
        status: "unavailable",
        reason: "invalid_configuration",
      });
  });
  it("distinguishes complete, partial and stale samples and preserves observation identity", async () =>
    fixture(async (path, config) => {
      for (const [change, status] of [
        [{}, "ready"],
        [{ coverage: "partial" }, "partial"],
        [{ observed_at_ms: Date.now() - 60_000 }, "stale"],
      ] as const) {
        const value = artifact(change);
        await writeFile(path, JSON.stringify(value), { mode: 0o600 });
        expect(await read(config)).toMatchObject({
          status,
          snapshot: value,
          expires_at_ms: value.observed_at_ms + 30_000,
        });
      }
    }));
  it("rejects wrong source, future sample, corrupted digest and private metadata", async () =>
    fixture(async (path, config) => {
      for (const [value, reason] of [
        [artifact({ source: { ...source, host_id: "other" } }), "source_mismatch"],
        [artifact({ observed_at_ms: Date.now() + 60_000 }), "future_sample"],
        [{ ...artifact(), snapshot_token: "0".repeat(64) }, "invalid_artifact"],
        [artifact({ prompt: "private text" }), "invalid_artifact"],
      ] as const) {
        await writeFile(path, JSON.stringify(value), { mode: 0o600 });
        expect(await read(config)).toEqual({ status: "unavailable", reason });
      }
    }));
  it("never treats missing, malformed, oversized, symlinked or permissive files as an empty queue", async () =>
    fixture(async (path, config) => {
      expect(await read(config)).toEqual({ status: "unavailable", reason: "source_unavailable" });
      await writeFile(path, "not json", { mode: 0o600 });
      expect(await read(config)).toEqual({ status: "unavailable", reason: "invalid_artifact" });
      await writeFile(path, Buffer.from([0xff, 0xfe]));
      expect(await read(config)).toEqual({ status: "unavailable", reason: "invalid_artifact" });
      await writeFile(path, Buffer.alloc(WORK_QUEUE_METADATA_MAX_BYTES + 1));
      expect(await read(config)).toEqual({ status: "unavailable", reason: "source_unavailable" });
      await writeFile(path, JSON.stringify(artifact()));
      await chmod(path, 0o644);
      expect(await read(config)).toEqual({ status: "unavailable", reason: "source_unavailable" });
      await chmod(path, 0o600);
      const link = `${path}.link`;
      await symlink(path, link);
      expect(await read({ status: "configured", path: link, source, maxAgeMs: 30_000 })).toEqual({
        status: "unavailable",
        reason: "source_unavailable",
      });
      const uid = process.getuid?.();
      if (uid !== undefined) {
        const owner = vi.spyOn(process, "getuid").mockReturnValue(uid + 1);
        try {
          expect(await read(config)).toEqual({
            status: "unavailable",
            reason: "source_unavailable",
          });
        } finally {
          owner.mockRestore();
        }
      }
      const directory = `${path}.directory`;
      await mkdir(directory);
      const directoryLink = `${path}.directory-link`;
      await symlink(directory, directoryLink);
      await writeFile(join(directory, "snapshot.json"), JSON.stringify(artifact()), {
        mode: 0o600,
      });
      expect(
        await read({
          status: "configured",
          path: join(directoryLink, "snapshot.json"),
          source,
          maxAgeMs: 30_000,
        }),
      ).toEqual({ status: "unavailable", reason: "source_unavailable" });
      expect(
        await read({ status: "configured", path: directory, source, maxAgeMs: 30_000 }),
      ).toEqual({ status: "unavailable", reason: "source_unavailable" });
      const response = JSON.stringify(
        await read({ status: "configured", path: `${path}.missing`, source, maxAgeMs: 30_000 }),
      );
      expect(response).not.toContain(path);
    }));
});
