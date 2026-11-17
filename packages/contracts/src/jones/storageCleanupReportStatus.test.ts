import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { StorageCleanupReport } from "../settings.ts";

const decodeReport = Schema.decodeUnknownSync(StorageCleanupReport);

const legacy = {
  trigger: "manual",
  startedAt: "2026-11-16T22:00:00.000Z",
  finishedAt: "2026-11-16T22:01:00.000Z",
  entries: [],
  counts: { removed: 0, kept: 0, failed: 0 },
  omittedCount: 0,
  bytesFreed: 0,
};

it("decodes retained legacy reports without inventing an operation", () => {
  expect(decodeReport(legacy).operation).toBeUndefined();
});

it("keeps reports readable when a newer server supplies an unknown operation", () => {
  expect(decodeReport({ ...legacy, operation: "future-action" }).operation).toBeUndefined();
});

it.each(["cleanup", "propose", "retire"] as const)(
  "preserves the %s operation across report decoding",
  (operation) => {
    expect(decodeReport({ ...legacy, operation }).operation).toBe(operation);
  },
);
