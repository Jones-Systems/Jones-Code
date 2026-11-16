import { describe, expect, it } from "vite-plus/test";
import { inspectMigrationHistory } from "./migrationHistory.ts";

const originals = ["WorktreeOwnershipLeases", "ProjectionThreadRuntimeIdentity", "NativeCreationIntents",
  "NativeCreationCommandIdentities", "WorkstreamsNativeAttempts", "WorkstreamsProviderEnrollments"];
const manifest = [...originals.map((name, i) => [i + 1, name] as const), [100, "Forward"] as const];
const rows = manifest.map(([id, name]) => ({ id, name }));
const foreign = ["V2NativeAcceptance", "DeletionWorktreeAdmission", "OrdinaryCheckoutOwnership",
  "AttachmentCleanup", "OrdinaryCheckoutExecutionLifetime", "ImportedApplicationAttachments", "CommandNormalizationWitness"];

describe("independent ledger inspection", () => {
  it.each([5, 7])("preserves known foreign prefix length %i", (length) => {
    const history = [...rows.slice(0, 6), ...foreign.slice(0, length).map((name, i) => ({ id: i + 7, name }))];
    expect(inspectMigrationHistory(manifest, history, "Jones")).toEqual({ pending: [100] });
  });
  it("accepts the lone released lookup 7", () => {
    expect(inspectMigrationHistory(manifest, [...rows.slice(0, 6), { id: 7, name: "ThreadCreationLookupIndex" }], "Jones"))
      .toEqual({ pending: [100] });
  });
  it.each([
    [...rows.slice(0, 5), { id: 7, name: foreign[0]! }],
    [...rows.slice(0, 6), { id: 8, name: foreign[1]! }],
    [...rows, { id: 101, name: "Newer" }],
    rows.map((row) => row.id === 100 ? { ...row, name: "Mismatch" } : row),
  ])("rejects invalid Jones history without an execution adapter", (history) => {
    expect(inspectMigrationHistory(manifest, history, "Jones").error).toBeDefined();
  });
  it("rejects upstream gaps and returns independent pending ids", () => {
    const upstream = [[1, "First"], [2, "Second"], [3, "Third"]] as const;
    expect(inspectMigrationHistory(upstream, [{ id: 1, name: "First" }, { id: 3, name: "Third" }], "upstream").error).toBeDefined();
    expect(inspectMigrationHistory(upstream, [{ id: 1, name: "First" }], "upstream")).toEqual({ pending: [2, 3] });
    expect(inspectMigrationHistory(manifest, rows, "Jones")).toEqual({ pending: [] });
  });
});
