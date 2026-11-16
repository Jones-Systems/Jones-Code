// @effect-diagnostics nodeBuiltinImport:off -- Valid native Git fixtures and reader failure seams only; every allocation has exact finally cleanup.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { command } from "./Conformance.testkit.ts";
import {
  observeReftable,
  inflateReftableBlock,
  type ReftableReaderOptions,
} from "./ReftableObservation.ts";

async function fixture(run: (repo: string, directory: string) => Promise<void>, format = "sha1") {
  const parent = NodePath.resolve(
    import.meta.dirname,
    "../../../../../node_modules/.cache/jones/reftable-reader-tests",
  );
  await NodeFSP.mkdir(parent, { recursive: true });
  const base = await NodeFSP.mkdtemp(NodePath.join(parent, "case-"));
  try {
    await command(base, [
      "init",
      "--ref-format=reftable",
      `--object-format=${format}`,
      "-b",
      "main",
    ]);
    await run(base, NodePath.join(base, ".git/reftable"));
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(base), { code: "ENOENT" });
  }
}
async function complete(
  directory: string,
  format: "sha1" | "sha256" = "sha1",
  options?: ReftableReaderOptions,
) {
  const result = await observeReftable(directory, format, options);
  NodeAssert.equal(result.outcome, "complete", JSON.stringify(result));
  NodeAssert.ok(result.outcome === "complete");
  NodeAssert.equal(
    result.inputBytes,
    result.tables.reduce((sum, table) => sum + table.bytes, 0) +
      (await NodeFSP.readFile(NodePath.join(directory, "tables.list"))).length,
  );
  return result;
}
export const reftableReaderCases: Record<string, () => Promise<void>> = {
  "valid adjacent zlib streams expose exact consumed input through the public async API":
    async () => {
      const a = Buffer.from("first valid stream".repeat(64)),
        b = Buffer.from("second valid stream".repeat(64));
      const compressedA = NodeZlib.deflateSync(a),
        compressedB = NodeZlib.deflateSync(b);
      const decoded = await inflateReftableBlock(
        Buffer.concat([compressedA, compressedB]),
        a.length,
        new AbortController().signal,
      );
      NodeAssert.deepEqual(decoded.bytes, a);
      NodeAssert.equal(decoded.consumed, compressedA.length);
      const next = await inflateReftableBlock(compressedB, b.length, new AbortController().signal);
      NodeAssert.deepEqual(next.bytes, b);
      NodeAssert.equal(next.consumed, compressedB.length);
    },
  "valid SHA1 and SHA256 empty, ref-only, mixed and compacted stacks preserve exact operation endpoints":
    async () => {
      for (const format of ["sha1", "sha256"] as const)
        await fixture(async (repo, directory) => {
          NodeAssert.deepEqual((await complete(directory, format)).endpoints, []);
          await command(repo, [
            "-c",
            "core.logAllRefUpdates=false",
            "commit",
            "--allow-empty",
            "-m",
            "base without log",
          ]);
          const base = await command(repo, ["rev-parse", "HEAD"]);
          await command(repo, ["tag", "-a", "native-peeled-tag", "-m", "valid annotated tag"]);
          const refOnly = await complete(directory, format);
          NodeAssert.deepEqual(refOnly.endpoints, []);
          await command(repo, ["config", "reftable.geometricFactor", "1"]);
          const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
          const orphan = await command(repo, ["commit-tree", tree, "-m", "old only orphan"]);
          await command(repo, ["update-ref", "--create-reflog", "HEAD", orphan]);
          await command(repo, ["update-ref", "HEAD", base]);
          NodeAssert.deepEqual(
            (await complete(directory, format)).endpoints,
            [base, orphan].sort(),
          );
          await command(repo, ["reflog", "delete", "HEAD@{1}"]);
          // The surviving transition still retains the disconnected old endpoint.
          NodeAssert.deepEqual(
            (await complete(directory, format)).endpoints,
            [base, orphan].sort(),
          );
          await command(repo, ["pack-refs", "--all"]);
          NodeAssert.deepEqual(
            (await complete(directory, format)).endpoints,
            [base, orphan].sort(),
          );
          await command(repo, ["reflog", "expire", "--expire=all", "--all"]);
          NodeAssert.deepEqual((await complete(directory, format)).endpoints, []);
        }, format);
    },
  "valid small blocks account for multiple log blocks, object records and multilevel indexes": () =>
    fixture(async (repo, directory) => {
      await command(repo, ["config", "reftable.blockSize", "256"]);
      await command(repo, ["config", "reftable.restartInterval", "2"]);
      await command(repo, ["config", "reftable.indexObjects", "true"]);
      await command(repo, ["commit", "--allow-empty", "-m", "base"]);
      const base = await command(repo, ["rev-parse", "HEAD"]);
      const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
      const expected = new Set([base]);
      for (let i = 0; i < 45; i++) {
        const tip = await command(repo, ["commit-tree", tree, "-m", `unique-${i}`]);
        expected.add(tip);
        await command(repo, [
          "update-ref",
          "--create-reflog",
          `refs/heads/history-${String(i).padStart(4, "0")}-${"x".repeat(60)}`,
          tip,
        ]);
        await command(repo, ["update-ref", "HEAD", tip, "-m", `log ${i} ${"message ".repeat(8)}`]);
      }
      await command(repo, ["pack-refs", "--all"]);
      const observedStart = performance.now();
      const result = await complete(directory);
      // @effect-diagnostics-next-line globalConsole:off -- Shared native/Vitest fixture reports only bounded aggregate cost as qualification evidence.
      console.info(
        JSON.stringify({
          fixture: "native-small-blocks",
          readerMs: performance.now() - observedStart,
          inputBytes: result.inputBytes,
          expandedBytes: result.expandedBytes,
          keyBytes: result.keyBytes,
          tables: result.tables.length,
          blocks: result.blocks,
          records: result.records,
        }),
      );
      NodeAssert.deepEqual(result.endpoints, [...expected].sort());
      NodeAssert.ok(result.blocks > 30 && result.records > 100, JSON.stringify(result));
      // Re-run the same valid bytes through a measured inflater seam to prove
      // actual compressed block boundaries, rather than only synthetic zlib use.
      let logBlocks = 0,
        adjacent = 0;
      const repeated = await complete(directory, "sha1", {
        inflate: async (input, length, signal) => {
          const result = await inflateReftableBlock(input, length, signal);
          logBlocks++;
          if (input[result.consumed] === 0x67) adjacent++;
          return result;
        },
      });
      NodeAssert.deepEqual(repeated.endpoints, result.endpoints);
      NodeAssert.ok(logBlocks > 1 && adjacent > 0);
    }),
  "exact-key tombstones suppress only their log and safe .log carriers use manifest order": () =>
    fixture(async (repo, directory) => {
      await command(repo, ["config", "reftable.geometricFactor", "1"]);
      await command(repo, [
        "-c",
        "core.logAllRefUpdates=false",
        "commit",
        "--allow-empty",
        "-m",
        "ref only",
      ]);
      const base = await command(repo, ["rev-parse", "HEAD"]);
      const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
      const orphan = await command(repo, ["commit-tree", tree, "-m", "independent"]);
      await command(repo, ["update-ref", "--create-reflog", "refs/worktree/one", orphan]);
      await command(repo, ["update-ref", "--create-reflog", "refs/worktree/two", base]);
      NodeAssert.deepEqual((await complete(directory)).endpoints, [base, orphan].sort());
      await command(repo, ["reflog", "delete", "refs/worktree/one@{0}"]);
      const result = await complete(directory);
      NodeAssert.deepEqual(result.endpoints, [base]);
      NodeAssert.ok(
        (
          await Promise.all(
            result.tables.map(async (table) => {
              const bytes = await NodeFSP.readFile(NodePath.join(directory, table.name));
              return bytes[bytes[4] === 1 ? 24 : 28] === 0x67;
            }),
          )
        ).some(Boolean),
        "native reflog deletion must exercise a log-only table",
      );
      // Copy unchanged native bytes into a reader-only snapshot. Suffixes are
      // carriers, not content types. This snapshot is never opened by Git.
      const copy = NodePath.join(repo, "reader-snapshot");
      await NodeFSP.mkdir(copy);
      const names = result.tables.map((table, i) => `table-${i}.log`);
      for (let i = 0; i < names.length; i++)
        await NodeFSP.copyFile(
          NodePath.join(directory, result.tables[i]!.name),
          NodePath.join(copy, names[i]!),
        );
      await NodeFSP.writeFile(NodePath.join(copy, "tables.list"), names.join("\n") + "\n");
      await NodeFSP.writeFile(NodePath.join(copy, "unlisted.ref"), "not an active table");
      NodeAssert.deepEqual((await complete(copy)).endpoints, [base]);
      await command(repo, ["pack-refs", "--all"]);
      NodeAssert.deepEqual((await complete(directory)).endpoints, [base]);
    }),
  "reader failures never expose partial tips after valid records were decoded": () =>
    fixture(async (repo, directory) => {
      await command(repo, ["config", "reftable.geometricFactor", "1"]);
      await command(repo, ["commit", "--allow-empty", "-m", "first"]);
      await command(repo, ["commit", "--allow-empty", "-m", "second"]);
      const valid = await complete(directory);
      NodeAssert.ok(valid.endpoints.length === 2 && valid.tables.length > 1);
      const seams: ReftableReaderOptions[] = [
        {
          inflate: async () => {
            throw new Error("injected inflater failure");
          },
        },
        {
          inflate: async (input, length, signal) => ({
            ...(await inflateReftableBlock(input, length, signal)),
            consumed: NaN,
          }),
        },
        {
          inflate: async (input, length, signal) => ({
            ...(await inflateReftableBlock(input, length, signal)),
            bytes: Buffer.alloc(0),
          }),
        },
        { identity: async () => "injected source change" },
        { remaining: () => 0 },
        { signal: AbortSignal.abort() },
        ...["input", "tables", "tips", "expanded", "keys", "blocks", "records"].map((key) => ({
          limits: { [key]: 1 },
        })),
      ];
      const aborter = new AbortController();
      let expired = false;
      seams.push({
        signal: aborter.signal,
        inflate: async (input, length, signal) => {
          const result = await inflateReftableBlock(input, length, signal);
          aborter.abort();
          return result;
        },
      });
      seams.push({
        remaining: () => (expired ? 0 : 60000),
        inflate: async (input, length, signal) => {
          const result = await inflateReftableBlock(input, length, signal);
          expired = true;
          return result;
        },
      });
      let calls = 0;
      seams.push({
        inflate: async (input, length, signal) => {
          if (++calls === 2) throw new Error("later block unavailable");
          return inflateReftableBlock(input, length, signal);
        },
      });
      for (const options of seams) {
        const result = await observeReftable(directory, "sha1", options);
        NodeAssert.equal(result.outcome, "unknown", JSON.stringify(result));
        NodeAssert.ok(!("endpoints" in result));
      }
      // A late read failure and a changed manifest are injected without writing
      // damaged files, and without sending invented bytes to native Git.
      const fs = await import("./Filesystem.ts");
      for (const mode of ["read", "manifest"] as const) {
        let manifests = 0;
        const result = await observeReftable(directory, "sha1", {
          read: async (path, limit, check) => {
            check();
            if (mode === "read" && path.endsWith(valid.tables.at(-1)!.name))
              throw new Error("injected read failure");
            const bytes = await fs.readBounded(path, limit);
            const file = await NodeFSP.stat(path, { bigint: true });
            return {
              bytes,
              identity:
                mode === "manifest" && path.endsWith("tables.list") && ++manifests === 2
                  ? "changed"
                  : fs.snapshot(file),
            };
          },
        });
        NodeAssert.equal(result.outcome, "unknown");
        NodeAssert.ok(!("endpoints" in result));
      }
    }),
};
