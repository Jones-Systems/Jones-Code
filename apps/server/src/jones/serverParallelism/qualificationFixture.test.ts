// @effect-diagnostics nodeBuiltinImport:off -- One task-owned fixed native traversal fixture; no provider/live state.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { FileFinder, type GrepCursor } from "@ff-labs/fff-node";
import * as NodeProcess from "node:process";
import { expect, it } from "vite-plus/test";
import {
  qualificationContent,
  qualificationVariant,
  qualificationResidentOrder,
  qualificationFixture,
  qualificationFixtureId,
  assertBalancedCohorts,
} from "./qualificationFixture.ts";

it.each([2, 3] as const)(
  "freezes balanced N%s variant blocks and refuses an actually unbalanced captured cohort",
  (count) => {
    const order = qualificationResidentOrder(count);
    expect([...order].sort((left, right) => left - right)).toEqual(
      Array.from({ length: 4 * count }, (_, index) => index),
    );
    const cohorts = Array.from({ length: count }, (_, slot) =>
      order
        .map((key, admission) => ({ key, admission, variant: qualificationVariant(key) }))
        .filter((entry) => entry.admission % count === slot)
        .map((entry) => ({ key: { variant: entry.variant } })),
    );
    expect(() => assertBalancedCohorts(cohorts)).not.toThrow();
    expect(() =>
      assertBalancedCohorts(
        cohorts.map((cohort, index) =>
          index === 0 ? cohort.map(() => ({ key: { variant: "paths" as const } })) : cohort,
        ),
      ),
    ).toThrow("lacks two path and two content");
    expect(qualificationFixtureId).toMatch(/^[a-f0-9]{64}$/);
    for (let key = 0; key < 8 * count; key++) {
      const bytes = Buffer.byteLength(qualificationContent(key));
      expect(bytes).toBeLessThanOrEqual(128 * 1024);
      expect(bytes * 512).toBeLessThanOrEqual(64 * 1024 * 1024);
    }
  },
);

it("real installed regex traverses every fixed dense content file without matches or fallback", async () => {
  const root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-native-fixed-traversal-")),
  );
  const workspace = NodePath.join(root, "workspace");
  await NodeFSP.mkdir(workspace);
  const content = qualificationContent(23);
  // Exactly512 regular content files; separate native cache/history/log roots are outside the workspace.
  for (let file = 0; file < 512; file++)
    await NodeFSP.writeFile(NodePath.join(workspace, `file-${file}.txt`), content);
  const created = FileFinder.create({
    basePath: workspace,
    frecencyDbPath: NodePath.join(root, "frecency"),
    historyDbPath: NodePath.join(root, "history"),
    logFilePath: NodePath.join(root, "fff.log"),
    disableMmapCache: true,
    disableContentIndexing: false,
    disableWatch: false,
    cacheBudgetMaxFiles: 1024,
    cacheBudgetMaxBytes: 32 * 1024 * 1024,
    aiMode: false,
    enableFsRootScanning: true,
    enableHomeDirScanning: true,
  });
  if (!created.ok) throw new Error(created.error);
  const finder = created.value;
  let destroyed = false;
  try {
    expect(await finder.waitForIndexReady(15000)).toEqual({ ok: true, value: true });
    const positive = finder.grep("needle", {
      mode: "plain",
      smartCase: false,
      pageSize: 1,
      maxMatchesPerFile: 1,
    });
    expect(positive.ok).toBe(true);
    if (!positive.ok) throw new Error(positive.error);
    expect(positive.value.items).toHaveLength(1);
    expect(positive.value.items[0]!.lineContent).toBe("needle-23 alpha beta");
    let cursor: GrepCursor | null = null;
    let searched = 0;
    const pages: Array<{ searched: number; total: number; eligible: number }> = [];
    do {
      const response = finder.grep(qualificationFixture.zeroMatchQuery, {
        mode: "regex",
        smartCase: false,
        pageSize: 33,
        maxMatchesPerFile: 100,
        timeBudgetMs: 10,
        enforceTimeBudget: true,
        cursor,
      });
      expect(response.ok).toBe(true);
      if (!response.ok) throw new Error(response.error);
      const result = response.value;
      expect(result.regexFallbackError).toBeUndefined();
      expect(result.items).toEqual([]);
      expect(result.totalMatched).toBe(0);
      expect(result.totalFiles).toBe(512);
      expect(result.filteredFileCount).toBe(512);
      searched += result.totalFilesSearched;
      pages.push({
        searched: result.totalFilesSearched,
        total: result.totalFiles,
        eligible: result.filteredFileCount,
      });
      cursor = result.nextCursor;
    } while (cursor !== null);
    expect(searched).toBe(512);
    NodeProcess.stdout.write(
      JSON.stringify({
        schema: "jones.native-zero-match-proof/v2",
        fixtureId: qualificationFixtureId,
        fileBytes: Buffer.byteLength(content),
        files: 512,
        workspaceBytes: Buffer.byteLength(content) * 512,
        query: qualificationFixture.zeroMatchQuery,
        searched,
        pages,
      }) + "\n",
    );
  } finally {
    finder.destroy();
    destroyed = finder.isDestroyed;
    if (destroyed) await NodeFSP.rm(root, { recursive: true });
  }
  expect(destroyed).toBe(true);
});
