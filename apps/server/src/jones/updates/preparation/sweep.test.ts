// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Completion ordering uses explicit synthetic file mtimes.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { withQualifiedRuntimeLock } from "../../cloud/qualifiedRuntime.ts";
import { preparationJobLock, preparationJobsDirectory } from "./protocol.ts";
import { RETAINED_COMPLETED_STAGES, sweepPreparationLeftovers } from "./sweep.ts";
import { makePreparationHome, type PreparationHome } from "./Preparation.testkit.ts";

const homes: PreparationHome[] = [];
afterEach(async () => {
  for (const home of homes.splice(0)) await home.remove();
});
async function home(): Promise<PreparationHome> {
  const created = await makePreparationHome();
  homes.push(created);
  return created;
}
const exists = (path: string) =>
  NodeFSP.lstat(path).then(
    () => true,
    () => false,
  );
async function file(path: string, content = "fixture"): Promise<void> {
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  await NodeFSP.writeFile(path, content, { mode: 0o600 });
}
/** An Actions download attempt, optionally published by a completion index at `published`. */
async function attempt(
  cacheRoot: string,
  name: string,
  handle: string,
  published?: { readonly at: Date } | { readonly unreadable: true },
): Promise<string> {
  const directory = NodePath.join(cacheRoot, "attempts", name, handle);
  await file(NodePath.join(directory, "github-artifact.zip"));
  const index = NodePath.join(cacheRoot, "completed", `${handle}.json`);
  if (published !== undefined && "unreadable" in published) await file(index, "{");
  else if (published !== undefined) {
    await file(index, JSON.stringify({ schema: 1, handle, directory }));
    await NodeFSP.utimes(index, published.at, published.at);
  }
  return NodePath.dirname(directory);
}

describe("preparation sweep", () => {
  it("removes only leftovers whose owner is gone", async () => {
    const fixture = await home();
    const jobs = preparationJobsDirectory(fixture.base);
    const [keep, dead, live] = [
      NodeCrypto.randomUUID(),
      NodeCrypto.randomUUID(),
      NodeCrypto.randomUUID(),
    ];
    for (const id of [keep, dead, live])
      await file(NodePath.join(jobs, id, "tmp", ".jones-extract-1", "payload", "t3"));
    await NodeFSP.mkdir(NodePath.join(jobs, "not-a-job"));
    await NodeFSP.mkdir(fixture.cacheRoot, { recursive: true });
    const cacheRoot = await NodeFSP.realpath(fixture.cacheRoot);
    const selected = await attempt(cacheRoot, "stage-selected", "1".repeat(64), { at: new Date("2026-01-01T00:00:00.000Z") });
    const unselected = await attempt(cacheRoot, "stage-unselected", "2".repeat(64));
    const unreadable = await attempt(cacheRoot, "stage-unreadable", "3".repeat(64), {
      unreadable: true,
    });
    await NodeFSP.mkdir(NodePath.join(cacheRoot, "attempts", "unknown"));
    const runtime = NodePath.join(fixture.base, "runtime");
    await file(NodePath.join(runtime, "versions", ".jones-stage-1", "payload", "t3"));
    await NodeFSP.mkdir(NodePath.join(runtime, ".jones-dmg-mount-1"));
    await NodeFSP.mkdir(NodePath.join(runtime, ".jones-extract-1"));

    const sweep = await withQualifiedRuntimeLock(fixture.base, preparationJobLock(live), () =>
      sweepPreparationLeftovers({
        baseDir: fixture.base,
        cacheRoot: fixture.cacheRoot,
        keepJob: keep,
      }),
    );

    expect(sweep).toEqual({ removed: 3, retained: 7, failed: 0 });
    expect(await NodeFSP.readdir(jobs).then((names) => names.sort())).toEqual(
      [keep, live, "not-a-job"].sort(),
    );
    expect(await exists(NodePath.join(jobs, live, "tmp", ".jones-extract-1"))).toBe(true);
    expect(await exists(selected)).toBe(true);
    expect(await exists(unreadable)).toBe(true);
    expect(await exists(unselected)).toBe(false);
    expect(await NodeFSP.readdir(NodePath.join(runtime, "versions"))).not.toContain(
      ".jones-stage-1",
    );
    expect(await exists(NodePath.join(runtime, ".jones-dmg-mount-1"))).toBe(true);
    expect(await exists(NodePath.join(runtime, ".jones-extract-1"))).toBe(true);
  });

  it("keeps only the newest completed downloads so failed candidates cannot accumulate", async () => {
    const fixture = await home();
    await NodeFSP.mkdir(fixture.cacheRoot, { recursive: true });
    const cacheRoot = await NodeFSP.realpath(fixture.cacheRoot);
    const day = 24 * 60 * 60 * 1000;
    const now = Date.parse("2026-01-01T00:00:00.000Z");
    const legacy = NodePath.join(cacheRoot, "4".repeat(64));
    await file(NodePath.join(legacy, "github-artifact.zip"));
    await file(
      NodePath.join(cacheRoot, "completed", `${"4".repeat(64)}.json`),
      JSON.stringify({ schema: 1, handle: "4".repeat(64), directory: legacy }),
    );
    await NodeFSP.utimes(
      NodePath.join(cacheRoot, "completed", `${"4".repeat(64)}.json`),
      new Date(now - 9 * day),
      new Date(now - 9 * day),
    );
    const oldest = await attempt(cacheRoot, "stage-a", "5".repeat(64), {
      at: new Date(now - 3 * day),
    });
    const older = await attempt(cacheRoot, "stage-b", "6".repeat(64), {
      at: new Date(now - 2 * day),
    });
    const newer = await attempt(cacheRoot, "stage-c", "7".repeat(64), { at: new Date(now - day) });
    const newest = await attempt(cacheRoot, "stage-d", "8".repeat(64), { at: new Date(now) });
    expect(RETAINED_COMPLETED_STAGES).toBe(2);

    const sweep = await sweepPreparationLeftovers({
      baseDir: fixture.base,
      cacheRoot: fixture.cacheRoot,
      keepJob: NodeCrypto.randomUUID(),
    });

    expect(sweep).toEqual({ removed: 2, retained: 2, failed: 0 });
    expect(await exists(newest)).toBe(true);
    expect(await exists(newer)).toBe(true);
    expect(await exists(older)).toBe(false);
    expect(await exists(oldest)).toBe(false);
    expect(
      await NodeFSP.readdir(NodePath.join(cacheRoot, "completed")).then((names) => names.sort()),
    ).toEqual([`${"4".repeat(64)}.json`, `${"7".repeat(64)}.json`, `${"8".repeat(64)}.json`]);
    expect(await exists(legacy)).toBe(true);
  });

  it("retains a killed stage's runtime copy while the runtime lock is held", async () => {
    const fixture = await home();
    const scratch = NodePath.join(fixture.base, "runtime", "versions", ".jones-stage-1");
    await file(NodePath.join(scratch, "payload", "t3"));
    const sweep = await withQualifiedRuntimeLock(fixture.base, "qualified-runtime-lock", () =>
      sweepPreparationLeftovers({
        baseDir: fixture.base,
        cacheRoot: fixture.cacheRoot,
        keepJob: NodeCrypto.randomUUID(),
      }),
    );
    expect(sweep).toEqual({ removed: 0, retained: 1, failed: 0 });
    expect(await exists(scratch)).toBe(true);
  });
});
