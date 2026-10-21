// @effect-diagnostics nodeBuiltinImport:off
// Runs inside the preparation worker while it holds the per-home preparation lock, so no other
// preparation can own what it removes. A killed worker leaves its leftovers for this sweep.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  QualifiedRuntimeBlockedError,
  withQualifiedRuntimeLock,
} from "../../cloud/qualifiedRuntime.ts";
import { ownedScratchRemover } from "../../cloud/ownedScratch.ts";
import {
  isPreparationJobId,
  preparationJobLock,
  preparationJobsDirectory,
  readBoundedJson,
  type PreparationSweep,
} from "./protocol.ts";

const ATTEMPT = /^stage-[a-zA-Z0-9]+$/;
const HANDLE = /^[a-f0-9]{64}$/;
const COMPLETION = /^[a-f0-9]{64}\.json$/;
/** Completed downloads kept for a re-stage; older ones are retired so failed candidates cannot accumulate. */
export const RETAINED_COMPLETED_STAGES = 2;

async function names(directory: string): Promise<ReadonlyArray<string>> {
  try {
    const stat = await NodeFSP.lstat(directory);
    if (!stat.isDirectory()) return [];
    return await NodeFSP.readdir(directory);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  }
}

/**
 * Whether a completion index selects a directory in this attempt. Anything unreadable or unknown
 * counts as selected, so the attempt is retained rather than guessed about.
 */
async function selectedAttempt(root: string, attempt: string): Promise<boolean> {
  let entries: ReadonlyArray<string>;
  try {
    entries = await NodeFSP.readdir(attempt);
  } catch {
    return true;
  }
  for (const handle of entries) {
    if (!HANDLE.test(handle)) continue;
    try {
      const index = await readBoundedJson(NodePath.join(root, "completed", `${handle}.json`), 4096);
      if (
        typeof index !== "object" ||
        index === null ||
        !("directory" in index) ||
        typeof index.directory !== "string" ||
        index.directory === NodePath.join(attempt, handle)
      )
        return true;
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ENOENT") return true;
    }
  }
  return false;
}

/**
 * Unlinks the completion indexes of all but the newest `RETAINED_COMPLETED_STAGES` downloads, by
 * publication time; their attempts then count as unselected. An index naming a legacy completed
 * directory outside `attempts/` is never touched.
 */
async function retireStaleCompletions(cacheRoot: string): Promise<number> {
  const completed = NodePath.join(cacheRoot, "completed");
  const indexes: Array<{ readonly path: string; readonly published: number }> = [];
  for (const name of await names(completed)) {
    if (!COMPLETION.test(name)) continue;
    const path = NodePath.join(completed, name);
    const stat = await NodeFSP.lstat(path).catch(() => undefined);
    if (stat?.isFile()) indexes.push({ path, published: stat.mtimeMs });
  }
  indexes.sort((left, right) => right.published - left.published);
  let failed = 0;
  for (const { path } of indexes.slice(RETAINED_COMPLETED_STAGES)) {
    try {
      const index = await readBoundedJson(path, 4096);
      const directory =
        typeof index === "object" && index !== null && "directory" in index
          ? index.directory
          : undefined;
      const relative =
        typeof directory === "string"
          ? NodePath.relative(cacheRoot, directory).split(NodePath.sep)
          : [];
      if (relative.length === 3 && relative[0] === "attempts" && ATTEMPT.test(relative[1] ?? ""))
        await NodeFSP.unlink(path);
    } catch {
      failed += 1;
    }
  }
  return failed;
}

/**
 * Removes dead preparation leftovers:
 * - job directories whose owner is gone, each under its owner lock, with their extraction scratch
 *   and worker temporary files;
 * - Actions attempts that no completion index selects, after retiring all but the newest
 *   completed downloads;
 * - runtime copies a killed stage left in `runtime/versions/.jones-stage-*`, under the runtime lock.
 * Live jobs, DMG mounts and adoption or legacy extraction scratch are retained, never removed.
 */
export async function sweepPreparationLeftovers(input: {
  readonly baseDir: string;
  readonly cacheRoot: string;
  readonly keepJob: string;
}): Promise<PreparationSweep> {
  const remove = ownedScratchRemover();
  const baseDir = await NodeFSP.realpath(input.baseDir);
  let removed = 0;
  let retained = 0;
  let failed = 0;
  const removeEach = async (paths: ReadonlyArray<string>) => {
    for (const path of paths) {
      try {
        await remove(path);
        removed += 1;
      } catch {
        failed += 1;
      }
    }
  };

  const jobs = preparationJobsDirectory(baseDir);
  for (const name of await names(jobs)) {
    if (name === input.keepJob) continue;
    if (!isPreparationJobId(name)) {
      retained += 1;
      continue;
    }
    try {
      await withQualifiedRuntimeLock(baseDir, preparationJobLock(name), () =>
        remove(NodePath.join(jobs, name)),
      );
      removed += 1;
    } catch (cause) {
      if (cause instanceof QualifiedRuntimeBlockedError && cause.reason === "busy") retained += 1;
      // Its owner removed it first.
      else if ((cause as NodeJS.ErrnoException).code !== "ENOENT") failed += 1;
    }
  }

  const cacheRoot = await NodeFSP.realpath(input.cacheRoot).catch(() => undefined);
  if (cacheRoot !== undefined) {
    failed += await retireStaleCompletions(cacheRoot);
    const attempts = NodePath.join(cacheRoot, "attempts");
    const unselected: string[] = [];
    for (const name of await names(attempts)) {
      const attempt = NodePath.join(attempts, name);
      if (ATTEMPT.test(name) && !(await selectedAttempt(cacheRoot, attempt)))
        unselected.push(attempt);
      else retained += 1;
    }
    await removeEach(unselected);
  }

  const versions = NodePath.join(baseDir, "runtime", "versions");
  const stageScratch = (await names(versions))
    .filter((name) => name.startsWith(".jones-stage-"))
    .map((name) => NodePath.join(versions, name));
  if (stageScratch.length > 0) {
    try {
      await withQualifiedRuntimeLock(baseDir, "qualified-runtime-lock", () =>
        removeEach(stageScratch),
      );
    } catch {
      retained += stageScratch.length;
    }
  }

  for (const name of await names(NodePath.join(baseDir, "runtime")))
    if (name.startsWith(".jones-dmg-mount-") || name.startsWith(".jones-extract-")) retained += 1;
  return { removed, retained, failed };
}
