// @effect-diagnostics nodeBuiltinImport:off - Staging jobs run in the separate preparation process.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  JonesActionsClient,
  JONES_ACTIONS_RECEIPT_FILE,
  findJonesCompletedStage,
  validateJonesStagedArtifact,
  type JonesActionsCandidate,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";

import type { JonesStagedMacApp } from "./jonesActivation.ts";
import { bundleFileSystem, stageJonesMacApp } from "./jonesMacStaging.ts";

const HANDLE = /^[a-f0-9]{64}$/;
const MAC_APP_RECEIPT_FILE = "mac-app-receipt.json";
const MAX_ENTRIES = 1000;
const MAX_RECORD_BYTES = 1024 * 1024;
const MAX_PAYLOAD_ENTRIES = 200_000;

export interface JonesStagingRoots {
  readonly artifacts: string;
  readonly apps: string;
}

export type JonesStagingJob =
  | {
      readonly kind: "prepare";
      readonly candidate: JonesActionsCandidate;
      readonly roots: JonesStagingRoots;
      readonly platform: NodeJS.Platform;
    }
  | { readonly kind: "validate-artifact"; readonly directory: string }
  | { readonly kind: "reclaim"; readonly updatesRoot: string; readonly handle: string };

export interface JonesPreparedStage {
  readonly artifact: JonesStagedArtifact;
  readonly app: JonesStagedMacApp;
}

export type JonesReclaimResult =
  | { readonly status: "reclaimed"; readonly bytes: number }
  | { readonly status: "absent" }
  | { readonly status: "referenced" };

export interface JonesStagingJobResults {
  readonly prepare: JonesPreparedStage;
  readonly "validate-artifact": JonesStagedArtifact;
  readonly reclaim: JonesReclaimResult;
}

export interface JonesStagingHooks {
  readonly onVerifying?: () => void | Promise<void>;
  readonly actions?: Pick<JonesActionsClient, "stage">;
  readonly stageApp?: typeof stageJonesMacApp;
}

export async function prepareJonesStage(
  candidate: JonesActionsCandidate,
  roots: JonesStagingRoots,
  platform: NodeJS.Platform,
  hooks: JonesStagingHooks = {},
): Promise<JonesPreparedStage> {
  await NodeFSP.mkdir(roots.apps, { recursive: true, mode: 0o700 });
  const artifact = await (hooks.actions ?? new JonesActionsClient()).stage(
    candidate,
    roots.artifacts,
  );
  await hooks.onVerifying?.();
  const app = await (hooks.stageApp ?? stageJonesMacApp)(artifact, roots.apps, platform);
  return { artifact, app };
}

export async function runJonesStagingJob(
  job: JonesStagingJob,
  hooks: JonesStagingHooks = {},
): Promise<JonesStagingJobResults[JonesStagingJob["kind"]]> {
  switch (job.kind) {
    case "prepare":
      return prepareJonesStage(job.candidate, job.roots, job.platform, hooks);
    case "validate-artifact":
      return validateJonesStagedArtifact(job.directory);
    case "reclaim":
      return reclaimJonesStagePayload(job.updatesRoot, job.handle);
  }
}

async function boundedEntries(directory: string): Promise<string[]> {
  let names: string[];
  try {
    const info = await NodeFSP.lstat(directory);
    if (!info.isDirectory()) throw new Error("A staging record root is not a directory.");
    names = await NodeFSP.readdir(directory);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  }
  if (names.length > MAX_ENTRIES) throw new Error("Too many staging records to inspect.");
  return names;
}

/** Transaction reserves and state copies are not records; every other entry is read. */
async function recordMentions(file: string, handle: string, jsonOnly = false): Promise<boolean> {
  if (jsonOnly && !file.endsWith(".json")) return false;
  const info = await NodeFSP.lstat(file);
  if (!info.isFile() || info.size > MAX_RECORD_BYTES)
    throw new Error("A staging record is not a bounded regular file.");
  return (await NodeFSP.readFile(file, "utf8")).includes(handle);
}

/**
 * True when any selection, attempt, transaction or retired preparation names the handle. A
 * mention, however indirect, keeps the payload: those records are activation evidence.
 */
async function handleReferenced(updatesRoot: string, handle: string): Promise<boolean> {
  // `selections` belongs to the bundled server's qualified runtime; a mention there still counts.
  for (const [name, jsonOnly] of [
    ["staging", false],
    ["attempt-selections", false],
    ["selections", true],
  ] as const) {
    const root = NodePath.join(updatesRoot, name);
    for (const entry of await boundedEntries(root))
      if (await recordMentions(NodePath.join(root, entry), handle, jsonOnly)) return true;
  }
  for (const name of ["transactions", "retired-preparations"]) {
    const root = NodePath.join(updatesRoot, name);
    for (const entry of await boundedEntries(root)) {
      if (entry.includes(handle)) return true;
      const directory = NodePath.join(root, entry);
      const info = await NodeFSP.lstat(directory);
      if (!info.isDirectory()) throw new Error("A transaction record is not a directory.");
      for (const record of await boundedEntries(directory))
        if (await recordMentions(NodePath.join(directory, record), handle, true)) return true;
    }
  }
  return false;
}

/** Every entry must be an ordinary owned file, directory or link on the root's device. */
async function requireRemovablePayload(target: string, device: number): Promise<number> {
  const fs = bundleFileSystem().promises;
  let entries = 0;
  let bytes = 0;
  const visit = async (path: string): Promise<void> => {
    if (++entries > MAX_PAYLOAD_ENTRIES) throw new Error("The payload exceeds its entry bound.");
    const info = await fs.lstat(path);
    if (info.dev !== device) throw new Error("The payload crosses a mount point.");
    if (info.isSymbolicLink()) return;
    if (info.isFile()) {
      bytes += info.size;
      return;
    }
    if (!info.isDirectory()) throw new Error("Unexpected payload entry type.");
    for (const name of await fs.readdir(path)) await visit(NodePath.join(path, name));
  };
  await visit(target);
  return bytes;
}

async function syncDirectory(path: string): Promise<void> {
  const handle = await NodeFSP.open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/**
 * Removes the completed artifact and app payloads of a handle whose selection was already
 * discarded natively. The completion index goes first, so an interruption leaves only an
 * unindexed attempt that no stage reuses, never an index naming a partial tree.
 */
export async function reclaimJonesStagePayload(
  updatesRoot: string,
  handle: string,
): Promise<JonesReclaimResult> {
  if (!HANDLE.test(handle)) throw new Error("Invalid Jones stage handle.");
  if (await handleReferenced(updatesRoot, handle)) return { status: "referenced" };
  let bytes = 0;
  let found = false;
  for (const [name, receipt] of [
    ["artifacts", JONES_ACTIONS_RECEIPT_FILE],
    ["apps", MAC_APP_RECEIPT_FILE],
  ] as const) {
    const root = NodePath.join(updatesRoot, name);
    let canonicalRoot: string;
    try {
      canonicalRoot = await NodeFSP.realpath(root);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ENOENT") continue;
      throw cause;
    }
    const directory = await findJonesCompletedStage(canonicalRoot, handle, receipt);
    if (directory === undefined) continue;
    found = true;
    const parts = NodePath.relative(canonicalRoot, directory).split(NodePath.sep);
    const target =
      parts.length === 3 && parts[0] === "attempts"
        ? NodePath.join(canonicalRoot, "attempts", parts[1]!)
        : directory;
    bytes += await requireRemovablePayload(target, (await NodeFSP.lstat(canonicalRoot)).dev);
    const index = NodePath.join(canonicalRoot, "completed", `${handle}.json`);
    try {
      await NodeFSP.unlink(index);
      await syncDirectory(NodePath.dirname(index));
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
    }
    await bundleFileSystem().promises.rm(target, { recursive: true });
    await syncDirectory(NodePath.dirname(target));
  }
  return found ? { status: "reclaimed", bytes } : { status: "absent" };
}
