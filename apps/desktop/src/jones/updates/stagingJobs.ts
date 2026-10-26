// @effect-diagnostics nodeBuiltinImport:off - Staging jobs run in the separate preparation process.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  JonesActionsClient,
  JONES_ACTIONS_RECEIPT_FILE,
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

async function boundedEntries(directory: string, device?: number): Promise<string[]> {
  let names: string[];
  try {
    await ownedDirectory(directory, device);
    names = await NodeFSP.readdir(directory);
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  }
  if (names.length > MAX_ENTRIES) throw new Error("Too many staging records to inspect.");
  return names;
}

/** Transaction reserves and state copies are not records; every other entry is read. */
async function recordMentions(
  file: string,
  handle: string,
  jsonOnly = false,
  device?: number,
): Promise<boolean> {
  if (jsonOnly && !file.endsWith(".json")) return false;
  const info = await NodeFSP.lstat(file);
  if (
    !info.isFile() ||
    info.uid !== process.getuid?.() ||
    (device !== undefined && info.dev !== device) ||
    info.size > MAX_RECORD_BYTES
  )
    throw new Error("A staging record is not a bounded regular file.");
  return (await NodeFSP.readFile(file, "utf8")).includes(handle);
}

/**
 * True when any selection, attempt, transaction or retired preparation names the handle. A
 * mention, however indirect, keeps the payload: those records are activation evidence.
 */
async function handleReferenced(updatesRoot: string, handle: string): Promise<boolean> {
  const device = (await ownedDirectory(updatesRoot)).dev;
  // `selections` belongs to the bundled server's qualified runtime; a mention there still counts.
  for (const [name, jsonOnly] of [
    ["staging", false],
    ["attempt-selections", false],
    ["selections", true],
  ] as const) {
    const root = NodePath.join(updatesRoot, name);
    for (const entry of await boundedEntries(root, device))
      if (await recordMentions(NodePath.join(root, entry), handle, jsonOnly, device)) return true;
  }
  for (const name of ["transactions", "retired-preparations"]) {
    const root = NodePath.join(updatesRoot, name);
    for (const entry of await boundedEntries(root, device)) {
      if (entry.includes(handle)) return true;
      const directory = NodePath.join(root, entry);
      await ownedDirectory(directory, device);
      for (const record of await boundedEntries(directory, device))
        if (await recordMentions(NodePath.join(directory, record), handle, true, device))
          return true;
    }
  }
  return false;
}

type Identity = { readonly dev: number; readonly ino: number; readonly uid: number };
type Completion = { readonly schema: 1; readonly handle: string; readonly directory: string };
type CleanupMarker = {
  readonly schema: 1;
  readonly purpose: "jones-stage-cleanup";
  readonly root: string;
  readonly rootIdentity: Identity;
  readonly handle: string;
  readonly target: string;
  readonly targetIdentity: Identity;
  readonly parentIdentity: Identity;
  readonly completion: Completion;
  readonly index: { readonly identity: Identity; readonly raw: string } | null;
};
const ATTEMPT = /^stage-[a-zA-Z0-9]+$/;
const CLEANUP = "stage-cleanup";

const missing = (cause: unknown) => (cause as NodeJS.ErrnoException).code === "ENOENT";
const identity = (info: Identity): Identity => ({ dev: info.dev, ino: info.ino, uid: info.uid });
const sameIdentity = (a: Identity, b: Identity) =>
  a.dev === b.dev && a.ino === b.ino && a.uid === b.uid;

async function optionalInfo(path: string) {
  try {
    return await NodeFSP.lstat(path);
  } catch (cause) {
    if (!missing(cause)) throw cause;
    return undefined;
  }
}

async function ownedDirectory(path: string, device?: number) {
  const info = await NodeFSP.lstat(path);
  if (
    !info.isDirectory() ||
    info.uid !== process.getuid?.() ||
    (device !== undefined && info.dev !== device) ||
    (await NodeFSP.realpath(path)) !== path
  )
    throw new Error("Cleanup requires an ordinary owned canonical directory on its device.");
  return info;
}

async function boundedRecord(path: string, device: number, maxBytes = 4096) {
  const info = await NodeFSP.lstat(path);
  if (
    !info.isFile() ||
    info.uid !== process.getuid?.() ||
    info.dev !== device ||
    info.size > maxBytes
  )
    throw new Error("Cleanup requires a bounded owned ordinary record.");
  return { info, raw: await NodeFSP.readFile(path, "utf8") };
}

function completionRecord(raw: string, root: string, handle?: string): Completion {
  const value = JSON.parse(raw) as Partial<Completion> | null;
  if (
    value === null ||
    value.schema !== 1 ||
    typeof value.handle !== "string" ||
    !HANDLE.test(value.handle) ||
    (handle !== undefined && value.handle !== handle) ||
    typeof value.directory !== "string"
  )
    throw new Error("Invalid cleanup completion evidence.");
  const parts = NodePath.relative(root, value.directory).split(NodePath.sep);
  if (
    value.directory !== NodePath.resolve(value.directory) ||
    !(
      (parts.length === 1 && parts[0] === value.handle) ||
      (parts.length === 3 &&
        parts[0] === "attempts" &&
        ATTEMPT.test(parts[1] ?? "") &&
        parts[2] === value.handle)
    )
  )
    throw new Error("Cleanup completion evidence escapes its publication layout.");
  return value as Completion;
}

function targetOf(root: string, completion: Completion) {
  return NodePath.dirname(completion.directory) === root
    ? completion.directory
    : NodePath.dirname(completion.directory);
}

/** Completion publication proves copier exit and detach; extra attempt entries invalidate it. */
async function requireAttemptLayout(root: string, completion: Completion, partial: boolean) {
  const target = targetOf(root, completion);
  if (target === completion.directory) return;
  const entries = await NodeFSP.readdir(target);
  if (
    entries.some((name) => name !== completion.handle && name !== "completion.json") ||
    (!partial &&
      (entries.length !== 2 ||
        !entries.includes("completion.json") ||
        !entries.includes(completion.handle)))
  )
    throw new Error("Unknown mount, copier or entry ownership in a staging attempt.");
}

async function requirePayloadLayout(directory: string, receipt: string) {
  const entries = await NodeFSP.readdir(directory);
  if (entries.length > MAX_ENTRIES) throw new Error("Too many stage payload entries.");
  for (const name of entries) {
    const info = await NodeFSP.lstat(NodePath.join(directory, name));
    if (receipt === MAC_APP_RECEIPT_FILE) {
      if (!((name === receipt && info.isFile()) || (name.endsWith(".app") && info.isDirectory())))
        throw new Error("Unknown mount, copier or entry ownership in a Mac payload.");
    } else if (!info.isFile() || /^(mount|copier)([.-]|$)/.test(name)) {
      throw new Error("Unknown mount, copier or entry ownership in an artifact payload.");
    }
  }
}

/** Never traverse links or mount points, including during replay of a partly removed tree. */
async function requireRemovablePayload(target: string, device: number): Promise<number> {
  const fs = bundleFileSystem().promises;
  let entries = 0;
  let bytes = 0;
  const visit = async (path: string): Promise<void> => {
    if (++entries > MAX_PAYLOAD_ENTRIES) throw new Error("The payload exceeds its entry bound.");
    const info = await fs.lstat(path);
    if (info.dev !== device || info.uid !== process.getuid?.())
      throw new Error("The payload crosses a mount point or ownership boundary.");
    if (info.isSymbolicLink()) {
      const link = NodePath.resolve(NodePath.dirname(path), await fs.readlink(path));
      const relative = NodePath.relative(target, link);
      if (
        relative === ".." ||
        relative.startsWith(`..${NodePath.sep}`) ||
        NodePath.isAbsolute(relative)
      )
        throw new Error("The payload contains an escaping link.");
      return;
    }
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

function decodeMarker(raw: string, root: string, handle: string): CleanupMarker {
  const value = JSON.parse(raw) as CleanupMarker;
  const validIdentity = (item: Identity | undefined) =>
    item !== undefined &&
    item !== null &&
    [item.dev, item.ino, item.uid].every((n) => Number.isSafeInteger(n) && n >= 0);
  if (
    !value ||
    value.schema !== 1 ||
    value.purpose !== "jones-stage-cleanup" ||
    value.root !== root ||
    value.handle !== handle ||
    !validIdentity(value.rootIdentity) ||
    !validIdentity(value.targetIdentity) ||
    !validIdentity(value.parentIdentity)
  )
    throw new Error("Invalid cleanup marker.");
  const completion = completionRecord(JSON.stringify(value.completion), root, handle);
  if (
    value.target !== targetOf(root, completion) ||
    value.targetIdentity.uid !== value.rootIdentity.uid ||
    value.targetIdentity.dev !== value.rootIdentity.dev ||
    value.parentIdentity.uid !== value.rootIdentity.uid ||
    value.parentIdentity.dev !== value.rootIdentity.dev
  )
    throw new Error("Invalid cleanup target identity.");
  if (value.index !== null) {
    if (
      !value.index ||
      !validIdentity(value.index.identity) ||
      typeof value.index.raw !== "string" ||
      value.index.identity.dev !== value.rootIdentity.dev ||
      value.index.identity.uid !== value.rootIdentity.uid ||
      completionRecord(value.index.raw, root, handle).directory !== completion.directory
    )
      throw new Error("Invalid cleanup index evidence.");
  }
  return value;
}

async function publishMarker(path: string, marker: CleanupMarker) {
  const raw = JSON.stringify(marker) + "\n";
  if (Buffer.byteLength(raw) > 4096) throw new Error("Cleanup marker exceeds its byte bound.");
  const scratch = `${path}.${NodeCrypto.randomBytes(8).toString("hex")}.tmp`;
  try {
    const file = await NodeFSP.open(scratch, "wx", 0o600);
    try {
      await file.writeFile(raw, "utf8");
      await file.sync();
    } finally {
      await file.close();
    }
    await NodeFSP.link(scratch, path); // Exclusive publication; never replace cleanup authority.
  } finally {
    await NodeFSP.unlink(scratch);
  }
  await syncDirectory(NodePath.dirname(path));
}

async function discoverCleanup(
  root: string,
  handle: string,
  receipt: string,
  pinned?: CleanupMarker,
): Promise<CleanupMarker | undefined> {
  const rootInfo = await ownedDirectory(root);
  for (const name of await boundedEntries(root, rootInfo.dev)) {
    if (name !== "attempts" && name !== "completed" && !HANDLE.test(name))
      throw new Error("Unexplained entry in staging root.");
  }
  const completed = NodePath.join(root, "completed");
  const indexPath = NodePath.join(completed, `${handle}.json`);
  let index: CleanupMarker["index"] = null;
  let completion: Completion | undefined;
  if (await optionalInfo(completed)) await ownedDirectory(completed, rootInfo.dev);
  if (await optionalInfo(indexPath)) {
    const record = await boundedRecord(indexPath, rootInfo.dev);
    completion = completionRecord(record.raw, root, handle);
    index = { identity: identity(record.info), raw: record.raw };
    if (pinned !== undefined) {
      if (
        pinned.index === null ||
        !sameIdentity(record.info, pinned.index.identity) ||
        record.raw !== pinned.index.raw
      )
        throw new Error("A conflicting completion index holds cleanup.");
      completion = undefined;
    }
  }
  const legacy = NodePath.join(root, handle);
  if ((await optionalInfo(legacy)) && legacy !== pinned?.target) {
    if (completion !== undefined && completion.directory !== legacy)
      throw new Error("Conflicting legacy stage and completion index.");
    completion = { schema: 1, handle, directory: legacy };
  }
  const attempts = NodePath.join(root, "attempts");
  if (await optionalInfo(attempts)) {
    await ownedDirectory(attempts, rootInfo.dev);
    for (const name of await boundedEntries(attempts)) {
      if (!ATTEMPT.test(name)) throw new Error("Unknown staging attempt.");
      const attempt = NodePath.join(attempts, name);
      if (attempt === pinned?.target) continue;
      await ownedDirectory(attempt, rootInfo.dev);
      const record = await boundedRecord(NodePath.join(attempt, "completion.json"), rootInfo.dev);
      const candidate = completionRecord(record.raw, root);
      if (targetOf(root, candidate) !== attempt)
        throw new Error("Completion evidence does not describe its own attempt.");
      await requireAttemptLayout(root, candidate, false);
      if (candidate.handle !== handle) continue;
      if (pinned !== undefined) throw new Error("Another unindexed target holds cleanup.");
      if (completion !== undefined && completion.directory !== candidate.directory)
        throw new Error("Conflicting staging completion targets.");
      completion = candidate;
    }
  }
  if (completion === undefined) return undefined;
  if (pinned !== undefined) throw new Error("Another legacy target holds cleanup.");
  const target = targetOf(root, completion);
  const targetInfo = await ownedDirectory(target, rootInfo.dev);
  await ownedDirectory(completion.directory, rootInfo.dev);
  await requireAttemptLayout(root, completion, false);
  await boundedRecord(NodePath.join(completion.directory, receipt), rootInfo.dev, 131072);
  await requirePayloadLayout(completion.directory, receipt);
  await requireRemovablePayload(target, rootInfo.dev);
  return {
    schema: 1,
    purpose: "jones-stage-cleanup",
    root,
    rootIdentity: identity(rootInfo),
    handle,
    target,
    targetIdentity: identity(targetInfo),
    parentIdentity: identity(await ownedDirectory(NodePath.dirname(target), rootInfo.dev)),
    completion,
    index,
  };
}

async function removePinnedPayload(marker: CleanupMarker) {
  const { root, handle, target } = marker;
  const rootInfo = await ownedDirectory(root);
  if (
    !sameIdentity(rootInfo, marker.rootIdentity) ||
    !sameIdentity(
      await ownedDirectory(NodePath.dirname(target), rootInfo.dev),
      marker.parentIdentity,
    )
  )
    throw new Error("Cleanup root or parent identity changed.");
  const targetInfo = await optionalInfo(target);
  let bytes = 0;
  if (targetInfo !== undefined) {
    await ownedDirectory(target, rootInfo.dev);
    if (!sameIdentity(targetInfo, marker.targetIdentity))
      throw new Error("Cleanup target identity changed.");
    await requireAttemptLayout(root, marker.completion, true);
    bytes = await requireRemovablePayload(target, rootInfo.dev);
  }
  // A replacement index, legacy directory or second unindexed attempt is never ours.
  const receipt =
    NodePath.basename(root) === "apps" ? MAC_APP_RECEIPT_FILE : JONES_ACTIONS_RECEIPT_FILE;
  await discoverCleanup(root, handle, receipt, marker);
  if (targetInfo !== undefined && (await optionalInfo(marker.completion.directory)))
    await requirePayloadLayout(marker.completion.directory, receipt);
  if (await handleReferenced(NodePath.dirname(root), handle)) return undefined;
  const completed = NodePath.join(root, "completed");
  if (await optionalInfo(completed)) {
    await ownedDirectory(completed, rootInfo.dev);
    const index = NodePath.join(completed, `${handle}.json`);
    if (await optionalInfo(index)) {
      const record = await boundedRecord(index, rootInfo.dev);
      if (
        marker.index === null ||
        !sameIdentity(record.info, marker.index.identity) ||
        record.raw !== marker.index.raw
      )
        throw new Error("A conflicting completion index holds cleanup.");
      await NodeFSP.unlink(index);
    }
    // Also required on replay when the unlink succeeded but its sync did not.
    await syncDirectory(completed);
  } else if (marker.index !== null)
    throw new Error("The cleanup completion directory disappeared.");
  if (targetInfo !== undefined) {
    if (target === marker.completion.directory) {
      await bundleFileSystem().promises.rm(target, { recursive: true });
    } else {
      const payload = marker.completion.directory;
      if (await optionalInfo(payload))
        await bundleFileSystem().promises.rm(payload, { recursive: true });
      const completion = NodePath.join(target, "completion.json");
      if (await optionalInfo(completion)) await NodeFSP.unlink(completion);
      await NodeFSP.rmdir(target);
    }
  }
  if (await optionalInfo(target)) throw new Error("Cleanup could not prove target absence.");
  await syncDirectory(NodePath.dirname(target));
  return bytes;
}

/**
 * Called under the controller's exclusive reclaim scope. Cleanup-only evidence is durable
 * outside the target before index-first removal; activation never consumes that evidence.
 */
export async function reclaimJonesStagePayload(
  updatesRoot: string,
  handle: string,
): Promise<JonesReclaimResult> {
  if (!HANDLE.test(handle)) throw new Error("Invalid Jones stage handle.");
  updatesRoot = NodePath.resolve(updatesRoot);
  const updatesInfo = await ownedDirectory(updatesRoot);
  if (await handleReferenced(updatesRoot, handle)) return { status: "referenced" };
  const cleanup = NodePath.join(updatesRoot, CLEANUP);
  await NodeFSP.mkdir(cleanup, { mode: 0o700, recursive: true });
  await ownedDirectory(cleanup, updatesInfo.dev);
  await syncDirectory(updatesRoot);
  let bytes = 0;
  let found = false;
  const markers: string[] = [];
  for (const [kind, receipt] of [
    ["artifacts", JONES_ACTIONS_RECEIPT_FILE],
    ["apps", MAC_APP_RECEIPT_FILE],
  ] as const) {
    const root = NodePath.join(updatesRoot, kind);
    const markerPath = NodePath.join(cleanup, `${kind}-${handle}.json`);
    let marker: CleanupMarker | undefined;
    if (await optionalInfo(markerPath)) {
      marker = decodeMarker((await boundedRecord(markerPath, updatesInfo.dev)).raw, root, handle);
    } else if (await optionalInfo(root)) {
      marker = await discoverCleanup(root, handle, receipt);
      if (marker !== undefined) await publishMarker(markerPath, marker);
    }
    // Retry publication durability before using even an already visible marker.
    await syncDirectory(cleanup);
    if (marker === undefined) {
      if (await optionalInfo(root)) await syncDirectory(root);
      else await syncDirectory(updatesRoot);
      continue;
    }
    found = true;
    const removed = await removePinnedPayload(marker);
    if (removed === undefined) return { status: "referenced" };
    bytes += removed;
    markers.push(markerPath);
  }
  // Both roots must be reconciled before cleanup authority can be retired.
  for (const path of markers) {
    await NodeFSP.unlink(path);
    await syncDirectory(cleanup);
  }
  await syncDirectory(cleanup);
  return found ? { status: "reclaimed", bytes } : { status: "absent" };
}
