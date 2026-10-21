// @effect-diagnostics nodeBuiltinImport:off
// The serving process never takes a worker's word for a stage. It reads the bounded durable
// receipt itself and checks home, database, active runtime and candidate provenance; the tree
// hashes behind that receipt ran in the worker, and the launcher re-verifies at activation.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { JonesActionsCandidate } from "@t3tools/shared/jones/jonesActions";
import {
  decodeStagedQualifiedRuntime,
  QualifiedRuntimeBlockedError,
  type StagedQualifiedRuntime,
} from "../../cloud/qualifiedRuntime.ts";
import { readBoundedJson, type PreparationHost } from "./protocol.ts";

const HANDLE = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;

const blocked = (reason: QualifiedRuntimeBlockedError["reason"], message: string): never => {
  throw new QualifiedRuntimeBlockedError(reason, message);
};

async function durableStage(
  baseDir: string,
  reported: StagedQualifiedRuntime,
): Promise<StagedQualifiedRuntime> {
  if (!HANDLE.test(reported.stagedHandle))
    return blocked("invalid-artifact", "Invalid staged update handle.");
  const durable = decodeStagedQualifiedRuntime(
    await readBoundedJson(
      NodePath.join(baseDir, "runtime", "staged-updates", `${reported.stagedHandle}.json`),
      64 * 1024,
    ).catch(() => undefined),
  );
  if (durable === undefined)
    return blocked("invalid-artifact", "The prepared stage has no valid durable receipt.");
  if (JSON.stringify(durable) !== JSON.stringify(reported))
    return blocked(
      "integrity-mismatch",
      "The preparation worker's stage does not match its durable receipt.",
    );
  return durable;
}

/** Accepts a stage job's result for `candidate` only as its own durable, bound receipt. */
export async function adoptPreparedStage(input: {
  readonly baseDir: string;
  readonly dbPath: string;
  readonly activeVersion: string;
  readonly candidate: JonesActionsCandidate;
  readonly host: PreparationHost;
  readonly staged: StagedQualifiedRuntime;
}): Promise<StagedQualifiedRuntime> {
  const baseDir = await NodeFSP.realpath(input.baseDir);
  const durable = await durableStage(baseDir, input.staged);
  const { binding, receipt } = durable;
  if (
    binding.baseDir !== baseDir ||
    binding.dbPath !== (await NodeFSP.realpath(input.dbPath)) ||
    binding.activeVersion !== input.activeVersion
  )
    return blocked(
      "binding-mismatch",
      "The prepared stage belongs to a different home, database or active runtime.",
    );
  const candidate = input.candidate;
  if (
    receipt.sourceSha !== candidate.source ||
    receipt.sourceTree !== candidate.tree ||
    receipt.installedSourceSha !== candidate.installedSource ||
    receipt.workflow !== candidate.workflow ||
    receipt.runId !== candidate.runId ||
    receipt.runAttempt !== candidate.runAttempt ||
    receipt.artifactId !== candidate.artifactId ||
    receipt.artifactDigest !== `sha256:${candidate.artifactDigest.replace(/^sha256:/, "")}` ||
    receipt.version !== candidate.version ||
    receipt.platform !== candidate.platform ||
    receipt.architecture !== candidate.architecture ||
    receipt.platform !== input.host.platform ||
    receipt.architecture !== input.host.architecture
  )
    return blocked(
      "invalid-artifact",
      "The prepared stage does not carry the selected candidate's provenance.",
    );
  return durable;
}

/** Accepts an install-time verification only for the requested handle and active runtime. */
export async function adoptVerifiedStage(input: {
  readonly baseDir: string;
  readonly activeVersion: string;
  readonly stagedHandle: string;
  readonly staged: StagedQualifiedRuntime;
}): Promise<StagedQualifiedRuntime> {
  if (input.staged.stagedHandle !== input.stagedHandle)
    return blocked("invalid-artifact", "The preparation worker verified a different handle.");
  const baseDir = await NodeFSP.realpath(input.baseDir);
  const durable = await durableStage(baseDir, input.staged);
  if (durable.binding.baseDir !== baseDir || durable.binding.activeVersion !== input.activeVersion)
    return blocked(
      "binding-mismatch",
      "The staged handle belongs to a different home or active runtime.",
    );
  return durable;
}
