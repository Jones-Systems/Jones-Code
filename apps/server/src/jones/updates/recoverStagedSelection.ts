// @effect-diagnostics nodeBuiltinImport:off
// Standalone recovery shares the launcher's built-in-only native boundary.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { parseServiceState } from "../../cloud/serviceProtocol.ts";
import {
  QualifiedRuntimeBlockedError,
  verifyStagedQualifiedRuntime,
  withQualifiedRuntimeLock,
  type StagedQualifiedRuntime,
} from "../cloud/qualifiedRuntime.ts";
import { assertNoUnreconciledUpdateOperations } from "./launcherOperation.ts";
import { assertStageNotRevoked } from "./stageRevocation.ts";
import { restoreStagedSelection, retainStagedSelection, selectionPath } from "./stagedSelection.ts";

export interface RecoverStagedSelectionInput {
  readonly baseDir: string;
  readonly activeVersion: string;
  readonly expectedActiveSource: string;
  readonly environmentId: string;
  readonly targetSource: string;
  readonly targetVersion: string;
  readonly stagedHandle: string;
  readonly apply?: boolean;
}

export interface RecoverStagedSelectionResult {
  readonly status: "would-retain" | "retained" | "already-retained" | "refused" | "unknown";
  readonly reason: string;
  readonly selectionPath?: string;
  readonly receipt?: {
    readonly stagedHandle: string;
    readonly environmentId: string;
    readonly activeSource: string;
    readonly targetSource: string;
    readonly targetVersion: string;
  };
}

interface RecoveryDependencies {
  readonly host?: { readonly platform: string; readonly architecture: string };
  readonly lock?: typeof withQualifiedRuntimeLock;
  readonly retain?: typeof retainStagedSelection;
  readonly sync?: (path: string) => Promise<void>;
}

async function ownedRegularFile(path: string) {
  const stat = await NodeFSP.lstat(path);
  if (!stat.isFile() || (stat.mode & 0o077) !== 0 || stat.uid !== NodeOS.userInfo().uid)
    throw new Error("Unknown file ownership.");
  return stat;
}

async function checkDirectories(baseDir: string): Promise<void> {
  for (const parts of [
    [],
    ["runtime"],
    ["runtime", "jones-updates"],
    ["runtime", "jones-updates", "selections"],
  ]) {
    const stat = await NodeFSP.lstat(NodePath.join(baseDir, ...parts)).catch(
      (error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT" && parts.length > 1) return undefined;
        throw error;
      },
    );
    if (
      stat !== undefined &&
      (!stat.isDirectory() || stat.uid !== NodeOS.userInfo().uid || (stat.mode & 0o022) !== 0)
    )
      throw new Error("Unknown directory ownership.");
  }
}

async function syncSelection(path: string): Promise<void> {
  const file = await NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    await file.sync();
  } finally {
    await file.close();
  }
  // Retaining may create both directories; persist their entries up to the existing runtime.
  let directory = NodePath.dirname(path);
  for (let depth = 0; depth < 3; depth++) {
    const parent = await NodeFSP.open(
      directory,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
    );
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    directory = NodePath.dirname(directory);
  }
}

export async function recoverStagedSelection(
  input: RecoverStagedSelectionInput,
  deps: RecoveryDependencies = {},
): Promise<RecoverStagedSelectionResult> {
  let effectStarted = false;
  let reason = "invalid-input";
  let path: string | undefined;
  let receipt: RecoverStagedSelectionResult["receipt"];
  try {
    if (
      !/^[a-f0-9]{40}$/.test(input.expectedActiveSource) ||
      !/^[a-f0-9]{40}$/.test(input.targetSource) ||
      input.environmentId.trim() === ""
    )
      throw new Error("Invalid identity.");
    reason = "base-directory-mismatch";
    if ((await NodeFSP.realpath(input.baseDir)) !== input.baseDir)
      throw new Error("Base must be canonical.");
    reason = "directory-ownership";
    await checkDirectories(input.baseDir);
    reason = "lock-unavailable";
    const lockPath = NodePath.join(input.baseDir, "runtime", "qualified-runtime-lock.sqlite");
    const originalLock = await ownedRegularFile(lockPath);
    return await (deps.lock ?? withQualifiedRuntimeLock)(
      input.baseDir,
      "qualified-runtime-lock",
      async () => {
        const locked = await ownedRegularFile(lockPath);
        if (locked.dev !== originalLock.dev || locked.ino !== originalLock.ino)
          throw new Error("Lock changed.");
        reason = "directory-ownership";
        await checkDirectories(input.baseDir);
        reason = "service-state-invalid";
        const statePath = NodePath.join(input.baseDir, "runtime", "service-state.json");
        const stateFile = await NodeFSP.open(
          statePath,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
        );
        let state;
        try {
          const stat = await stateFile.stat();
          if (
            !stat.isFile() ||
            stat.size > 65536 ||
            stat.uid !== NodeOS.userInfo().uid ||
            (stat.mode & 0o022) !== 0
          )
            throw new Error("Unknown service state.");
          state = parseServiceState(await stateFile.readFile("utf8"));
        } finally {
          await stateFile.close();
        }
        if (state === undefined) throw new Error("Invalid service state.");
        reason = "active-version-mismatch";
        if (state.activeVersion !== input.activeVersion) throw new Error("Active version changed.");
        reason = "pending-update";
        if (state.update?.status === "pending") throw new Error("Update pending.");
        reason = "unreconciled-operation";
        await assertNoUnreconciledUpdateOperations(input.baseDir, state.update);
        reason = "stage-verification-failed";
        const staged: StagedQualifiedRuntime = await verifyStagedQualifiedRuntime(
          input.baseDir,
          input.activeVersion,
          input.stagedHandle,
          deps.host,
        );
        reason = "expected-binding-mismatch";
        if (
          staged.stagedHandle !== input.stagedHandle ||
          staged.binding.environmentId !== input.environmentId ||
          staged.binding.activeSourceSha !== input.expectedActiveSource ||
          staged.receipt.installedSourceSha !== input.expectedActiveSource ||
          staged.receipt.sourceSha !== input.targetSource ||
          staged.receipt.version !== input.targetVersion
        )
          throw new Error("Unexpected staged identity.");
        path = selectionPath(staged.binding);
        receipt = {
          stagedHandle: staged.stagedHandle,
          environmentId: staged.binding.environmentId,
          activeSource: staged.binding.activeSourceSha,
          targetSource: staged.receipt.sourceSha,
          targetVersion: staged.receipt.version,
        };
        reason = "stage-revoked-or-unknown";
        await assertStageNotRevoked(input.baseDir, input.stagedHandle);
        reason = "occupied-selection";
        const existing = await restoreStagedSelection(input.baseDir, input.activeVersion);
        if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(staged))
          throw new Error("Another selection exists.");
        if (existing === undefined && !input.apply)
          return {
            status: "would-retain",
            reason: "verified-orphan",
            selectionPath: path,
            receipt,
          };
        // Once retain or durability starts, a lost reply must never authorize blind retry.
        effectStarted = true;
        if (existing === undefined) {
          reason = "retention-incomplete";
          await (deps.retain ?? retainStagedSelection)(staged);
        }
        reason = "durability-unconfirmed";
        await (deps.sync ?? syncSelection)(path);
        reason = "selection-readback-mismatch";
        if (
          JSON.stringify(await restoreStagedSelection(input.baseDir, input.activeVersion)) !==
          JSON.stringify(staged)
        )
          throw new Error("Selection changed.");
        reason = "stage-revoked-or-unknown";
        await assertStageNotRevoked(input.baseDir, input.stagedHandle);
        return {
          status: existing === undefined ? "retained" : "already-retained",
          reason: "verified-selection",
          selectionPath: path,
          receipt,
        };
      },
    );
  } catch (cause) {
    return {
      status: effectStarted ? "unknown" : "refused",
      reason: cause instanceof QualifiedRuntimeBlockedError ? cause.reason : reason,
      ...(path === undefined ? {} : { selectionPath: path }),
      ...(receipt === undefined ? {} : { receipt }),
    };
  }
}
