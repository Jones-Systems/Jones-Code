// @effect-diagnostics nodeBuiltinImport:off - Standalone native recovery boundary shares the launcher's filesystem-only protocol.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  qualifiedRuntimeArtifactFromJonesStage,
  currentQualifiedRuntimeBinding,
  readQualifiedRuntimeReceipt,
  stageQualifiedRuntime,
  verifyStagedQualifiedRuntime,
  withQualifiedRuntimeLock,
  type QualifiedRuntimeBinding,
  type QualifiedRuntimeReceipt,
  type StagedQualifiedRuntime,
} from "../cloud/qualifiedRuntime.ts";
import { validateJonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import { parseServiceState, SERVICE_STATE_FILE } from "../../cloud/serviceProtocol.ts";
import { assertNoUnreconciledUpdateOperations, isUpdateOperationId } from "./launcherOperation.ts";
import { assertStageNotRevoked } from "./stageRevocation.ts";
import { restoreStagedSelection, retainStagedSelection } from "./stagedSelection.ts";

export interface RecoveryInput {
  readonly operationId: string;
  readonly baseDir: string;
  readonly currentVersion: string;
  readonly currentSource: string;
  readonly environmentId: string;
  readonly targetVersion: string;
  readonly targetSource: string;
  readonly targetTree: string;
  readonly runId: number;
  readonly runAttempt: number;
  readonly artifactId: number;
  readonly archiveSha256: string;
  readonly transportStageDirectory: string;
  readonly transportStageReceiptSha256: string;
  readonly adoptionReceiptSha256: string;
  readonly runtimeReceiptSha256: string;
}

export interface RecoveryPlan {
  readonly schema: 1;
  readonly input: RecoveryInput;
  readonly binding: QualifiedRuntimeBinding;
  readonly receipt: QualifiedRuntimeReceipt;
  readonly serviceStateSha256: string;
  readonly databaseIdentity: string;
  readonly artifactMetadata: "available" | "unavailable-404";
  readonly trustBasis: "preserved-original-admission";
  readonly sha256: string;
}

export interface RecoveryApi {
  readonly nativeHost?: { readonly platform: string; readonly architecture: string };
  readonly get: (endpoint: string) => Promise<{ readonly status: number; readonly body?: unknown }>;
}

const repository = "Jones-Systems/Jones-Code";
const apiRoot = `repos/${repository}`;
const hash = (bytes: string | Buffer): string =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const same = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left) === JSON.stringify(right);
const object = (value: unknown): Record<string, unknown> => {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Recovery evidence is not an object.");
  return value as Record<string, unknown>;
};

async function controlledBytes(path: string): Promise<Buffer> {
  const file = await NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (
      !stat.isFile() ||
      stat.uid !== NodeOS.userInfo().uid ||
      (stat.mode & 0o022) !== 0 ||
      stat.size > 131072
    )
      throw new Error("Recovery evidence is not a bounded owner-controlled regular file.");
    return await file.readFile();
  } finally {
    await file.close();
  }
}

async function boundDocument(path: string, digest: string): Promise<Record<string, unknown>> {
  const bytes = await controlledBytes(path);
  if (hash(bytes) !== digest) throw new Error("Preserved admission evidence checksum differs.");
  return object(JSON.parse(bytes.toString("utf8")));
}

function canonicalRun(value: unknown, workflow: string, source: string): Record<string, unknown> {
  const run = object(value);
  if (
    run.path !== workflow ||
    run.head_sha !== source ||
    run.head_branch !== "main" ||
    run.event !== "push" ||
    run.status !== "completed" ||
    run.conclusion !== "success" ||
    object(run.repository).full_name !== repository ||
    object(run.head_repository).full_name !== repository
  )
    throw new Error(
      "Historical workflow or same-source CI is not a successful canonical main run.",
    );
  return run;
}

function verifyArtifactMetadata(
  value: unknown,
  plan: RecoveryInput,
  candidate: Awaited<ReturnType<typeof validateJonesStagedArtifact>>["candidate"],
  repositoryId: unknown,
): void {
  const metadata = object(value),
    run = object(metadata.workflow_run);
  if (
    metadata.id !== plan.artifactId ||
    metadata.name !== candidate.artifactName ||
    metadata.digest !== `sha256:${plan.archiveSha256}` ||
    metadata.size_in_bytes !== candidate.artifactBytes ||
    metadata.expired !== false ||
    run.id !== plan.runId ||
    run.head_sha !== plan.targetSource ||
    run.head_branch !== "main" ||
    run.repository_id !== repositoryId ||
    run.head_repository_id !== repositoryId
  )
    throw new Error("Original artifact metadata does not bind the enrolled runtime and archive.");
}

async function inspect(input: RecoveryInput, api: RecoveryApi): Promise<RecoveryPlan> {
  if (
    !isUpdateOperationId(input.operationId) ||
    ![input.baseDir, input.transportStageDirectory].every((value) => NodePath.isAbsolute(value)) ||
    ![input.currentVersion, input.targetVersion].every((value) =>
      /^\d+\.\d+\.\d+-preview\.\d+\.\d+(?:\.\d+)?$/.test(value),
    ) ||
    ![input.currentSource, input.targetSource, input.targetTree].every((value) =>
      /^[a-f0-9]{40}$/.test(value),
    ) ||
    ![
      input.archiveSha256,
      input.transportStageReceiptSha256,
      input.adoptionReceiptSha256,
      input.runtimeReceiptSha256,
    ].every((value) => /^[a-f0-9]{64}$/.test(value)) ||
    ![input.runId, input.runAttempt, input.artifactId].every(
      (value) => Number.isSafeInteger(value) && value > 0,
    )
  )
    throw new Error(
      "Recovery requires explicit native, artifact and original admission identities.",
    );
  const binding = await currentQualifiedRuntimeBinding(
    input.baseDir,
    input.currentVersion,
    api.nativeHost,
  );
  if (
    binding.baseDir !== input.baseDir ||
    binding.environmentId !== input.environmentId ||
    binding.activeSourceSha !== input.currentSource
  )
    throw new Error("The current native binding differs from the recovery input.");
  const stateBytes = await controlledBytes(
    NodePath.join(input.baseDir, "runtime", SERVICE_STATE_FILE),
  );
  const state = parseServiceState(stateBytes.toString("utf8"));
  if (
    state === undefined ||
    state.activeVersion !== input.currentVersion ||
    state.update?.status === "pending" ||
    state.update?.status === "failed"
  )
    throw new Error("Native activation is pending, unknown or bound to another version.");
  await assertNoUnreconciledUpdateOperations(input.baseDir, state.update);
  await assertStageNotRevoked(input.baseDir, input.operationId, input.operationId);
  const adoption = await boundDocument(
    NodePath.join(input.baseDir, "runtime", "jones-host-adoption.json"),
    input.adoptionReceiptSha256,
  );
  if (
    adoption.schema !== 1 ||
    adoption.status !== "applied" ||
    adoption.baseDir !== binding.baseDir ||
    adoption.environmentId !== binding.environmentId ||
    adoption.activeVersion !== binding.activeVersion ||
    adoption.launcherVersion !== input.targetVersion
  )
    throw new Error("The completed native adoption does not bind this existing runtime pair.");
  const target = NodePath.join(input.baseDir, "runtime", "versions", input.targetVersion);
  await boundDocument(
    NodePath.join(target, ".jones-runtime-receipt.json"),
    input.runtimeReceiptSha256,
  );
  const receipt = await readQualifiedRuntimeReceipt(
    input.baseDir,
    input.targetVersion,
    api.nativeHost,
  );
  if (
    receipt.platform !== "darwin" ||
    receipt.architecture !== "arm64" ||
    receipt.workflow !== ".github/workflows/artifact-desktop-mac.yml" ||
    receipt.sourceSha !== input.targetSource ||
    receipt.sourceTree !== input.targetTree ||
    receipt.runId !== input.runId ||
    receipt.runAttempt !== input.runAttempt ||
    receipt.artifactId !== input.artifactId ||
    receipt.artifactDigest !== `sha256:${input.archiveSha256}`
  )
    throw new Error("Recovery only stages the exact already enrolled desktop runtime.");
  await boundDocument(
    NodePath.join(input.transportStageDirectory, "jones-actions-stage.json"),
    input.transportStageReceiptSha256,
  );
  const staged = await validateJonesStagedArtifact(input.transportStageDirectory);
  const artifact = qualifiedRuntimeArtifactFromJonesStage(staged, target);
  const { payloadDirectory: _payload, installedSourceSha: _history, ...immutable } = artifact;
  for (const key of Object.keys(immutable) as Array<keyof typeof immutable>)
    if (receipt[key] !== immutable[key])
      throw new Error("The retained archive differs from original runtime admission.");
  const get = async (endpoint: string): Promise<unknown> => {
    const result = await api.get(`${apiRoot}/${endpoint}`);
    if (result.status !== 200)
      throw new Error("Required historical qualification evidence is unavailable.");
    return result.body;
  };
  const run = canonicalRun(
    await get(`actions/runs/${input.runId}/attempts/${input.runAttempt}`),
    receipt.workflow,
    receipt.sourceSha,
  );
  if (
    run.id !== input.runId ||
    run.run_attempt !== input.runAttempt ||
    run.workflow_id !== staged.candidate.workflowId
  )
    throw new Error("The historical workflow attempt differs from original admission.");
  const commit = object(await get(`git/commits/${input.targetSource}`));
  if (commit.sha !== input.targetSource || object(commit.tree).sha !== input.targetTree)
    throw new Error("Historical source tree differs from the admitted build.");
  const ci = canonicalRun(
    await get(`actions/runs/${staged.candidate.ciRunId}`),
    ".github/workflows/ci.yml",
    input.targetSource,
  );
  if (ci.id !== staged.candidate.ciRunId) throw new Error("Same-source CI identity differs.");
  const main = object(await get("commits/main"));
  if (typeof main.sha !== "string" || !/^[a-f0-9]{40}$/.test(main.sha))
    throw new Error("Canonical main source is unavailable.");
  for (const [base, head] of [
    [input.currentSource, input.targetSource],
    [input.targetSource, main.sha],
  ]) {
    if (base === head) continue;
    const comparison = object(await get(`compare/${base}...${head}`));
    if (comparison.status !== "ahead" || object(comparison.merge_base_commit).sha !== base)
      throw new Error("The installed and target sources are not on canonical main ancestry.");
  }
  const live = await api.get(`${apiRoot}/actions/artifacts/${input.artifactId}`);
  if (live.status === 200)
    verifyArtifactMetadata(live.body, input, staged.candidate, object(run.repository).id);
  else if (live.status !== 404)
    throw new Error("Artifact metadata lookup has an unresolved result.");
  const selection = await restoreStagedSelection(
    input.baseDir,
    input.currentVersion,
    api.nativeHost,
  );
  if (
    selection !== undefined &&
    (selection.stagedHandle !== input.operationId ||
      !same(selection.binding, binding) ||
      !same(selection.receipt, receipt))
  )
    throw new Error("A foreign staged selection occupies the current native binding.");
  const stagePath = NodePath.join(
    binding.baseDir,
    "runtime",
    "staged-updates",
    `${input.operationId}.json`,
  );
  const stageExists = await NodeFSP.lstat(stagePath).catch((cause: NodeJS.ErrnoException) => {
    if (cause.code === "ENOENT") return undefined;
    throw cause;
  });
  if (stageExists !== undefined) {
    const native = await verifyStagedQualifiedRuntime(
      input.baseDir,
      input.currentVersion,
      input.operationId,
      api.nativeHost,
    );
    if (!same(native.receipt, receipt))
      throw new Error("Fixed recovery handle belongs to a different artifact.");
  }
  const recoveryDirectory = NodePath.join(binding.baseDir, "runtime", "jones-stage-recoveries");
  const operationNames = await NodeFSP.readdir(recoveryDirectory).catch(
    (cause: NodeJS.ErrnoException) => {
      if (cause.code === "ENOENT") return [];
      throw cause;
    },
  );
  if (operationNames.length > 4096)
    throw new Error("Recovery operation inspection limit exceeded.");
  for (const name of operationNames) {
    const id = name.replace(/\.json$/, "");
    if (!isUpdateOperationId(id) || name !== `${id}.json`)
      throw new Error("Recovery operation has an unknown entry; publication held.");
    const prior = object(
      JSON.parse((await controlledBytes(NodePath.join(recoveryDirectory, name))).toString("utf8")),
    );
    const { sha256, ...preimage } = prior;
    if (
      prior.schema !== 1 ||
      hash(JSON.stringify(preimage)) !== sha256 ||
      object(prior.input).operationId !== id
    )
      throw new Error("Recovery operation has an unknown preimage; publication held.");
    const priorBinding = object(prior.binding);
    if (
      id !== input.operationId &&
      priorBinding.baseDir === binding.baseDir &&
      priorBinding.activeVersion === binding.activeVersion &&
      priorBinding.environmentId === binding.environmentId
    )
      throw new Error(
        "Another recovery operation owns this native binding; reconcile its fixed handle.",
      );
  }
  const database = await NodeFSP.lstat(binding.dbPath);
  const body = {
    schema: 1 as const,
    input,
    binding,
    receipt,
    serviceStateSha256: hash(stateBytes),
    databaseIdentity: `${database.dev}:${database.ino}`,
    artifactMetadata: live.status === 404 ? ("unavailable-404" as const) : ("available" as const),
    trustBasis: "preserved-original-admission" as const,
  };
  return { ...body, sha256: hash(JSON.stringify(body)) };
}

/** Inspection reads existing admission and payload bytes; it never creates a cache or launches code. */
export async function inspectQualifiedStageRecovery(
  input: RecoveryInput,
  api: RecoveryApi,
): Promise<RecoveryPlan> {
  return inspect(input, api);
}

async function syncFileAndParent(path: string): Promise<void> {
  for (const entry of [path, NodePath.dirname(path)]) {
    const file = await NodeFSP.open(entry, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
    try {
      await file.sync();
    } finally {
      await file.close();
    }
  }
}

/** A fixed operation ID is also its stage handle: unknown publication can never allocate another handle. */
export async function applyQualifiedStageRecovery(
  plan: RecoveryPlan,
  api: RecoveryApi,
): Promise<StagedQualifiedRuntime> {
  return withQualifiedRuntimeLock(plan.input.baseDir, "qualified-stage-recovery-lock", async () => {
    const fresh = await inspect(plan.input, api);
    if (!same(plan, fresh)) throw new Error("Recovery plan preimage changed; inspect again.");
    const directory = NodePath.join(plan.binding.baseDir, "runtime", "jones-stage-recoveries");
    await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
    if ((await NodeFSP.realpath(directory)) !== directory)
      throw new Error("Recovery operation directory is not canonical.");
    const path = NodePath.join(directory, `${plan.input.operationId}.json`);
    const exists = await NodeFSP.lstat(path).catch((cause: NodeJS.ErrnoException) => {
      if (cause.code === "ENOENT") return undefined;
      throw cause;
    });
    if (exists !== undefined) {
      if (!same(JSON.parse((await controlledBytes(path)).toString("utf8")), plan))
        throw new Error(
          "Recovery operation has an unknown or different preimage; it was preserved.",
        );
    } else {
      const nativeStage = NodePath.join(
        plan.binding.baseDir,
        "runtime",
        "staged-updates",
        `${plan.input.operationId}.json`,
      );
      const occupied = await NodeFSP.lstat(nativeStage).catch((cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT") return undefined;
        throw cause;
      });
      if (occupied !== undefined)
        throw new Error("A native stage exists without this recovery operation's intent.");
      // Persist intent before the native stage or selection pointer can become visible.
      const file = await NodeFSP.open(path, "wx", 0o600);
      try {
        await file.writeFile(JSON.stringify(plan) + "\n");
        await file.sync();
      } finally {
        await file.close();
      }
    }
    await syncFileAndParent(path);
    const { protocol: _protocol, payloadSha256: _digest, ...artifact } = plan.receipt;
    const staged = await stageQualifiedRuntime({
      stagedHandle: plan.input.operationId,
      ...(api.nativeHost === undefined ? {} : { host: api.nativeHost }),
      binding: plan.binding,
      artifact: {
        ...artifact,
        installedSourceSha: plan.binding.activeSourceSha,
        payloadDirectory: NodePath.join(
          plan.binding.baseDir,
          "runtime",
          "versions",
          plan.receipt.version,
        ),
      },
      validate: async () => {
        const current = await inspect(plan.input, api);
        if (!same(current, plan))
          throw new Error("Recovery evidence changed before native stage publication.");
      },
    });
    await retainStagedSelection(staged);
    if (
      !same(
        await restoreStagedSelection(
          plan.binding.baseDir,
          plan.binding.activeVersion,
          api.nativeHost,
        ),
        staged,
      )
    )
      throw new Error("Native selection readback differs; publication requires reconciliation.");
    if (!same(await inspect(plan.input, api), plan))
      throw new Error(
        "Native state changed during stage publication; reconcile before installation.",
      );
    return staged;
  });
}
