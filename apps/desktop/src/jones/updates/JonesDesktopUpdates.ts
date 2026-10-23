// @effect-diagnostics nodeBuiltinImport:off - Native updater uses ordinary-UID filesystem and process operations.
import type { DesktopFleetStore } from "../fleetUpdates/store.ts";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import {
  JonesActionsClient,
  JonesActionsError,
  validateJonesStagedArtifact,
  requireJonesStageDirectory,
  type JonesActionsCandidate,
} from "@t3tools/shared/jones/jonesActions";
import type { DesktopUpdateState } from "@t3tools/contracts";
import { readJonesBuildStartedAt } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { EnvironmentId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import {
  JonesActiveInstall,
  JonesInstallIntent,
  requireWarmJonesInstallIntent,
  JonesStagedMacApp,
  type JonesActiveInstall as ActiveInstall,
  type JonesStagedMacApp as StagedMacApp,
} from "./jonesActivation.ts";
import {
  readMacBundleIdentity,
  requireStableMacAppPath,
  JonesCandidateStartupGateUnavailableError,
  preflightJonesCandidateStartupGate,
  runNativeCommand,
  stageJonesMacApp,
  writeJonesNativeFile,
} from "./jonesMacStaging.ts";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";
import { jonesPreparationText } from "./jonesNativePreparation.ts";
import type {
  JonesDesktopPreparationState,
  PreparationHold,
} from "./backgroundPreparationPolicy.ts";
import {
  prepareJonesStage,
  type JonesPreparedStage,
  type JonesStagingRoots,
} from "./stagingJobs.ts";

const SourceHash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const BuildMetadata = Schema.Struct({
  version: Schema.optionalKey(Schema.Unknown),
  jonesBuildStartedAt: Schema.optionalKey(Schema.Unknown),
  t3codeCommitHash: Schema.optionalKey(SourceHash),
  jonesSource: Schema.optionalKey(
    Schema.Struct({
      repository: Schema.Literal("Jones-Systems/Jones-Code"),
      sha: SourceHash,
      tree: SourceHash,
    }),
  ),
});
const ActivationJournal = Schema.Struct({
  intent: JonesInstallIntent,
  phase: Schema.String,
  message: Schema.optionalKey(Schema.String),
  recovery: Schema.optionalKey(
    Schema.Struct({
      method: Schema.Literals(["clone", "copy", "sqlite-backup"]),
      bytes: Schema.Number,
      startedAt: Schema.Number,
      completedAt: Schema.Number,
    }),
  ),
});
const StageSelection = Schema.Struct({
  schema: Schema.Literal(1),
  source: Schema.Literal("jones-actions"),
  home: Schema.String,
  profile: Schema.NullOr(Schema.String),
  currentVersion: Schema.String,
  installedSource: SourceHash,
  active: Schema.NullOr(JonesActiveInstall),
  artifactDirectory: Schema.String,
  app: JonesStagedMacApp,
});
const NativeStageReceipt = Schema.Struct({
  app: JonesStagedMacApp,
  artifact: Schema.Unknown,
  candidate: Schema.Unknown,
});

const decodeNativeSelectionResult = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    operation: Schema.Literals(["discard-staged", "claim-preparation", "claim-activation"]),
    handle: Schema.String,
    transactionId: Schema.optionalKey(Schema.String),
    status: Schema.Literals([
      "discarded",
      "preparation-claimed",
      "claimed",
      "refused",
      "uncertain",
    ]),
    intentPath: Schema.optionalKey(Schema.String),
    reason: Schema.optionalKey(Schema.String),
  }),
);

const decodeNativePreparationResult = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    operation: Schema.Literals(["inspect-preparation", "retire-preparation"]),
    transactionId: Schema.String,
    status: Schema.Literals(["ready", "retired", "refused", "uncertain"]),
    evidence: Schema.optionalKey(Schema.String),
    planSha256: Schema.optionalKey(Schema.String),
    reason: Schema.optionalKey(Schema.String),
  }),
);
const decodePreparationFailure = Schema.decodeUnknownSync(
  Schema.Struct({ protocol: Schema.Literal(1), reason: Schema.String }),
);
const decodeNativeWithdrawalResult = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    operation: Schema.Literal("withdraw-preparation"),
    transactionId: Schema.String,
    status: Schema.Literals(["withdrawn", "refused", "uncertain"]),
    claimSha256: Schema.optionalKey(Schema.String),
    proofPath: Schema.optionalKey(Schema.String),
    proofSha256: Schema.optionalKey(Schema.String),
    reason: Schema.optionalKey(Schema.String),
  }),
);
const decodeNativeRetirementResult = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    operation: Schema.Literals(["inspect-retirement", "retire-transaction"]),
    transactionId: Schema.String,
    status: Schema.Literals(["ready", "retired", "refused", "uncertain"]),
    planSha256: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))),
    activeBinding: Schema.optionalKey(JonesActiveInstall),
    journalSha256: Schema.optionalKey(Schema.String),
    targets: Schema.optionalKey(Schema.Array(Schema.Unknown)),
    reconciliation: Schema.optionalKey(Schema.Boolean),
    reason: Schema.optionalKey(Schema.String),
  }),
);

const decodeBuildMetadata = Schema.decodeUnknownSync(BuildMetadata);
const decodeActiveInstall = Schema.decodeUnknownSync(JonesActiveInstall);
const decodeStagedApp = Schema.decodeUnknownSync(JonesStagedMacApp);
const decodeActivationJournal = (value: unknown) => {
  requireWarmJonesInstallIntent(
    typeof value === "object" && value !== null && "intent" in value ? value.intent : undefined,
  );
  return Schema.decodeUnknownSync(ActivationJournal)(value);
};
const decodeEnvironmentId = Schema.decodeUnknownSync(EnvironmentId);
const decodeStageSelection = Schema.decodeUnknownSync(StageSelection);
const decodeNativeStageReceipt = Schema.decodeUnknownSync(NativeStageReceipt);
const decodeAttemptSelection = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    transactionId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    stagedHandle: Schema.String,
    selectionPath: Schema.String,
    selectionSha256: Schema.String,
    expected: JonesActiveInstall,
  }),
);

function jonesContinuationReceiptPath(home: string, transactionId: string): string {
  if (!/^[a-f0-9]{64}$/.test(transactionId))
    throw new Error("Invalid Jones native transaction ID.");
  return NodePath.join(
    home,
    "runtime",
    "jones-updates",
    "transactions",
    transactionId,
    "continuation.json",
  );
}

/** A displayable native cause. System errors carry paths, so only their code is shown. */
function nativeFailureReason(cause: unknown): string {
  if (typeof cause === "object" && cause !== null && "code" in cause)
    return typeof cause.code === "string" && /^[A-Z0-9_]{1,32}$/.test(cause.code)
      ? `system error ${cause.code}`
      : "unexpected system error";
  const text = cause instanceof Error ? jonesPreparationText(cause.message) : "";
  return text.length === 0 ? "unexpected error" : text.replace(/\.$/, "");
}

function provenance(candidate: JonesActionsCandidate, buildStartedAt: string | undefined) {
  return {
    repository: candidate.repository,
    sourceSha: candidate.source,
    sourceTree: candidate.tree,
    workflow: candidate.workflow,
    runId: candidate.runId,
    runAttempt: candidate.runAttempt,
    artifactId: candidate.artifactId,
    artifactDigest: candidate.artifactDigest,
    platform: candidate.platform,
    architecture: candidate.architecture,
    version: candidate.version,
    ...(buildStartedAt === undefined ? {} : { buildStartedAt }),
  } as const;
}

function downloadFailureCategory(cause: unknown): string {
  const codes: Record<string, string> = {
    ENOSPC: "not enough disk space",
    EACCES: "file access denied",
    EPERM: "file access denied",
    ENOENT: "required file or tool unavailable",
    EIO: "disk I/O error",
    ETIMEDOUT: "network timeout",
    ECONNRESET: "network connection interrupted",
    ENOTFOUND: "network unavailable",
  };
  const reasons: Record<string, string> = {
    "gh-missing": "GitHub CLI unavailable",
    "authentication-required": "GitHub sign-in required",
    "rate-limited": "GitHub rate limit reached",
    unavailable: "GitHub unavailable",
    unqualified: "build no longer qualified",
    integrity: "artifact integrity check failed",
    "occupied-cache": "download cache needs recovery",
    "unsupported-platform": "unsupported platform",
  };
  let current = cause;
  for (let depth = 0; depth < 5 && typeof current === "object" && current !== null; depth++) {
    if (current instanceof JonesActionsError)
      return reasons[current.reason] ?? "artifact staging failed";
    if ("code" in current && typeof current.code === "string" && codes[current.code])
      return codes[current.code]!;
    if (current instanceof Error) {
      if (/digest|checksum|integrity/i.test(current.message))
        return "artifact integrity check failed";
      if (/network|fetch failed/i.test(current.message)) return "network unavailable";
      if (/extract|archive|dmg|hdiutil/i.test(current.message)) return "app extraction failed";
    }
    current = "cause" in current ? current.cause : undefined;
  }
  return "artifact staging failed";
}

export interface JonesDesktopDownloadSelection {
  readonly artifactId: number;
  readonly sourceSha: string;
}
export interface JonesDesktopDownloadResult {
  readonly accepted: boolean;
  readonly completed: boolean;
  readonly refusal?: "selection-mismatch";
}

export interface JonesDesktopUpdateOptions {
  readonly home: string;
  readonly appRoot: string;
  readonly appPath: string;
  readonly executablePath: string;
  readonly profile: string | undefined;
  readonly activeGeneration: string | undefined;
  readonly architecture: "x64" | "arm64";
  readonly platform: "darwin";
  readonly initialState: DesktopUpdateState;
  readonly disabledByEnv: boolean;
  readonly fleet?: Pick<DesktopFleetStore, "bindInstall" | "recordOutcome">;
  readonly onState: (state: DesktopUpdateState) => Promise<void>;
  /** Resolves with the backend's state; its message explains a missing preparation receipt. */
  readonly prepareNative?: (
    stagedHandle: string,
    transactionId: string,
    active: ActiveInstall,
  ) => Promise<{ readonly phase?: string; readonly message?: string } | void>;
  readonly processProofs: () => Promise<readonly { pid: number; identity: string }[]>;
  readonly listener: () => Promise<string>;
  readonly timestamp: () => Promise<string>;
  readonly client?: Pick<JonesActionsClient, "check" | "stage">;
  readonly stageApp?: typeof stageJonesMacApp;
  readonly validateArtifact?: typeof validateJonesStagedArtifact;
  /**
   * Downloads and verifies the artifact and app. Production runs it in the separate preparation
   * process; the default stages in this process with `client` and `stageApp`.
   */
  readonly prepareStage?: (
    candidate: JonesActionsCandidate,
    roots: JonesStagingRoots,
    onVerifying: () => Promise<void>,
  ) => Promise<JonesPreparedStage>;
}

export type JonesSupersedeResult = "replaced" | "refused" | "uncertain" | "download-failed";

/** One host-owned checker. Downloads retain a fixed candidate; later checks cannot replace Install. */
export class JonesDesktopUpdateController {
  #state: DesktopUpdateState;
  #source: string | undefined;
  #installedSource: JonesUpdateState["installedSource"];
  #candidate: JonesActionsCandidate | undefined;
  #candidateBuildStartedAt: string | undefined;
  #currentBuildStartedAt: string | undefined;
  #staged: StagedMacApp | undefined;
  #selectedAttempt: { readonly stagedHandle: string; readonly transactionId: string } | undefined;
  #busy: "check" | "download" | "install" | "discard" | "reclaim" | null = null;
  /** The newest build the last check offered, even while an older one stays staged. */
  #latest: JonesActionsCandidate | undefined;
  #latestBuildStartedAt: string | undefined;
  #bootstrap: ActiveInstall | undefined;
  #activationBlocked = false;
  #candidateStartupGateUnavailable = false;
  #terminalPhase: "committed" | "rolled-back" | undefined;
  #activationMessage: string | undefined;
  #lastJournal: typeof ActivationJournal.Type | undefined;
  readonly #options: JonesDesktopUpdateOptions;
  readonly #client: Pick<JonesActionsClient, "check" | "stage">;

  constructor(options: JonesDesktopUpdateOptions) {
    this.#options = options;
    this.#client = options.client ?? new JonesActionsClient();
    this.#state = options.initialState;
  }

  get state() {
    return this.#state;
  }
  get busy() {
    return this.#busy;
  }
  get latestAvailable() {
    return this.#latest;
  }
  get stagedBuild():
    | { readonly handle: string; readonly source: string; readonly version: string }
    | undefined {
    return this.#staged === undefined
      ? undefined
      : {
          handle: this.#staged.handle,
          source: this.#staged.sourceSha,
          version: this.#staged.version,
        };
  }
  /** Why automatic preparation must not touch the stage; a claimed install pins it. */
  get preparationHold(): PreparationHold | undefined {
    const phase = this.#state.jones?.phase;
    if (this.#busy === "install" || phase === "preparing" || phase === "installing")
      return { kind: "install" };
    if (this.#activationBlocked)
      return {
        kind: "blocked",
        ...(this.#state.message === null ? {} : { message: this.#state.message }),
      };
    if (this.#source === undefined || this.#options.disabledByEnv || this.#bootstrap === undefined)
      return {
        kind: "unavailable",
        ...(this.#state.message === null ? {} : { message: this.#state.message }),
      };
    return undefined;
  }
  get manifestPath() {
    return NodePath.join(this.#options.home, "runtime", "jones-active-install.json");
  }
  get updaterRoot() {
    return NodePath.join(this.#options.home, "runtime", "jones-updates");
  }

  async #selectionBinding() {
    return {
      schema: 1 as const,
      source: "jones-actions" as const,
      home: await NodeFSP.realpath(this.#options.home),
      profile:
        this.#options.profile === undefined ? null : await NodeFSP.realpath(this.#options.profile),
      currentVersion: this.#state.currentVersion,
      installedSource: this.#source ?? "",
      active: this.#bootstrap ?? null,
    };
  }

  async #selectionFile(): Promise<string> {
    const binding = await this.#selectionBinding();
    const digest = NodeCrypto.createHash("sha256")
      .update(
        JSON.stringify({
          ...binding,
          terminalTransaction: this.#lastJournal?.intent.transactionId,
        }),
      )
      .digest("hex");
    return NodePath.join(this.updaterRoot, "staging", `${binding.installedSource}-${digest}.json`);
  }

  async #selectAttempt(
    stagedHandle: string,
    selectionPath: string,
    selectionSha256: string,
    expected: ActiveInstall,
  ): Promise<string> {
    const binding = {
      protocol: 1 as const,
      stagedHandle,
      selectionPath,
      selectionSha256,
      expected,
    };
    const directory = NodePath.join(this.updaterRoot, "attempt-selections");
    const key = NodeCrypto.createHash("sha256")
      .update(`${selectionPath}\n${selectionSha256}`)
      .digest("hex");
    const path = NodePath.join(directory, `${key}.json`);
    const read = async () => {
      const info = await NodeFSP.lstat(path);
      if (!info.isFile() || info.isSymbolicLink() || info.size > 131072)
        throw new Error("Native attempt selection is not a bounded regular file.");
      const attempt = decodeAttemptSelection(JSON.parse(await NodeFSP.readFile(path, "utf8")));
      for (const key of [
        "protocol",
        "stagedHandle",
        "selectionPath",
        "selectionSha256",
        "expected",
      ] as const)
        if (JSON.stringify(attempt[key]) !== JSON.stringify(binding[key]))
          throw new Error("The persisted native attempt binding changed.");
      for (const parentPath of [directory, this.updaterRoot]) {
        const parent = await NodeFSP.open(parentPath, "r");
        try {
          await parent.sync();
        } finally {
          await parent.close();
        }
      }
      return attempt.transactionId;
    };
    try {
      return await read();
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
    }
    await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
    const scratch = NodePath.join(
      directory,
      `${key}.${NodeCrypto.randomBytes(16).toString("hex")}.pending`,
    );
    try {
      await writeJonesNativeFile(
        scratch,
        JSON.stringify({ ...binding, transactionId: NodeCrypto.randomBytes(32).toString("hex") }) +
          "\n",
        0o600,
      );
      try {
        await NodeFSP.link(scratch, path);
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
      }
      const parent = await NodeFSP.open(directory, "r");
      try {
        await parent.sync();
      } finally {
        await parent.close();
      }
    } finally {
      await NodeFSP.unlink(scratch).catch((cause: NodeJS.ErrnoException) => {
        if (cause.code !== "ENOENT") throw cause;
      });
    }
    // Read the winning immutable record before dispatch; a lost reply must retain this attempt ID.
    return read();
  }

  async #hydrateStage(): Promise<void> {
    const selectionFile = await this.#selectionFile();
    let raw: string;
    try {
      const info = await NodeFSP.lstat(selectionFile);
      if (!info.isFile() || info.size > 131072)
        throw new Error("Unknown occupied native staging selection.");
      raw = await NodeFSP.readFile(selectionFile, "utf8");
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ENOENT") return;
      throw cause;
    }
    const selection = decodeStageSelection(JSON.parse(raw));
    const binding = await this.#selectionBinding();
    for (const key of [
      "schema",
      "source",
      "home",
      "profile",
      "currentVersion",
      "installedSource",
      "active",
    ] as const) {
      if (JSON.stringify(selection[key]) !== JSON.stringify(binding[key]))
        throw new Error("Staged native runtime binding changed.");
    }
    if (!/^[a-f0-9]{64}$/.test(selection.app.handle))
      throw new Error("Unknown native staged handle.");
    const appsRoot = await NodeFSP.realpath(NodePath.join(this.updaterRoot, "apps"));
    const expectedDirectory = await requireJonesStageDirectory(
      NodePath.join(this.updaterRoot, "artifacts"),
      selection.app.handle,
      selection.artifactDirectory,
    );
    const appDirectory = await requireJonesStageDirectory(
      appsRoot,
      selection.app.handle,
      NodePath.dirname(selection.app.appPath),
    );
    const artifact = await (this.#options.validateArtifact ?? validateJonesStagedArtifact)(
      expectedDirectory,
    );
    if (
      artifact.stagedHandle !== selection.app.handle ||
      artifact.candidate.installedSource !== this.#source ||
      artifact.candidate.platform !== "darwin" ||
      artifact.candidate.architecture !== this.#options.architecture ||
      selection.app.sourceSha !== artifact.candidate.source ||
      selection.app.sourceTree !== artifact.candidate.tree ||
      selection.app.version !== artifact.candidate.version ||
      selection.app.receiptPath !== NodePath.join(appDirectory, "mac-app-receipt.json") ||
      (selection.app.bundleIdentifier !== undefined &&
        (await readMacBundleIdentity(selection.app.appPath)).bundleIdentifier !==
          selection.app.bundleIdentifier)
    )
      throw new Error("Staged native app or artifact integrity changed.");
    const info = await NodeFSP.lstat(selection.app.receiptPath);
    if (!info.isFile() || info.size > 131072) throw new Error("Unknown native app receipt.");
    const receipt = decodeNativeStageReceipt(
      JSON.parse(await NodeFSP.readFile(selection.app.receiptPath, "utf8")),
    );
    if (
      JSON.stringify(receipt.app) !== JSON.stringify(selection.app) ||
      JSON.stringify(receipt.artifact) !== JSON.stringify(artifact) ||
      JSON.stringify(receipt.candidate) !== JSON.stringify(artifact.candidate)
    )
      throw new Error("Native app receipt bindings changed.");
    this.#candidate = artifact.candidate;
    this.#candidateBuildStartedAt = undefined;
    this.#staged = selection.app;
  }

  async #publish(
    phase: JonesUpdateState["phase"],
    status: DesktopUpdateState["status"],
    message?: string,
  ): Promise<void> {
    const candidateGateUnavailable =
      this.#candidateStartupGateUnavailable ||
      (this.#staged !== undefined && this.#staged.startupGateProtocol !== 1);
    if (candidateGateUnavailable && message === undefined) {
      message =
        "The staged candidate lacks qualified startupGateProtocol:1; its fixed selection was retained.";
    }
    const checkedAt =
      this.#busy === "check" ? await this.#options.timestamp() : this.#state.checkedAt;
    const reason =
      this.#source === undefined
        ? "source-unqualified"
        : this.#activationBlocked
          ? "blocked"
          : this.#options.disabledByEnv || this.#bootstrap === undefined
            ? "bootstrap-required"
            : candidateGateUnavailable
              ? "source-unqualified"
              : undefined;
    const journal = this.#lastJournal;
    const currentAttempt =
      this.#selectedAttempt?.stagedHandle === this.#staged?.handle
        ? this.#selectedAttempt?.transactionId
        : undefined;
    const journalDescribesPhase =
      phase === "committed" ||
      phase === "rolled-back" ||
      (phase === "blocked" && journal?.phase !== "resumed" && journal?.phase !== "rolled-back");
    const updateId =
      currentAttempt ?? (journalDescribesPhase ? journal?.intent.transactionId : undefined);
    const outcomeStatus =
      journal?.phase === "resumed"
        ? "committed"
        : journal?.phase === "rolled-back"
          ? "rolled-back"
          : journal === undefined
            ? undefined
            : "blocked";
    const jones: JonesUpdateState = {
      source: "jones-actions",
      channel: "jones-main",
      phase,
      ...(this.#installedSource === undefined ? {} : { installedSource: this.#installedSource }),
      ...(updateId === undefined ? {} : { updateId }),
      ...(journal === undefined || outcomeStatus === undefined
        ? {}
        : {
            outcome: {
              status: outcomeStatus,
              fromVersion: journal.intent.expected.version,
              targetVersion: journal.intent.staged.version,
              ...(journal.message === undefined ? {} : { reason: journal.message }),
            },
          }),
      ...(journal?.recovery === undefined
        ? {}
        : {
            recovery: {
              method: journal.recovery.method === "clone" ? "clone" : "copy",
              bytes: journal.recovery.bytes,
              completedAt: DateTime.formatIso(
                DateTime.fromEpochSeconds(journal.recovery.completedAt),
              ),
              durationMs: Math.round(
                (journal.recovery.completedAt - journal.recovery.startedAt) * 1000,
              ),
            },
          }),
      capability: {
        check: this.#source !== undefined && !this.#options.disabledByEnv,
        download: this.#source !== undefined && !this.#options.disabledByEnv,
        install: reason === undefined && phase !== "installing",
        ...(reason === undefined ? {} : { reason }),
      },
      ...(this.#candidate === undefined
        ? {}
        : { provenance: provenance(this.#candidate, this.#candidateBuildStartedAt) }),
      ...(this.#staged === undefined ? {} : { stagedHandle: this.#staged.handle }),
      ...(this.#bootstrap === undefined
        ? {}
        : {
            environmentId: decodeEnvironmentId(this.#bootstrap.environmentId),
            currentVersion: this.#state.currentVersion,
            ...(this.#currentBuildStartedAt === undefined
              ? {}
              : { currentBuildStartedAt: this.#currentBuildStartedAt }),
          }),
      ...(message === undefined ? {} : { message }),
      ...(checkedAt === null ? {} : { checkedAt }),
    };
    this.#state = {
      ...this.#state,
      jones,
      status,
      checkedAt,
      enabled: jones.capability.check,
      availableVersion: this.#staged?.version ?? this.#candidate?.version ?? null,
      downloadedVersion: this.#staged?.version ?? null,
      downloadPercent: this.#staged === undefined ? null : 100,
      message: message ?? null,
      canRetry: phase === "error" || phase === "blocked" || phase === "staged",
    };
    await this.#options.onState(this.#state);
  }

  async #adopt(
    metadata: typeof BuildMetadata.Type,
    previous?: ActiveInstall,
  ): Promise<ActiveInstall> {
    if (
      metadata.jonesSource === undefined ||
      this.#options.profile === undefined ||
      this.#options.disabledByEnv
    )
      throw new Error("Source-qualified app and native profile are required for setup.");
    const transactions = await NodeFSP.readdir(
      NodePath.join(this.updaterRoot, "transactions"),
    ).catch((cause: NodeJS.ErrnoException) => {
      if (cause.code === "ENOENT") return [];
      throw cause;
    });
    if (transactions.length > 0 && previous === undefined)
      throw new Error("Retained native transactions require reconciliation before app setup.");
    const appPath = await requireStableMacAppPath(this.#options.appPath);
    const bundle = await readMacBundleIdentity(appPath);
    const executablePath = await NodeFSP.realpath(this.#options.executablePath);
    if (bundle.version !== this.#state.currentVersion || executablePath !== bundle.executablePath)
      throw new Error("The running executable does not match its app bundle.");
    const home = await NodeFSP.realpath(this.#options.home);
    const databasePath = await NodeFSP.realpath(NodePath.join(home, "userdata", "statev2.sqlite"));
    const environmentId = decodeEnvironmentId(
      (await NodeFSP.readFile(NodePath.join(home, "userdata", "environment-id"), "utf8")).trim(),
    );
    const generation = NodeCrypto.randomBytes(32).toString("hex");
    const active: ActiveInstall = {
      protocol: 1,
      owner: "desktop",
      generation,
      transactionId: "bootstrap",
      home,
      databasePath,
      profile: await NodeFSP.realpath(this.#options.profile),
      environmentId,
      appPath,
      executablePath,
      version: bundle.version,
      sourceSha: metadata.jonesSource.sha,
      sourceTree: metadata.jonesSource.tree,
      bundleIdentifier: bundle.bundleIdentifier,
    };
    await NodeFSP.mkdir(NodePath.dirname(this.manifestPath), { recursive: true, mode: 0o700 });
    if (previous !== undefined) return this.#reconcileRunning(active, previous);
    try {
      await writeJonesNativeFile(this.manifestPath, JSON.stringify(active) + "\n", 0o600);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
    }
    return decodeActiveInstall(JSON.parse(await NodeFSP.readFile(this.manifestPath, "utf8")));
  }

  async #reconcileRunning(observed: ActiveInstall, expected = observed): Promise<ActiveInstall> {
    const raw = await NodeFSP.readFile(this.manifestPath, "utf8");
    if (JSON.stringify(decodeActiveInstall(JSON.parse(raw))) !== JSON.stringify(expected))
      throw new Error("The app binding changed during recovery. Restart Jones Code to retry.");
    const request =
      JSON.stringify({
        manifestSha256: NodeCrypto.createHash("sha256").update(raw).digest("hex"),
        observed,
      }) + "\n";
    const digest = NodeCrypto.createHash("sha256").update(request).digest("hex");
    await NodeFSP.mkdir(this.updaterRoot, { recursive: true, mode: 0o700 });
    const requestPath = NodePath.join(this.updaterRoot, `running-${digest}.json`);
    try {
      await writeJonesNativeFile(requestPath, request, 0o600);
    } catch (cause) {
      if (
        (cause as NodeJS.ErrnoException).code !== "EEXIST" ||
        (await NodeFSP.readFile(requestPath, "utf8")) !== request
      )
        throw cause;
    }
    const result = Schema.decodeUnknownSync(
      Schema.Struct({
        protocol: Schema.Literal(1),
        status: Schema.Literals(["reconciled", "refused"]),
        active: Schema.optionalKey(JonesActiveInstall),
        reason: Schema.optionalKey(Schema.String),
      }),
    )(
      JSON.parse(
        await runNativeCommand("/usr/bin/python3", [
          await this.#helperPath(),
          "--manifest",
          this.manifestPath,
          "--reconcile-running",
          requestPath,
          "--plan-sha256",
          digest,
        ]),
      ),
    );
    if (result.status !== "reconciled" || result.active === undefined) {
      const message =
        result.reason === "busy"
          ? "An update helper is still running. Let it finish, then restart Jones Code to retry."
          : result.reason === "restore-incomplete"
            ? "A state restore is incomplete. Finish the retained update recovery before retrying Install."
            : result.reason === "preparation-incomplete"
              ? "Update preparation is incomplete. Restart Jones Code to finish preparation before retrying Install."
              : result.reason === "activation-incomplete"
                ? "The interrupted update did not finish committing. Complete its retained recovery before retrying Install."
                : "The running app could not resolve the retained update. Reinstall the current Jones Code app, then restart to retry.";
      throw new Error(message);
    }
    const active = decodeActiveInstall(
      JSON.parse(await NodeFSP.readFile(this.manifestPath, "utf8")),
    );
    if (JSON.stringify(active) !== JSON.stringify(result.active))
      throw new Error("The app binding changed during recovery. Restart Jones Code to retry.");
    return active;
  }

  async configure(): Promise<void> {
    this.#selectedAttempt = undefined;
    const metadata = decodeBuildMetadata(
      JSON.parse(
        await NodeFSP.readFile(NodePath.join(this.#options.appRoot, "package.json"), "utf8"),
      ),
    );
    this.#source = metadata.jonesSource?.sha;
    this.#currentBuildStartedAt =
      metadata.version === this.#state.currentVersion
        ? readJonesBuildStartedAt(metadata)
        : undefined;
    this.#installedSource = undefined;
    this.#terminalPhase = undefined;
    this.#activationMessage = undefined;
    this.#lastJournal = undefined;
    let boundManifest = false;
    try {
      let active: ActiveInstall;
      try {
        active = decodeActiveInstall(JSON.parse(await NodeFSP.readFile(this.manifestPath, "utf8")));
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") {
          this.#activationBlocked = true;
          throw cause;
        }
        active = await this.#adopt(metadata);
      }
      boundManifest = true;
      const appPath = await requireStableMacAppPath(this.#options.appPath);
      let matches = true;
      if (active.bundleIdentifier !== undefined) {
        const bundle = await readMacBundleIdentity(appPath);
        if (
          bundle.bundleIdentifier !== active.bundleIdentifier ||
          bundle.version !== active.version ||
          bundle.executablePath !== active.executablePath
        )
          matches = false;
      }
      if (
        active.home !== (await NodeFSP.realpath(this.#options.home)) ||
        active.appPath !== (await NodeFSP.realpath(this.#options.appPath)) ||
        active.executablePath !== (await NodeFSP.realpath(this.#options.executablePath)) ||
        this.#options.profile === undefined ||
        active.profile !== (await NodeFSP.realpath(this.#options.profile)) ||
        (this.#options.activeGeneration !== undefined &&
          this.#options.activeGeneration !== active.generation) ||
        active.version !== this.#state.currentVersion ||
        (metadata.jonesSource?.sha ?? metadata.t3codeCommitHash) !== active.sourceSha ||
        (metadata.jonesSource !== undefined && metadata.jonesSource.tree !== active.sourceTree)
      )
        matches = false;
      if (!matches) active = await this.#adopt(metadata, active);
      this.#bootstrap = active;
      this.#source = active.sourceSha;
      if (
        metadata.jonesSource !== undefined &&
        /^[a-f0-9]{40}$/.test(active.sourceSha) &&
        /^[a-f0-9]{40}$/.test(active.sourceTree)
      )
        this.#installedSource = {
          repository: "Jones-Systems/Jones-Code",
          sha: active.sourceSha,
          tree: active.sourceTree,
        };
      this.#activationBlocked = false;
      const transactions = NodePath.join(this.updaterRoot, "transactions");
      const entries = await NodeFSP.readdir(transactions).catch((cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT") return [];
        throw cause;
      });
      // Withdraw protocol-3 preparations under the activation lock before retirement.
      // Older claims still require the helper's original no-dispatch proof.
      const preparationHolds = new Map<string, string>();
      const released = new Set<string>();
      for (const entry of entries) {
        if (!/^[a-f0-9]{64}$/.test(entry)) continue;
        const names = await NodeFSP.readdir(NodePath.join(transactions, entry)).catch(
          (): string[] => [],
        );
        if (
          !names.includes("prepare-intent.json") ||
          names.includes("intent.json") ||
          names.includes("journal.json")
        )
          continue;
        const recovery = await this.#retirePreparation(entry);
        if (recovery.retired) released.add(entry);
        else preparationHolds.set(entry, recovery.reason);
      }
      const retained = entries.filter((entry) => !released.has(entry));
      for (const entry of retained) {
        const directory = NodePath.join(transactions, entry);
        let phase: unknown;
        try {
          const journal = decodeActivationJournal(
            JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "journal.json"), "utf8")),
          );
          phase = journal.phase;
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
          try {
            requireWarmJonesInstallIntent(
              JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "intent.json"), "utf8")),
            );
          } catch (intentError) {
            if ((intentError as NodeJS.ErrnoException).code === "ENOENT") continue;
            throw intentError;
          }
        }
        if (phase !== "resumed" && phase !== "rolled-back") {
          active = await this.#reconcileRunning(active);
          this.#bootstrap = active;
          break;
        }
      }
      let latestOutcomeTime = -Infinity;
      for (const entry of retained) {
        if (!/^[a-f0-9]{64}$/.test(entry))
          throw new Error("Unknown native transaction requires reconciliation.");
        const directory = NodePath.join(transactions, entry);
        let journal: typeof ActivationJournal.Type;
        try {
          journal = decodeActivationJournal(
            JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "journal.json"), "utf8")),
          );
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code === "ENOENT") {
            try {
              await NodeFSP.lstat(NodePath.join(directory, "intent.json"));
              this.#activationBlocked = true;
              throw new Error("Native activation has unknown effects.", { cause });
            } catch (intentError) {
              if ((intentError as NodeJS.ErrnoException).code !== "ENOENT") throw intentError;
            }
            try {
              await NodeFSP.lstat(NodePath.join(directory, "prepare-intent.json"));
              this.#activationBlocked = true;
              const failure = await this.#preparationFailure(directory);
              throw new Error(
                `Update preparation ${entry} was not activated${failure === undefined ? "" : ` (${failure})`} and could not be proved safe to retry (${preparationHolds.get(entry) ?? "unknown-state"}). Quit Jones Code, then run the native helper's --retire-preparation recovery.`,
                { cause },
              );
            } catch (preparationError) {
              if ((preparationError as NodeJS.ErrnoException).code !== "ENOENT")
                throw preparationError;
            }
            continue;
          }
          throw cause;
        }
        if (
          journal.intent.transactionId !== entry ||
          (journal.phase !== "resumed" && journal.phase !== "rolled-back")
        ) {
          this.#activationBlocked = true;
          this.#lastJournal = journal;
          throw new Error(journal.message ?? "Native activation requires reconciliation.");
        }
        const committed =
          journal.phase === "resumed" &&
          journal.intent.transactionId === active.transactionId &&
          journal.intent.staged.sourceSha === active.sourceSha &&
          journal.intent.staged.appDigest === active.appDigest;
        const rolledBack =
          journal.phase === "rolled-back" &&
          JSON.stringify(journal.intent.expected) === JSON.stringify(active);
        if (committed || rolledBack) {
          const outcomeTime = (await NodeFSP.stat(NodePath.join(directory, "journal.json")))
            .mtimeMs;
          if (outcomeTime >= latestOutcomeTime) {
            latestOutcomeTime = outcomeTime;
            this.#lastJournal = journal;
            this.#terminalPhase = committed ? "committed" : "rolled-back";
            this.#activationMessage =
              journal.message ??
              (committed
                ? `Update ${entry} committed; Jones Code ${active.version} is active.`
                : `Update ${entry} rolled back; Jones Code ${active.version} remains active.`);
          }
        }
      }
      if (this.#lastJournal !== undefined && this.#terminalPhase !== undefined) {
        await this.#options.fleet?.recordOutcome({
          transactionId: this.#lastJournal.intent.transactionId,
          status: this.#terminalPhase,
          activeGeneration: active.generation,
          activeSource: active.sourceSha,
        });
      }
      if (!(await this.#retireSuccessfulTransactions(active, retained))) {
        this.#activationMessage = [
          this.#activationMessage,
          "Older recovery data remains retained; cleanup is waiting for a safe recovery boundary.",
        ]
          .filter(Boolean)
          .join(" ");
      }
    } catch (cause) {
      if (boundManifest) this.#activationBlocked = true;
      this.#activationMessage =
        cause instanceof Error ? cause.message : "Native app setup could not be proved.";
      this.#bootstrap = undefined;
      this.#terminalPhase = undefined;
      if (this.#lastJournal?.phase === "resumed" || this.#lastJournal?.phase === "rolled-back")
        this.#lastJournal = undefined;
    }
    try {
      await this.#hydrateStage();
    } catch {
      this.#activationBlocked = true;
      await this.#publish(
        "blocked",
        "error",
        "The retained native stage could not be qualified; its files were preserved.",
      );
      return;
    }
    await this.#publish(
      this.#source === undefined || this.#options.disabledByEnv || this.#activationBlocked
        ? "blocked"
        : (this.#terminalPhase ?? (this.#staged === undefined ? "no-new" : "staged")),
      this.#source === undefined || this.#options.disabledByEnv
        ? "disabled"
        : this.#activationBlocked
          ? "error"
          : this.#terminalPhase === undefined
            ? this.#staged === undefined
              ? "idle"
              : "downloaded"
            : "up-to-date",
      this.#source === undefined
        ? "This preview requires a source-qualified Jones launcher bootstrap."
        : this.#options.disabledByEnv
          ? "The native launcher disables updates; Jones bootstrap must enable them."
          : this.#activationBlocked
            ? (this.#activationMessage ??
              "Retained native activation or preparation requires reconciliation.")
            : this.#bootstrap === undefined
              ? (this.#activationMessage ??
                "Move Jones Code to a stable writable Applications folder to enable Restart.")
              : this.#activationMessage,
    );
  }

  async check(): Promise<boolean> {
    if (this.#busy !== null || this.#source === undefined || this.#options.disabledByEnv)
      return false;
    // Reload a missing stable-path binding after an independently completed native transaction.
    // The production startup owner holds readiness until its exact commit grant.
    if (this.#bootstrap === undefined) await this.configure();
    this.#busy = "check";
    try {
      await this.#publish(
        this.#staged === undefined ? "checking" : "staged",
        this.#staged === undefined ? "checking" : "downloaded",
      );
      const result = await this.#client.check({
        installedSource: this.#source,
        platform: "darwin",
        architecture: this.#options.architecture,
      });
      if (result.state === "available") {
        this.#latest = result.candidate;
        this.#latestBuildStartedAt = result.buildStartedAt;
        if (this.#staged === undefined) {
          this.#candidate = result.candidate;
          this.#candidateBuildStartedAt = result.buildStartedAt;
        }
        await this.#publish(
          this.#staged === undefined ? "available" : "staged",
          this.#staged === undefined ? "available" : "downloaded",
        );
      } else if (result.state === "blocked")
        await this.#publish(
          this.#staged === undefined ? "blocked" : "staged",
          this.#staged === undefined ? "error" : "downloaded",
          `Jones main check blocked: ${result.reason}.`,
        );
      else {
        if (result.state === "no-new") {
          this.#latest = undefined;
          this.#latestBuildStartedAt = undefined;
        }
        await this.#publish(
          this.#staged === undefined
            ? result.state === "no-new"
              ? (this.#terminalPhase ?? "no-new")
              : result.state
            : "staged",
          this.#staged === undefined
            ? result.state === "no-new"
              ? "up-to-date"
              : "idle"
            : "downloaded",
          result.state === "building"
            ? "The latest main build is still being qualified."
            : undefined,
        );
      }
      return true;
    } catch {
      await this.#publish(
        this.#staged === undefined ? "error" : "staged",
        this.#staged === undefined ? "error" : "downloaded",
        "Jones main check could not complete.",
      );
      return true;
    } finally {
      this.#busy = null;
    }
  }

  async download(selection?: JonesDesktopDownloadSelection): Promise<JonesDesktopDownloadResult> {
    if (
      selection !== undefined &&
      (this.#candidate === undefined ||
        this.#candidate.artifactId !== selection.artifactId ||
        this.#candidate.source !== selection.sourceSha)
    ) {
      return { accepted: false, completed: false, refusal: "selection-mismatch" };
    }
    if (this.#busy === null && this.#staged !== undefined && !this.#options.disabledByEnv) {
      return { accepted: true, completed: true };
    }
    const candidate = this.#candidate;
    if (
      this.#busy !== null ||
      candidate === undefined ||
      this.#options.disabledByEnv ||
      this.#staged !== undefined
    )
      return { accepted: false, completed: false };
    this.#busy = "download";
    this.#selectedAttempt = undefined;
    try {
      await NodeFSP.mkdir(NodePath.join(this.updaterRoot, "apps"), {
        recursive: true,
        mode: 0o700,
      });
      await this.#publish("downloading", "downloading");
      const roots = {
        artifacts: NodePath.join(this.updaterRoot, "artifacts"),
        apps: NodePath.join(this.updaterRoot, "apps"),
      };
      const onVerifying = () => this.#publish("verifying", "downloading");
      const prepared = await (this.#options.prepareStage === undefined
        ? prepareJonesStage(candidate, roots, this.#options.platform, {
            actions: this.#client,
            stageApp: this.#options.stageApp ?? stageJonesMacApp,
            onVerifying,
          })
        : this.#options.prepareStage(candidate, roots, onVerifying));
      const artifact = prepared.artifact;
      this.#staged = await this.#bindPreparedStage(candidate, prepared);
      const selectionFile = await this.#selectionFile();
      await NodeFSP.mkdir(NodePath.dirname(selectionFile), { recursive: true, mode: 0o700 });
      const selection = {
        ...(await this.#selectionBinding()),
        artifactDirectory: NodePath.dirname(artifact.payloadPath),
        app: this.#staged,
      };
      try {
        await writeJonesNativeFile(selectionFile, JSON.stringify(selection) + "\n", 0o600);
      } catch (cause) {
        if (
          (cause as NodeJS.ErrnoException).code !== "EEXIST" ||
          JSON.stringify(
            decodeStageSelection(JSON.parse(await NodeFSP.readFile(selectionFile, "utf8"))),
          ) !== JSON.stringify(selection)
        )
          throw cause;
      }
      await this.#publish("staged", "downloaded");
      return { accepted: true, completed: true };
    } catch (cause) {
      this.#staged = undefined;
      await this.#publish(
        "error",
        "available",
        `Download failed (${downloadFailureCategory(cause)}). Check for builds and retry; the active app is still running.`,
      );
      return { accepted: true, completed: false };
    } finally {
      this.#busy = null;
    }
  }

  async #helperPath(): Promise<string> {
    const helperHash = NodeCrypto.createHash("sha256")
      .update(jonesNativeHelperSource)
      .digest("hex");
    const helper = NodePath.join(this.updaterRoot, `activate-v1-${helperHash}.py`);
    try {
      await writeJonesNativeFile(helper, jonesNativeHelperSource, 0o700);
    } catch (cause) {
      if (
        (cause as NodeJS.ErrnoException).code !== "EEXIST" ||
        (await NodeFSP.readFile(helper, "utf8")) !== jonesNativeHelperSource
      )
        throw cause;
    }
    return helper;
  }

  /** Keep current, recovery-parent and failure journals; native ownership guards decide every deletion. */
  async #retireSuccessfulTransactions(
    active: ActiveInstall,
    entries: ReadonlyArray<string>,
  ): Promise<boolean> {
    let complete = true;
    for (const transactionId of entries) {
      if (transactionId === active.transactionId || transactionId === active.generation) continue;
      const path = NodePath.join(this.updaterRoot, "transactions", transactionId, "journal.json");
      try {
        const info = await NodeFSP.lstat(path);
        if (!info.isFile() || info.isSymbolicLink() || info.size > 1024 * 1024) continue;
        const raw = await NodeFSP.readFile(path, "utf8");
        const journal = decodeActivationJournal(JSON.parse(raw));
        if (journal.phase !== "resumed" || journal.intent.transactionId !== transactionId) continue;
        const helper = await this.#helperPath();
        const run = async (
          operation: "inspect-retirement" | "retire-transaction",
          plan?: string,
        ) => {
          const result = decodeNativeRetirementResult(
            JSON.parse(
              await runNativeCommand("/usr/bin/python3", [
                helper,
                "--manifest",
                this.manifestPath,
                `--${operation}`,
                transactionId,
                ...(plan === undefined ? [] : ["--plan-sha256", plan]),
              ]),
            ),
          );
          if (result.operation !== operation || result.transactionId !== transactionId)
            throw new Error("Native retirement identity changed.");
          return result;
        };
        const inspection = await run("inspect-retirement");
        if (
          inspection.status === "retired" ||
          (inspection.status === "refused" && inspection.reason === "protected-recovery-parent")
        )
          continue;
        if (
          inspection.status !== "ready" ||
          inspection.planSha256 === undefined ||
          !NodeUtil.isDeepStrictEqual(inspection.activeBinding, active) ||
          inspection.journalSha256 !== NodeCrypto.createHash("sha256").update(raw).digest("hex") ||
          inspection.targets === undefined
        ) {
          complete = false;
          continue;
        }
        if (inspection.targets.length === 0 && inspection.reconciliation !== true) continue;
        const result = await run("retire-transaction", inspection.planSha256);
        if (result.status !== "retired" || result.planSha256 !== inspection.planSha256)
          complete = false;
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") complete = false;
      }
    }
    return complete;
  }

  /** Retires only a proven withdrawal or a legacy claim with no possible dispatch. */
  async #retirePreparation(transactionId: string): Promise<{ retired: boolean; reason: string }> {
    try {
      const helper = await this.#helperPath();
      const withdrawal = await this.#withdrawPreparation(transactionId, helper);
      if (withdrawal !== undefined && !withdrawal.withdrawn)
        return { retired: false, reason: withdrawal.reason };
      const run = async (
        operation: "inspect-preparation" | "retire-preparation",
        plan?: string,
      ) => {
        const result = decodeNativePreparationResult(
          JSON.parse(
            await runNativeCommand("/usr/bin/python3", [
              helper,
              "--manifest",
              this.manifestPath,
              `--${operation}`,
              transactionId,
              ...(plan === undefined ? [] : ["--plan-sha256", plan]),
            ]),
          ),
        );
        if (result.operation !== operation || result.transactionId !== transactionId)
          throw new Error("Native preparation recovery identity changed.");
        return result;
      };
      const inspected = await run("inspect-preparation");
      if (inspected.status === "retired") return { retired: true, reason: "retired" };
      if (inspected.status !== "ready" || inspected.planSha256 === undefined)
        return { retired: false, reason: inspected.reason ?? inspected.status };
      const retired = await run("retire-preparation", inspected.planSha256);
      return { retired: retired.status === "retired", reason: retired.reason ?? retired.status };
    } catch {
      return { retired: false, reason: "helper-unavailable" };
    }
  }

  async #withdrawPreparation(
    transactionId: string,
    helper: string,
  ): Promise<{ withdrawn: boolean; reason: string } | undefined> {
    const directory = NodePath.join(this.updaterRoot, "transactions", transactionId);
    const read = async (path: string) => {
      const info = await NodeFSP.lstat(path);
      if (!info.isFile() || info.isSymbolicLink() || info.size > 1024 * 1024)
        throw new Error("Native preparation proof is not a bounded regular file.");
      const raw = await NodeFSP.readFile(path, "utf8");
      return { raw, value: JSON.parse(raw) as Record<string, unknown> };
    };
    const claim = await read(NodePath.join(directory, "prepare-intent.json")).catch(
      () => undefined,
    );
    // A legacy or unreadable claim still goes through the native retirement guard.
    if (claim?.value?.preparationClaimProtocol !== 3) return undefined;
    const value = claim.value;
    if (
      value.transactionId !== transactionId ||
      typeof value.stagedHandle !== "string" ||
      !/^[a-f0-9]{64}$/.test(value.stagedHandle) ||
      typeof value.selectionPath !== "string" ||
      NodePath.dirname(value.selectionPath) !== NodePath.join(this.updaterRoot, "staging") ||
      typeof value.selectionSha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(value.selectionSha256)
    )
      return { withdrawn: false, reason: "withdrawal-binding-invalid" };
    const claimSha256 = NodeCrypto.createHash("sha256").update(claim.raw).digest("hex");
    const result = decodeNativeWithdrawalResult(
      JSON.parse(
        await runNativeCommand("/usr/bin/python3", [
          helper,
          "--manifest",
          this.manifestPath,
          "--withdraw-preparation",
          transactionId,
          "--staged-handle",
          value.stagedHandle,
          "--selection",
          value.selectionPath,
          "--selection-sha256",
          value.selectionSha256,
          "--claim-sha256",
          claimSha256,
        ]),
      ),
    );
    if (result.transactionId !== transactionId)
      return { withdrawn: false, reason: "withdrawal-binding-invalid" };
    if (result.status !== "withdrawn")
      return { withdrawn: false, reason: result.reason ?? result.status };
    const proofPath = NodePath.join(directory, "preparation-withdrawal.json");
    if (result.claimSha256 !== claimSha256 || result.proofPath !== proofPath)
      return { withdrawn: false, reason: "withdrawal-binding-invalid" };
    const proof = await read(proofPath);
    if (
      NodeCrypto.createHash("sha256").update(proof.raw).digest("hex") !== result.proofSha256 ||
      !NodeUtil.isDeepStrictEqual(proof.value, {
        protocol: 1,
        withdrawalProtocol: 1,
        transactionId,
        claimSha256,
        claim: value,
        expected: this.#bootstrap,
        evidence: "withdrawal-protocol",
      })
    )
      return { withdrawn: false, reason: "withdrawal-proof-invalid" };
    return { withdrawn: true, reason: "withdrawn" };
  }

  async #preparationFailure(directory: string): Promise<string | undefined> {
    try {
      const raw = await NodeFSP.readFile(NodePath.join(directory, "prepare-failure.json"), "utf8");
      if (raw.length > 4096) return undefined;
      return jonesPreparationText(decodePreparationFailure(JSON.parse(raw)).reason);
    } catch {
      return undefined;
    }
  }

  async discard(handle: string): Promise<{ accepted: boolean; completed: boolean }> {
    if (
      this.#busy !== null ||
      this.#staged?.handle !== handle ||
      this.#activationBlocked ||
      this.#options.disabledByEnv ||
      this.#bootstrap === undefined
    ) {
      return { accepted: false, completed: false };
    }
    this.#busy = "discard";
    const dispatch = { dispatched: false };
    try {
      if ((await this.#runNativeDiscard(handle, dispatch)) === "refused") {
        await this.#publish(
          "blocked",
          "downloaded",
          "The retained stage cannot be discarded while native work needs reconciliation.",
        );
        return { accepted: false, completed: false };
      }
      this.#staged = undefined;
      this.#candidate = undefined;
      this.#candidateBuildStartedAt = undefined;
      this.#selectedAttempt = undefined;
      this.#candidateStartupGateUnavailable = false;
      await this.#publish(
        "no-new",
        "idle",
        "Downloaded selection discarded. Check for builds to select another update.",
      );
      return { accepted: true, completed: true };
    } catch {
      if (dispatch.dispatched) this.#activationBlocked = true;
      await this.#publish(
        "blocked",
        "downloaded",
        dispatch.dispatched
          ? "The discard outcome needs reconciliation; the retained stage remains held."
          : "The downloaded selection could not be verified; its files were preserved.",
      );
      return { accepted: false, completed: false };
    } finally {
      this.#busy = null;
    }
  }

  /** The helper's validated discard. The caller owns the busy latch and what it publishes. */
  async #runNativeDiscard(
    handle: string,
    dispatch: { dispatched: boolean },
  ): Promise<"discarded" | "refused"> {
    const selection = await this.#selectionFile();
    const raw = await NodeFSP.readFile(selection, "utf8");
    if (decodeStageSelection(JSON.parse(raw)).app.handle !== handle)
      throw new Error("Selection changed.");
    const helper = await this.#helperPath();
    dispatch.dispatched = true;
    const result = decodeNativeSelectionResult(
      JSON.parse(
        await runNativeCommand("/usr/bin/python3", [
          helper,
          "--manifest",
          this.manifestPath,
          "--discard-staged",
          handle,
          "--selection",
          selection,
          "--selection-sha256",
          NodeCrypto.createHash("sha256").update(raw).digest("hex"),
        ]),
      ),
    );
    if (
      result.operation !== "discard-staged" ||
      result.handle !== handle ||
      result.status === "uncertain"
    )
      throw new Error("Native discard outcome is uncertain.");
    if (result.status === "discarded") return "discarded";
    if (result.status !== "refused") throw new Error("Unexpected native discard outcome.");
    return "refused";
  }

  /**
   * Replaces an unaccepted stage with the newest checked build under one latch hold: native
   * discard, removal of the replaced payload, then download. A refused discard keeps the stage;
   * an uncertain one blocks activation exactly like a manual discard.
   */
  async supersede(
    handle: string,
    next: JonesActionsCandidate,
    reclaim?: (handle: string) => Promise<void>,
  ): Promise<JonesSupersedeResult> {
    const staged = this.#staged;
    if (
      this.#busy !== null ||
      staged?.handle !== handle ||
      next !== this.#latest ||
      next.source === staged.sourceSha ||
      this.#activationBlocked ||
      this.#options.disabledByEnv ||
      this.#bootstrap === undefined
    )
      return "refused";
    const nextBuildStartedAt = this.#latestBuildStartedAt;
    this.#busy = "discard";
    const dispatch = { dispatched: false };
    let discarded: "discarded" | "refused" | "failed";
    try {
      discarded = await this.#runNativeDiscard(handle, dispatch);
    } catch {
      discarded = "failed";
    }
    if (discarded !== "discarded") {
      const uncertain = discarded === "failed" && dispatch.dispatched;
      if (uncertain) this.#activationBlocked = true;
      this.#busy = null;
      await this.#publish(
        "blocked",
        "downloaded",
        uncertain
          ? "The discard outcome needs reconciliation; the retained stage remains held."
          : discarded === "refused"
            ? "The retained stage cannot be discarded while native work needs reconciliation."
            : "The downloaded selection could not be verified; its files were preserved.",
      );
      return uncertain ? "uncertain" : "refused";
    }
    this.#staged = undefined;
    this.#candidate = next;
    this.#candidateBuildStartedAt = nextBuildStartedAt;
    this.#selectedAttempt = undefined;
    this.#candidateStartupGateUnavailable = false;
    if (reclaim !== undefined) {
      this.#busy = "reclaim";
      await this.#publish(
        "available",
        "available",
        `Replacing ${staged.version} with ${next.version}.`,
      );
      try {
        await reclaim(handle);
      } catch {
        /* The caller keeps the replaced handle and retries its removal later. */
      }
    }
    this.#busy = null;
    return (await this.download()).completed ? "replaced" : "download-failed";
  }

  /** Runs payload removal under the busy latch, so no download can select the same handle. */
  async reclaimExclusive(reclaim: () => Promise<void>): Promise<boolean> {
    if (this.#busy !== null) return false;
    this.#busy = "reclaim";
    try {
      await reclaim();
      return true;
    } finally {
      this.#busy = null;
    }
  }

  /** Background preparation status rides along with every later publication. */
  async setPreparationStatus(status: JonesDesktopPreparationState | undefined): Promise<void> {
    if (JSON.stringify(this.#state.jonesPreparation) === JSON.stringify(status)) return;
    const { jonesPreparation: _previous, ...rest } = this.#state;
    this.#state = status === undefined ? rest : { ...rest, jonesPreparation: status };
    await this.#options.onState(this.#state);
  }

  /**
   * Binds a prepared stage to its immutable on-disk receipts before selection, so a reply from
   * the preparation process is never trusted beyond what those receipts already prove.
   */
  async #bindPreparedStage(
    candidate: JonesActionsCandidate,
    prepared: JonesPreparedStage,
  ): Promise<StagedMacApp> {
    const app = decodeStagedApp(prepared.app);
    const { artifact } = prepared;
    if (
      JSON.stringify(artifact.candidate) !== JSON.stringify(candidate) ||
      artifact.stagedHandle !== app.handle ||
      app.sourceSha !== candidate.source ||
      app.sourceTree !== candidate.tree ||
      app.version !== candidate.version
    )
      throw new Error("The prepared stage does not bind the selected candidate.");
    await requireJonesStageDirectory(
      NodePath.join(this.updaterRoot, "artifacts"),
      app.handle,
      NodePath.dirname(artifact.payloadPath),
    );
    const appDirectory = await requireJonesStageDirectory(
      await NodeFSP.realpath(NodePath.join(this.updaterRoot, "apps")),
      app.handle,
      NodePath.dirname(app.appPath),
    );
    if (app.receiptPath !== NodePath.join(appDirectory, "mac-app-receipt.json"))
      throw new Error("The prepared app receipt is outside its stage.");
    const info = await NodeFSP.lstat(app.receiptPath);
    if (!info.isFile() || info.size > 131072) throw new Error("Unknown native app receipt.");
    const receipt = decodeNativeStageReceipt(
      JSON.parse(await NodeFSP.readFile(app.receiptPath, "utf8")),
    );
    if (
      JSON.stringify(receipt.app) !== JSON.stringify(app) ||
      JSON.stringify(receipt.artifact) !== JSON.stringify(artifact) ||
      JSON.stringify(receipt.candidate) !== JSON.stringify(candidate)
    )
      throw new Error("The prepared app receipt does not bind this stage.");
    return app;
  }

  async install(
    handle?: string,
    campaignId?: string,
  ): Promise<{
    accepted: boolean;
    completed: boolean;
    failed: boolean;
    refusal?: "startup-gate-unavailable";
  }> {
    const staged = this.#staged;
    const expected = this.#bootstrap;
    if (
      this.#busy !== null ||
      staged === undefined ||
      handle !== staged.handle ||
      expected === undefined ||
      this.#options.disabledByEnv ||
      this.#activationBlocked
    ) {
      return { accepted: false, completed: false, failed: false };
    }
    this.#busy = "install";
    this.#selectedAttempt = undefined;
    try {
      // The helper checks the fixed candidate digest before stopping writers; preparation binds this active manifest.
      const current = decodeActiveInstall(
        JSON.parse(await NodeFSP.readFile(this.manifestPath, "utf8")),
      );
      if (JSON.stringify(current) !== JSON.stringify(expected))
        throw new Error("Active install changed.");
      decodeStagedApp(staged);
      await preflightJonesCandidateStartupGate(staged);
      const helper = await this.#helperPath();
      const selectionPath = await this.#selectionFile();
      const selectionRaw = await NodeFSP.readFile(selectionPath, "utf8");
      if (decodeStageSelection(JSON.parse(selectionRaw)).app.handle !== handle)
        throw new Error("The selected native stage changed.");
      await runNativeCommand("/usr/bin/python3", ["--version"]);
      const selectionSha256 = NodeCrypto.createHash("sha256").update(selectionRaw).digest("hex");
      const transactionId = await this.#selectAttempt(
        handle,
        selectionPath,
        selectionSha256,
        expected,
      );
      this.#selectedAttempt = { stagedHandle: handle, transactionId };
      const tx = NodePath.dirname(jonesContinuationReceiptPath(this.#options.home, transactionId));
      await NodeFSP.mkdir(tx, { recursive: true, mode: 0o700 });
      const intentPath = NodePath.join(tx, "intent.json");
      try {
        await NodeFSP.lstat(intentPath);
        throw new Error("An occupied activation intent requires reconciliation.");
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
      }
      // Every process that can carry this claim's one backend request, captured before the claim.
      const requesters = await this.#options.processProofs();
      await this.#publish("preparing", "downloaded");
      // Claim preparation under the same native lock as discard before the backend can create effects.
      this.#activationBlocked = true;
      const preparation = decodeNativeSelectionResult(
        JSON.parse(
          await runNativeCommand("/usr/bin/python3", [
            helper,
            "--manifest",
            this.manifestPath,
            "--claim-preparation",
            handle,
            "--transaction",
            transactionId,
            "--selection",
            selectionPath,
            "--selection-sha256",
            NodeCrypto.createHash("sha256").update(selectionRaw).digest("hex"),
            "--withdrawable",
          ]),
        ),
      );
      if (
        preparation.operation !== "claim-preparation" ||
        preparation.handle !== handle ||
        preparation.transactionId !== transactionId
      )
        throw new Error("Native preparation claim identity changed.");
      if (preparation.status === "refused" && preparation.reason === "selection-mismatch") {
        this.#activationBlocked = false;
        this.#selectedAttempt = undefined;
      }
      if (preparation.status !== "preparation-claimed")
        throw new Error("Native preparation claim was not confirmed.");
      // A missing preparation receipt is a real capability blocker; never manufacture it.
      const continuationReceipt = jonesContinuationReceiptPath(this.#options.home, transactionId);
      let continuation: Record<string, unknown>;
      try {
        const claimRaw = await NodeFSP.readFile(NodePath.join(tx, "prepare-intent.json"));
        await writeJonesNativeFile(
          NodePath.join(tx, "prepare-request.json"),
          JSON.stringify({
            protocol: 1,
            transactionId,
            stagedHandle: handle,
            claimSha256: NodeCrypto.createHash("sha256").update(claimRaw).digest("hex"),
            processes: requesters,
          }) + "\n",
          0o600,
        );
        const response = await this.#options.prepareNative?.(handle, transactionId, expected);
        try {
          continuation = JSON.parse(await NodeFSP.readFile(continuationReceipt, "utf8")) as Record<
            string,
            unknown
          >;
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
          throw new Error(
            response?.phase === "blocked" && response.message !== undefined
              ? response.message
              : "The backend did not record native preparation",
            { cause },
          );
        }
      } catch (cause) {
        return await this.#preparationFailed(transactionId, tx, cause);
      }
      if (
        continuation.prepared !== true ||
        continuation.preparationClaimProtocol !== 3 ||
        continuation.transactionId !== transactionId ||
        continuation.stagedHandle !== handle
      )
        return await this.#preparationFailed(
          transactionId,
          tx,
          new Error("Native continuation preparation is required."),
        );
      const intent = {
        protocol: 1,
        transactionId,
        staged,
        expected,
        continuationReceipt,
        processes: await this.#options.processProofs(),
        listener: await this.#options.listener(),
      };
      const requestPath = NodePath.join(tx, "activation-request.json");
      const requestBytes = JSON.stringify(intent) + "\n";
      try {
        await writeJonesNativeFile(requestPath, requestBytes, 0o600);
      } catch (cause) {
        if (
          (cause as NodeJS.ErrnoException).code !== "EEXIST" ||
          (await NodeFSP.readFile(requestPath, "utf8")) !== requestBytes
        )
          throw cause;
      }
      if (campaignId !== undefined) {
        if (this.#options.fleet === undefined) throw new Error("Fleet persistence is unavailable.");
        await this.#options.fleet.bindInstall({
          campaignId,
          stagedHandle: handle,
          targetSource: staged.sourceSha,
          transactionId,
          fromGeneration: expected.generation,
        });
      }
      this.#activationBlocked = true;
      const claim = decodeNativeSelectionResult(
        JSON.parse(
          await runNativeCommand("/usr/bin/python3", [
            helper,
            "--manifest",
            this.manifestPath,
            "--claim-activation",
            requestPath,
            "--staged-handle",
            handle,
            "--selection",
            selectionPath,
            "--selection-sha256",
            NodeCrypto.createHash("sha256").update(selectionRaw).digest("hex"),
          ]),
        ),
      );
      if (
        claim.operation !== "claim-activation" ||
        claim.handle !== handle ||
        claim.transactionId !== transactionId ||
        claim.status !== "claimed" ||
        claim.intentPath !== intentPath
      )
        throw new Error("Native activation claim was not confirmed.");
      // This child is detached and owns its lock/journal; it outlives this Electron process.
      await new Promise<void>((resolve, reject) => {
        const child = NodeChildProcess.spawn(
          "/usr/bin/python3",
          [helper, "--manifest", this.manifestPath, "--activate", intentPath],
          { detached: true, stdio: "ignore" },
        );
        child.once("error", () => reject(new Error("Native activation helper could not start.")));
        child.once("exit", () => {
          if (this.#busy !== "install") return;
          this.#busy = null;
          this.#activationBlocked = true;
          void (async () => {
            try {
              this.#lastJournal = decodeActivationJournal(
                JSON.parse(await NodeFSP.readFile(NodePath.join(tx, "journal.json"), "utf8")),
              );
            } catch {
              /* The transaction files remain available for reconciliation. */
            }
            await this.#publish(
              "blocked",
              "downloaded",
              this.#lastJournal?.message ??
                "The native helper exited; retained transaction state requires reconciliation.",
            );
          })();
        });
        child.once("spawn", () => {
          child.unref();
          resolve();
        });
      });
      await this.#publish(
        "installing",
        "downloaded",
        `Installing update ${transactionId}; Jones Code is restarting. The outcome appears after restart.`,
      );
      return { accepted: true, completed: false, failed: false };
    } catch (cause) {
      this.#busy = null;
      if (cause instanceof JonesCandidateStartupGateUnavailableError) {
        this.#candidateStartupGateUnavailable = true;
        await this.#publish(
          "blocked",
          "downloaded",
          "The staged candidate lacks qualified startupGateProtocol:1; its fixed selection was retained.",
        );
        return {
          accepted: false,
          completed: false,
          failed: false,
          refusal: "startup-gate-unavailable",
        };
      }
      await this.#publish(
        "blocked",
        "downloaded",
        this.#activationBlocked
          ? `Native installation needs reconciliation (${nativeFailureReason(cause)}); the retained transaction will not be retried automatically.`
          : `Native install could not prove its preparation and launcher binding (${nativeFailureReason(cause)}); the current app remains active.`,
      );
      return { accepted: false, completed: false, failed: true };
    }
  }

  /** Keeps the original cause visible and releases only a proven, inactive claim. */
  async #preparationFailed(
    transactionId: string,
    tx: string,
    cause: unknown,
  ): Promise<{ accepted: boolean; completed: boolean; failed: boolean }> {
    const reason = nativeFailureReason(cause);
    try {
      await writeJonesNativeFile(
        NodePath.join(tx, "prepare-failure.json"),
        JSON.stringify({ protocol: 1, reason }) + "\n",
        0o600,
      );
    } catch {
      /* The response below still reports the cause; an occupied record is preserved. */
    }
    const recovery = await this.#retirePreparation(transactionId);
    this.#busy = null;
    if (recovery.retired) {
      this.#activationBlocked = false;
      this.#selectedAttempt = undefined;
      await this.#publish(
        "staged",
        "downloaded",
        `Update preparation stopped safely: ${reason}. Install can be retried.`,
      );
    } else {
      await this.#publish(
        "blocked",
        "downloaded",
        `Update preparation stopped: ${reason}. Restart Jones Code to release this update attempt, then retry Install.`,
      );
    }
    return { accepted: false, completed: false, failed: true };
  }
}
