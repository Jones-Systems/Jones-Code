// @effect-diagnostics nodeBuiltinImport:off - Native updater uses ordinary-UID filesystem and process operations.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  JonesActionsClient,
  validateJonesStagedArtifact,
  requireJonesStageDirectory,
  type JonesActionsCandidate,
} from "@t3tools/shared/jones/jonesActions";
import type { DesktopUpdateState } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { EnvironmentId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import {
  JonesActiveInstall,
  JonesInstallIntent,
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

const SourceHash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const BuildMetadata = Schema.Struct({
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

const decodeBuildMetadata = Schema.decodeUnknownSync(BuildMetadata);
const decodeActiveInstall = Schema.decodeUnknownSync(JonesActiveInstall);
const decodeStagedApp = Schema.decodeUnknownSync(JonesStagedMacApp);
const decodeActivationJournal = Schema.decodeUnknownSync(ActivationJournal);
const decodeEnvironmentId = Schema.decodeUnknownSync(EnvironmentId);
const decodeStageSelection = Schema.decodeUnknownSync(StageSelection);
const decodeNativeStageReceipt = Schema.decodeUnknownSync(NativeStageReceipt);

function jonesContinuationReceiptPath(home: string, handle: string): string {
  if (!/^[a-zA-Z0-9_-]+$/.test(handle)) throw new Error("Invalid Jones staged handle.");
  return NodePath.join(
    home,
    "runtime",
    "jones-updates",
    "transactions",
    handle,
    "continuation.json",
  );
}

function provenance(candidate: JonesActionsCandidate) {
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
  } as const;
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
  readonly onState: (state: DesktopUpdateState) => Promise<void>;
  readonly prepareNative?: (handle: string, active: ActiveInstall) => Promise<void>;
  readonly processProofs: () => Promise<readonly { pid: number; identity: string }[]>;
  readonly listener: () => Promise<string>;
  readonly timestamp: () => Promise<string>;
  readonly client?: Pick<JonesActionsClient, "check" | "stage">;
  readonly stageApp?: typeof stageJonesMacApp;
  readonly validateArtifact?: typeof validateJonesStagedArtifact;
}

/** One host-owned checker. Downloads retain a fixed candidate; later checks cannot replace Install. */
export class JonesDesktopUpdateController {
  #state: DesktopUpdateState;
  #source: string | undefined;
  #candidate: JonesActionsCandidate | undefined;
  #staged: StagedMacApp | undefined;
  #busy: "check" | "download" | "install" | null = null;
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
      ...(this.#source === undefined ? {} : { installedSource: this.#source }),
      phase,
      ...((phase === "preparing" || phase === "installing") && this.#staged !== undefined
        ? { updateId: this.#staged.handle }
        : journal === undefined
          ? {}
          : { updateId: journal.intent.transactionId }),
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
      ...(this.#candidate === undefined ? {} : { provenance: provenance(this.#candidate) }),
      ...(this.#staged === undefined ? {} : { stagedHandle: this.#staged.handle }),
      ...(this.#bootstrap === undefined
        ? {}
        : {
            environmentId: decodeEnvironmentId(this.#bootstrap.environmentId),
            currentVersion: this.#state.currentVersion,
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

  async #adopt(metadata: typeof BuildMetadata.Type): Promise<ActiveInstall> {
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
    if (transactions.length > 0)
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
    try {
      await writeJonesNativeFile(this.manifestPath, JSON.stringify(active) + "\n", 0o600);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
    }
    return decodeActiveInstall(JSON.parse(await NodeFSP.readFile(this.manifestPath, "utf8")));
  }

  async configure(): Promise<void> {
    const metadata = decodeBuildMetadata(
      JSON.parse(
        await NodeFSP.readFile(NodePath.join(this.#options.appRoot, "package.json"), "utf8"),
      ),
    );
    this.#source = metadata.jonesSource?.sha;
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
      if (active.bundleIdentifier !== undefined) {
        const bundle = await readMacBundleIdentity(appPath);
        if (
          bundle.bundleIdentifier !== active.bundleIdentifier ||
          bundle.version !== active.version ||
          bundle.executablePath !== active.executablePath
        )
          throw new Error("The native manifest does not match the running bundle identity.");
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
        throw new Error("The native launcher does not bind the running app.");
      this.#bootstrap = active;
      this.#source = active.sourceSha;
      this.#activationBlocked = false;
      const transactions = NodePath.join(this.updaterRoot, "transactions");
      const entries = await NodeFSP.readdir(transactions).catch((cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT") return [];
        throw cause;
      });
      let latestOutcomeTime = -Infinity;
      for (const entry of entries) {
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
              try {
                await NodeFSP.lstat(NodePath.join(directory, "continuation.json"));
              } catch (preparationError) {
                if ((preparationError as NodeJS.ErrnoException).code === "ENOENT") {
                  this.#activationBlocked = true;
                  throw new Error("Native preparation has unknown effects.", {
                    cause: preparationError,
                  });
                }
                throw preparationError;
              }
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
        if (this.#staged === undefined) this.#candidate = result.candidate;
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
      else
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
    try {
      await NodeFSP.mkdir(NodePath.join(this.updaterRoot, "apps"), {
        recursive: true,
        mode: 0o700,
      });
      await this.#publish("downloading", "downloading");
      const artifact = await this.#client.stage(
        candidate,
        NodePath.join(this.updaterRoot, "artifacts"),
      );
      await this.#publish("verifying", "downloading");
      this.#staged = await (this.#options.stageApp ?? stageJonesMacApp)(
        artifact,
        NodePath.join(this.updaterRoot, "apps"),
        this.#options.platform,
      );
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
    } catch {
      this.#staged = undefined;
      await this.#publish(
        "error",
        "available",
        "The qualified native app could not be staged; the active app and state were left running.",
      );
      return { accepted: true, completed: false };
    } finally {
      this.#busy = null;
    }
  }

  async install(handle?: string): Promise<{
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
    try {
      // The helper checks the fixed candidate digest before stopping writers; preparation binds this active manifest.
      const current = decodeActiveInstall(
        JSON.parse(await NodeFSP.readFile(this.manifestPath, "utf8")),
      );
      if (JSON.stringify(current) !== JSON.stringify(expected))
        throw new Error("Active install changed.");
      decodeStagedApp(staged);
      await preflightJonesCandidateStartupGate(staged);
      const tx = NodePath.dirname(jonesContinuationReceiptPath(this.#options.home, handle));
      await NodeFSP.mkdir(tx, { recursive: true, mode: 0o700 });
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
      await runNativeCommand("/usr/bin/python3", ["--version"]);
      const intentPath = NodePath.join(tx, "intent.json");
      try {
        await NodeFSP.lstat(intentPath);
        throw new Error("An occupied activation intent requires reconciliation.");
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
      }
      await this.#publish("preparing", "downloaded");
      await this.#options.prepareNative?.(handle, expected);
      // A missing preparation receipt is a real capability blocker; never manufacture it.
      const continuationReceipt = jonesContinuationReceiptPath(this.#options.home, handle);
      const continuation = JSON.parse(
        await NodeFSP.readFile(continuationReceipt, "utf8"),
      ) as Record<string, unknown>;
      if (continuation.prepared !== true || continuation.transactionId !== handle)
        throw new Error("Native continuation preparation is required.");
      const intent = {
        protocol: 1,
        transactionId: handle,
        staged,
        expected,
        continuationReceipt,
        processes: await this.#options.processProofs(),
        listener: await this.#options.listener(),
      };
      await writeJonesNativeFile(intentPath, JSON.stringify(intent) + "\n", 0o600);
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
        `Installing update ${handle}; Jones Code is restarting. The outcome appears after restart.`,
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
        "Native install could not prove its preparation and launcher binding; the current app remains active.",
      );
      return { accepted: false, completed: false, failed: true };
    }
  }
}
