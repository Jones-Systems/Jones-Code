// @effect-diagnostics nodeBuiltinImport:off - Staging jobs run in the separate preparation process.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  JonesActionsClient,
  JONES_ACTIONS_RECEIPT_FILE,
  validateJonesStagedArtifact,
  type JonesActionsCandidate,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import * as Schema from "effect/Schema";

import {
  JonesActiveInstall,
  JonesInstallIntent,
  JonesStagedMacApp,
  requireWarmJonesInstallIntent,
} from "./jonesActivation.ts";
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

export type JonesStagingJob = { readonly token?: string } & (
  | {
      readonly kind: "prepare";
      readonly candidate: JonesActionsCandidate;
      readonly roots: JonesStagingRoots;
      readonly platform: NodeJS.Platform;
    }
  | { readonly kind: "validate-artifact"; readonly directory: string }
  | { readonly kind: "reclaim"; readonly updatesRoot: string; readonly handle: string }
);

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
  readonly admit?: () => Promise<void>;
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
  await hooks.admit?.();
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

// Match the controller's selection formats; unknown formats can only retain payloads.
const decodeStageReference = Schema.decodeUnknownSync(
  Schema.Struct({
    schema: Schema.Literal(1),
    source: Schema.Literal("jones-actions"),
    home: Schema.String,
    profile: Schema.NullOr(Schema.String),
    currentVersion: Schema.String,
    installedSource: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    active: Schema.NullOr(JonesActiveInstall),
    artifactDirectory: Schema.String,
    app: JonesStagedMacApp,
  }),
);
const decodeAttemptReference = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    transactionId: Schema.String.check(Schema.isPattern(HANDLE)),
    stagedHandle: Schema.String,
    selectionPath: Schema.String,
    selectionSha256: Schema.String,
    expected: JonesActiveInstall,
  }),
);

const ReferenceHash = Schema.String.check(Schema.isPattern(HANDLE));
const ReferenceSource = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const ReferenceText = Schema.NonEmptyString.check(
  Schema.makeFilter((value) => value.trim() !== ""),
);
const ReferencePath = Schema.NonEmptyString.check(Schema.makeFilter(NodePath.isAbsolute));
const ReferencePid = Schema.Int.check(Schema.isGreaterThan(0));
const LegacyProcessReference = Schema.Struct({
  pid: ReferencePid,
  identity: Schema.NonEmptyString,
});
const KernelProcessReference = Schema.Struct({
  pid: ReferencePid,
  startSec: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  startUsec: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 999999 })),
  executable: ReferencePath,
});
const ProcessReferences = Schema.Union([
  Schema.Array(LegacyProcessReference).check(Schema.isMinLength(1), Schema.isMaxLength(256)),
  Schema.Array(KernelProcessReference).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(256),
    Schema.makeFilter((proofs) => new Set(proofs.map((proof) => proof.pid)).size === proofs.length),
  ),
]);
const ActivationProcesses = Schema.Union([
  ProcessReferences,
  Schema.Array(
    Schema.Struct({ pid: ReferencePid, role: Schema.Literals(["main", "backend"]) }),
  ).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(256),
    Schema.makeFilter(
      (proofs) =>
        new Set(proofs.map((proof) => proof.pid)).size === proofs.length &&
        proofs.filter((proof) => proof.role === "main").length === 1,
    ),
  ),
]);
const RetirementEvidence = Schema.Literals([
  "backend-refused",
  "requester-exited",
  "writers-quiescent",
  "withdrawn",
]);

// This is the bounded reference shape from qualifiedRuntime.ts's two decoders.
// Keep it local: the desktop worker must not load the server implementation.
const decodeServerReference = Schema.decodeUnknownSync(
  Schema.Struct({
    protocol: Schema.Literal(1),
    stagedHandle: Schema.String.check(
      Schema.isPattern(/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/),
    ),
    binding: Schema.Struct({
      baseDir: ReferencePath,
      dbPath: ReferencePath,
      environmentId: ReferenceText,
      activeVersion: ReferenceText,
      activeSourceSha: ReferenceSource,
    }),
    receipt: Schema.Struct({
      protocol: Schema.Literal(1),
      repository: Schema.Literal("Jones-Systems/Jones-Code"),
      channel: Schema.Literal("jones-main"),
      platform: Schema.Literals(["linux", "darwin"]),
      architecture: Schema.Literals(["x64", "arm64"]),
      workflow: Schema.String,
      version: Schema.String.check(
        Schema.isPattern(
          /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-preview\.[0-9]+\.[0-9]+(?:\.[0-9]+)?$/,
        ),
      ),
      sourceSha: ReferenceSource,
      sourceTree: ReferenceSource,
      installedSourceSha: ReferenceSource,
      archiveSha256: ReferenceHash,
      payloadSha256: ReferenceHash,
      artifactDigest: Schema.String.check(Schema.isPattern(/^sha256:[a-f0-9]{64}$/)),
      runId: ReferencePid,
      runAttempt: ReferencePid,
      artifactId: ReferencePid,
    }),
  }),
);

function validateServerReference(value: unknown, file: string, updatesRoot: string): void {
  const selection = decodeServerReference(value);
  const { binding, receipt } = selection;
  const key = NodeCrypto.createHash("sha256").update(JSON.stringify(binding)).digest("hex");
  const workflows =
    receipt.platform === "linux"
      ? [".github/workflows/artifact-cli-linux.yml"]
      : [".github/workflows/artifact-desktop-mac.yml", ".github/workflows/artifact-cli-mac.yml"];
  if (
    NodePath.join(binding.baseDir, "runtime", "jones-updates") !== updatesRoot ||
    NodePath.basename(file) !== `${key}.json` ||
    (receipt.platform === "darwin" && receipt.architecture !== "arm64") ||
    !workflows.includes(receipt.workflow)
  )
    throw new Error("Unknown server selection binding; cleanup held.");
}

type NativeBindingFacts = {
  readonly home: string;
  readonly databasePath: string;
  readonly profile: string;
  readonly environmentId: string;
};
type NativeTrialFacts = {
  readonly stagedHandle: string;
  readonly version: string;
  readonly sourceSha: string;
  readonly sourceTree: string;
  readonly listener: string;
};
type NativeReferenceContext = {
  legacy: boolean;
  implicitTrialHandle: boolean;
  modernClaim: boolean;
  consistent: boolean;
  binding?: NativeBindingFacts;
  trial?: NativeTrialFacts;
  legacyClaim?: {
    readonly variant: "unversioned" | "claim1";
    readonly selectionPath?: string;
    readonly selectionSha256?: string;
  };
  resumedJournal?: boolean;
  legacyDescriptor?: boolean;
};

function nativeReferenceObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Unknown native reference object; cleanup held.");
  return value as Record<string, unknown>;
}

function rememberNativeBinding(context: NativeReferenceContext, value: NativeBindingFacts): void {
  const binding = {
    home: value.home,
    databasePath: value.databasePath,
    profile: value.profile,
    environmentId: value.environmentId,
  };
  if (context.binding && JSON.stringify(context.binding) !== JSON.stringify(binding))
    context.consistent = false;
  context.binding = binding;
}

function rememberNativeTrial(context: NativeReferenceContext, value: NativeTrialFacts): void {
  const trial = {
    stagedHandle: value.stagedHandle,
    version: value.version,
    sourceSha: value.sourceSha,
    sourceTree: value.sourceTree,
    listener: value.listener,
  };
  if (context.trial && JSON.stringify(context.trial) !== JSON.stringify(trial))
    context.consistent = false;
  context.trial = trial;
}

function finalizeNativeReferences(
  context: NativeReferenceContext,
  directory: string,
  updatesRoot: string,
): void {
  if (!context.legacy) return;
  const transactionId = NodePath.basename(directory);
  const home = NodePath.dirname(NodePath.dirname(updatesRoot));
  const canonical = (path: string) =>
    NodePath.isAbsolute(path) && NodePath.normalize(path) === path;
  if (
    updatesRoot !== NodePath.join(home, "runtime", "jones-updates") ||
    directory !== NodePath.join(updatesRoot, "transactions", transactionId) ||
    !context.resumedJournal ||
    !context.consistent ||
    context.modernClaim ||
    context.trial?.stagedHandle !== transactionId ||
    !context.binding ||
    !canonical(context.binding.databasePath) ||
    !canonical(context.binding.profile) ||
    (context.implicitTrialHandle && !context.legacyDescriptor)
  )
    throw new Error("Unproved legacy native reference context; cleanup held.");
}

/** Validate reference identity, never grant activation or retirement authority. */
function validateNativeReference(
  value: unknown,
  file: string,
  updatesRoot: string,
  context: NativeReferenceContext,
): void {
  const transactionId = NodePath.basename(NodePath.dirname(file));
  if (!HANDLE.test(transactionId)) throw new Error("Unknown native transaction; cleanup held.");
  const raw = nativeReferenceObject(value);
  if (raw.preparationClaimProtocol === 2 || raw.preparationClaimProtocol === 3)
    context.modernClaim = true;
  const home = NodePath.dirname(NodePath.dirname(updatesRoot));
  const tx = NodePath.join(updatesRoot, "transactions", transactionId);
  const binding = {
    protocol: Schema.Literal(1),
    transactionId: Schema.Literal(transactionId),
    home: Schema.Literal(home),
    databasePath: ReferencePath,
    profile: ReferencePath,
    environmentId: Schema.NonEmptyString,
  };
  const trial = {
    ...binding,
    startupGateProtocol: Schema.Literal(1),
    stagedHandle: ReferenceHash,
    version: Schema.NonEmptyString,
    sourceSha: ReferenceSource,
    sourceTree: ReferenceSource,
    listener: ReferenceText,
  };
  const trialReceipt = Schema.Struct({
    ...trial,
    resumeHeld: Schema.Literal(true),
    backendProcess: LegacyProcessReference,
  });
  const trialValue = (original: unknown) => {
    const raw = nativeReferenceObject(original);
    if (Object.hasOwn(raw, "stagedHandle")) return original;
    context.legacy = true;
    context.implicitTrialHandle = true;
    // Only a temporary validation value; the directory still needs its format/completion witnesses.
    return { ...raw, stagedHandle: transactionId };
  };
  const rememberTrial = (record: NativeBindingFacts & NativeTrialFacts) => {
    rememberNativeBinding(context, record);
    rememberNativeTrial(context, record);
  };
  // preparation_claim / preparation_context in jonesNativeHelperSource.ts.
  const claim = {
    ...binding,
    preparationClaimProtocol: Schema.Literals([2, 3]),
    stagedHandle: ReferenceHash,
    selectionPath: ReferencePath.check(
      Schema.makeFilter((path) => NodePath.dirname(path) === NodePath.join(updatesRoot, "staging")),
    ),
    selectionSha256: ReferenceHash,
  };
  const intent = Schema.Struct({
    ...JonesInstallIntent.fields,
    transactionId: binding.transactionId,
    staged: Schema.Struct({ ...JonesStagedMacApp.fields, handle: ReferenceHash }),
    expected: Schema.Struct({ ...JonesActiveInstall.fields, home: binding.home }),
    continuationReceipt: Schema.Literal(NodePath.join(tx, "continuation.json")),
    processes: ActivationProcesses,
    listener: Schema.NonEmptyString,
  });
  const rememberIntent = (record: typeof intent.Type) => {
    rememberNativeBinding(context, record.expected);
    rememberNativeTrial(context, {
      stagedHandle: record.staged.handle,
      version: record.staged.version,
      sourceSha: record.staged.sourceSha,
      sourceTree: record.staged.sourceTree,
      listener: record.listener,
    });
  };
  const plan = Schema.Struct({
    protocol: binding.protocol,
    transactionId: binding.transactionId,
    activeBinding: Schema.Struct({ ...JonesActiveInstall.fields, home: binding.home }),
    records: Schema.Record(Schema.String, ReferenceHash).check(
      Schema.makeFilter(
        (records) =>
          !Array.isArray(records) &&
          "prepare-intent.json" in records &&
          Object.keys(records).every((name) =>
            [
              "prepare-intent.json",
              "prepare-request.json",
              "prepare-refused.json",
              "prepare-failure.json",
              "prepare-dispatched.json",
              "continuation.json",
              "activation-request.json",
              "preparation-withdrawal.json",
            ].includes(name),
          ),
      ),
    ),
    attemptSelection: ReferencePath.check(
      Schema.makeFilter(
        (path) => NodePath.dirname(path) === NodePath.join(updatesRoot, "attempt-selections"),
      ),
    ),
    attemptSha256: ReferenceHash,
    retiredPath: Schema.Literal(NodePath.join(updatesRoot, "retired-preparations", transactionId)),
  });
  // Match the native helper's named records, plus the server's trialGate.ts receipts.
  switch (NodePath.basename(file)) {
    case "intent.json":
    case "activation-request.json":
      requireWarmJonesInstallIntent(value);
      rememberIntent(Schema.decodeUnknownSync(intent)(value));
      return;
    case "journal.json": {
      // Inspect the original intent before decoding can strip future discriminators.
      requireWarmJonesInstallIntent(raw.intent);
      const natural = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
      const decimal = Schema.String.check(Schema.isPattern(/^[0-9]+$/));
      const diagnostics = Schema.Record(
        Schema.String,
        Schema.Union([Schema.String, Schema.Number]),
      );
      const journal = Schema.decodeUnknownSync(
        Schema.Struct({
          intent,
          phase: Schema.Literals(["resumed", "rolled-back"]),
          message: Schema.optionalKey(Schema.String),
          phaseBeforeBlock: Schema.optionalKey(Schema.String),
          incomingBundle: Schema.optionalKey(ReferencePath),
          previousBundle: Schema.optionalKey(ReferencePath),
          pairedState: Schema.optionalKey(ReferencePath),
          candidateWriter: Schema.optionalKey(
            Schema.Union([KernelProcessReference, LegacyProcessReference]),
          ),
          backendWriter: Schema.optionalKey(
            Schema.Union([KernelProcessReference, LegacyProcessReference]),
          ),
          trialReceipt: Schema.optionalKey(trialReceipt),
          diagnostics: Schema.optionalKey(diagnostics),
          recoveryDiagnostics: Schema.optionalKey(diagnostics),
          relaunchDiagnostics: Schema.optionalKey(diagnostics),
          relaunch: Schema.optionalKey(Schema.Literal("failed")),
          recoveryReserveCleanup: Schema.optionalKey(Schema.Literal("held")),
          recovery: Schema.optionalKey(
            Schema.Struct({
              method: Schema.Literals(["clone", "copy", "sqlite-backup"]),
              bytes: Schema.Number,
              startedAt: Schema.Number,
              completedAt: Schema.Number,
            }),
          ),
          storage: Schema.optionalKey(
            Schema.Struct({
              stateBytes: natural,
              candidateBytes: natural,
              requiredBytes: natural,
              headroomBytesPerVolume: natural,
              stateRecovery: Schema.optionalKey(Schema.Literal("clone-required")),
              volumes: Schema.Array(
                Schema.Struct({
                  path: ReferencePath,
                  device: decimal,
                  requiredBytes: natural,
                  availableBytes: natural,
                }),
              ),
            }),
          ),
          recoveryReserves: Schema.optionalKey(
            Schema.Array(
              Schema.Struct({
                path: ReferencePath,
                bytes: natural,
                device: decimal,
                inode: decimal,
                purpose: Schema.Literals(["journal", "recovery"]),
              }),
            ),
          ),
          preEffect: Schema.optionalKey(
            Schema.Struct({
              phase: Schema.Literals(["intent", "preparing", "quiescent"]),
              step: Schema.Literals([
                "storage",
                "reserve",
                "bundle",
                "stop-processes",
                "quiescence",
                "pair",
              ]),
              reason: Schema.String,
              diagnostics: Schema.optionalKey(diagnostics),
              relaunch: Schema.Literals(["started", "skipped"]),
            }),
          ),
          legacySettlement: Schema.optionalKey(
            Schema.Struct({
              basis: Schema.Literals(["interrupted-preparation", "unstarted-activation"]),
              settledBy: Schema.Literal("startup"),
              evidence: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Boolean])),
            }),
          ),
          preSwapPreparationRecovery: Schema.optionalKey(
            Schema.Struct({
              transactionId: binding.transactionId,
              journalSha256: ReferenceHash,
              intentSha256: ReferenceHash,
            }),
          ),
        }),
      )(
        Object.hasOwn(raw, "trialReceipt")
          ? { ...raw, trialReceipt: trialValue(raw.trialReceipt) }
          : value,
      );
      context.resumedJournal = journal.phase === "resumed";
      rememberIntent(journal.intent);
      if (journal.trialReceipt) rememberTrial(journal.trialReceipt);
      return;
    }
    case "prepare-intent.json":
    case "prepare-dispatched.json":
    case "continuation.json": {
      const continuation = NodePath.basename(file) === "continuation.json";
      const requirePrepared = () => {
        if (continuation)
          Schema.decodeUnknownSync(Schema.Struct({ prepared: Schema.Literal(true) }))(value);
      };
      let legacyClaim: NonNullable<NativeReferenceContext["legacyClaim"]>;
      if (!Object.hasOwn(raw, "preparationClaimProtocol")) {
        if (
          NodePath.basename(file) === "prepare-dispatched.json" ||
          ["stagedHandle", "selectionPath", "selectionSha256"].some((key) =>
            Object.hasOwn(raw, key),
          )
        )
          throw new Error("Unknown undiscriminated preparation reference; cleanup held.");
        const record = Schema.decodeUnknownSync(
          Schema.Struct({ ...binding, prepared: Schema.Literal(true) }),
        )(value);
        rememberNativeBinding(context, record);
        legacyClaim = { variant: "unversioned" };
      } else if (raw.preparationClaimProtocol === 1) {
        if (Object.hasOwn(raw, "stagedHandle"))
          throw new Error("Unknown legacy preparation handle; cleanup held.");
        const record = Schema.decodeUnknownSync(
          Schema.Struct({
            ...binding,
            preparationClaimProtocol: Schema.Literal(1),
            selectionPath: claim.selectionPath.check(
              Schema.makeFilter((path) => NodePath.normalize(path) === path),
            ),
            selectionSha256: ReferenceHash,
          }),
        )(value);
        requirePrepared();
        rememberNativeBinding(context, record);
        legacyClaim = {
          variant: "claim1",
          selectionPath: record.selectionPath,
          selectionSha256: record.selectionSha256,
        };
      } else {
        // A present discriminator selects the strict current decoder; failure never falls back.
        Schema.decodeUnknownSync(Schema.Struct(claim))(value);
        requirePrepared();
        context.modernClaim = true;
        return;
      }
      context.legacy = true;
      if (
        context.legacyClaim &&
        JSON.stringify(context.legacyClaim) !== JSON.stringify(legacyClaim)
      )
        context.consistent = false;
      context.legacyClaim = legacyClaim;
      return;
    }
    case "prepare-request.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          transactionId: binding.transactionId,
          stagedHandle: ReferenceHash,
          claimSha256: ReferenceHash,
          processes: ProcessReferences,
        }),
      )(value);
      return;
    case "prepare-refused.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          transactionId: binding.transactionId,
          stagedHandle: ReferenceHash,
          claimSha256: ReferenceHash,
          reason: Schema.String,
        }),
      )(value);
      return;
    case "prepare-failure.json":
      Schema.decodeUnknownSync(
        Schema.Struct({ protocol: binding.protocol, reason: Schema.String }),
      )(value);
      return;
    case "preparation-withdrawal.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          transactionId: binding.transactionId,
          withdrawalProtocol: Schema.Literal(1),
          claimSha256: ReferenceHash,
          claim: Schema.Struct({ ...claim, preparationClaimProtocol: Schema.Literal(3) }),
          expected: Schema.Struct({ ...JonesActiveInstall.fields, home: binding.home }),
          evidence: Schema.Literal("withdrawal-protocol"),
        }),
      )(value);
      context.modernClaim = true;
      return;
    case "preparation-retirement.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          planSha256: ReferenceHash,
          plan,
          evidence: RetirementEvidence,
        }),
      )(value);
      return;
    case "preparation-retirement-receipt.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          transactionId: binding.transactionId,
          planSha256: ReferenceHash,
          status: Schema.Literal("retired"),
          evidence: RetirementEvidence,
        }),
      )(value);
      return;
    case "retirement-intent.json": {
      const natural = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
      const decimal = Schema.String.check(Schema.isPattern(/^[0-9]+$/));
      const kind = Schema.Literals(["file", "directory", "link"]);
      // retirement_context's inventories are evidence only; this worker never executes them.
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          planSha256: ReferenceHash,
          plan: Schema.Struct({
            protocol: binding.protocol,
            transactionId: binding.transactionId,
            activeBinding: Schema.Struct({ ...JonesActiveInstall.fields, home: binding.home }),
            journalSha256: ReferenceHash,
            targets: Schema.Array(
              Schema.Struct({
                path: ReferencePath,
                kind,
                device: decimal,
                inode: decimal,
                bytes: natural,
                parentDevice: decimal,
                parentInode: decimal,
                entries: Schema.Array(
                  Schema.Struct({
                    path: Schema.NonEmptyString,
                    kind,
                    device: decimal,
                    inode: decimal,
                    bytes: natural,
                    mode: natural,
                  }),
                ),
              }),
            ),
          }),
        }),
      )(value);
      return;
    }
    case "retirement-receipt.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          transactionId: binding.transactionId,
          planSha256: ReferenceHash,
          status: Schema.Literal("retired"),
        }),
      )(value);
      return;
    case "advanced-state-authority-guard.json":
      Schema.decodeUnknownSync(
        Schema.Struct({
          protocol: binding.protocol,
          guard: Schema.Union([
            Schema.Struct({ kind: Schema.Literal("absent") }),
            Schema.Struct({
              kind: Schema.Literal("fenced"),
              environmentId: Schema.NonEmptyString,
              authorityNamespace: Schema.NonEmptyString,
              storeGeneration: ReferencePid,
              transitionId: Schema.NonEmptyString,
              recordSha256: ReferenceHash,
            }),
          ]),
        }),
      )(value);
      return;
    case "attempt-selection.json": {
      const attempt = decodeAttemptReference(value);
      if (
        attempt.transactionId !== transactionId ||
        attempt.expected.home !== home ||
        !HANDLE.test(attempt.stagedHandle) ||
        !HANDLE.test(attempt.selectionSha256) ||
        NodePath.dirname(attempt.selectionPath) !== NodePath.join(updatesRoot, "staging")
      )
        throw new Error("Unknown retired attempt binding; cleanup held.");
      return;
    }
    default: {
      switch (NodePath.basename(file)) {
        case "trial-descriptor.json":
          rememberTrial(
            Schema.decodeUnknownSync(
              Schema.Struct({
                ...trial,
                trialReceiptPath: Schema.Literal(NodePath.join(tx, "trial-receipt.json")),
                commitGrantPath: Schema.Literal(NodePath.join(tx, "commit-grant.json")),
              }),
            )(trialValue(value)),
          );
          context.legacyDescriptor = !Object.hasOwn(raw, "stagedHandle");
          return;
        case "commit-grant.json":
          rememberTrial(
            Schema.decodeUnknownSync(
              Schema.Struct({ ...trial, generation: binding.transactionId }),
            )(trialValue(value)),
          );
          return;
        case "trial-receipt.json":
        case "resume-dispatched.json":
          rememberTrial(Schema.decodeUnknownSync(trialReceipt)(trialValue(value)));
          return;
        default:
          throw new Error("Unknown staging reference format; cleanup held.");
      }
    }
  }
}

/** Only an interpretable record can prove that a handle is unrelated. */
async function recordMentions(
  file: string,
  handle: string,
  format: "stage" | "attempt" | "server" | "native",
  updatesRoot: string,
  device?: number,
  nativeContext?: NativeReferenceContext,
): Promise<boolean> {
  const requireRecord = (info: NodeFS.Stats) => {
    if (
      !info.isFile() ||
      info.uid !== process.getuid?.() ||
      (device !== undefined && info.dev !== device) ||
      info.size > MAX_RECORD_BYTES
    )
      throw new Error("A staging record is not a bounded regular file.");
  };
  requireRecord(await NodeFSP.lstat(file));
  const record = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    requireRecord(await record.stat());
    const raw = await record.readFile("utf8");
    if (Buffer.byteLength(raw) > MAX_RECORD_BYTES)
      throw new Error("A staging record is not a bounded regular file.");
    const value: unknown = JSON.parse(raw);
    // Normalize before matching so escaped keys and values still retain their handles.
    if (JSON.stringify(value).includes(handle)) return true;
    if (format === "stage") decodeStageReference(value);
    else if (format === "attempt") decodeAttemptReference(value);
    else if (format === "server") validateServerReference(value, file, updatesRoot);
    else {
      if (!nativeContext) throw new Error("Missing native reference context; cleanup held.");
      validateNativeReference(value, file, updatesRoot, nativeContext);
    }
    return false;
  } finally {
    await record.close();
  }
}

/**
 * True when any selection, attempt, transaction or retired preparation names the handle. A
 * mention, however indirect, keeps the payload: those records are activation evidence.
 */
async function handleReferenced(updatesRoot: string, handle: string): Promise<boolean> {
  const device = (await ownedDirectory(updatesRoot)).dev;
  // `selections` belongs to the bundled server's qualified runtime; a mention there still counts.
  for (const [name, format] of [
    ["staging", "stage"],
    ["attempt-selections", "attempt"],
    ["selections", "server"],
  ] as const) {
    const root = NodePath.join(updatesRoot, name);
    for (const entry of await boundedEntries(root, device))
      if (await recordMentions(NodePath.join(root, entry), handle, format, updatesRoot, device))
        return true;
  }
  for (const name of ["transactions", "retired-preparations"]) {
    const root = NodePath.join(updatesRoot, name);
    for (const entry of await boundedEntries(root, device)) {
      if (entry.includes(handle)) return true;
      const directory = NodePath.join(root, entry);
      await ownedDirectory(directory, device);
      const context: NativeReferenceContext = {
        legacy: false,
        implicitTrialHandle: false,
        modernClaim: false,
        consistent: true,
      };
      for (const record of await boundedEntries(directory, device)) {
        // Native recovery reserves and state-copy directories carry no reference records.
        if (["previous-pair", "advanced-state"].includes(record)) {
          await ownedDirectory(NodePath.join(directory, record), device);
          continue;
        }
        if (
          ["reserve.bin", "recovery-reserve.bin", "recovery-journal-reserve.bin"].includes(record)
        ) {
          const info = await NodeFSP.lstat(NodePath.join(directory, record));
          if (!info.isFile() || info.uid !== process.getuid?.() || info.dev !== device)
            throw new Error("Unknown staging reserve ownership or type.");
          continue;
        }
        if (
          await recordMentions(
            NodePath.join(directory, record),
            handle,
            "native",
            updatesRoot,
            device,
            context,
          )
        )
          return true;
      }
      finalizeNativeReferences(context, directory, updatesRoot);
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
