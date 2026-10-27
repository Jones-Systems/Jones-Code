// @effect-diagnostics globalTimers:off globalDate:off
// Promise boundary is also exercised without an Effect runtime; timers drive subscriptions and installation progress.
import type {
  JonesOperationReceipt,
  JonesRecoveryPolicy,
} from "@t3tools/contracts/jones/jonesOperations";
import * as Schema from "effect/Schema";
import { JonesCheckClassification } from "@t3tools/contracts/jones/jonesUpdates";
import type { PreparationJoin } from "./preparation/supervisor.ts";
import { readJonesBuildStartedAt } from "@t3tools/contracts/jones/fleet-updates";
import type {
  JonesUpdatePolicy,
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateExactInput,
  JonesUpdateInstallInput,
} from "@t3tools/contracts/jones/jonesUpdates";
import { JonesActionsClient, type JonesActionsCandidate } from "@t3tools/shared/jones/jonesActions";
import {
  findActivationWithdrawal,
  followerTargetRefusal,
  type FollowerOrigin,
} from "./followerPolicy.ts";

export interface JonesStagedRetirementInput {
  readonly operationId: string;
  readonly environmentId: string;
  readonly currentVersion: string;
  readonly expectedInstalledSource: string;
  readonly targetSource: string;
  readonly stagedHandle: string;
}

/** One downloaded, verified and staged build, bound to this host. */
export interface JonesPreparedStage {
  readonly stagedHandle: string;
  readonly version: string;
  readonly payloadSha256: string;
  readonly migrationPlan?: JonesUpdateState["migrationPlan"];
}

export interface JonesUpdaterHost {
  readonly initialState: JonesUpdateState;
  readonly installedSource: () => Promise<string>;
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
  /**
   * Downloads, verifies and stages exactly `candidate`. The host runs this off the serving
   * process; the signal stops a preparation whose target was superseded.
   */
  readonly prepare: (
    candidate: JonesActionsCandidate,
    options: {
      readonly signal: AbortSignal;
      readonly onVerifying: () => void;
      readonly jobId?: string;
      readonly onStarted?: (settled: Promise<PreparationJoin>) => void;
      readonly onAdopting?: () => Promise<void>;
    },
  ) => Promise<JonesPreparedStage>;
  readonly install: (
    request: JonesUpdaterInstallInput,
    reportStep?: (step: "marking" | "verifying" | "preflight" | "capturing" | "handoff") => void,
  ) => Promise<void | {
    readonly updateId?: string;
    readonly migrationPlan?: JonesUpdateState["migrationPlan"];
  }>;
  readonly retireStaged?: (input: JonesStagedRetirementInput) => Promise<void>;
  readonly stateChanged?: (state: JonesUpdateState) => void;
  readonly startupOutcome?: () =>
    | {
        status: "committed" | "rolled-back" | "failed";
        id: string;
        fromVersion: string;
        targetVersion: string;
        reason?: string;
        settlement?: JonesUpdateState["settlement"];
        settlementRefusal?: JonesUpdateState["settlementRefusal"];
        cause?: string;
      }
    | undefined;
}

/** What one check run produced; the candidate belongs to this run alone. */
interface JonesCheckRun {
  readonly state: JonesUpdateState;
  readonly candidate: JonesActionsCandidate | undefined;
  readonly buildStartedAt?: string;
}

export function readCurrentJonesBuildStartedAt(
  metadata: unknown,
  currentVersion: string,
): string | undefined {
  return typeof metadata === "object" &&
    metadata !== null &&
    "version" in metadata &&
    metadata.version === currentVersion
    ? readJonesBuildStartedAt(metadata)
    : undefined;
}

function candidateProvenance(
  candidate: JonesActionsCandidate,
  buildStartedAt: string | undefined,
): NonNullable<JonesUpdateState["provenance"]> {
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
  };
}

export type JonesInstallBeforeDispatchRefusal = JonesUpdateInstallInput & {
  readonly kind: "before-dispatch";
  readonly operationId: string;
  readonly targetSource: string;
  readonly expectedInstalledSource: string;
};

export type JonesUpdaterInstallInput = JonesUpdateInstallInput & {
  readonly operationId?: string;
  readonly targetSource?: string;
  readonly expectedInstalledSource?: string;
  /** Private callback, invoked only before calling the host's install adapter. */
  readonly onBeforeDispatchRefusal?: (proof: JonesInstallBeforeDispatchRefusal) => void;
  readonly recovery?: { readonly policy: JonesRecoveryPolicy; readonly preflightId: string };
  /** Who asked: a person on this host, or a fleet operation. Defaults to manual. */
  readonly origin?: FollowerOrigin;
  /**
   * Checked again just before the launcher handoff; a reason withdraws an automatic activation
   * while it can still be refused, leaving the staged build ready.
   */
  readonly admit?: () => Promise<string | undefined>;
};

export interface JonesPreparationOperation {
  readonly operationId: string;
  readonly jobId: string;
  readonly signal: AbortSignal;
  readonly onStarting?: () => void;
  readonly onAdopting: () => Promise<void>;
}
export class PreparationStopRequested extends Error {
  readonly operationId: string;
  constructor(operationId: string) {
    super("Download stopped. Nothing was installed.");
    this.name = "PreparationStopRequested";
    this.operationId = operationId;
  }
}

const FOLLOWER_WAITING = "Waiting for the laptop to report its installed build.";
const RETIRED_WITHOUT_TARGET =
  "The previous staged selection was retired, but the requested target was not staged. Start a fresh exact-source download without replacement.";

/** A follower's laptop moved on; the unaccepted preparation of its earlier build is dropped. */
class PreparationSuperseded extends Error {
  constructor() {
    super("The laptop's installed build changed while this build was being prepared.");
    this.name = "PreparationSuperseded";
  }
}

/** One checker belongs to the host. Connected clients observe the same fixed staging handle. */
export class JonesUpdater {
  private state: JonesUpdateState;
  private revision = 0;
  private busy = false;
  private candidate: JonesActionsCandidate | undefined;
  private candidateBuildStartedAt: string | undefined;
  /** The one check run on this host, keyed by its target (`latest` for the newest main build). */
  private inflight: { readonly key: string; readonly promise: Promise<JonesCheckRun> } | undefined;
  /** The one unaccepted preparation; a policy change can stop it. Installs are never here. */
  private preparation:
    | {
        readonly source: string;
        readonly controller: AbortController;
        readonly done: Promise<JonesUpdateState>;
        readonly operationId?: string;
        readonly jobId?: string;
        adopting: boolean;
        join?: Promise<PreparationJoin>;
      }
    | undefined;
  private readonly heldJobs = new Set<string>();
  setHeldJobs(jobIds: ReadonlyArray<string>): void {
    for (const id of jobIds) this.heldJobs.add(id);
  }
  releaseHeldJob(jobId: string): void {
    this.heldJobs.delete(jobId);
  }
  admissionHeld(): boolean {
    return this.heldJobs.size > 0 || this.busy;
  }
  stopPreparation(operationId: string): "requested" | "not-running" | "not-stoppable-phase" {
    const preparation = this.preparation;
    if (preparation?.operationId !== operationId) return "not-running";
    if (preparation.adopting) return "not-stoppable-phase";
    preparation.controller.abort(new PreparationStopRequested(operationId));
    return "requested";
  }
  private readonly listeners = new Set<() => void>();

  private readonly host: JonesUpdaterHost;
  private readonly actions: Pick<JonesActionsClient, "check">;
  constructor(
    host: JonesUpdaterHost,
    actions: Pick<JonesActionsClient, "check"> = new JonesActionsClient(),
  ) {
    this.host = host;
    this.actions = actions;
    this.state = host.initialState;
  }

  snapshot(): JonesUpdateState {
    const outcome =
      this.state.outcome !== undefined || ["preparing", "installing"].includes(this.state.phase)
        ? undefined
        : this.host.startupOutcome?.();
    return {
      ...this.state,
      ...(outcome === undefined
        ? {}
        : {
            updateId: this.state.updateId ?? outcome.id,
            ...(outcome.status !== "failed"
              ? {}
              : {
                  settlement: outcome.settlement ?? "none",
                  ...(outcome.cause === undefined ? {} : { cause: outcome.cause }),
                  ...(outcome.settlementRefusal === undefined
                    ? {}
                    : { settlementRefusal: outcome.settlementRefusal }),
                }),
            outcome: {
              status: outcome.status === "failed" ? ("blocked" as const) : outcome.status,
              fromVersion: outcome.fromVersion,
              targetVersion: outcome.targetVersion,
              ...(outcome.reason === undefined ? {} : { reason: outcome.reason }),
            },
            ...((this.revision === 0 || this.state.phase === "error") &&
            this.state.stagedHandle === undefined
              ? {
                  phase: outcome.status === "failed" ? ("error" as const) : outcome.status,
                  message:
                    outcome.status === "committed"
                      ? "The selected Jones runtime was installed."
                      : outcome.status === "rolled-back"
                        ? "Installation rolled back to its retained binary and state pair."
                        : outcome.settlement === "launcher-recorded" ||
                            outcome.settlement === "legacy-reconciled"
                          ? `Update did not start: ${outcome.cause ?? "cause unavailable"}. Nothing changed.`
                          : "Previous attempt needs reconciliation",
                }
              : {}),
          }),
      revision: this.revision,
    };
  }

  recordOperation(operation: JonesOperationReceipt): void {
    if (this.state.operation === undefined || this.state.operation.updatedAt <= operation.updatedAt)
      this.publish({ operation });
  }

  /** Notify observers after the host rereads this attempt's supplemental settlement evidence. */
  refreshStartupOutcome(): JonesUpdateState {
    return this.publish({});
  }

  /**
   * Sets which builds this host may take. A follower stops offering any build other than its
   * laptop's; a staged build stays visible (and refused) until it is retired or replaced.
   */
  setPolicy(policy: JonesUpdatePolicy | undefined): JonesUpdateState {
    const current = this.state.policy;
    if (
      current?.role === policy?.role &&
      current?.leaderSource === policy?.leaderSource &&
      current?.leaderTree === policy?.leaderTree &&
      current?.reviewTarget?.directiveId === policy?.reviewTarget?.directiveId
    )
      return this.snapshot();
    const { policy: _previous, ...rest } = this.state;
    this.state = policy === undefined ? rest : { ...rest, policy };
    // Latest wins: a follower stops preparing a build its laptop no longer runs.
    const running = this.preparation;
    if (
      running !== undefined &&
      typeof policy?.leaderSource === "string" &&
      running.source !== policy.leaderSource &&
      running.source !== policy.reviewTarget?.source
    )
      running.controller.abort(new PreparationSuperseded());
    const offeredOther =
      policy !== undefined &&
      this.state.stagedHandle === undefined &&
      this.state.phase === "available" &&
      this.state.provenance?.sourceSha !== (policy.leaderSource ?? undefined);
    if (offeredOther) {
      const { provenance: _provenance, ...withoutOffer } = this.state;
      this.state = withoutOffer;
      this.candidate = undefined;
    }
    const idle =
      this.state.stagedHandle === undefined &&
      !this.busy &&
      !["preparing", "installing"].includes(this.state.phase);
    return this.publish(
      offeredOther || (idle && policy?.leaderSource === null)
        ? {
            phase: "no-new",
            message:
              policy?.leaderSource === null
                ? FOLLOWER_WAITING
                : "The laptop's installed build changed. Checking for it…",
          }
        : {},
    );
  }

  private clearOutcome(): void {
    const { outcome: _outcome, updateId: _updateId, ...state } = this.state;
    this.state = state;
  }

  private publish(patch: Partial<JonesUpdateState>): JonesUpdateState {
    this.state = { ...this.state, ...patch };
    this.revision += 1;
    this.host.stateChanged?.(this.snapshot());
    for (const listener of this.listeners) listener();
    return this.snapshot();
  }

  async observe(after?: number, signal?: AbortSignal): Promise<JonesUpdateState> {
    if (after === undefined || after !== this.revision) return this.snapshot();
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        this.listeners.delete(changed);
        signal?.removeEventListener("abort", aborted);
      };
      const changed = () => {
        cleanup();
        resolve(this.snapshot());
      };
      const aborted = () => {
        cleanup();
        reject(new Error("Update subscription closed."));
      };
      const timer = setTimeout(changed, 25_000);
      this.listeners.add(changed);
      signal?.addEventListener("abort", aborted, { once: true });
      if (signal?.aborted) aborted();
    });
  }

  private fail(error: unknown): JonesUpdateState {
    // The Actions adapter sanitizes transport failures; injected host errors must also be safe to publish.
    const message = error instanceof Error ? error.message : "Jones update failed.";
    return this.publish({
      phase: "blocked",
      message,
      // An offered build reports its version for display; only a staged build is an attempt.
      ...(this.state.currentVersion === undefined ||
      this.state.stagedHandle === undefined ||
      this.state.provenance?.version === undefined
        ? {}
        : {
            outcome: {
              status: "blocked" as const,
              fromVersion: this.state.currentVersion,
              targetVersion: this.state.provenance.version,
              reason: message,
            },
          }),
    });
  }

  /**
   * Checks for a build. Callers of one target share the running check; a caller of another target
   * waits for it and then runs its own. A check yields to any other operation on the host.
   */
  async check(targetSource?: string): Promise<JonesUpdateState> {
    const policy = this.state.policy;
    if (targetSource === undefined && policy !== undefined) {
      // A follower never asks for the newest main build; it checks for its laptop's build.
      if (policy.leaderSource === null) return { ...this.snapshot(), message: FOLLOWER_WAITING };
      return (await this.checkRun(policy.leaderSource)).state;
    }
    return (await this.checkRun(targetSource)).state;
  }

  private async checkRun(targetSource?: string): Promise<JonesCheckRun> {
    const key = targetSource ?? "latest";
    for (;;) {
      const running = this.inflight;
      if (running === undefined) break;
      if (running.key === key) return running.promise;
      await running.promise.catch(() => undefined);
    }
    if (
      this.heldJobs.size > 0 ||
      this.busy ||
      this.state.phase === "installing" ||
      this.state.phase === "preparing" ||
      !this.state.capability.check
    )
      return { state: this.snapshot(), candidate: undefined };
    // Claim the host before the run starts so nothing can interleave with its first publication.
    this.busy = true;
    let settle!: { resolve: (run: JonesCheckRun) => void; reject: (cause: unknown) => void };
    const promise = new Promise<JonesCheckRun>((resolve, reject) => (settle = { resolve, reject }));
    this.inflight = { key, promise };
    void this.runCheck(targetSource).then(
      (run) => {
        this.busy = false;
        this.inflight = undefined;
        settle.resolve(run);
      },
      (cause: unknown) => {
        this.busy = false;
        this.inflight = undefined;
        settle.reject(cause);
      },
    );
    return promise;
  }

  /** Runs one check under the busy latch; its result keeps candidate display metadata paired. */
  private async runCheck(targetSource: string | undefined): Promise<JonesCheckRun> {
    const staged = this.state.stagedHandle !== undefined;
    // A host that already holds a settled result keeps showing it until this check replaces it.
    if (!staged && this.state.checkedAt === undefined)
      this.publish({ phase: "checking", message: "Checking qualified main builds…" });
    try {
      const result = await this.actions.check({
        installedSource: await this.host.installedSource(),
        platform: this.host.platform,
        architecture: this.host.architecture,
        ...(targetSource === undefined ? {} : { targetSource }),
      });
      const checkedAt = new Date().toISOString();
      if ("classification" in result && Schema.is(JonesCheckClassification)(result.classification))
        this.publish({ checkClassification: result.classification });
      if (result.state === "available") {
        const candidate = result.candidate;
        const buildStartedAt = result.buildStartedAt;
        // A follower offers only its laptop's build, with the tree the repository records for it.
        const refusal = followerTargetRefusal(this.state.policy, candidate.source, candidate.tree);
        if (refusal !== undefined) {
          this.candidate = undefined;
          this.candidateBuildStartedAt = undefined;
          return {
            state: this.publish(
              staged ? { checkedAt } : { phase: "blocked", checkedAt, message: refusal },
            ),
            candidate: undefined,
          };
        }
        this.candidate = candidate;
        this.candidateBuildStartedAt = buildStartedAt;
        if (staged)
          return {
            state: this.publish({ checkedAt }),
            candidate,
            ...(buildStartedAt === undefined ? {} : { buildStartedAt }),
          };
        const { migrationPlan: _previousPlan, ...state } = this.state;
        this.state = state;
        return {
          state: this.publish({
            phase: "available",
            checkedAt,
            message: "A qualified main build is ready to download.",
            provenance: candidateProvenance(candidate, buildStartedAt),
          }),
          candidate,
          ...(buildStartedAt === undefined ? {} : { buildStartedAt }),
        };
      }
      if (staged) return { state: this.publish({ checkedAt }), candidate: undefined };
      return {
        state: this.publish({
          phase: result.state,
          checkedAt,
          message:
            result.state === "blocked"
              ? result.reason
              : result.state === "building"
                ? "A fresh main build is still being qualified."
                : targetSource === undefined
                  ? "No newer qualified main build."
                  : targetSource === this.state.policy?.leaderSource
                    ? "This host runs the laptop's installed build."
                    : "This host already runs the requested source.",
        }),
        candidate: undefined,
      };
    } catch (error) {
      return {
        state: staged ? this.publish({ checkedAt: new Date().toISOString() }) : this.fail(error),
        candidate: undefined,
      };
    }
  }

  /**
   * Stages the one qualified build of `targetSource`. The target is never widened to a newer build,
   * replacement requires the original immutable binding, and the same staged source is unchanged.
   */
  async stageExact(
    targetSource: string,
    replace?: JonesUpdateExactInput["replace"],
    operation?: JonesPreparationOperation,
  ): Promise<JonesUpdateState> {
    if (this.heldJobs.size > 0)
      return {
        ...this.snapshot(),
        phase: "blocked",
        message: "An earlier preparation has not proved its exit.",
      };
    if (operation?.signal.aborted) return this.snapshot();
    const refusal = followerTargetRefusal(
      this.state.policy,
      targetSource,
      this.state.provenance?.sourceSha === targetSource
        ? this.state.provenance.sourceTree
        : undefined,
    );
    if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
    const settled = this.stagedOutcome(targetSource);
    if (
      settled !== undefined &&
      (replace === undefined || settled.provenance?.sourceSha === targetSource)
    )
      return settled;
    if (replace !== undefined) {
      const refusal = this.replacementRefusal(replace);
      if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
    }
    // A superseded preparation releases the host as soon as it is stopped; the newer target
    // goes next. A running one is observed through the busy host, never awaited.
    const running = this.preparation;
    if (running?.controller.signal.aborted === true) await running.done;
    const run = await this.checkRun(targetSource);
    // Download only the candidate this run returned for the target; a skipped run, or a build
    // offered by any other check, must not be staged.
    const candidate = run.candidate;
    if (
      candidate?.source !== targetSource ||
      (replace === undefined && run.state.phase !== "available")
    ) {
      if (replace !== undefined)
        return {
          ...this.snapshot(),
          phase: "blocked",
          message:
            "The requested source is not available from the current check. The staged selection was retained.",
        };
      return run.state.phase === "available" || run.state.phase === "checking"
        ? {
            ...run.state,
            message: "The requested source is not available from the current check. Try again.",
          }
        : run.state;
    }
    // Another request may have claimed the host while this one waited for its turn.
    while (this.inflight !== undefined) await this.inflight.promise.catch(() => undefined);
    const latestRefusal = followerTargetRefusal(
      this.state.policy,
      candidate.source,
      candidate.tree,
    );
    if (latestRefusal !== undefined)
      return { ...this.snapshot(), phase: "blocked", message: latestRefusal };
    if (replace !== undefined) {
      const same = this.stagedOutcome(targetSource);
      if (same?.provenance?.sourceSha === targetSource) return same;
      const refusal = this.replacementRefusal(replace);
      if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
      const retired = await this.retireStagedOperation(
        {
          operationId: replace.operationId,
          environmentId: replace.environmentId,
          currentVersion: replace.currentVersion,
          expectedInstalledSource: replace.expectedInstalledSource,
          targetSource: replace.sourceSha,
          stagedHandle: replace.stagedHandle,
        },
        () => followerTargetRefusal(this.state.policy, candidate.source, candidate.tree),
      );
      if (!retired.retired)
        return {
          ...this.snapshot(),
          phase: "blocked",
          message: retired.reason ?? "The staged selection could not be retired safely.",
        };
      const afterRetirement = followerTargetRefusal(
        this.state.policy,
        candidate.source,
        candidate.tree,
      );
      if (
        afterRetirement !== undefined ||
        this.busy ||
        ["preparing", "installing"].includes(this.state.phase) ||
        this.state.stagedHandle !== undefined ||
        !this.state.capability.download
      )
        return {
          ...this.snapshot(),
          phase: "blocked",
          message: `${RETIRED_WITHOUT_TARGET} ${afterRetirement ?? "The host's update state changed."}`,
        };
      return this.stageCandidate(candidate, run.buildStartedAt, true);
    }
    return (
      this.stagedOutcome(targetSource) ??
      this.stageCandidate(candidate, run.buildStartedAt, false, operation)
    );
  }

  private replacementRefusal(
    replace: NonNullable<JonesUpdateExactInput["replace"]>,
  ): string | undefined {
    if (
      !this.state.capability.download ||
      this.state.capability.exactSource !== true ||
      this.state.capability.replaceStaged !== true ||
      this.host.retireStaged === undefined
    )
      return "Replacing a staged selection is unavailable on this host.";
    if (
      this.heldJobs.size > 0 ||
      this.busy ||
      ["preparing", "installing"].includes(this.state.phase)
    )
      return "An update operation is still active.";
    // An empty slot cannot prove that this replacement completed in an earlier server lifetime.
    if (this.state.stagedHandle === undefined)
      return "No staged selection matches this replacement. Start a fresh exact-source download without replacement.";
    if (
      replace.stagedHandle !== this.state.stagedHandle ||
      replace.sourceSha !== this.state.provenance?.sourceSha ||
      replace.environmentId !== this.state.environmentId ||
      replace.currentVersion !== this.state.currentVersion ||
      replace.expectedInstalledSource !== this.state.installedSource?.sha
    )
      return "The retained stage belongs to another immutable binding.";
    return undefined;
  }

  /** The result of asking for `targetSource` when a build is already staged, otherwise nothing. */
  private stagedOutcome(targetSource: string): JonesUpdateState | undefined {
    if (this.state.stagedHandle === undefined) return undefined;
    const staged = this.state.provenance?.sourceSha;
    return staged === targetSource
      ? this.snapshot()
      : {
          ...this.snapshot(),
          phase: "blocked",
          message:
            staged === undefined
              ? "Another source is already staged."
              : `Another source (${staged.slice(0, 12)}) is already staged and was not replaced.`,
        };
  }

  async download(
    input: JonesUpdateDownloadInput,
    operation?: JonesPreparationOperation,
  ): Promise<JonesUpdateState> {
    const refusal = followerTargetRefusal(
      this.state.policy,
      input.sourceSha,
      this.candidate?.source === input.sourceSha ? this.candidate.tree : undefined,
    );
    if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
    if (
      this.heldJobs.size > 0 ||
      this.busy ||
      !this.state.capability.download ||
      this.state.stagedHandle !== undefined
    )
      return this.snapshot();
    const candidate = this.candidate;
    if (
      candidate === undefined ||
      candidate.artifactId !== input.artifactId ||
      candidate.source !== input.sourceSha
    ) {
      return this.publish({
        phase: "blocked",
        message: "The selected build changed. Check for updates again.",
      });
    }
    return this.stageCandidate(candidate, this.candidateBuildStartedAt, false, operation);
  }

  /** Downloads and stages exactly `candidate` under the busy latch; the caller has chosen it. */
  private stageCandidate(
    candidate: JonesActionsCandidate,
    buildStartedAt: string | undefined,
    retiredSelection = false,
    operation?: JonesPreparationOperation,
  ): Promise<JonesUpdateState> {
    const refusal = followerTargetRefusal(this.state.policy, candidate.source, candidate.tree);
    if (refusal !== undefined)
      return Promise.resolve({ ...this.snapshot(), phase: "blocked", message: refusal });
    if (
      this.heldJobs.size > 0 ||
      this.busy ||
      !this.state.capability.download ||
      this.state.stagedHandle !== undefined
    )
      return Promise.resolve(this.snapshot());
    this.busy = true;
    const controller = new AbortController();
    let settle!: (state: JonesUpdateState) => void;
    const done = new Promise<JonesUpdateState>((resolve) => (settle = resolve));
    const forwardStop = () => controller.abort(operation?.signal.reason);
    operation?.signal.addEventListener("abort", forwardStop, { once: true });
    if (operation?.signal.aborted) forwardStop();
    this.preparation = {
      source: candidate.source,
      controller,
      done,
      adopting: false,
      ...(operation === undefined
        ? {}
        : { operationId: operation.operationId, jobId: operation.jobId }),
    };
    this.publish({
      phase: "downloading",
      message: "Downloading the selected main build…",
      provenance: candidateProvenance(candidate, buildStartedAt),
    });
    const finish = (state: JonesUpdateState) => {
      operation?.signal.removeEventListener("abort", forwardStop);
      this.preparation = undefined;
      this.busy = false;
      settle(state);
    };
    void this.prepareCandidate(
      candidate,
      buildStartedAt,
      controller.signal,
      retiredSelection,
      operation,
    )
      .then(async (state) => {
        await this.preparation?.join;
        return state;
      })
      .then(finish, (cause: unknown) => finish(this.fail(cause)));
    return done;
  }

  /** Publishes the outcome, including a failure, as the host's state. */
  private async prepareCandidate(
    candidate: JonesActionsCandidate,
    buildStartedAt: string | undefined,
    signal: AbortSignal,
    retiredSelection: boolean,
    operation?: JonesPreparationOperation,
  ): Promise<JonesUpdateState> {
    try {
      const refusal = followerTargetRefusal(this.state.policy, candidate.source, candidate.tree);
      if (refusal !== undefined) throw new Error(refusal);
      signal.throwIfAborted();
      operation?.onStarting?.();
      const staged = await this.host.prepare(candidate, {
        signal,
        ...(operation === undefined ? {} : { jobId: operation.jobId }),
        onStarted: (join) => {
          if (this.preparation !== undefined) this.preparation.join = join;
        },
        onAdopting: async () => {
          signal.throwIfAborted();
          await operation?.onAdopting();
          if (this.preparation !== undefined) this.preparation.adopting = true;
        },
        onVerifying: () => {
          if (!signal.aborted)
            this.publish({
              phase: "verifying",
              message: "Verifying and staging the downloaded build…",
            });
        },
      });
      this.clearOutcome();
      return this.publish({
        phase: "staged",
        stagedHandle: staged.stagedHandle,
        ...(staged.migrationPlan === undefined ? {} : { migrationPlan: staged.migrationPlan }),
        message: "Downloaded and verified. Install is a separate action.",
        provenance: {
          ...candidateProvenance(
            candidate,
            staged.version === candidate.version ? buildStartedAt : undefined,
          ),
          version: staged.version,
          payloadSha256: staged.payloadSha256,
        },
      });
    } catch (error) {
      const preparation = this.preparation;
      const join = await preparation?.join;
      if (join?.kind === "unreaped" || join?.kind === "cleanup-unconfirmed") {
        this.heldJobs.add(join.jobId);
        return this.publish({
          phase: "blocked",
          message:
            "Stop not confirmed: preparation exit or native cleanup is unproved. Nothing new starts until release is proved.",
        });
      }
      if (signal.reason instanceof PreparationStopRequested && preparation?.adopting !== true)
        return this.publish({
          phase: "available",
          message: "Download stopped. Nothing was installed.",
        });
      if (signal.reason instanceof PreparationSuperseded) {
        const { provenance: _provenance, ...rest } = this.state;
        this.state = rest;
        if (this.candidate?.source === candidate.source) this.candidate = undefined;
        return this.publish({
          phase: "no-new",
          message: retiredSelection
            ? RETIRED_WITHOUT_TARGET
            : "The laptop's installed build changed. Checking for it…",
        });
      }
      return this.fail(
        retiredSelection
          ? new Error(
              `${RETIRED_WITHOUT_TARGET} ${error instanceof Error ? error.message : "Preparation failed."}`,
            )
          : error,
      );
    }
  }

  async retireStagedOperation(
    input: JonesStagedRetirementInput,
    admit?: () => string | undefined,
  ): Promise<{ retired: boolean; reason?: string }> {
    if (
      this.heldJobs.size > 0 ||
      this.busy ||
      ["preparing", "installing"].includes(this.state.phase)
    )
      return { retired: false, reason: "An update operation is still active." };
    if (this.host.retireStaged === undefined)
      return { retired: false, reason: "Exact staged retirement is unavailable on this host." };
    if (
      input.environmentId !== this.state.environmentId ||
      input.currentVersion !== this.state.currentVersion ||
      (this.state.stagedHandle !== undefined &&
        (input.stagedHandle !== this.state.stagedHandle ||
          input.targetSource !== this.state.provenance?.sourceSha))
    )
      return { retired: false, reason: "The retained stage belongs to another immutable binding." };
    this.busy = true;
    try {
      if ((await this.host.installedSource()) !== input.expectedInstalledSource)
        return { retired: false, reason: "The installed source changed before staged retirement." };
      const refusal = admit?.();
      if (refusal !== undefined) return { retired: false, reason: refusal };
      // The host rechecks authoritative native absence before removing only the pointer.
      // Payload and operation evidence remain available for reconciliation.
      await this.host.retireStaged(input);
      const {
        stagedHandle: _handle,
        provenance: _provenance,
        migrationPlan: _plan,
        ...rest
      } = this.state;
      this.state = rest;
      this.candidate = undefined;
      this.candidateBuildStartedAt = undefined;
      this.publish({
        phase: "no-new",
        message: "The unaccepted staged selection was retired; its payload was retained.",
      });
      return { retired: true };
    } catch (cause) {
      return {
        retired: false,
        reason:
          cause instanceof Error
            ? cause.message
            : "The retained stage could not be retired safely.",
      };
    } finally {
      this.busy = false;
    }
  }

  async install(input: JonesUpdaterInstallInput): Promise<JonesUpdateState> {
    const refused = (state: JonesUpdateState) => {
      if (
        input.operationId !== undefined &&
        input.targetSource !== undefined &&
        input.expectedInstalledSource !== undefined
      )
        input.onBeforeDispatchRefusal?.({
          kind: "before-dispatch",
          operationId: input.operationId,
          targetSource: input.targetSource,
          expectedInstalledSource: input.expectedInstalledSource,
          stagedHandle: input.stagedHandle,
          environmentId: input.environmentId,
          currentVersion: input.currentVersion,
        });
      return state;
    };
    while (this.inflight !== undefined) await this.inflight.promise.catch(() => undefined);
    if (this.heldJobs.size > 0 || this.busy || this.state.phase === "installing")
      return refused(this.snapshot());
    if (!this.state.capability.install)
      return refused(
        this.publish({
          phase: "blocked",
          message: "A qualified launcher bootstrap is required before Install.",
        }),
      );
    if (
      input.stagedHandle !== this.state.stagedHandle ||
      input.environmentId !== this.state.environmentId ||
      input.currentVersion !== this.state.currentVersion
    ) {
      return refused(
        this.publish({
          phase: "blocked",
          message: "Install does not match this staged build and running environment.",
        }),
      );
    }
    // A manual Install skips the quiet window and spacing, never this check.
    const refusal = followerTargetRefusal(
      this.state.policy,
      this.state.provenance?.sourceSha,
      this.state.provenance?.sourceTree,
    );
    if (refusal !== undefined)
      return refused({ ...this.snapshot(), phase: "blocked", message: refusal });
    this.busy = true;
    this.clearOutcome();
    this.publish({
      phase: "preparing",
      message: "Preparing the selected installation…",
    });
    const started = Date.now();
    const messages = {
      marking: "Marking running threads for continuation…",
      verifying: "Verifying the selected runtime…",
      preflight: "Checking candidate startup readiness…",
      capturing: "Capturing planned update continuity…",
      handoff: "Waiting for the native launcher…",
    };
    let currentMessage = "Preparing the selected installation…";
    const publishElapsed = () =>
      this.publish({
        message: `${currentMessage} (${Math.floor((Date.now() - started) / 1000)}s elapsed)`,
      });
    const heartbeat = setInterval(publishElapsed, 15_000);
    try {
      const { onBeforeDispatchRefusal: _refusal, ...hostInput } = input;
      const accepted = await this.host.install(hostInput, (step) => {
        currentMessage = messages[step];
        publishElapsed();
      });
      return this.publish({
        phase: "installing",
        ...(accepted?.updateId === undefined ? {} : { updateId: accepted.updateId }),
        ...(accepted?.migrationPlan === undefined ? {} : { migrationPlan: accepted.migrationPlan }),
        message: "The native launcher accepted the installation.",
      });
    } catch (error) {
      const withdrawn = findActivationWithdrawal(error);
      if (withdrawn !== undefined)
        return this.publish({
          phase: "staged",
          message: `The automatic update was withdrawn before the launcher accepted it: ${withdrawn.message}`,
        });
      return this.fail(error);
    } finally {
      clearInterval(heartbeat);
      this.busy = false;
    }
  }
}
