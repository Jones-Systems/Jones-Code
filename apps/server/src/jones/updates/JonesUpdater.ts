// @effect-diagnostics globalTimers:off globalDate:off
// Promise boundary is also exercised without an Effect runtime; timers drive subscriptions and installation progress.
import type {
  JonesUpdatePolicy,
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
} from "@t3tools/contracts/jones/jonesUpdates";
import {
  JonesActionsClient,
  type JonesActionsCandidate,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
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

export interface JonesUpdaterHost {
  readonly initialState: JonesUpdateState;
  readonly installedSource: () => Promise<string>;
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
  readonly cacheRoot: string;
  readonly stage: (artifact: JonesStagedArtifact) => Promise<{
    stagedHandle: string;
    version: string;
    migrationPlan?: JonesUpdateState["migrationPlan"];
  }>;
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
      }
    | undefined;
}

/** What one check run produced; the candidate belongs to this run alone. */
interface JonesCheckRun {
  readonly state: JonesUpdateState;
  readonly candidate: JonesActionsCandidate | undefined;
}

function candidateProvenance(
  candidate: JonesActionsCandidate,
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
  };
}

export type JonesUpdaterInstallInput = JonesUpdateInstallInput & {
  readonly operationId?: string;
  /** Who asked: a person on this host, or a fleet operation. Defaults to manual. */
  readonly origin?: FollowerOrigin;
  /**
   * Checked again just before the launcher handoff; a reason withdraws an automatic activation
   * while it can still be refused, leaving the staged build ready.
   */
  readonly admit?: () => Promise<string | undefined>;
};

const FOLLOWER_WAITING = "Waiting for the laptop to report its installed build.";

/** One checker belongs to the host. Connected clients observe the same fixed staging handle. */
export class JonesUpdater {
  private state: JonesUpdateState;
  private revision = 0;
  private busy = false;
  private candidate: JonesActionsCandidate | undefined;
  /** The one check run on this host, keyed by its target (`latest` for the newest main build). */
  private inflight: { readonly key: string; readonly promise: Promise<JonesCheckRun> } | undefined;
  private readonly listeners = new Set<() => void>();

  private readonly host: JonesUpdaterHost;
  private readonly actions: Pick<JonesActionsClient, "check" | "stage">;
  constructor(
    host: JonesUpdaterHost,
    actions: Pick<JonesActionsClient, "check" | "stage"> = new JonesActionsClient(),
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
            outcome: {
              status: outcome.status === "failed" ? ("blocked" as const) : outcome.status,
              fromVersion: outcome.fromVersion,
              targetVersion: outcome.targetVersion,
              ...(outcome.reason === undefined ? {} : { reason: outcome.reason }),
            },
            ...(this.revision === 0 && this.state.stagedHandle === undefined
              ? {
                  phase: outcome.status === "failed" ? ("error" as const) : outcome.status,
                  message:
                    outcome.status === "committed"
                      ? "The selected Jones runtime was installed."
                      : outcome.status === "rolled-back"
                        ? "Installation rolled back to its retained binary and state pair."
                        : "Native installation requires reconciliation.",
                }
              : {}),
          }),
      revision: this.revision,
    };
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
      current?.leaderTree === policy?.leaderTree
    )
      return this.snapshot();
    const { policy: _previous, ...rest } = this.state;
    this.state = policy === undefined ? rest : { ...rest, policy };
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

  /** Runs one check under the busy latch held by `checkRun`; its candidate is returned, not kept. */
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
      if (result.state === "available") {
        const candidate = result.candidate;
        // A follower offers only its laptop's build, with the tree the repository records for it.
        const refusal = followerTargetRefusal(this.state.policy, candidate.source, candidate.tree);
        if (refusal !== undefined) {
          this.candidate = undefined;
          return {
            state: this.publish(
              staged ? { checkedAt } : { phase: "blocked", checkedAt, message: refusal },
            ),
            candidate: undefined,
          };
        }
        this.candidate = candidate;
        if (staged) return { state: this.publish({ checkedAt }), candidate };
        return {
          state: this.publish({
            phase: "available",
            checkedAt,
            message: "A qualified main build is ready to download.",
            provenance: candidateProvenance(candidate),
          }),
          candidate,
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
   * a different staged source is never replaced, and the same staged source is returned unchanged.
   */
  async stageExact(targetSource: string): Promise<JonesUpdateState> {
    const refusal = followerTargetRefusal(this.state.policy, targetSource);
    if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
    const settled = this.stagedOutcome(targetSource);
    if (settled !== undefined) return settled;
    const run = await this.checkRun(targetSource);
    // Download only the candidate this run returned for the target; a skipped run, or a build
    // offered by any other check, must not be staged.
    const candidate = run.candidate;
    if (run.state.phase !== "available" || candidate?.source !== targetSource)
      return run.state.phase === "available" || run.state.phase === "checking"
        ? {
            ...run.state,
            message: "The requested source is not available from the current check. Try again.",
          }
        : run.state;
    // Another request may have claimed the host while this one waited for its turn.
    while (this.inflight !== undefined) await this.inflight.promise.catch(() => undefined);
    return this.stagedOutcome(targetSource) ?? this.stageCandidate(candidate);
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

  async download(input: JonesUpdateDownloadInput): Promise<JonesUpdateState> {
    const refusal = followerTargetRefusal(
      this.state.policy,
      input.sourceSha,
      this.candidate?.source === input.sourceSha ? this.candidate.tree : undefined,
    );
    if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
    if (this.busy || !this.state.capability.download || this.state.stagedHandle !== undefined)
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
    return this.stageCandidate(candidate);
  }

  /** Downloads and stages exactly `candidate` under the busy latch; the caller has chosen it. */
  private async stageCandidate(candidate: JonesActionsCandidate): Promise<JonesUpdateState> {
    if (this.busy || !this.state.capability.download || this.state.stagedHandle !== undefined)
      return this.snapshot();
    this.busy = true;
    this.publish({ phase: "downloading", message: "Downloading the selected main build…" });
    try {
      const artifact = await this.actions.stage(candidate, this.host.cacheRoot);
      this.publish({ phase: "verifying", message: "Verifying and staging the downloaded build…" });
      const staged = await this.host.stage(artifact);
      this.clearOutcome();
      return this.publish({
        phase: "staged",
        stagedHandle: staged.stagedHandle,
        ...(staged.migrationPlan === undefined ? {} : { migrationPlan: staged.migrationPlan }),
        message: "Downloaded and verified. Install is a separate action.",
        provenance: {
          ...candidateProvenance(candidate),
          version: staged.version,
          payloadSha256: artifact.receipt.sha256,
        },
      });
    } catch (error) {
      return this.fail(error);
    } finally {
      this.busy = false;
    }
  }

  async retireStagedOperation(
    input: JonesStagedRetirementInput,
  ): Promise<{ retired: boolean; reason?: string }> {
    if (this.busy || ["preparing", "installing"].includes(this.state.phase))
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
    while (this.inflight !== undefined) await this.inflight.promise.catch(() => undefined);
    if (this.busy || this.state.phase === "installing") return this.snapshot();
    if (!this.state.capability.install)
      return this.publish({
        phase: "blocked",
        message: "A qualified launcher bootstrap is required before Install.",
      });
    if (
      input.stagedHandle !== this.state.stagedHandle ||
      input.environmentId !== this.state.environmentId ||
      input.currentVersion !== this.state.currentVersion
    ) {
      return this.publish({
        phase: "blocked",
        message: "Install does not match this staged build and running environment.",
      });
    }
    // A manual Install skips the quiet window and spacing, never this check.
    const refusal = followerTargetRefusal(
      this.state.policy,
      this.state.provenance?.sourceSha,
      this.state.provenance?.sourceTree,
    );
    if (refusal !== undefined) return { ...this.snapshot(), phase: "blocked", message: refusal };
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
      const accepted = await this.host.install(input, (step) => {
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
