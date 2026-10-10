// @effect-diagnostics globalTimers:off globalDate:off
// Promise boundary is also exercised without an Effect runtime; timeout only bounds subscriptions.
import type {
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
} from "@t3tools/contracts/jones/jonesUpdates";
import {
  JonesActionsClient,
  type JonesActionsCandidate,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";

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
  readonly install: (request: JonesUpdateInstallInput) => Promise<void | {
    readonly updateId?: string;
    readonly migrationPlan?: JonesUpdateState["migrationPlan"];
  }>;
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

/** One checker belongs to the host. Connected clients observe the same fixed staging handle. */
export class JonesUpdater {
  private state: JonesUpdateState;
  private revision = 0;
  private busy = false;
  private candidate: JonesActionsCandidate | undefined;
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
      ...(this.state.currentVersion === undefined || this.state.provenance?.version === undefined
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

  async check(): Promise<JonesUpdateState> {
    if (
      this.busy ||
      this.state.phase === "installing" ||
      this.state.phase === "preparing" ||
      !this.state.capability.check
    )
      return this.snapshot();
    this.busy = true;
    const staged = this.state.stagedHandle !== undefined;
    if (!staged) this.publish({ phase: "checking", message: "Checking qualified main builds…" });
    try {
      const result = await this.actions.check({
        installedSource: await this.host.installedSource(),
        platform: this.host.platform,
        architecture: this.host.architecture,
      });
      const checkedAt = new Date().toISOString();
      if (result.state === "available") {
        this.candidate = result.candidate;
        if (staged) return this.publish({ checkedAt });
        const candidate = result.candidate;
        return this.publish({
          phase: "available",
          checkedAt,
          message: "A qualified main build is ready to download.",
          provenance: {
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
          },
        });
      }
      if (staged) return this.publish({ checkedAt });
      return this.publish({
        phase: result.state,
        checkedAt,
        message:
          result.state === "blocked"
            ? result.reason
            : result.state === "building"
              ? "A fresh main build is still being qualified."
              : "No newer qualified main build.",
      });
    } catch (error) {
      return staged ? this.publish({ checkedAt: new Date().toISOString() }) : this.fail(error);
    } finally {
      this.busy = false;
    }
  }

  async download(input: JonesUpdateDownloadInput): Promise<JonesUpdateState> {
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
          ...this.state.provenance!,
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

  async install(input: JonesUpdateInstallInput): Promise<JonesUpdateState> {
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
    this.busy = true;
    this.clearOutcome();
    this.publish({
      phase: "preparing",
      message: "Preparing the selected installation…",
    });
    try {
      const accepted = await this.host.install(input);
      return this.publish({
        phase: "installing",
        ...(accepted?.updateId === undefined ? {} : { updateId: accepted.updateId }),
        ...(accepted?.migrationPlan === undefined ? {} : { migrationPlan: accepted.migrationPlan }),
        message: "The native launcher accepted the installation.",
      });
    } catch (error) {
      return this.fail(error);
    } finally {
      this.busy = false;
    }
  }
}
