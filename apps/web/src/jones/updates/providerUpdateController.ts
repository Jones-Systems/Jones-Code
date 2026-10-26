import type { EnvironmentId, ServerProvider } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import {
  canOneClickUpdateProviderCandidate,
  type ProviderUpdateCandidate,
} from "@t3tools/client-runtime/jones/provider-updates";
import {
  getProviderUpdateRunOutcome,
  type ProviderUpdateRun,
} from "../../components/ProviderUpdateLaunchNotification.logic";

export interface ProviderUpdateMachine {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly candidates: ReadonlyArray<ProviderUpdateCandidate>;
}
export interface ProviderUpdateAttempt {
  readonly generation: number;
  readonly machine: ProviderUpdateMachine;
  readonly phase: "running" | "succeeded" | "failed" | "unchanged" | "interrupted";
  readonly runs: ReadonlyArray<ProviderUpdateRun>;
  readonly message: string;
}

// Keep captured progress until an attempt finishes. A new target or advertised
// version then supersedes terminal feedback, while refresh/reorder/removal of
// completed targets preserves it.
export function providerUpdatePresentationAttempt(
  attempt: ProviderUpdateAttempt | undefined,
  candidates: ReadonlyArray<ProviderUpdateCandidate>,
): ProviderUpdateAttempt | undefined {
  if (!attempt || attempt.phase === "running") return attempt;
  const basis = (candidate: ProviderUpdateCandidate) =>
    JSON.stringify([
      candidate.driver,
      candidate.instanceId,
      candidate.versionAdvisory.latestVersion,
      candidate.versionAdvisory.updateCommand,
      candidate.versionAdvisory.canUpdate,
    ]);
  const captured = new Set(attempt.machine.candidates.map(basis));
  return candidates.some((candidate) => !captured.has(basis(candidate))) ? undefined : attempt;
}

type Update = (input: {
  environmentId: EnvironmentId;
  input: { provider: ServerProvider["driver"]; instanceId: ServerProvider["instanceId"] };
}) => Promise<ProviderUpdateRun["result"]>;

const failed = (
  machine: ProviderUpdateMachine,
  target: ProviderUpdateCandidate,
  message: string,
): ProviderUpdateRun => ({
  environmentId: machine.environmentId,
  machineLabel: machine.label,
  driver: target.driver,
  instanceId: target.instanceId,
  result: AsyncResult.failure(Cause.die(new Error(message))),
});

export function createProviderUpdateController(deps: {
  readonly readCurrent: (environmentId: EnvironmentId) => {
    readonly connected: boolean;
    readonly permitted: boolean;
    readonly providers: ReadonlyArray<ServerProvider>;
  };
  readonly timeoutMs?: number;
}) {
  let snapshot: ReadonlyMap<EnvironmentId, ProviderUpdateAttempt> = new Map();
  const listeners = new Set<() => void>();
  const pending = new Map<EnvironmentId, Promise<ReadonlyArray<ProviderUpdateRun>>>();
  const publish = (environmentId: EnvironmentId, attempt: ProviderUpdateAttempt) => {
    snapshot = new Map(snapshot).set(environmentId, attempt);
    for (const listener of listeners) listener();
  };
  const start = (
    machine: ProviderUpdateMachine,
    update: Update,
  ): Promise<ReadonlyArray<ProviderUpdateRun>> => {
    const existing = pending.get(machine.environmentId);
    if (existing) return existing;
    const generation = (snapshot.get(machine.environmentId)?.generation ?? 0) + 1;
    const attempt: ProviderUpdateAttempt = {
      generation,
      machine,
      phase: "running",
      runs: [],
      message: "Updating providers…",
    };
    let resolve!: (runs: ReadonlyArray<ProviderUpdateRun>) => void;
    const promise = new Promise<ReadonlyArray<ProviderUpdateRun>>((done) => {
      resolve = done;
    });
    pending.set(machine.environmentId, promise);
    publish(machine.environmentId, attempt);
    const isCurrent = () => {
      const current = snapshot.get(machine.environmentId);
      return current?.generation === generation && current.phase === "running";
    };
    const finish = (runs: ReadonlyArray<ProviderUpdateRun>, timedOut = false) => {
      if (!isCurrent()) return;
      const outcomes = runs.map(getProviderUpdateRunOutcome);
      const errors = outcomes.flatMap((outcome) =>
        outcome.status === "failed" || outcome.status === "unchanged" ? [outcome.line] : [],
      );
      const phase =
        timedOut || outcomes.some((outcome) => outcome.status === "failed")
          ? "failed"
          : outcomes.some((outcome) => outcome.status === "unchanged")
            ? "unchanged"
            : outcomes.some((outcome) => outcome.status === "interrupted")
              ? "interrupted"
              : runs.length === machine.candidates.length &&
                  runs.length > 0 &&
                  outcomes.every((outcome) => outcome.status === "updated")
                ? "succeeded"
                : "failed";
      const message = timedOut
        ? "Update timed out. Work may still be running; refresh provider settings before retrying."
        : phase === "unchanged"
          ? `Provider update could not be verified. ${errors.join("\n")}`
          : phase === "interrupted"
            ? "Provider update interrupted. Refresh provider settings before retrying."
            : phase === "succeeded"
              ? "Providers updated. New sessions will use the updated providers."
              : errors.join("\n") || "Provider update did not finish.";
      pending.delete(machine.environmentId);
      publish(machine.environmentId, { ...attempt, phase, runs, message });
      resolve(runs);
    };
    const timeout = setTimeout(
      () => {
        if (!isCurrent()) return;
        const completed = snapshot.get(machine.environmentId)?.runs ?? [];
        const done = new Set(completed.map((run) => run.instanceId));
        finish(
          [
            ...completed,
            ...machine.candidates
              .filter((candidate) => !done.has(candidate.instanceId))
              .map((candidate) =>
                failed(machine, candidate, "Provider update timed out; its result is unknown."),
              ),
          ],
          true,
        );
      },
      deps.timeoutMs ?? 6 * 60_000,
    );
    void Promise.all(
      machine.candidates.map(async (candidate): Promise<ProviderUpdateRun> => {
        let run: ProviderUpdateRun;
        try {
          const current = deps.readCurrent(machine.environmentId);
          const reason = !current.connected
            ? "Connect this environment before updating providers."
            : !current.permitted
              ? "This connection cannot manage provider accounts."
              : !canOneClickUpdateProviderCandidate(candidate, current.providers)
                ? "This provider update is no longer available. Refresh provider settings."
                : null;
          run =
            reason === null
              ? {
                  environmentId: machine.environmentId,
                  machineLabel: machine.label,
                  driver: candidate.driver,
                  instanceId: candidate.instanceId,
                  result: await update({
                    environmentId: machine.environmentId,
                    input: { provider: candidate.driver, instanceId: candidate.instanceId },
                  }),
                }
              : failed(machine, candidate, reason);
        } catch (error) {
          run = failed(
            machine,
            candidate,
            error instanceof Error ? error.message : "Provider update failed.",
          );
        }
        if (isCurrent()) {
          const prior = snapshot.get(machine.environmentId)!;
          publish(machine.environmentId, { ...prior, runs: [...prior.runs, run] });
        }
        return run;
      }),
    ).then((runs) => {
      clearTimeout(timeout);
      finish(runs);
    });
    return promise;
  };
  return {
    start,
    getSnapshot: () => snapshot,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
