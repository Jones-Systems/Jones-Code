import {
  jonesExactSourceResultError,
  resolveJonesExactSourceAction,
  shortJonesSource,
  type JonesAction,
  type JonesExactSourceStep,
  type JonesUpdateObservation,
} from "@t3tools/client-runtime/jones/updates";
import {
  isAtomCommandInterrupted,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { useRef, useState } from "react";
import { Button } from "../../components/ui/button";

/** The host an exact-source notice acts on: what it shows now and how it asks for changes. */
export interface ExactSourceHost {
  readonly snapshot: ExactSourceSnapshot;
  /** Values at call time: a confirmation or request outlives the render that started it. */
  readonly readLatest: () => ExactSourceSnapshot;
  readonly run: (
    request: JonesAction,
  ) => Promise<AtomCommandResult<JonesUpdateState | null, unknown>>;
  readonly confirm: (message: string) => Promise<boolean>;
}

interface ExactSourceSnapshot {
  readonly observation: JonesUpdateObservation;
  readonly canOperate: boolean;
}

export interface ExactSourceTarget {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  /** The running app's own full source commit; the only source this action moves a host toward. */
  readonly targetSource: string;
}

/** An install whose response was lost, bound to the host state it was sent against. */
interface UnresolvedInstall {
  readonly stagedHandle: string;
  readonly revision: number | undefined;
}

type ExactSourceOutcome =
  | { readonly kind: "done"; readonly message: string }
  | { readonly kind: "cancelled" }
  /** The host or the latest state refused; nothing was changed for this request. */
  | { readonly kind: "refused"; readonly message: string }
  /** A download request that did not finish. Downloads are idempotent, so retrying is safe. */
  | { readonly kind: "failed"; readonly message: string }
  /** An install request whose response was lost. It must be reconciled, never re-sent. */
  | {
      readonly kind: "unknown";
      readonly message: string;
      readonly unresolvedInstall: UnresolvedInstall;
    };

function request(step: JonesExactSourceStep): JonesAction {
  switch (step.kind) {
    case "download-exact":
      return { action: "downloadExact", input: { targetSource: step.targetSource } };
    case "download":
      return { action: "download", input: step.input };
    case "install":
      return { action: "install", input: step.input };
  }
}

function sameStep(left: JonesExactSourceStep, right: JonesExactSourceStep) {
  if (left.kind !== right.kind) return false;
  if (left.kind === "install" && right.kind === "install")
    return left.input.stagedHandle === right.input.stagedHandle;
  if (left.kind === "download" && right.kind === "download")
    return left.input.artifactId === right.input.artifactId;
  return true;
}

/**
 * Runs one resolved step. The decision is resolved again after any confirmation, so a host that
 * moved on while the dialog was open is refused rather than acted on from a stale render.
 */
export async function performExactSourceStep({
  environmentId,
  label,
  targetSource,
  step,
  unresolvedInstall,
  host,
}: ExactSourceTarget & {
  readonly step: JonesExactSourceStep;
  readonly unresolvedInstall?: UnresolvedInstall | undefined;
  readonly host: Pick<ExactSourceHost, "readLatest" | "run" | "confirm">;
}): Promise<ExactSourceOutcome> {
  const short = shortJonesSource(targetSource);
  if (step.kind === "install") {
    const confirmed = await host.confirm(
      `Install source ${short} on ${label}? The host restarts to apply it and disconnects briefly.`,
    );
    if (!confirmed) return { kind: "cancelled" };
  }
  const latest = host.readLatest();
  const fresh = resolveJonesExactSourceAction({
    environmentId,
    ...latest,
    targetSource,
    unresolvedInstall,
  });
  if (fresh.status !== "ready")
    return { kind: "refused", message: `Nothing was sent. ${fresh.message}` };
  if (!sameStep(step, fresh.step))
    return {
      kind: "refused",
      message: `Nothing was sent: ${label} changed while this was pending. Review the notice and try again.`,
    };
  const result = await host.run(request(fresh.step)).catch(() => null);
  if (result === null || result._tag === "Failure") {
    if (result !== null && isAtomCommandInterrupted(result)) return { kind: "cancelled" };
    if (fresh.step.kind !== "install")
      return {
        kind: "failed",
        message: "The download request did not finish. Check the host's status and try again.",
      };
    return {
      kind: "unknown",
      message:
        "The install request was sent but no response arrived. The host may be restarting; its reported outcome will appear here and the request will not be sent again.",
      unresolvedInstall: {
        stagedHandle: fresh.step.input.stagedHandle,
        revision: latest.observation.state?.revision,
      },
    };
  }
  const problem = jonesExactSourceResultError(fresh.step, result, targetSource);
  if (problem !== null) return { kind: "refused", message: problem };
  return {
    kind: "done",
    message:
      fresh.step.kind === "install"
        ? `Install requested on ${label}. Waiting for the host to restart and report the outcome.`
        : `Source ${short} download requested on ${label}.`,
  };
}

type Report = { readonly tone: "error" | "info"; readonly text: string };

/**
 * Runs `work`, then `release`, whether `work` resolved or threw, so a rejected confirmation or
 * `readLatest` still clears the click latch. The rejection propagates after `release`. This lives
 * outside the component because React Compiler cannot compile `try`/`finally` without `catch`.
 */
async function releaseAfter(work: () => Promise<void>, release: () => void): Promise<void> {
  try {
    await work();
  } finally {
    release();
  }
}

/**
 * The notice's action toward this app's exact source commit. Download and Install are separate
 * explicit steps; the notice stays mounted through every host transition so progress, refusals
 * and lost responses stay visible instead of the action disappearing.
 */
export function ExactSourceUpdateAction({
  environmentId,
  label,
  targetSource,
  host,
}: ExactSourceTarget & { readonly host: ExactSourceHost }) {
  const [working, setWorking] = useState<"download" | "install" | null>(null);
  const [report, setReport] = useState<Report | null>(null);
  const [unresolvedInstall, setUnresolvedInstall] = useState<UnresolvedInstall | undefined>();
  const inFlight = useRef(false);
  const decision = resolveJonesExactSourceAction({
    environmentId,
    ...host.snapshot,
    targetSource,
    unresolvedInstall,
  });
  const short = shortJonesSource(targetSource);

  const start = async () => {
    if (decision.status !== "ready" || inFlight.current) return;
    const step = decision.step;
    inFlight.current = true;
    setWorking(step.kind === "install" ? "install" : "download");
    setReport(null);
    await releaseAfter(
      async () => {
        const outcome = await performExactSourceStep({
          environmentId,
          label,
          targetSource,
          step,
          unresolvedInstall,
          host,
        });
        // An unknown install is explained by the resolver's "reconciling" state until the host moves.
        if (outcome.kind === "unknown") setUnresolvedInstall(outcome.unresolvedInstall);
        else if (outcome.kind === "done") setReport({ tone: "info", text: outcome.message });
        else if (outcome.kind !== "cancelled") setReport({ tone: "error", text: outcome.message });
      },
      () => {
        inFlight.current = false;
        setWorking(null);
      },
    );
  };

  const status =
    working !== null
      ? null
      : report?.tone === "error"
        ? report
        : decision.status === "unavailable"
          ? ({ tone: "info", text: decision.message } as const)
          : report;
  return (
    <>
      {status ? (
        <span
          role="status"
          className={
            status.tone === "error"
              ? "max-w-72 text-end text-destructive text-xs"
              : "max-w-72 text-end text-muted-foreground text-xs"
          }
        >
          {status.text}
        </span>
      ) : null}
      {working !== null || decision.status === "ready" ? (
        <Button size="xs" variant="ghost" disabled={working !== null} onClick={() => void start()}>
          {working === "install"
            ? "Installing…"
            : working === "download"
              ? "Downloading…"
              : decision.status === "ready" && decision.step.kind === "install"
                ? `Install ${short}`
                : `Download ${short}`}
        </Button>
      ) : null}
    </>
  );
}
