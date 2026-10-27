import {
  jonesExactSourceResultError,
  jonesInstallRefusal,
  jonesUpdateActionError,
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
import type {
  JonesUpdateInstallInput,
  JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";
import { useId, useRef, useState } from "react";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { randomUUID } from "../../lib/utils";

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
  /** The verified installed source this action moves a host toward. */
  readonly targetSource: string;
  /** A laptop target must still come from the same fresh, verified local connection. */
  readonly readTargetSource?: (() => string | undefined) | undefined;
  readonly allowReplace?: boolean | undefined;
  readonly allowInstall?: boolean | undefined;
  readonly subject?: "this app" | "this laptop" | undefined;
  readonly buttonLabel?: string | undefined;
  readonly compact?: boolean | undefined;
}

/** An install whose response was lost, bound to the host state it was sent against. */
interface UnresolvedInstall {
  readonly stagedHandle: string;
  readonly revision: number | undefined;
}

type ExactSourceOutcome =
  | { readonly kind: "done"; readonly message: string }
  | { readonly kind: "cancelled" }
  /** The latest state refused the action, or the host reported a refusal or partial failure. */
  | { readonly kind: "refused"; readonly message: string }
  /** A download or replacement with no acknowledged result; the next click resolves fresh state. */
  | { readonly kind: "failed"; readonly message: string }
  /** An install request whose response was lost. It must be reconciled, never re-sent. */
  | {
      readonly kind: "unknown";
      readonly message: string;
      readonly unresolvedInstall: UnresolvedInstall;
    };

function request(step: JonesExactSourceStep, operationId: string): JonesAction {
  switch (step.kind) {
    case "download-exact":
      return { action: "downloadExact", input: { targetSource: step.targetSource } };
    case "replace-exact":
      return {
        action: "downloadExact",
        input: { targetSource: step.targetSource, replace: { ...step.replace, operationId } },
      };
    case "download":
      return { action: "download", input: step.input };
    case "install":
      return { action: "install", input: step.input };
  }
}

function sameStep(left: JonesExactSourceStep, right: JonesExactSourceStep) {
  if (left.kind !== right.kind) return false;
  if (left.kind === "install" && right.kind === "install")
    return (
      left.input.stagedHandle === right.input.stagedHandle &&
      left.input.environmentId === right.input.environmentId &&
      left.input.currentVersion === right.input.currentVersion
    );
  if (left.kind === "replace-exact" && right.kind === "replace-exact")
    return (
      left.targetSource === right.targetSource &&
      left.replace.stagedHandle === right.replace.stagedHandle &&
      left.replace.sourceSha === right.replace.sourceSha &&
      left.replace.environmentId === right.replace.environmentId &&
      left.replace.currentVersion === right.replace.currentVersion &&
      left.replace.expectedInstalledSource === right.replace.expectedInstalledSource
    );
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
  readTargetSource,
  allowReplace,
  allowInstall,
  subject,
  step,
  operationId = randomUUID(),
  unresolvedInstall,
  host,
}: ExactSourceTarget & {
  readonly step: JonesExactSourceStep;
  readonly operationId?: string | undefined;
  readonly unresolvedInstall?: UnresolvedInstall | undefined;
  readonly host: Pick<ExactSourceHost, "readLatest" | "run" | "confirm">;
}): Promise<ExactSourceOutcome> {
  const short = shortJonesSource(targetSource);
  if (step.kind === "install") {
    const confirmed = await host.confirm(
      `Install source ${short} on ${label}? The host restarts to apply it and disconnects briefly.`,
    );
    if (!confirmed) return { kind: "cancelled" };
  } else if (step.kind === "replace-exact") {
    const confirmed = await host.confirm(
      `Replace downloaded source ${shortJonesSource(step.replace.sourceSha)} on ${label} with ${short}? The old download is retired; its files are retained. Nothing installs until you choose Install.`,
    );
    if (!confirmed) return { kind: "cancelled" };
  }
  if (readTargetSource !== undefined && readTargetSource() !== targetSource)
    return {
      kind: "refused",
      message:
        "Nothing was sent: the laptop's verified installed build changed or is unavailable. Review its current build and try again.",
    };
  const latest = host.readLatest();
  const fresh = resolveJonesExactSourceAction({
    environmentId,
    ...latest,
    targetSource,
    allowReplace,
    allowInstall,
    subject,
    unresolvedInstall,
  });
  if (fresh.status !== "ready")
    return { kind: "refused", message: `Nothing was sent. ${fresh.message}` };
  if (!sameStep(step, fresh.step))
    return {
      kind: "refused",
      message: `Nothing was sent: ${label} changed while this was pending. Review the notice and try again.`,
    };
  const result = await host.run(request(fresh.step, operationId)).catch(() => null);
  if (result === null || result._tag === "Failure") {
    if (result !== null && isAtomCommandInterrupted(result)) return { kind: "cancelled" };
    if (fresh.step.kind !== "install")
      return {
        kind: "failed",
        message:
          fresh.step.kind === "replace-exact"
            ? "The replacement request was sent but no response arrived. Its effect is unknown. Check the host's current status before trying again; nothing is resent automatically."
            : "The download request did not finish. Check the host's status and try again.",
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

/** Confirms a Connections install and revalidates its host binding before sending. */
export async function performConfirmedInstall({
  input,
  host,
  label,
}: {
  readonly input: JonesUpdateInstallInput;
  readonly label: string;
  readonly host: Pick<ExactSourceHost, "readLatest" | "run" | "confirm">;
}): Promise<string | null> {
  if (
    !(await host.confirm(
      `Install the downloaded build on ${label}? The host restarts and disconnects briefly.`,
    ))
  )
    return null;
  const latest = host.readLatest();
  const refusal = jonesInstallRefusal(input, latest.observation);
  if (refusal !== undefined || !latest.canOperate)
    return `Nothing was sent. ${refusal ?? "Your session can no longer change updates on this host."}`;
  return jonesUpdateActionError(await host.run({ action: "install", input }));
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
 * An action toward a verified installed source commit. Download and Install are separate
 * explicit steps; the notice stays mounted through every host transition so progress, refusals
 * and lost responses stay visible instead of the action disappearing.
 */
export function ExactSourceUpdateAction({
  environmentId,
  label,
  targetSource,
  readTargetSource,
  allowReplace = false,
  allowInstall = true,
  subject,
  buttonLabel,
  compact = false,
  host,
}: ExactSourceTarget & { readonly host: ExactSourceHost }) {
  const [working, setWorking] = useState<"download" | "install" | null>(null);
  const [report, setReport] = useState<Report | null>(null);
  const [unresolvedInstall, setUnresolvedInstall] = useState<UnresolvedInstall | undefined>();
  const inFlight = useRef(false);
  const replacementIds = useRef(new Map<string, string>());
  const decision = resolveJonesExactSourceAction({
    environmentId,
    ...host.snapshot,
    targetSource,
    allowReplace,
    allowInstall,
    subject,
    unresolvedInstall,
  });
  const short = shortJonesSource(targetSource);

  const start = async () => {
    if (decision.status !== "ready" || inFlight.current) return;
    const step = decision.step;
    let operationId: string | undefined;
    if (step.kind === "replace-exact") {
      const key = JSON.stringify([environmentId, step]);
      operationId = replacementIds.current.get(key) ?? randomUUID();
      replacementIds.current.set(key, operationId);
    }
    inFlight.current = true;
    setWorking(step.kind === "install" ? "install" : "download");
    setReport(null);
    await releaseAfter(
      async () => {
        const outcome = await performExactSourceStep({
          environmentId,
          label,
          targetSource,
          readTargetSource,
          allowReplace,
          allowInstall,
          subject,
          operationId,
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
      {status && (!compact || report !== null) ? (
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
      {working !== null || decision.status === "ready" || buttonLabel !== undefined ? (
        <ExactActionButton
          reason={
            working !== null
              ? "An update request is in progress."
              : decision.status === "unavailable"
                ? decision.message
                : undefined
          }
          onClick={() => void start()}
          label={
            working === "install"
              ? "Installing…"
              : working === "download"
                ? "Downloading…"
                : (buttonLabel ??
                  (decision.status === "ready" && decision.step.kind === "install"
                    ? `Install ${short}`
                    : decision.status === "ready" && decision.step.kind === "replace-exact"
                      ? `Replace with ${short}`
                      : `Download ${short}`))
          }
        />
      ) : null}
    </>
  );
}

function ExactActionButton({
  reason,
  label,
  onClick,
}: {
  readonly reason: string | undefined;
  readonly label: string;
  readonly onClick: () => void;
}) {
  const reasonId = useId();
  return (
    <Tooltip disabled={reason === undefined}>
      <TooltipTrigger
        render={
          <Button
            size="xs"
            variant="outline"
            aria-disabled={reason === undefined ? undefined : true}
            aria-describedby={reason === undefined ? undefined : reasonId}
            onClick={() => {
              if (reason === undefined) onClick();
            }}
          >
            {label}
          </Button>
        }
      />
      <TooltipPopup side="top" role="tooltip" id={reasonId}>
        {reason}
      </TooltipPopup>
    </Tooltip>
  );
}
