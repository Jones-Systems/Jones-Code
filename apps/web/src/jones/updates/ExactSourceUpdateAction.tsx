import { prepareReviewedInstall, type JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import { executeWebJonesAction, unresolvedJonesExecution, retainUnknownJonesExecution } from "../installStatus/execution";
import { legacyConnectionsGate } from "../installStatus/entrypoints";
import {
  jonesExactSourceResultError,
  jonesInstallRefusal,
  resolveJonesExactSourceAction,
  shortJonesSource,
  type JonesAction,
  type JonesExactSourceStep,
  type JonesUpdateObservation,
} from "@t3tools/client-runtime/jones/updates";
import {
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
  readonly executor?: JonesExecutorHost;
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
  readonly host: Pick<ExactSourceHost, "readLatest" | "run" | "confirm" | "executor">;
}): Promise<ExactSourceOutcome> {
  if (unresolvedJonesExecution(environmentId) !== undefined) return { kind: "refused", message: "The previous request is not confirmed. Read its exact receipt before another action." };
  if (readTargetSource !== undefined && readTargetSource() !== targetSource)
    return { kind: "refused", message: "Nothing was sent: the laptop's verified installed build changed or is unavailable." };
  if (step.kind === "replace-exact") {
    if (!(await host.confirm(`Replace downloaded source ${shortJonesSource(step.replace.sourceSha)} on ${label} with ${shortJonesSource(targetSource)}? The old download is retired; its files are retained. Nothing installs until you choose Install.`))) return { kind: "cancelled" };
    const fresh = resolveJonesExactSourceAction({ environmentId, ...host.readLatest(), targetSource, allowReplace, allowInstall, subject, unresolvedInstall });
    if (fresh.status !== "ready" || !sameStep(step, fresh.step) || (readTargetSource !== undefined && readTargetSource() !== targetSource))
      return { kind: "refused", message: `Nothing was sent: ${label} changed while this was pending.` };
    const gate = await legacyConnectionsGate(targetSource);
    if (gate !== null) return { kind: "refused", message: gate };
    const result = await host.run(request(fresh.step, operationId)).catch(() => null);
    if (result === null || result._tag === "Failure") {
      const message = "The replacement request may have been accepted. Its effect is unknown. Read the exact outcome; nothing is resent automatically.";
      retainUnknownJonesExecution(environmentId, { kind: "unknown", message });
      return { kind: "failed", message };
    }
    const problem = jonesExactSourceResultError(fresh.step, result, targetSource);
    return problem === null ? { kind: "done", message: `Source ${shortJonesSource(targetSource)} replacement requested on ${label}.` } : { kind: "refused", message: problem };
  }
  const executor: JonesExecutorHost = host.executor ?? {
    readLatest: host.readLatest, confirm: host.confirm,
    legacy: async (input) => { const result = await host.run(input); if (result._tag !== "Success") throw new Error("Request not confirmed."); return result.value; },
    start: async () => { throw new Error("This control has no durable operation transport."); },
  };
  const reviewed = step.kind === "install" ? await prepareReviewedInstall({ environmentId, targetSource, label, host: executor, now: Date.now }) : undefined;
  if (reviewed?.kind === "refused") return reviewed.code === "cancelled" ? { kind: "cancelled" } : { kind: "refused", message: reviewed.message };
  if (readTargetSource !== undefined && readTargetSource() !== targetSource)
    return { kind: "refused", message: "Nothing was sent: the laptop's verified installed build changed or is unavailable." };
  const action = reviewed?.kind === "ready" ? reviewed.action : step.kind === "download-exact" ? { kind: "download-exact" as const, targetSource: step.targetSource } : step.kind === "download" ? { kind: "download" as const, ...step.input } : undefined;
  if (action === undefined) return { kind: "refused", message: "Review the downloaded build again." };
  const result = await executeWebJonesAction({ environmentId, targetSource, action, operationId, host: executor,
    ...(reviewed?.kind === "ready" ? { review: reviewed.review } : {}) });
  if (result.kind === "unknown") return { kind: "unknown", message: result.message, unresolvedInstall: { stagedHandle: step.kind === "install" ? step.input.stagedHandle : operationId, revision: host.readLatest().observation.state?.revision } };
  return result.kind === "accepted" ? { kind: "done", message: result.message } : { kind: "refused", message: result.message };
}

/** Confirms a Connections install and revalidates its host binding before sending. */
export async function performConfirmedInstall({
  input,
  host,
  label,
}: {
  readonly input: JonesUpdateInstallInput;
  readonly label: string;
  readonly host: Pick<ExactSourceHost, "readLatest" | "run" | "confirm" | "executor">;
}): Promise<string | null> {
  const latest = host.readLatest();
  const source = latest.observation.state?.provenance?.sourceSha;
  const refusal = jonesInstallRefusal(input, latest.observation);
  if (refusal !== undefined || !latest.canOperate || source === undefined)
    return `Nothing was sent. ${refusal ?? "Fresh downloaded source and permission are required."}`;
  const outcome = await performExactSourceStep({ environmentId: input.environmentId, label, targetSource: source,
    step: { kind: "install", input }, host });
  return outcome.kind === "cancelled" || outcome.kind === "done" ? null : `Nothing was sent or confirmed. ${outcome.message}`;
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
  const inFlight = useRef(false);
  const replacementIds = useRef(new Map<string, string>());
  const decision = resolveJonesExactSourceAction({
    environmentId,
    ...host.snapshot,
    targetSource,
    allowReplace,
    allowInstall,
    subject,
  });
  const short = shortJonesSource(targetSource);

  const unresolved = unresolvedJonesExecution(environmentId, host.snapshot.observation.state?.operation);
  const start = async () => {
    if (unresolved !== undefined || decision.status !== "ready" || inFlight.current) return;
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
          host,
        });
        // Unknown work is also retained outside the notice, independent of target and revision.
        if (outcome.kind === "unknown") setReport({ tone: "info", text: outcome.message });
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
      : unresolved !== undefined
        ? { tone: "info" as const, text: unresolved.message }
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
            unresolved !== undefined
              ? unresolved.message
              : working !== null
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
