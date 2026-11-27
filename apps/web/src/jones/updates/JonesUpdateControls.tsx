import {
  jonesCheckOutcome,
  jonesTargetStatus,
} from "@t3tools/client-runtime/jones/updates/target-status";
import {
  isProvisionalUnsupportedReceipt,
  isSettledOperationReceipt,
  validateReceipt,
} from "@t3tools/client-runtime/jones/install-plan";
import {
  prepareReviewedInstall,
  resolveDownloadAction,
  type JonesDownloadResolution,
  type JonesReviewedInstall,
} from "@t3tools/client-runtime/jones/updates/executor";
import {
  connectionExecutorHost,
  readUnresolvedJonesExecution,
} from "../installStatus/connectionHost";
import {
  executeWebJonesAction,
  unresolvedJonesExecution,
  pendingJonesOperationId,
  reconcileJonesExecution,
  stopWebJonesDownload,
} from "../installStatus/execution";
import { useJonesExecutionJournal } from "../installStatus/executionJournal";
import { useWebReviewAuthority } from "../installStatus/webReviewAuthority";
import { Button } from "../../components/ui/button";
import { randomUUID } from "../../lib/utils";
import {
  jonesUpdateActionError,
  jonesInstallRefusal,
  compareJonesSource,
  isJonesOperationNotSentError,
} from "@t3tools/client-runtime/jones/updates";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  AuthOrchestrationOperateScope,
  sessionGrantsScope,
  type EnvironmentId,
} from "@t3tools/contracts";
import {
  verifiedRunningLaptop,
  VERIFIED_LAPTOP_MAX_AGE_MS,
} from "@t3tools/contracts/jones/installPlan";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import * as Effect from "effect/Effect";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import { AsyncResult, Atom } from "effect/reactivity";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { ProductionExactSourceUpdateAction } from "./ProductionExactSourceUpdateAction";
import { jonesUpdates } from "./jonesUpdates";
import {
  JonesBuildStatus,
  type JonesBuildRequest,
  type JonesBuildStatusLayout,
} from "./JonesBuildStatus";
import {
  installedComparisonFor,
  installedSourcePair,
  readInstalledSourceAncestry,
} from "./installedSourceObservation";
import type { JonesSourceComparison } from "../fleetUpdates/sourceCurrency";
import { connectionAtomRuntime } from "../../connection/runtime";
import { isElectron } from "../../env";
import { useNowAfter } from "../../hooks/useDeadline";
import { installStatusStore, type InstallStatusSnapshot } from "../installStatus/panelUi";
import { useInstallPlan } from "../installStatus/useInstallPlan";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

function runningLaptop({ native, nativeRefusal }: InstallStatusSnapshot, now: number) {
  return isElectron &&
    native?.view.matchRunningLaptop === 1 &&
    native.readOnly === undefined &&
    nativeRefusal === null
    ? verifiedRunningLaptop(native.laptop, native.laptop.environmentId, now)
    : undefined;
}

type AncestryAtom = Atom.Atom<AsyncResult.AsyncResult<JonesSourceComparison | undefined, unknown>>;

const noAncestryAtom: AncestryAtom = Atom.make(AsyncResult.success(undefined));

/**
 * Keyed by base and head commits (40 characters each) then the local environment, so a result
 * only ever answers the exact pair it was asked about. Proven ancestry never changes; a failed
 * lookup is retried by Check rather than by a poller.
 */
const installedSourceAncestryAtom = Atom.family((key: string): AncestryAtom => {
  const pair = { base: key.slice(0, 40), head: key.slice(40, 80) };
  const environmentId = key.slice(80) as EnvironmentId;
  return connectionAtomRuntime
    .atom((get) => {
      const prepared = get(environmentSession.preparedConnectionValueAtom(environmentId));
      return prepared._tag === "None" || prepared.value.environmentId !== environmentId
        ? Effect.sync(() => undefined)
        : readInstalledSourceAncestry(prepared.value, pair).pipe(
            Effect.provide(layerRemoteHttpClient(globalThis.fetch)),
          );
    })
    .pipe(
      Atom.setIdleTTL(5 * 60_000),
      Atom.withLabel(`jones-installed-source-ancestry:${pair.base}:${pair.head}`),
    );
});

/** Jones main-build status for one host. Download and Install stay separate host actions. */
export function JonesUpdateControls({
  environmentId,
  layout = "inline",
  compareWithLaptop = false,
  extraAction,
}: {
  readonly environmentId: EnvironmentId;
  readonly layout?: JonesBuildStatusLayout;
  /** Compares this host with the verified laptop build and offers an explicit matching download. */
  readonly compareWithLaptop?: boolean;
  /** A compact control placed before Check and Download, such as automatic updates. */
  readonly extraAction?: ReactNode;
}) {
  const observed = useAtomValue(jonesUpdates.observation(environmentId));
  const journal = useJonesExecutionJournal();
  const stopDuty = journal.duties.find(
    (d) => d.environmentId === environmentId && d.kind === "stop",
  );
  const settled = [...journal.history]
    .reverse()
    .find(
      (d) =>
        d.environmentId === environmentId &&
        d.receipt !== undefined &&
        (observed.state?.operation === undefined ||
          observed.state.operation.operationId === d.operationId),
    );
  const retained =
    stopDuty?.receipt ??
    journal.duties.find((d) => d.environmentId === environmentId)?.receipt ??
    settled?.receipt;
  const liveReceipt = observed.freshness === "fresh" ? observed.state?.operation : undefined;
  const freshSettlement =
    liveReceipt !== undefined &&
    retained !== undefined &&
    isSettledOperationReceipt(liveReceipt) &&
    validateReceipt(retained, liveReceipt);
  const observation =
    observed.state === null || retained === undefined || freshSettlement
      ? observed
      : {
          ...observed,
          state: { ...observed.state, operation: retained },
        };
  const awaitingLedgerRead =
    retained !== undefined &&
    liveReceipt?.operationId === retained.operationId &&
    isProvisionalUnsupportedReceipt(retained);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Reconcile exact fresh receipts after any provisional refusal has been verified.
  useEffect(() => {
    if (liveReceipt !== undefined && !awaitingLedgerRead)
      void reconcileJonesExecution(environmentId, liveReceipt).catch(() => undefined);
  }, [liveReceipt, environmentId, awaitingLedgerRead]);
  const installStatus = useInstallPlan();
  const observedAt = Date.parse(installStatus.native?.laptop.observedAt ?? "");
  const laptopNow = useNowAfter(
    compareWithLaptop && Number.isFinite(observedAt)
      ? observedAt + VERIFIED_LAPTOP_MAX_AGE_MS
      : null,
  );
  const laptop = compareWithLaptop ? runningLaptop(installStatus, laptopNow) : undefined;
  const laptopSource =
    laptop === undefined
      ? undefined
      : { repository: laptop.target.repository, sha: laptop.target.sourceSha };
  const hostInstalledSource =
    observation.freshness === "fresh" ? observation.state?.installedSource : undefined;
  const pair = compareWithLaptop
    ? installedSourcePair(hostInstalledSource, laptopSource)
    : undefined;
  const ancestryAtom: AncestryAtom =
    pair === undefined || laptop === undefined
      ? noAncestryAtom
      : installedSourceAncestryAtom(`${pair.base}${pair.head}${laptop.environmentId}`);
  const ancestryResult = useAtomValue(ancestryAtom);
  const refreshAncestry = useAtomRefresh(ancestryAtom);
  const ancestry = AsyncResult.isSuccess(ancestryResult)
    ? ancestryResult.value
    : AsyncResult.isFailure(ancestryResult)
      ? undefined
      : ("loading" as const);
  const action = useAtomCommand(jonesUpdates.action, { reportFailure: false });
  const [pending, setPending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const stop = useAtomCommand(jonesUpdates.stop, { reportFailure: false });
  const session = useAtomValue(environmentSession.sessionStateAtom(environmentId));
  const allowed =
    session._tag === "Success" &&
    !session.waiting &&
    session.value.authenticated &&
    sessionGrantsScope(session.value, AuthOrchestrationOperateScope);
  const authority = useWebReviewAuthority(environmentId, allowed);
  const status = jonesTargetStatus(observation, {
    canOperate: allowed,
    ...(observation.state?.operation?.correlation === undefined
      ? {}
      : { planId: observation.state.operation.correlation.planId }),
  });
  const inFlight = useRef(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const run = async (input: JonesBuildRequest) => {
    if (inFlight.current) return;
    inFlight.current = true;
    setPending(true);
    setActionError(null);
    await (async () => {
      if (input.action === "install" || input.action === "download") {
        const host = connectionExecutorHost(
          environmentId,
          () => {
            const session = appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId));
            return {
              observation: appAtomRegistry.get(jonesUpdates.observation(environmentId)),
              canOperate:
                authority.isCurrent() &&
                session._tag === "Success" &&
                session.value.authenticated &&
                sessionGrantsScope(session.value, AuthOrchestrationOperateScope),
            };
          },
          async (request) => {
            const result = await action({ environmentId, input: request });
            if (result._tag !== "Success") throw new Error("Request not confirmed.");
            return result.value;
          },
          authority,
        );
        if (input.action === "install") {
          const refusal = jonesInstallRefusal(input.input, host.readLatest().observation);
          if (refusal !== undefined) {
            setActionError(refusal);
            return;
          }
        }
        const targetSource = host.readLatest().observation.state?.provenance?.sourceSha;
        if (targetSource === undefined) {
          setActionError("Fresh downloaded or offered source is required.");
          return;
        }
        const proposed: JonesDownloadResolution & { readonly review?: JonesReviewedInstall } =
          input.action === "install"
            ? await prepareReviewedInstall({
                environmentId,
                targetSource,
                label: "this host",
                host,
                now: Date.now,
                ...(input.input.continueRunningThreads === undefined
                  ? {}
                  : { continueRunningThreads: input.input.continueRunningThreads }),
              })
            : resolveDownloadAction({ environmentId, targetSource, ...host.readLatest() });
        if (proposed.kind !== "ready") {
          setActionError(proposed.message);
          return;
        }
        const result = await executeWebJonesAction({
          environmentId,
          targetSource,
          action: proposed.action,
          operationId: randomUUID(),
          host,
          ...(proposed.review === undefined ? {} : { review: proposed.review }),
        });
        setActionError(result.message);
        return;
      }
      if (input.action === "check") {
        await readUnresolvedJonesExecution(environmentId);
        if (unresolvedJonesExecution(environmentId) !== undefined) return;
      }
      if (!authority.isCurrent()) return;
      setActionError(jonesUpdateActionError(await action({ environmentId, input })));
      // Check is the explicit refresh: retry an ancestry this laptop's server could not prove.
      if (input.action === "check" && pair !== undefined && ancestry === undefined)
        refreshAncestry();
    })()
      .catch((error: unknown) =>
        setActionError(
          error instanceof Error
            ? error.message
            : "Request not confirmed. Read its exact receipt before continuing.",
        ),
      )
      .finally(() => {
        inFlight.current = false;
        setPending(false);
      });
  };
  const comparison = compareWithLaptop
    ? compareJonesSource(
        laptopSource,
        observation.freshness === "fresh" ? observation.state?.provenance : undefined,
      )
    : null;
  const installedComparison = compareWithLaptop
    ? installedComparisonFor({ host: hostInstalledSource, laptop: laptopSource, ancestry })
    : null;
  const showMatch =
    compareWithLaptop &&
    isElectron &&
    environmentId !== installStatus.native?.laptop.environmentId &&
    !(laptopSource !== undefined && hostInstalledSource?.sha === laptopSource.sha) &&
    !(
      laptopSource !== undefined &&
      observation.freshness === "fresh" &&
      observation.state?.stagedHandle !== undefined &&
      observation.state.provenance?.sourceSha === laptopSource.sha
    );
  return (
    <div className="min-w-0 space-y-1">
      <JonesBuildStatus
        environmentId={environmentId}
        state={observation.state}
        freshness={observation.freshness}
        freshnessMessage={observation.message}
        comparison={comparison}
        installedComparison={installedComparison}
        pending={
          pending || unresolvedJonesExecution(environmentId, undefined, journal) !== undefined
        }
        actionError={actionError}
        layout={layout}
        check={jonesCheckOutcome(observation.state)}
        operation={
          status.operation === undefined
            ? undefined
            : { ...status.operation, headline: status.headline }
        }
        extraAction={
          showMatch || extraAction !== undefined ? (
            <>
              {extraAction}
              {showMatch ? (
                <ProductionExactSourceUpdateAction
                  environmentId={environmentId}
                  label="this host"
                  targetSource={laptopSource?.sha ?? ""}
                  matchRunningGeneration={laptop?.generation}
                  readTargetSource={() => {
                    const latest = runningLaptop(installStatusStore.get(), Date.now());
                    return latest !== undefined &&
                      laptop !== undefined &&
                      latest.environmentId === laptop.environmentId &&
                      latest.generation === laptop.generation
                      ? latest.target.sourceSha
                      : undefined;
                  }}
                  subject="this laptop"
                  allowInstall={false}
                  buttonLabel="Match laptop"
                  compact
                />
              ) : null}
            </>
          ) : undefined
        }
        onRequest={(request) => void run(request)}
      />
      <div className="flex flex-wrap justify-end gap-1.5 empty:hidden">
        {status.actions.stop ? (
          <Button
            size="xs"
            variant="outline"
            disabled={
              stopping ||
              stopDuty !== undefined ||
              settled?.kind === "stop" ||
              !journal.ready ||
              journal.fault !== undefined
            }
            onClick={() => {
              const receipt = observation.state?.operation;
              if (receipt === undefined || stopping || stopDuty !== undefined) return;
              setStopping(true);
              void stopWebJonesDownload({
                environmentId,
                receipt,
                uuid: randomUUID,
                canOperate: () =>
                  authority.isCurrent() &&
                  (() => {
                    const latest = appAtomRegistry.get(
                      environmentSession.sessionStateAtom(environmentId),
                    );
                    return (
                      latest._tag === "Success" &&
                      !latest.waiting &&
                      latest.value.authenticated &&
                      sessionGrantsScope(latest.value, AuthOrchestrationOperateScope)
                    );
                  })(),
                send: async (input, beforeSend) => {
                  const result = await stop({
                    environmentId,
                    input: { action: "operationStop", input, beforeSend },
                  });
                  if (result._tag === "Success") return result.value;
                  const error = squashAtomCommandFailure(result);
                  if (isJonesOperationNotSentError(error)) throw error;
                  throw new Error("Stop is not confirmed.");
                },
              })
                .then(setActionError)
                .catch(() => setActionError("Stop is not confirmed. Read its exact operation."))
                .finally(() => setStopping(false));
            }}
          >
            Stop download
          </Button>
        ) : null}
        {pendingJonesOperationId(environmentId, journal) !== undefined ||
        journal.duties.some((d) => d.environmentId === environmentId && d.kind === "native") ? (
          <Button
            size="xs"
            variant="outline"
            onClick={() => {
              void readUnresolvedJonesExecution(environmentId).catch(() =>
                setActionError("The exact outcome is unavailable. Nothing will be resent."),
              );
            }}
          >
            Read operation status
          </Button>
        ) : null}
      </div>
    </div>
  );
}
