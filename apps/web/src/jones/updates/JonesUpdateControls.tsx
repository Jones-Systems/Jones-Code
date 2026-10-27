import { jonesTargetStatus } from "@t3tools/client-runtime/jones/updates/target-status";
import { prepareReviewedInstall, resolveDownloadAction } from "@t3tools/client-runtime/jones/updates/executor";
import { connectionExecutorHost, readUnresolvedJonesExecution } from "../installStatus/connectionHost";
import { executeWebJonesAction, unresolvedJonesExecution } from "../installStatus/execution";
import { Button } from "../../components/ui/button";
import { randomUUID } from "../../lib/utils";
import { jonesUpdateActionError, jonesInstallRefusal, compareJonesSource } from "@t3tools/client-runtime/jones/updates";
import {
  AuthOrchestrationOperateScope,
  sessionGrantsScope,
  type EnvironmentId,
} from "@t3tools/contracts";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import * as Effect from "effect/Effect";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import { AsyncResult, Atom } from "effect/reactivity";
import { useRef, useState } from "react";
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
import { readPrimaryEnvironmentTarget } from "../../environments/primary/target";
import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

const noLaptopSourceAtom = Atom.make(() => undefined);

/** This laptop's installed source and the verified local connection that reported it. */
const laptopInstalledSourceAtom = Atom.make((get) => {
  if (!isElectron) return undefined;
  const environmentId = get(primaryEnvironmentIdAtom);
  if (environmentId === null) return undefined;
  try {
    const localTarget = readPrimaryEnvironmentTarget();
    if (localTarget?.source !== "desktop-managed") return undefined;
    const prepared = get(environmentSession.preparedConnectionValueAtom(environmentId));
    if (prepared._tag === "None" || prepared.value.target._tag !== "PrimaryConnectionTarget")
      return undefined;
    const target = prepared.value.target;
    if (
      new URL(target.httpBaseUrl).toString() !==
        new URL(localTarget.target.httpBaseUrl).toString() ||
      new URL(target.wsBaseUrl).toString() !== new URL(localTarget.target.wsBaseUrl).toString()
    )
      return undefined;
  } catch {
    return undefined;
  }
  const observation = get(jonesUpdates.observation(environmentId));
  const source = observation.freshness === "fresh" ? observation.state?.installedSource : undefined;
  return source === undefined ? undefined : { environmentId, source };
});

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
}: {
  readonly environmentId: EnvironmentId;
  readonly layout?: JonesBuildStatusLayout;
  /** Compares this host with the verified laptop build and offers an explicit matching download. */
  readonly compareWithLaptop?: boolean;
}) {
  const observation = useAtomValue(jonesUpdates.observation(environmentId));
  const laptop = useAtomValue(compareWithLaptop ? laptopInstalledSourceAtom : noLaptopSourceAtom);
  const hostInstalledSource =
    observation.freshness === "fresh" ? observation.state?.installedSource : undefined;
  const pair = compareWithLaptop
    ? installedSourcePair(hostInstalledSource, laptop?.source)
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
  const status = jonesTargetStatus(observation, { canOperate: session._tag === "Success" && session.value.authenticated && sessionGrantsScope(session.value, AuthOrchestrationOperateScope), ...(observation.state?.operation?.correlation === undefined ? {} : { planId: observation.state.operation.correlation.planId }) });
  const inFlight = useRef(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const run = async (input: JonesBuildRequest) => {
    if (inFlight.current) return;
    inFlight.current = true;
    setPending(true);
    setActionError(null);
    await (async () => {
      if (input.action === "install" || input.action === "download") {
        const host = connectionExecutorHost(environmentId, () => {
          const session = appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId));
          return { observation: appAtomRegistry.get(jonesUpdates.observation(environmentId)), canOperate: session._tag === "Success" && session.value.authenticated && sessionGrantsScope(session.value, AuthOrchestrationOperateScope) };
        }, async (request) => { const result = await action({ environmentId, input: request }); if (result._tag !== "Success") throw new Error("Request not confirmed."); return result.value; });
        if (input.action === "install") { const refusal = jonesInstallRefusal(input.input, host.readLatest().observation); if (refusal !== undefined) { setActionError(refusal); return; } }
        const targetSource = host.readLatest().observation.state?.provenance?.sourceSha;
        if (targetSource === undefined) { setActionError("Fresh downloaded or offered source is required."); return; }
        const proposed = input.action === "install" ? await prepareReviewedInstall({ environmentId, targetSource, label: "this host", host, now: Date.now,
          ...(input.input.continueRunningThreads === undefined ? {} : { continueRunningThreads: input.input.continueRunningThreads }) }) : resolveDownloadAction({ environmentId, targetSource, ...host.readLatest() });
        if (proposed.kind !== "ready") { setActionError(proposed.message); return; }
        const result = await executeWebJonesAction({ environmentId, targetSource, action: proposed.action, operationId: randomUUID(), host,
          ...("review" in proposed ? { review: proposed.review } : {}) });
        setActionError(result.message);
        return;
      }
      if (input.action === "check") await readUnresolvedJonesExecution(environmentId);
      setActionError(jonesUpdateActionError(await action({ environmentId, input })));
      // Check is the explicit refresh: retry an ancestry this laptop's server could not prove.
      if (input.action === "check" && pair !== undefined && ancestry === undefined)
        refreshAncestry();
    })().catch((error: unknown) => setActionError(error instanceof Error ? error.message : "Request not confirmed. Read its exact receipt before continuing.")).finally(() => {
      inFlight.current = false;
      setPending(false);
    });
  };
  const comparison = compareWithLaptop
    ? compareJonesSource(
        laptop?.source,
        observation.freshness === "fresh" ? observation.state?.provenance : undefined,
      )
    : null;
  const installedComparison = compareWithLaptop
    ? installedComparisonFor({ host: hostInstalledSource, laptop: laptop?.source, ancestry })
    : null;
  const showMatch =
    compareWithLaptop &&
    isElectron &&
    environmentId !== laptop?.environmentId &&
    !(laptop !== undefined && hostInstalledSource?.sha === laptop.source.sha) &&
    !(
      laptop !== undefined &&
      observation.freshness === "fresh" &&
      observation.state?.stagedHandle !== undefined &&
      observation.state.provenance?.sourceSha === laptop.source.sha
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
      pending={pending || unresolvedJonesExecution(environmentId) !== undefined}
      actionError={actionError}
      layout={layout}
      extraAction={
        showMatch ? (
          <ProductionExactSourceUpdateAction
            environmentId={environmentId}
            label="this host"
            targetSource={laptop?.source.sha ?? ""}
            readTargetSource={() => {
              const latest = appAtomRegistry.get(laptopInstalledSourceAtom);
              return latest !== undefined && latest.environmentId === laptop?.environmentId
                ? latest.source.sha
                : undefined;
            }}
            subject="this laptop"
            allowInstall={false}
            buttonLabel="Match laptop"
            compact
          />
        ) : undefined
      }
      onRequest={(request) => void run(request)}
    />
    {status.check ? <p className="text-xs text-muted-foreground">{status.check.text}</p> : null}
    {status.operation ? <p className="select-text text-xs text-muted-foreground" role="status">{status.headline} · Operation {status.operation.operationId}{status.operation.planCorrelated ? " · Part of the Laptop's update plan" : ""}</p> : null}
    {status.actions.stop ? <Button size="xs" variant="outline" disabled={stopping} onClick={() => {
      const receipt = observation.state?.operation;
      if (receipt === undefined || stopping) return;
      setStopping(true);
      void stop({ environmentId, input: { action: "operationStop", input: { environmentId, operationId: receipt.operationId, stopId: randomUUID() } } }).then((result) => {
        setActionError(result._tag !== "Success" ? "Stop not confirmed. Check the exact operation receipt." : result.value.message ??
          (result.value.phase === "stop-requested" ? "Stop requested. Waiting for the bound operation receipt." : result.value.phase === "stopped" ? "Download stopped. Nothing was installed." : result.value.phase === "held" ? "Stop not confirmed. No new work starts." : result.value.stop?.result === "too-late" ? "Download completed before Stop. Review installation separately." : "This phase cannot be stopped. Read the current operation status."));
      }).finally(() => setStopping(false));
    }}>Stop download</Button> : null}
    </div>
  );
}
