import { jonesUpdateActionError, compareJonesSource } from "@t3tools/client-runtime/jones/updates";
import type { EnvironmentId } from "@t3tools/contracts";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import * as Effect from "effect/Effect";
import { AsyncResult, Atom } from "effect/reactivity";
import { useState } from "react";
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
        ? Effect.succeed(undefined)
        : readInstalledSourceAncestry(prepared.value, pair);
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
  /** Badges this host's installed, offered, or staged build against this laptop's installed source. */
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
  const [actionError, setActionError] = useState<string | null>(null);
  const run = async (input: JonesBuildRequest) => {
    setPending(true);
    setActionError(null);
    await (async () => {
      setActionError(jonesUpdateActionError(await action({ environmentId, input })));
      // Check is the explicit refresh: retry an ancestry this laptop's server could not prove.
      if (input.action === "check" && pair !== undefined && ancestry === undefined)
        refreshAncestry();
    })().finally(() => {
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
  return (
    <JonesBuildStatus
      environmentId={environmentId}
      state={observation.state}
      freshness={observation.freshness}
      freshnessMessage={observation.message}
      comparison={comparison}
      installedComparison={installedComparison}
      pending={pending}
      actionError={actionError}
      layout={layout}
      onRequest={(request) => void run(request)}
    />
  );
}
