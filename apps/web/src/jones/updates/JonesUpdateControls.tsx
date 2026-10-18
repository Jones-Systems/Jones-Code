import { jonesUpdateActionError, compareJonesSource } from "@t3tools/client-runtime/jones/updates";
import type { EnvironmentId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { useState } from "react";
import { jonesUpdates } from "./jonesUpdates";
import {
  JonesBuildStatus,
  type JonesBuildRequest,
  type JonesBuildStatusLayout,
} from "./JonesBuildStatus";
import { isElectron } from "../../env";
import { readPrimaryEnvironmentTarget } from "../../environments/primary/target";
import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

const noLaptopSourceAtom = Atom.make(() => undefined);

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
  return observation.freshness === "fresh" ? observation.state?.installedSource : undefined;
});

/** Jones main-build status for one host. Download and Install stay separate host actions. */
export function JonesUpdateControls({
  environmentId,
  layout = "inline",
  compareWithLaptop = false,
}: {
  readonly environmentId: EnvironmentId;
  readonly layout?: JonesBuildStatusLayout;
  /** Badges another machine's offered or staged build against this laptop's installed source. */
  readonly compareWithLaptop?: boolean;
}) {
  const observation = useAtomValue(jonesUpdates.observation(environmentId));
  const laptopInstalledSource = useAtomValue(
    compareWithLaptop ? laptopInstalledSourceAtom : noLaptopSourceAtom,
  );
  const action = useAtomCommand(jonesUpdates.action, { reportFailure: false });
  const [pending, setPending] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const run = async (input: JonesBuildRequest) => {
    setPending(true);
    setActionError(null);
    try {
      setActionError(jonesUpdateActionError(await action({ environmentId, input })));
    } finally {
      setPending(false);
    }
  };
  const comparison = compareWithLaptop
    ? compareJonesSource(
        laptopInstalledSource,
        observation.freshness === "fresh" ? observation.state?.provenance : undefined,
      )
    : null;
  return (
    <JonesBuildStatus
      environmentId={environmentId}
      state={observation.state}
      freshness={observation.freshness}
      freshnessMessage={observation.message}
      comparison={comparison}
      pending={pending}
      actionError={actionError}
      layout={layout}
      onRequest={(request) => void run(request)}
    />
  );
}
