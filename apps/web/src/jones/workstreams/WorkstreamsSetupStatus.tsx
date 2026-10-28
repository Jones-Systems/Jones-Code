import { useAtomValue } from "@effect/atom-react";
import { useEffect, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { WorkstreamSetupStatus } from "@t3tools/contracts/jones/workstreamOperations";
import { supportsWorkstreamOperations } from "@t3tools/client-runtime/jones/workstreams/operations";
import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { environmentSession } from "../../state/session";
import { Atom } from "effect/reactivity";
import { SettingsPageContainer, SettingsSection } from "../../components/settings/settingsLayout";
import { Button } from "../../components/ui/button";
import { environmentOperationsTransport } from "./environmentWorkstreamsRequest";

import { SetupStatusContent } from "./SetupStatusContent";
const emptyConfig = Atom.make(null);
function SetupStatusRequest({ environmentId }: { environmentId: EnvironmentId | null }) {
  const [status, setStatus] = useState<WorkstreamSetupStatus | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  // oxlint-disable-next-line t3code/react-effect-budget -- Read setup capability for the selected environment and abort when its identity changes.
  useEffect(() => {
    if (!environmentId) return;
    const abort = new AbortController();
    void environmentOperationsTransport(environmentId)
      .setup(abort.signal)
      .then((next) => {
        if (!abort.signal.aborted) setStatus(next);
      })
      .catch(() => {
        if (!abort.signal.aborted)
          setMessage("Offline or unavailable. Check this environment's connection, then refresh.");
      });
    return () => abort.abort();
  }, [environmentId]);
  return status ? (
    <SetupStatusContent status={status} />
  ) : (
    <p role="status">{message ?? "Checking Workstreams setup…"}</p>
  );
}
export function WorkstreamsSetupStatus() {
  const environmentId = useAtomValue(primaryEnvironmentIdAtom);
  const config = useAtomValue(
    environmentId === null ? emptyConfig : environmentSession.initialConfigValueAtom(environmentId),
  );
  const supported = supportsWorkstreamOperations(config?.environment.capabilities);
  const [revision, setRevision] = useState(0);
  return (
    <SettingsPageContainer>
      <SettingsSection title="Workstreams setup" variant="plain">
        {!supported ? (
          <p className="text-sm text-muted-foreground">
            Setup diagnosis requires a server with Workstream operations support. Legacy Workstreams
            remain available.
          </p>
        ) : (
          <>
            <SetupStatusRequest
              key={`${environmentId}:${revision}`}
              environmentId={environmentId}
            />
            <div className="mt-3">
              <Button size="sm" variant="outline" onClick={() => setRevision((value) => value + 1)}>
                Refresh setup status
              </Button>
            </div>
          </>
        )}
      </SettingsSection>
    </SettingsPageContainer>
  );
}
