import { useAtomValue } from "@effect/atom-react";
import { useEffect, useState } from "react";
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
export function WorkstreamsSetupStatus() {
  const environmentId = useAtomValue(primaryEnvironmentIdAtom);
  const config = useAtomValue(
    environmentId === null ? emptyConfig : environmentSession.initialConfigValueAtom(environmentId),
  );
  const supported = supportsWorkstreamOperations(config?.environment.capabilities);
  const [status, setStatus] = useState<WorkstreamSetupStatus | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    setStatus(null);
    setMessage(null);
    if (!environmentId || !supported) return;
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
  }, [environmentId, supported, revision]);
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
            {status ? (
              <SetupStatusContent status={status} />
            ) : (
              <p role="status">{message ?? "Checking Workstreams setup…"}</p>
            )}
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
