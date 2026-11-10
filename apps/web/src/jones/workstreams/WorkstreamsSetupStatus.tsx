import { useAtomValue } from "@effect/atom-react";
import { useEffect, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { WorkstreamSetupStatus } from "@t3tools/contracts/jones/workstreamOperations";
import { supportsWorkstreamOperations } from "@t3tools/client-runtime/jones/workstreams/operations";
import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { environmentSession } from "../../state/session";
import { Atom } from "effect/reactivity";
import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { Button } from "../../components/ui/button";
import { RefreshIcon } from "../../components/ui/refresh-icon";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { environmentOperationsTransport } from "./environmentWorkstreamsRequest";

import { SetupStatusContent, SetupStatusLine } from "./SetupStatusContent";
import {
  nativeWorkstreamsCapability,
  type NativeWorkstreamsCapability,
} from "@t3tools/client-runtime/state/workstreams";
const emptyConfig = Atom.make(null);
function SetupStatusRequest({
  environmentId,
  capability,
}: {
  environmentId: EnvironmentId | null;
  capability: NativeWorkstreamsCapability | null;
}) {
  const [status, setStatus] = useState<WorkstreamSetupStatus | null>(null);
  const [failed, setFailed] = useState(false);
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
        if (!abort.signal.aborted) setFailed(true);
      });
    return () => abort.abort();
  }, [environmentId]);
  if (status) return <SetupStatusContent status={status} capability={capability} />;
  return failed ? (
    <SetupStatusLine tone="error" badge="Offline">
      Couldn’t reach this environment. Check its connection, then refresh.
    </SetupStatusLine>
  ) : (
    <p role="status">Checking setup…</p>
  );
}

function RefreshSetupButton({ onClick }: { onClick: () => void }) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            size="icon-xs"
            variant="ghost-muted"
            aria-label="Refresh setup status"
            onClick={onClick}
          >
            <RefreshIcon />
          </Button>
        }
      />
      <TooltipPopup side="top">Refresh setup status</TooltipPopup>
    </Tooltip>
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
    <SettingsSection title="Workstreams">
      {!supported ? (
        <SettingsRow
          title="Setup status"
          description="This server doesn’t support setup diagnosis. Legacy Workstreams still work."
        />
      ) : (
        <SettingsRow
          title="Setup status"
          status={
            <SetupStatusRequest
              key={`${environmentId}:${revision}`}
              environmentId={environmentId}
              capability={nativeWorkstreamsCapability(config?.environment.capabilities)}
            />
          }
          control={<RefreshSetupButton onClick={() => setRevision((value) => value + 1)} />}
        />
      )}
    </SettingsSection>
  );
}
