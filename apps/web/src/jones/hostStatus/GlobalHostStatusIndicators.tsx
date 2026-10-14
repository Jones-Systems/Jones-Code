import type { EnvironmentId } from "@t3tools/contracts";
import { useEffect, useState } from "react";

import { HostStatusIndicators } from "../../components/chat/HostStatusIndicators";
import { fetchHostStatus, observeHostStatus } from "../../hostStatus";
import {
  useConnectedEnvironmentIds,
  useEnvironmentIds,
  usePrimaryEnvironmentId,
} from "../../state/environments";
import { discoverGlobalHostStatusSource } from "./globalSource";

// Header remounts retain gateway identity only; metrics remain owned by the live observer.
let retainedEnvironmentId: EnvironmentId | null = null;

export function GlobalHostStatusIndicators() {
  const environmentIds = useEnvironmentIds();
  const connectedEnvironmentIds = useConnectedEnvironmentIds();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const [selection, setSelection] = useState(() => retainedEnvironmentId);
  const environmentId = selection !== null && environmentIds.includes(selection) ? selection : null;

  useEffect(() => {
    if (selection !== null && environmentId === null) {
      retainedEnvironmentId = null;
      setSelection(null);
    }
  }, [environmentId, selection]);

  useEffect(() => {
    if (environmentId !== null) return;
    let active = true;
    let stop = () => {};
    stop = observeHostStatus(
      document,
      () => {},
      async (signal) => {
        const source = await discoverGlobalHostStatusSource(
          { environmentIds, connectedEnvironmentIds, primaryEnvironmentId },
          signal,
          fetchHostStatus,
        );
        if (active && source !== null) {
          active = false;
          stop();
          retainedEnvironmentId = source;
          setSelection(source);
        }
        return { hosts: [] };
      },
    );
    return () => {
      active = false;
      stop();
    };
  }, [environmentId, environmentIds, connectedEnvironmentIds, primaryEnvironmentId]);

  return environmentId === null ? (
    <span role="status" className="shrink-0 text-xs text-muted-foreground">
      Host status unavailable
    </span>
  ) : (
    <HostStatusIndicators environmentId={environmentId} />
  );
}
