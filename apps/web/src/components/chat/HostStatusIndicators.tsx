import type { EnvironmentId, HostStatusSnapshot } from "@t3tools/contracts";
import { memo, useEffect, useState } from "react";

import {
  type HostStatusCache,
  HOST_STATUS_IDS,
  HOST_STATUS_NAMES,
  fetchHostStatus,
  hostStatusMetrics,
  observeHostStatus,
} from "../../hostStatus";
import { ResponsiveHostStatusPills } from "../../jones/hostStatus/ResponsiveHostStatusPills";

let retained: { environmentId: EnvironmentId; cache: HostStatusCache } | null = null;

export function clearRetainedHostStatus(environmentId: EnvironmentId) {
  if (retained?.environmentId === environmentId) retained = null;
}

export const HostStatusIndicators = memo(function HostStatusIndicators({
  environmentId,
}: {
  environmentId: EnvironmentId;
}) {
  return <EnvironmentHostStatusIndicators key={environmentId} environmentId={environmentId} />;
});

function EnvironmentHostStatusIndicators({ environmentId }: { environmentId: EnvironmentId }) {
  const [cache] = useState(() => {
    if (retained?.environmentId !== environmentId) {
      retained = { environmentId, cache: { snapshot: null, settledAt: -Infinity } };
    }
    return retained.cache;
  });
  const [snapshot, setSnapshot] = useState<HostStatusSnapshot | null>(() => cache.snapshot);
  // oxlint-disable-next-line t3code/react-effect-budget -- Own host-status observation and retire callbacks when the selected environment changes.
  useEffect(() => {
    let active = true;
    const stop = observeHostStatus(
      document,
      (snapshot) => {
        if (active) setSnapshot(snapshot);
      },
      (signal) => fetchHostStatus(environmentId, signal),
      cache,
    );
    return () => {
      active = false;
      stop();
    };
  }, [environmentId, cache]);
  const hosts = HOST_STATUS_IDS.map((id) => {
    const host = snapshot?.hosts.find((entry) => entry.id === id);
    const metrics = hostStatusMetrics(host);
    const detail = snapshot === null ? "Host status unavailable or loading" : metrics.detail;
    const name = HOST_STATUS_NAMES[id];
    return {
      id,
      metrics,
      label: `${name}: ${detail}`,
      name,
    };
  });
  return <ResponsiveHostStatusPills hosts={hosts} />;
}
