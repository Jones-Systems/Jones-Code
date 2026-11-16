import { hostElapsedMs } from "@t3tools/client-runtime/jones/install-plan";
import { useNowSecond } from "../../hooks/useNowSecond";
import { formatElapsed, heardAgoLabel } from "./copy";

/**
 * Live host time since `since`, a host-clock timestamp. `skewMs` is the host clock minus this
 * client's clock. Mount it only for running work: it re-renders every second.
 */
export function Elapsed({ since, skewMs }: { readonly since: number; readonly skewMs: number }) {
  const now = useNowSecond();
  return (
    <time data-elapsed className="tabular-nums">
      {formatElapsed(hostElapsedMs(since, now, skewMs))}
    </time>
  );
}

/** "Last heard 12s ago", ticking each second. Mount it only in an open detail. */
export function HeardAgo({ at }: { readonly at: number | undefined }) {
  const now = useNowSecond();
  return <p data-heard-ago>{heardAgoLabel(at, now)}</p>;
}
