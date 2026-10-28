import type { RemoteAdmissionFailureKind } from "@t3tools/client-runtime/jones/remote-admission";

export function ownerLoadMessage(failure: RemoteAdmissionFailureKind, receiver: string) {
  switch (failure) {
    case "unauthorized":
      return `Owner access is unavailable for this session. Manage evaluation and held requests from ${receiver}'s own Jones Code app or signed-in browser, using Requests or Settings → Connections. Refresh here to recheck owner access.`;
    case "unsupported":
      return `Owner request review is unsupported on ${receiver}. Check the receiving environment's version for support, then Refresh to recheck.`;
    case "transient":
    case "other":
      return `Owner review is unavailable on ${receiver}. Current state is unknown. Retrying automatically while this page is visible, with delays up to five minutes. Refresh to retry now.`;
  }
}
