import { useEffect } from "react";
import { createCanonicalReviewClient } from "@t3tools/client-runtime/jones/remoteAdmission/adapter";
import { useEnvironments, type EnvironmentPresentation } from "../../state/environments";
import { installReviewClient, reviewControllerFor } from "./reviewRuntime";
import { createReviewTransport } from "./reviewTransport";

/** Capability discovery uses the existing authenticated environment runtime.
 * Reconnection/configuration events own refresh; no feature-level retry timer.
 */
function ReceiverClient({ environment }: { environment: EnvironmentPresentation }) {
  const {
    environmentId,
    entry,
    connection: { phase },
    serverConfig,
  } = environment;
  const version = serverConfig?.environment.serverVersion;
  useEffect(() => {
    if (phase !== "connected" && phase !== "available") return;
    let current = true;
    const client = createCanonicalReviewClient(createReviewTransport(environmentId));
    void client
      .capabilities()
      .then((capabilities) => {
        if (!current || !capabilities.features.has("reviewV2")) return;
        if (!reviewControllerFor(environmentId)) installReviewClient(environmentId, client);
        const controller = reviewControllerFor(environmentId);
        void controller?.load();
        void controller?.watchResults();
      })
      .catch(() => {
        // The existing review/legacy reader presents permission, unsupported and
        // reachability states; discovery never widens the principal's authority.
      });
    return () => {
      current = false;
    };
  }, [environmentId, entry, phase, version]);
  return null;
}
export function ReviewClientsCoordinator() {
  const { environments } = useEnvironments();
  return environments.map((environment) => (
    <ReceiverClient key={environment.environmentId} environment={environment} />
  ));
}
