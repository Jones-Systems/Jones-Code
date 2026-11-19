import { useEffect } from "react";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { createCanonicalReviewClient } from "@t3tools/client-runtime/jones/remoteAdmission/adapter";
import { normalizeRemoteAdmissionError } from "@t3tools/client-runtime/jones/remote-admission";
import { environmentCatalog } from "../../connection/catalog";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import {
  readEnvironment,
  useEnvironments,
  type EnvironmentPresentation,
} from "../../state/environments";
import {
  fallBackToLegacyReview,
  installReviewClient,
  restoreReviewSupport,
  retainedReviewControllerFor,
  reviewControllerFor,
} from "./reviewRuntime";
import { createReviewTransport } from "./reviewTransport";
import { admissionStoreFor, hasLegacyReviewBarrier } from "./useAdmission";

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
  const stateAtom = environmentCatalog.stateAtom(environmentId);
  const generation = useAtomValue(
    stateAtom,
    (state) => AsyncResult.getOrElse(state, () => null)?.generation,
  );
  const version = serverConfig?.environment.serverVersion;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Bind capability discovery to the mounted authenticated connection/configuration and fence its pending reply when that binding changes or unmounts.
  useEffect(() => {
    if (phase !== "connected") return;
    let current = true;
    const isCurrent = () => {
      const latest = readEnvironment(environmentId);
      const connection = AsyncResult.getOrElse(appAtomRegistry.get(stateAtom), () => null);
      return (
        current &&
        latest?.entry === entry &&
        latest.serverConfig?.environment.serverVersion === version &&
        connection?.phase === "connected" &&
        connection.generation === generation
      );
    };
    const unsupported = () => {
      if (!isCurrent()) return;
      if (fallBackToLegacyReview(environmentId)) void admissionStoreFor(environmentId).refresh();
      else void reviewControllerFor(environmentId)?.load();
    };
    const client = createCanonicalReviewClient(createReviewTransport(environmentId));
    void client
      .capabilities()
      .then((capabilities) => {
        if (!isCurrent()) return;
        if (!capabilities.features.has("reviewV2")) {
          unsupported();
          return;
        }
        if (hasLegacyReviewBarrier(environmentId)) return;
        if (retainedReviewControllerFor(environmentId)) restoreReviewSupport(environmentId);
        else installReviewClient(environmentId, client);
        const controller = reviewControllerFor(environmentId);
        void controller?.load();
        void controller?.watchResults();
      })
      .catch((error: unknown) => {
        // Only the capabilities endpoint can establish that V2 is unsupported.
        if (normalizeRemoteAdmissionError(error).kind === "unsupported") unsupported();
      });
    return () => {
      current = false;
      retainedReviewControllerFor(environmentId)?.invalidateConnection();
    };
  }, [environmentId, entry, phase, version, stateAtom, generation]);
  return null;
}
export function ReviewClientsCoordinator() {
  const { environments } = useEnvironments();
  return environments.map((environment) => (
    <ReceiverClient key={environment.environmentId} environment={environment} />
  ));
}
