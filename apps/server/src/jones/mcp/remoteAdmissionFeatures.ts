import * as Review from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { RemoteAdmissionIntake } from "../remoteAdmissionIntegration/RemoteAdmissionIntake.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { requestManagementTable } from "../remoteAdmission/RequestManagementStorage.ts";

/** Feature support is distinct from a caller's permission. Upstream-only
 * harnesses and servers without the Jones migration advertise no new protocol.
 */
export const remoteAdmissionFeatures = Effect.gen(function* () {
  const intake = yield* Effect.serviceOption(RemoteAdmissionIntake);
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(intake) || Option.isNone(sql)) return {};
  const tables = makeJonesTablePresence(sql.value);
  if (
    !(yield* tables("jones_remote_admission_attempts")) ||
    !(yield* tables("jones_remote_admission_recovered_notices"))
  )
    return {};
  return {
    ...((yield* tables(requestManagementTable)) ? { requestManagement: true } : {}),
    remoteAdmissionObservationVersion: Review.REMOTE_ADMISSION_OBSERVATION_VERSION,
    remoteAdmissionPeerStatusVersion: Review.REMOTE_ADMISSION_PEER_STATUS_VERSION,
    remoteAdmissionRetryObservationVersion: Review.REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION,
    remoteAdmissionRecoveryStatusVersion: Review.REMOTE_ADMISSION_RECOVERY_STATUS_VERSION,
    remoteAdmissionRecoveredResultObservationVersion:
      Review.REMOTE_ADMISSION_RECOVERED_RESULT_VERSION,
    remoteAdmissionCapabilitiesRegisterVersion:
      Review.REMOTE_ADMISSION_CAPABILITIES_REGISTER_VERSION,
  };
}).pipe(Effect.orElseSucceed(() => ({})));
