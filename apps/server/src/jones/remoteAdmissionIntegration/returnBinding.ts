import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { RemoteAdmissionRecord } from "@t3tools/contracts";
import type { RemoteAdmissionReturnBinding } from "@t3tools/contracts/jones/remoteAdmissionReview";
import type * as Admission from "../remoteAdmission/RemoteAdmission.ts";
/** Derived from retained trusted receiver invocation identity; caller JSON never supplies a root. */
export function originalReturnBinding(
  record: Pick<RemoteAdmissionRecord, "envelope">,
): RemoteAdmissionReturnBinding | null {
  const payload = record.envelope.payload as {
    params?: { clientRequestId?: unknown };
    invocation?: {
      environmentId: string;
      client?: {
        sessionId: string;
        linked?: boolean;
        remoteOrigin?: { linkSessionId: string; thread?: { threadId: string } };
      };
    };
  };
  const invocation = payload.invocation,
    client = invocation?.client,
    origin = client?.remoteOrigin;
  if (
    !invocation ||
    !client?.linked ||
    client.sessionId !== record.envelope.identity.clientId ||
    origin?.linkSessionId !== client.sessionId ||
    !origin.thread ||
    typeof payload.params?.clientRequestId !== "string"
  )
    return null;
  const key = `origin:${invocation.environmentId}:${payload.params.clientRequestId}`;
  return { rootObservationKey: key, originThreadId: origin.thread.threadId, returnRouteKey: key };
}
export const makeReturnBindingReader = (
  sql: SqlClient.SqlClient,
  admissions: Pick<Admission.RemoteAdmission["Service"], "get">,
) =>
  Effect.fn("AdmissionRecovery.returnBinding")(function* (record: RemoteAdmissionRecord) {
    const attempts = yield* sql<{
      root_admission_id: string;
      root_observation_key: string;
    }>`SELECT root_admission_id,root_observation_key FROM jones_remote_admission_attempts WHERE child_admission_id=${record.id}`;
    if (!attempts[0]) return originalReturnBinding(record);
    const root = yield* admissions.get(attempts[0].root_admission_id);
    const binding = root ? originalReturnBinding(root) : null;
    return binding?.rootObservationKey === attempts[0].root_observation_key ? binding : null;
  });
