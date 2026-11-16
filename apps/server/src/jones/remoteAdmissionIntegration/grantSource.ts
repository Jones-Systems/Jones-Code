import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import type { Grant } from "../remoteAdmission/ReceiverAccess.ts";
/** Follow exact retained restore lineage to its verified invocation, never another admission from the session. */
export const findGrantSourceAdmission = Effect.fn("AdmissionReview.findGrantSourceAdmission")(
  function* (
    sql: SqlClient.SqlClient,
    admissions: Pick<Admission.RemoteAdmission["Service"], "get" | "access">,
    grant: Grant,
  ) {
    let current = grant;
    const visited = new Set<string>();
    while (!visited.has(current.grant_id)) {
      visited.add(current.grant_id);
      const source = yield* admissions.get(current.admission_id);
      if (source) return source;
      const lineage = (yield* sql<{
        predecessor_grant_id: string;
      }>`SELECT predecessor_grant_id FROM jones_remote_admission_grant_restores WHERE successor_grant_id=${current.grant_id}`)[0];
      if (!lineage) return null;
      const predecessor = yield* admissions.access.getGrant(lineage.predecessor_grant_id);
      if (!predecessor) return null;
      current = predecessor;
    }
    return null;
  },
);
