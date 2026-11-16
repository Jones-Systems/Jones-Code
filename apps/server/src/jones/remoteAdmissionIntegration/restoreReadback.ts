import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import type { makeReceiverAccess } from "../remoteAdmission/ReceiverAccess.ts";
/** Owner authorization must precede this pure exact-command lookup. Missing lineage is never recreated. */
export const lookupRestoreCommand = (
  sql: SqlClient.SqlClient,
  access: Pick<ReturnType<typeof makeReceiverAccess>, "getGrant">,
  predecessorId: string,
  commandKey: string,
) =>
  Effect.gen(function* () {
    const restored = yield* sql<{
      successor_grant_id: string;
    }>`SELECT successor_grant_id FROM jones_remote_admission_grant_restores
      WHERE predecessor_grant_id=${predecessorId} AND command_key=${commandKey}`;
    return restored[0] ? yield* access.getGrant(restored[0].successor_grant_id) : null;
  });
