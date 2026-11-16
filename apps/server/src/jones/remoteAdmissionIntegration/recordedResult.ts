import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { RemoteAdmissionLaunchResult } from "@t3tools/contracts/jones/remoteAdmissionReview";
/** Original result is immutable; absent stage evidence stays unknown. */
export const recordedLaunchResult = (sql: SqlClient.SqlClient, id: string) =>
  Effect.gen(function* () {
    const rows = yield* sql<{
      launch_result: string | null;
    }>`SELECT json_extract(result_json,'$.launchResult') launch_result FROM jones_remote_admission_results WHERE admission_id=${id}`;
    return rows[0]?.launch_result
      ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteAdmissionLaunchResult))(
          rows[0].launch_result,
        )
      : null;
  });
