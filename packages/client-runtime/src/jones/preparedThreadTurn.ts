import {
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2Command,
  OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

import type { PreparedThreadTurn } from "../operations/commands.ts";
import { freezePreparedTurnRequest } from "../operations/jonesPreparedTurn.ts";

const PreparedThreadTurnSource = Schema.Union([
  Schema.Struct({
    method: Schema.Literal(ORCHESTRATION_V2_WS_METHODS.launchThread),
    payload: OrchestrationV2ThreadLaunchInput,
  }),
  Schema.Struct({
    method: Schema.Literal(ORCHESTRATION_V2_WS_METHODS.dispatchCommand),
    payload: OrchestrationV2Command,
  }),
]);
const PreparedThreadTurnValue = PreparedThreadTurnSource.pipe(
  Schema.refine<typeof PreparedThreadTurnSource, PreparedThreadTurn>(
    (value): value is PreparedThreadTurn =>
      value.method === ORCHESTRATION_V2_WS_METHODS.launchThread
        ? value.payload.threadId !== undefined &&
          value.payload.creationSource !== undefined &&
          value.payload.generateTitle !== undefined &&
          value.payload.initialMessage?.messageId !== undefined
        : value.payload.type === "message.dispatch" &&
          value.payload.createdBy === "user" &&
          value.payload.dispatchMode.type !== "defer_start",
  ),
);

// Reuse the existing prepared-operation owner. Decoding never prepares again,
// consults capabilities/catalogs, generates IDs, uploads, or captures a session.
export const PreparedThreadTurnJson = Schema.toCodecJson(
  PreparedThreadTurnValue.pipe(
    Schema.decodeTo(
      Schema.toType(PreparedThreadTurnValue),
      SchemaTransformation.transform({
        decode: (prepared) => freezePreparedTurnRequest(prepared),
        encode: (prepared) => prepared,
      }),
    ),
  ),
);

export function encodePreparedThreadTurn(prepared: PreparedThreadTurn): unknown {
  return Schema.encodeSync(PreparedThreadTurnJson)(prepared);
}

export function decodePreparedThreadTurn(value: unknown): PreparedThreadTurn {
  return Schema.decodeUnknownSync(PreparedThreadTurnJson)(value);
}
