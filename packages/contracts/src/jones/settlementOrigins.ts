import * as Schema from "effect/Schema";
import { ThreadId } from "../baseSchemas.ts";

export const SETTLEMENT_ORIGINS_CAPABILITY = {
  schemaVersion: "jones.settlement-origins/v1",
} as const;
export const SettlementOriginsCapability = Schema.Struct({
  schemaVersion: Schema.Literal(SETTLEMENT_ORIGINS_CAPABILITY.schemaVersion),
});
export type SettlementOriginsCapability = typeof SettlementOriginsCapability.Type;

export const SettlementOrigin = Schema.Literals(["client", "other", "unknown"]);
export type SettlementOrigin = typeof SettlementOrigin.Type;

export const SettlementOriginQuery = Schema.Struct({
  threadId: ThreadId,
  settledAtMs: Schema.Number.check(
    Schema.isInt(),
    Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
  ),
});
export type SettlementOriginQuery = typeof SettlementOriginQuery.Type;

export const SettlementOriginsRequest = Schema.Struct({
  items: Schema.Array(SettlementOriginQuery).check(Schema.isMaxLength(100)),
});
export type SettlementOriginsRequest = typeof SettlementOriginsRequest.Type;

export const SettlementOriginsResult = Schema.Struct({
  items: Schema.Array(
    Schema.Struct({ ...SettlementOriginQuery.fields, origin: SettlementOrigin }),
  ).check(Schema.isMaxLength(100)),
});
export type SettlementOriginsResult = typeof SettlementOriginsResult.Type;
