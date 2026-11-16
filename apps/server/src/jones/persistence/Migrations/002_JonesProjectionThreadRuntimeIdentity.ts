import * as Effect from "effect/Effect";

// Released builds added runtime_identity_json to the upstream V1 session projection; nothing uses it now.
// This migration identity records no effect because Jones must not alter upstream tables.
// Existing columns/values remain; removal requires a separately authorized operation.
export default Effect.void;
