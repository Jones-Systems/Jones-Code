import * as Effect from "effect/Effect";

// Released builds added runtime_identity_json to the upstream V1 thread-session
// projection here. Nothing reads or writes that column, and Jones schema must
// not alter upstream tables, so this identity now records no effect. Databases
// that already ran it keep the column and its values; removing them needs a
// separately authorized operation.
export default Effect.void;
