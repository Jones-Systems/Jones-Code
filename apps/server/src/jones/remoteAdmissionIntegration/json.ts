import * as Schema from "effect/Schema";

export const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
export const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json));
