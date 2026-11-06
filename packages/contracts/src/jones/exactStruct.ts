import * as Schema from "effect/Schema";

// Effect 4 ignores parseOptions annotations. Retain extra keys until this check
// so decoding cannot silently discard attempted operations or authority claims.
export const exactStruct = <const Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  return Schema.StructWithRest(schema, [Schema.Record(Schema.String, Schema.Unknown)]).check(
    Schema.makeFilter((value) => Object.keys(value).every((key) => Object.hasOwn(fields, key))),
  );
};
