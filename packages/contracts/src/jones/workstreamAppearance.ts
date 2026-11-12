import * as Schema from "effect/Schema";

export const WORKSTREAM_APPEARANCE_CONTRACT = "workstream-appearance/1.0.0" as const;
export const WORKSTREAM_APPEARANCE_MANIFEST =
  "50b5aa14993573c99f4fe44cee0eb4b4c8184c50db77893f0d64e36601bac9ba" as const;
export const WORKSTREAM_APPEARANCE_ROUTE = "/workstream-appearance/v1" as const;
const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/));
const Version = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
export const WorkstreamBorderColor = Schema.NullOr(
  Schema.String.check(Schema.isPattern(/^#[0-9A-F]{6}$/)),
);
export const WorkstreamAppearance = Schema.Struct({
  workstream_id: Id,
  border_color: WorkstreamBorderColor,
  version: Version,
});
export type WorkstreamAppearance = typeof WorkstreamAppearance.Type;
export const WorkstreamAppearanceRead = Schema.Struct({
  workstream_ids: Schema.Array(Id).check(
    Schema.isMaxLength(100),
    Schema.makeFilter((ids) => new Set(ids).size === ids.length),
  ),
});
export const WorkstreamAppearanceWrite = Schema.Struct({
  command_id: Id.check(Schema.isMinLength(16)),
  workstream_id: Id,
  expected_server_generation: Version.check(Schema.isGreaterThan(0)),
  expected_version: Version,
  border_color: WorkstreamBorderColor,
});
export type WorkstreamAppearanceWrite = typeof WorkstreamAppearanceWrite.Type;
export const WorkstreamAppearancePage = Schema.Struct({
  owner_id: Id,
  server_generation: Version.check(Schema.isGreaterThan(0)),
  permissions: Schema.Array(Schema.Literals(["workstreams:read", "workstreams:write"])).check(
    Schema.isMaxLength(2),
    Schema.makeFilter((values) => new Set(values).size === values.length),
  ),
  items: Schema.Array(WorkstreamAppearance).check(Schema.isMaxLength(100)),
});
export type WorkstreamAppearancePage = typeof WorkstreamAppearancePage.Type;
export const WorkstreamAppearanceResult = Schema.Union([
  Schema.Struct({ supported: Schema.Literal(false) }),
  Schema.Struct({ supported: Schema.Literal(true), page: WorkstreamAppearancePage }),
]);
export type WorkstreamAppearanceResult = typeof WorkstreamAppearanceResult.Type;
