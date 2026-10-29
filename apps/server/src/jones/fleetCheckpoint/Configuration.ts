import * as Schema from "effect/Schema";

const absolute = Schema.String.check(
  Schema.isPattern(/^(?:\/(?:[^\x00-\x1f]+)|[A-Za-z]:[\\/][^\x00-\x1f]+)$/),
);

/** Explicit owner configuration; no executable, endpoint or binding path comes from tool input. */
export const Configuration = Schema.Struct({
  executable: absolute,
  arguments: Schema.Array(Schema.String.check(Schema.isMaxLength(4096))).check(
    Schema.isMaxLength(32),
  ),
  binding: absolute,
  cwd: absolute,
  controllerId: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:/@+%=-]{0,127}$/)),
});
export type Configuration = typeof Configuration.Type;
