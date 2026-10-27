import * as Schema from "effect/Schema";

export const JonesSourceIdentity = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  sha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  tree: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
});
export type JonesSourceIdentity = typeof JonesSourceIdentity.Type;

const isJonesSourceIdentity = Schema.is(JonesSourceIdentity);

/** Reads an embedded build stamp; this does not grant native update qualification. */
export function readJonesBuildSource(metadata: unknown): JonesSourceIdentity | undefined {
  if (typeof metadata !== "object" || metadata === null || !("jonesSource" in metadata)) {
    return undefined;
  }
  return isJonesSourceIdentity(metadata.jonesSource) ? metadata.jonesSource : undefined;
}

const BUILD_TIME_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,3})?Z$/;

export const JonesBuildStartedAt = Schema.String.check(
  Schema.makeFilter((value) => {
    const parts = BUILD_TIME_PATTERN.exec(value);
    if (parts === null) return false;
    const year = Number(parts[1]);
    const month = Number(parts[2]);
    const day = Number(parts[3]);
    const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    return (
      year >= 1 &&
      month >= 1 &&
      month <= 12 &&
      day >= 1 &&
      day <= (days[month - 1] ?? 0) &&
      Number(parts[4]) <= 23 &&
      Number(parts[5]) <= 59 &&
      Number(parts[6]) <= 59
    );
  }),
);
export type JonesBuildStartedAt = typeof JonesBuildStartedAt.Type;
const isJonesBuildStartedAt = Schema.is(JonesBuildStartedAt);

export function readJonesBuildStartedAt(metadata: unknown): JonesBuildStartedAt | undefined {
  if (typeof metadata !== "object" || metadata === null || !("jonesBuildStartedAt" in metadata)) {
    return undefined;
  }
  return isJonesBuildStartedAt(metadata.jonesBuildStartedAt)
    ? metadata.jonesBuildStartedAt
    : undefined;
}
