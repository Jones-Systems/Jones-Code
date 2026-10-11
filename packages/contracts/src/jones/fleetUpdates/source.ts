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
