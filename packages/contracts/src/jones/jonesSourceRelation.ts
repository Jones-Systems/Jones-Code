import * as Schema from "effect/Schema";

const SourceCommit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));

/** One exact pair: `base` is a host's installed commit, `head` this laptop's. */
export const JonesSourceRelationQuery = Schema.Struct({
  base: SourceCommit,
  head: SourceCommit,
});
export type JonesSourceRelationQuery = typeof JonesSourceRelationQuery.Type;

/**
 * GitHub ancestry of exactly `base` and `head` in the fixed repository. "ahead" means `head`
 * descends from `base`; "behind" the reverse. "unknown" means the relation was not proven, not
 * that the commits are unrelated. Ancestry implies neither main membership nor channel order.
 */
export const JonesSourceRelation = Schema.Struct({
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  base: SourceCommit,
  head: SourceCommit,
  relation: Schema.Literals(["ahead", "behind", "identical", "diverged", "unknown"]),
  observedAt: Schema.String,
});
export type JonesSourceRelation = typeof JonesSourceRelation.Type;
