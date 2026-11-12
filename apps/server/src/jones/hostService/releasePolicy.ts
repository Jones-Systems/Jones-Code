import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class JonesRuntimePolicyError extends Schema.TaggedError<JonesRuntimePolicyError>()(
  "JonesRuntimePolicyError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return `${this.reason} Use \`t3 jones host stage-runtime\` with a verified local Jones artifact.`;
  }
}

export function requireExplicitJonesReleaseBaseUrl(releaseBaseUrl: string | undefined) {
  const explicit = releaseBaseUrl?.trim();
  return explicit
    ? Effect.succeed(explicit)
    : Effect.fail(
        new JonesRuntimePolicyError({
          reason: "Jones runtime downloads require an explicit T3CODE_RELEASE_BASE_URL.",
        }),
      );
}
