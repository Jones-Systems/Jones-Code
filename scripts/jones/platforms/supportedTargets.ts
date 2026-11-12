import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class UnsupportedJonesBuildTargetError extends Schema.TaggedError<UnsupportedJonesBuildTargetError>()(
  "UnsupportedJonesBuildTargetError",
  { platform: Schema.String, arch: Schema.String },
) {
  override get message(): string {
    return `Jones Code does not support packaging ${this.platform}/${this.arch}. Use --platform mac --arch arm64, or --platform linux --arch arm64|x64.`;
  }
}

export const requireJonesBuildTarget = <
  T extends { readonly platform: string; readonly arch: string },
>(
  input: T,
): Effect.Effect<T, UnsupportedJonesBuildTargetError> =>
  (input.platform === "mac" && input.arch === "arm64") ||
  (input.platform === "linux" && (input.arch === "arm64" || input.arch === "x64"))
    ? Effect.succeed(input)
    : Effect.fail(
        new UnsupportedJonesBuildTargetError({ platform: input.platform, arch: input.arch }),
      );
