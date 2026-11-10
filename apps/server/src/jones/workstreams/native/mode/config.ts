import * as Schema from "effect/Schema";
import {
  WorkstreamsNativeRegistry,
  closedWorkstreamsNative,
} from "@t3tools/contracts/jones/workstreamsNative";

const NativeConfig = closedWorkstreamsNative(
  Schema.Struct({
    mode: Schema.Literals([
      "native-home",
      "native-member",
      "legacy",
      "transition-fenced",
      "disabled",
    ]),
    registry: Schema.NullOr(WorkstreamsNativeRegistry),
    bootstrap: Schema.optionalKey(
      closedWorkstreamsNative(
        Schema.Struct({
          home: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
          database: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
          key_provider: Schema.Literal("server-secret-store"),
          source_session_id: Schema.optionalKey(
            Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
          ),
        }),
      ),
    ),
  }),
);
export type NativeConfig = typeof NativeConfig.Type;
export const decodeNativeConfig = Schema.decodeUnknownSync(Schema.fromJsonString(NativeConfig));

/** Presence is checked by startup; this resolver neither opens nor creates a home or key. */
export function resolveNativeMode(input: {
  readonly config: NativeConfig | null;
  readonly legacyActivationPresent: boolean;
  readonly keyPresent: boolean;
}): NativeConfig["mode"] {
  if (!input.config) return input.legacyActivationPresent ? "legacy" : "disabled";
  if (input.config.mode === "legacy") return "legacy";
  if (input.legacyActivationPresent) return "transition-fenced";
  if (input.config.mode === "native-home" && (!input.keyPresent || !input.config.registry))
    return "transition-fenced";
  if (input.config.mode === "native-member" && !input.config.registry) return "transition-fenced";
  return input.config.mode;
}
