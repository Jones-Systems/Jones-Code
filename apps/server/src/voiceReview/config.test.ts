import { describe, expect, it } from "vite-plus/test";
import { voiceReviewNativeBindingFromEnv } from "./config.ts";

const binding = {
  registry_host: "native-host",
  registry_environment: "registered-environment",
  native_environment_id: "native-environment",
};
describe("voice native binding deployment metadata", () => {
  it("accepts only the explicit closed binding and supplies no fallback", () => {
    expect(
      voiceReviewNativeBindingFromEnv({
        T3CODE_VOICE_REVIEW_NATIVE_BINDING: JSON.stringify(binding),
      }),
    ).toEqual(binding);
    for (const value of [
      undefined,
      "",
      "{}",
      "null",
      "[]",
      "not-json",
      JSON.stringify({ ...binding, token: "unexpected" }),
      JSON.stringify({ ...binding, registry_host: "*" }),
      JSON.stringify({ ...binding, native_environment_id: " " }),
      JSON.stringify({ ...binding, registry_environment: " alias " }),
    ]) {
      expect(
        voiceReviewNativeBindingFromEnv({ T3CODE_VOICE_REVIEW_NATIVE_BINDING: value }),
      ).toBeNull();
    }
  });
});
