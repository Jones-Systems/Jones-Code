import type { WorkstreamsNativeCapability } from "@t3tools/contracts/jones/workstreamsNative";
import type { NativeConfig } from "./config.ts";
/** Advertise only after the parent has composed the corresponding runtime routes. */
export function nativeCapability(
  config: NativeConfig,
  homeLink: "ok" | "missing" | "expired" | "unknown" = "unknown",
): WorkstreamsNativeCapability {
  const common = {
    contract: "workstreams-native/1.0.0" as const,
    reads: ["workstreams-read/1.0.0" as const],
    placement_proofs: [
      "workstreams-t3-placement/1.0.0" as const,
      "workstreams-t3-placement/2.0.0" as const,
    ],
    rollups: { contract: "workstreams-rollups/1.0.0" as const, producer: "unconfigured" as const },
  };
  if (config.mode === "native-home" && config.registry)
    return { ...common, mode: "native-home", registry: config.registry };
  if (config.mode === "native-member" && config.registry)
    return { ...common, mode: "native-member", registry: config.registry, home_link: homeLink };
  return {
    ...common,
    mode:
      config.mode === "native-home" || config.mode === "native-member"
        ? "transition-fenced"
        : config.mode,
    registry: config.registry,
  };
}
