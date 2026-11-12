export type ServerParallelismProfile = "laptop" | "vps";

export interface ServerParallelismConfig {
  readonly profile: ServerParallelismProfile;
  readonly selectionSource: "default" | "environment";
  readonly readerCount: 2 | 3;
  readonly computeWorkerCount: 2 | 3;
}

/** Startup policy only; never changes provider-child CPU placement or allowances. */
export function resolveServerParallelismProfile(
  value: string | undefined,
): ServerParallelismConfig {
  if (value !== undefined && value !== "laptop" && value !== "vps") {
    throw new Error("JONES_SERVER_PARALLELISM_PROFILE must be laptop or vps.");
  }
  const profile = value ?? "laptop";
  const count = profile === "vps" ? 3 : 2;
  return Object.freeze({
    profile,
    selectionSource: value === undefined ? "default" : "environment",
    readerCount: count,
    computeWorkerCount: count,
  });
}
