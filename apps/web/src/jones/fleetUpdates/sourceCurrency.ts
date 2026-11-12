const sourceSha = /^[a-f0-9]{40}$/;

export type JonesSourceCurrency = "current" | "behind" | "ahead" | "diverged" | "unknown";

export interface JonesSourceComparison {
  readonly base: string;
  readonly head: string;
  readonly relation: "ahead" | "behind" | "identical" | "diverged";
}

/** Workflow versions identify artifacts, not ordering across platform builds. */
export function resolveJonesSourceCurrency(input: {
  readonly installedSource?: string | undefined;
  readonly targetSource?: string | undefined;
  readonly comparison?: JonesSourceComparison | undefined;
}): JonesSourceCurrency {
  const { installedSource, targetSource, comparison } = input;
  if (
    installedSource === undefined ||
    targetSource === undefined ||
    !sourceSha.test(installedSource) ||
    !sourceSha.test(targetSource)
  )
    return "unknown";
  if (installedSource === targetSource) return "current";
  if (comparison?.base !== installedSource || comparison.head !== targetSource) return "unknown";
  switch (comparison.relation) {
    case "ahead":
      return "behind";
    case "behind":
      return "ahead";
    case "diverged":
      return "diverged";
    case "identical":
      return "unknown";
  }
}
