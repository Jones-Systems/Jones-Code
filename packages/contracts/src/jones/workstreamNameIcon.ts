/** A selection or explicit clear. Legacy Workstream copies omit the entire value. */
export interface WorkstreamNameIcon {
  readonly projectKey: string | null;
  readonly updatedAt: string;
}

export function compareWorkstreamNameIcons(
  a: WorkstreamNameIcon | undefined,
  b: WorkstreamNameIcon | undefined,
): -1 | 0 | 1 {
  if (!a) return b ? -1 : 0;
  if (!b) return 1;
  if (a.updatedAt !== b.updatedAt) return a.updatedAt < b.updatedAt ? -1 : 1;
  const aKey = a.projectKey ?? "";
  const bKey = b.projectKey ?? "";
  return aKey < bKey ? -1 : aKey > bKey ? 1 : 0;
}

export function preserveWorkstreamNameIcon<T extends { readonly nameIcon?: WorkstreamNameIcon }>(
  candidate: T,
  observed: { readonly nameIcon?: WorkstreamNameIcon },
): T {
  const { nameIcon } = observed;
  return nameIcon && compareWorkstreamNameIcons(nameIcon, candidate.nameIcon) > 0
    ? { ...candidate, nameIcon }
    : candidate;
}
