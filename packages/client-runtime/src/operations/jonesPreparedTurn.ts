function freezeRecursively(value: unknown): void {
  if (value === null || typeof value !== "object") return;
  for (const child of Object.values(value)) freezeRecursively(child);
  Object.freeze(value);
}

// The prepared request owns every nested value; neither callers nor changing
// projections can alter the payload retained for an explicit retry.
export function freezePreparedTurnRequest<const T extends object>(request: T): T {
  const owned = structuredClone(request);
  freezeRecursively(owned);
  return owned;
}
