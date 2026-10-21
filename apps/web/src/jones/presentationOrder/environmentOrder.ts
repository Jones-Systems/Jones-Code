export function sortEnvironmentOptions<
  T extends {
    readonly environmentId: string;
    readonly isPrimary: boolean;
    readonly label: string;
  },
>(options: readonly T[], order: readonly string[]): T[] {
  const ranks = new Map(order.map((key, index) => [key, index]));
  return [...options].sort((a, b) => {
    const rankA = ranks.get(a.environmentId) ?? Infinity;
    const rankB = ranks.get(b.environmentId) ?? Infinity;
    if (rankA !== rankB) return rankA - rankB;
    if (a.isPrimary !== b.isPrimary) return a.isPrimary ? -1 : 1;
    return a.label.localeCompare(b.label);
  });
}
