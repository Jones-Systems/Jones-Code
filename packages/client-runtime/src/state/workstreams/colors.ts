export const WORKSTREAM_TINT_PALETTE = [
  "border-sky-200 bg-sky-50/70 dark:border-sky-800/60 dark:bg-sky-950/25",
  "border-violet-200 bg-violet-50/70 dark:border-violet-800/60 dark:bg-violet-950/25",
  "border-emerald-200 bg-emerald-50/70 dark:border-emerald-800/60 dark:bg-emerald-950/25",
  "border-amber-200 bg-amber-50/70 dark:border-amber-800/60 dark:bg-amber-950/25",
  "border-rose-200 bg-rose-50/70 dark:border-rose-800/60 dark:bg-rose-950/25",
] as const;

export function workstreamPaletteIndex(id: string): number {
  let hash = 0;
  for (const char of id) hash = (Math.imul(hash, 31) + char.charCodeAt(0)) >>> 0;
  return hash % WORKSTREAM_TINT_PALETTE.length;
}

export function workstreamTint(id: string): string {
  return WORKSTREAM_TINT_PALETTE[workstreamPaletteIndex(id)]!;
}
