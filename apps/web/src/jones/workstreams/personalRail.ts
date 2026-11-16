const automaticRailColors = [
  "var(--color-zinc-500)",
  "var(--color-orange-500)",
  "var(--color-green-500)",
  "var(--color-blue-500)",
] as const;

export function personalWorkstreamRailColor(item: { id: string; color: string | null }) {
  let hash = 2166136261;
  for (let index = 0; index < item.id.length; index += 1) {
    hash = Math.imul(hash ^ item.id.charCodeAt(index), 16777619);
  }
  const color = item.color ?? automaticRailColors[(hash >>> 0) % automaticRailColors.length];
  return `color-mix(in srgb, ${color} 28%, var(--sidebar))`;
}
