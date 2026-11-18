export const PERSONAL_WORKSTREAM_PALETTE = [
  { label: "Rose", color: "#F43F5E" },
  { label: "Rust", color: "#DC5836" },
  { label: "Orange", color: "#FB923C" },
  { label: "Amber", color: "#E5A11A" },
  { label: "Lemon", color: "#D6C83D" },
  { label: "Lime", color: "#84CC16" },
  { label: "Green", color: "#22C55E" },
  { label: "Mint", color: "#4FCCA1" },
  { label: "Teal", color: "#14B8A6" },
  { label: "Cyan", color: "#06B6D4" },
  { label: "Sky", color: "#38A4F0" },
  { label: "Blue", color: "#4B72E9" },
  { label: "Indigo", color: "#6558E5" },
  { label: "Violet", color: "#9961EC" },
  { label: "Orchid", color: "#C268E2" },
  { label: "Fuchsia", color: "#D83FBC" },
  { label: "Pink", color: "#F37DB0" },
  { label: "Clay", color: "#BA8471" },
] as const;

function preferredColorIndex(id: string): number {
  let hash = 2166136261;
  for (let index = 0; index < id.length; index += 1) {
    hash = Math.imul(hash ^ id.charCodeAt(index), 16777619);
  }
  return (hash >>> 0) % PERSONAL_WORKSTREAM_PALETTE.length;
}

export function personalWorkstreamAutomaticColors(
  items: readonly { readonly id: string }[],
): ReadonlyMap<string, string> {
  const colors = new Map<string, string>();
  const usedIndexes = new Set<number>();
  // Reserve a slot for every group, including groups with saved overrides, so editing
  // one color doesn't reshuffle its neighbors and Automatic previews the real result.
  for (const id of [...new Set(items.map((item) => item.id))].sort()) {
    const preferred = preferredColorIndex(id);
    let index = preferred;
    while (usedIndexes.has(index) && usedIndexes.size < PERSONAL_WORKSTREAM_PALETTE.length) {
      index = (index + 1) % PERSONAL_WORKSTREAM_PALETTE.length;
    }
    usedIndexes.add(index);
    colors.set(id, PERSONAL_WORKSTREAM_PALETTE[index]!.color);
  }
  return colors;
}

export function personalWorkstreamGroupColors(
  item: { id: string; color: string | null },
  automaticColor: string = PERSONAL_WORKSTREAM_PALETTE[preferredColorIndex(item.id)]!.color,
) {
  const color = item.color ?? automaticColor;
  return { color, automaticColor, rail: `color-mix(in srgb, ${color} 70%, var(--sidebar))` };
}
