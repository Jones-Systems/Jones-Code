import type { T3WorkstreamBinding, WorkstreamAppearancePage } from "@t3tools/contracts";

export const WORKSTREAM_COLOR_PRESETS = [
  ["Red", "#DC2626"],
  ["Coral", "#FB7185"],
  ["Orange", "#EA580C"],
  ["Amber", "#D97706"],
  ["Yellow", "#EAB308"],
  ["Lime", "#65A30D"],
  ["Green", "#16A34A"],
  ["Emerald", "#059669"],
  ["Teal", "#0D9488"],
  ["Cyan", "#0891B2"],
  ["Sky", "#0284C7"],
  ["Blue", "#2563EB"],
  ["Indigo", "#4F46E5"],
  ["Violet", "#7C3AED"],
  ["Purple", "#9333EA"],
  ["Fuchsia", "#C026D3"],
  ["Pink", "#DB2777"],
  ["Rose", "#E11D48"],
  ["Brown", "#92400E"],
  ["Slate", "#64748B"],
  ["Charcoal", "#374151"],
  ["Silver", "#CBD5E1"],
] as const;

export function normalizeWorkstreamColor(value: string): string | null {
  const text = value.trim();
  return /^#[0-9a-f]{6}$/i.test(text) ? text.toUpperCase() : null;
}

export function validateAppearanceBinding(
  page: WorkstreamAppearancePage,
  binding: T3WorkstreamBinding,
  ids: readonly string[],
): void {
  if (
    page.owner_id !== binding.ownerId ||
    page.server_generation !== binding.serverGeneration ||
    page.items.length !== ids.length ||
    page.items.some((item, index) => item.workstream_id !== ids[index])
  ) {
    throw new Error("Workstream appearance binding changed. Refresh before retrying.");
  }
}

export function workstreamAppearanceBorder(
  color: string | null | undefined,
): { borderLeftColor: string } | undefined {
  return color && normalizeWorkstreamColor(color) ? { borderLeftColor: color } : undefined;
}
