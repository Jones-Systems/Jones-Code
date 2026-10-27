import type { ServerProviderUsageLimits } from "@t3tools/contracts";

export function providerShortcutLabelColor(limits: ServerProviderUsageLimits | undefined): string {
  if (!limits || limits.unavailable || limits.windows.length === 0) return "white";

  // The tightest reported quota determines the account's remaining capacity.
  const remaining = 100 - Math.max(...limits.windows.map((window) => window.usedPercent));
  if (remaining >= 50) return "#72df9c";
  if (remaining >= 25) return "#f2d46c";
  if (remaining >= 10) return "#ffa05e";
  return "#ff747d";
}
