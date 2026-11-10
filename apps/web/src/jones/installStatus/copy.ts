export const INSTALL_PLAN_INFO =
  "Downloading never installs. Others don't start until Laptop is updated and verified.";
export const INSTALL_PLAN_WAIT = "After Laptop is verified";
export const INSTALL_PLAN_GO = "Laptop verified · Others go";

export function lastUpdateLabel(at: string) {
  const date = new Date(at);
  return Number.isFinite(date.getTime())
    ? `Last update · ${new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric" }).format(date)}`
    : "Last update";
}
/** Compact elapsed time such as "just now", "4m ago" or "2h ago". */
export function relativeAge(at: number, now: number) {
  const minutes = Math.floor(Math.max(0, now - at) / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return hours < 48 ? `${hours}h ago` : `${Math.floor(hours / 24)}d ago`;
}

export function lastHeardLabel(at: number | undefined, now: number) {
  return at === undefined ? "not heard yet" : `last heard ${relativeAge(at, now)}`;
}

export function updateCheckFailedLabel(checkedAt: string | undefined, now: number) {
  const at = checkedAt === undefined ? Number.NaN : Date.parse(checkedAt);
  return Number.isFinite(at)
    ? `Update check failed ${relativeAge(at, now)}`
    : "Update check failed";
}
