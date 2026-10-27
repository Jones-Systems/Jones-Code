export const INSTALL_PLAN_INFO = "Downloading never installs. Others don't start until Laptop is updated and verified.";
export const INSTALL_PLAN_WAIT = "After Laptop is verified";
export const INSTALL_PLAN_GO = "Laptop verified · Others go";

export function lastUpdateLabel(at: string) {
  const date = new Date(at);
  return Number.isFinite(date.getTime())
    ? `Last update · ${new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric" }).format(date)}`
    : "Last update";
}
export function lastSeenLabel(at: string | undefined) {
  if (at === undefined || !Number.isFinite(Date.parse(at))) return "Last seen time unavailable";
  return `Last seen ${new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
  }).format(new Date(at))}`;
}
