const monitorWindows = new WeakSet<object>();
export function markThreadMonitorWindow(window: object): void {
  monitorWindows.add(window);
}
export function isThreadMonitorWindow(window: object): boolean {
  return monitorWindows.has(window);
}
