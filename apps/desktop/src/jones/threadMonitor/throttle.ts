export interface MonitorThrottleTarget {
  readonly getBackgroundThrottling: () => boolean;
  readonly setBackgroundThrottling: (enabled: boolean) => void;
}
interface ThrottlePolicy {
  normal: boolean;
  visible: boolean;
}
const policies = new WeakMap<MonitorThrottleTarget, ThrottlePolicy>();

/** Shared native writers retain their intended policy while the visible monitor holds its lease. */
export function setMonitorAwareBackgroundThrottling(
  target: MonitorThrottleTarget,
  enabled: boolean,
): void {
  const policy = policies.get(target);
  if (!policy) {
    target.setBackgroundThrottling(enabled);
    return;
  }
  policy.normal = enabled;
  target.setBackgroundThrottling(policy.visible ? false : enabled);
}

export function monitorThrottleLease(target: MonitorThrottleTarget) {
  if (policies.has(target)) throw new Error("Publisher throttle already belongs to a monitor host");
  const policy: ThrottlePolicy = { normal: target.getBackgroundThrottling(), visible: false };
  policies.set(target, policy);
  return {
    visible: (visible: boolean) => {
      if (visible && !policy.visible) policy.normal = target.getBackgroundThrottling();
      policy.visible = visible;
      target.setBackgroundThrottling(visible ? false : policy.normal);
    },
    normal: (enabled: boolean) => setMonitorAwareBackgroundThrottling(target, enabled),
    dispose: () => {
      policies.delete(target);
      target.setBackgroundThrottling(policy.normal);
    },
  };
}
