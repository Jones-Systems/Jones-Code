import * as RuntimeObservation from "../provider/observations/ProviderThreadRuntimeObservation.ts";

/** Use the recorded foreground state when the native observation cannot describe it. */
export const project = (
  thread: RuntimeObservation.ProviderThreadActivitySource,
  native: RuntimeObservation.ProviderRuntimeObservation | null,
  sampledAt: number,
): RuntimeObservation.ProviderThreadActivityObservation => {
  const activity = RuntimeObservation.providerThreadActivityObservation(thread, native, sampledAt);
  return {
    ...activity,
    foreground:
      native === null || (native.status === "unknown" && native.reason !== "runtime_not_resident")
        ? RuntimeObservation.providerThreadForegroundActivity(thread)
        : activity.foreground,
  };
};
