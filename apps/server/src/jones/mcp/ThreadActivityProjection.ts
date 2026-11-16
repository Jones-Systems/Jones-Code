import * as RuntimeObservation from "../provider/observations/ProviderThreadRuntimeObservation.ts";

/** Preserve the mainline #1023 caller fallback with the current typed shell. */
export const project = (
  thread: RuntimeObservation.ProviderThreadActivitySource,
  native: RuntimeObservation.ProviderRuntimeObservation | null,
  sampledAt: number,
): RuntimeObservation.ProviderThreadActivityObservation => {
  const activity = RuntimeObservation.providerThreadActivityObservation(thread, native, sampledAt);
  return {...activity, foreground: native === null ||
    (native.status === "unknown" && native.reason !== "runtime_not_resident")
    ? RuntimeObservation.providerThreadForegroundActivity(thread) : activity.foreground};
};
