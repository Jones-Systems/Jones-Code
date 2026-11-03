import type { ServerProvider } from "@t3tools/contracts";

export const hasAuthoritativeEmptyModelOptions = (
  provider: ServerProvider,
  model: ServerProvider["models"][number],
): boolean =>
  provider.driver === "opencode" &&
  provider.enabled &&
  provider.installed &&
  provider.status === "ready" &&
  model.capabilities?.optionDescriptors?.length === 0;
