import { AcpLinuxCgroupControllerRef } from "@t3tools/provider-acp/server/AcpSessionRuntime";
import * as Layer from "effect/Layer";
import type { ProcessPlacementBinding } from "./processPlacement.ts";

// systemd owns the workload hierarchy; ACP must use the captured AgentScope lifecycle.
export const layer = (binding: ProcessPlacementBinding | undefined) =>
  binding === undefined
    ? Layer.empty
    : Layer.succeed(AcpLinuxCgroupControllerRef, { create: () => undefined });
