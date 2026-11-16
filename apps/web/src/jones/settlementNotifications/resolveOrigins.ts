import {
  resolveEnvironmentSettlementOrigins,
  type SettlementArrival,
  type SettlementOriginDetails,
} from "@t3tools/client-runtime/jones/settlement-notifications";
import { createEnvironmentCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { appAtomRegistry } from "../../rpc/atomRegistry";

// Defer the platform runtime import until an observed arrival needs resolution.
const makeCommand = (
  runtime: (typeof import("../../connection/runtime"))["connectionAtomRuntime"],
) =>
  createEnvironmentCommand(runtime, {
    label: "web-settlement-notifications:origins",
    execute: (arrivals: readonly SettlementArrival[], _registry, environmentId) =>
      resolveEnvironmentSettlementOrigins(environmentId, arrivals),
  });
let command: Promise<ReturnType<typeof makeCommand>> | undefined;

export async function resolveSettlementOrigins(
  environmentId: EnvironmentId,
  arrivals: readonly SettlementArrival[],
): Promise<readonly SettlementOriginDetails[] | null> {
  command ??= import("../../connection/runtime").then(({ connectionAtomRuntime }) =>
    makeCommand(connectionAtomRuntime),
  );
  const result = await runAtomCommand(
    appAtomRegistry,
    await command,
    { environmentId, input: arrivals },
    { reportFailure: false, reportDefect: false },
  );
  if (result._tag === "Failure") throw Cause.squash(result.cause);
  return result.value;
}
