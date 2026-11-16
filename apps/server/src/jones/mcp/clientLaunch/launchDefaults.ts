import { type ProjectId, OrchestratorMcpFailure } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Effect from "effect/Effect";
import type { Caller } from "../../../mcp/threadAccess.ts";
import { unavailable } from "../../../mcp/threadAccess.ts";
import type { StartedModes } from "../../../mcp/McpToolAccess.ts";
import { resolveRuntimeMode } from "../../../mcp/delegatedTaskModes.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import { inheritsLinkedDelegationModes } from "../delegatedTaskSupervision/DelegatedTaskSupervision.ts";
import type { ClientLaunchRequest } from "./ClientLaunchIdentity.ts";

/** Only first admission resolves omitted fields; accepted replay uses recorded bindings. */
export const resolveLaunchDefaults = Effect.fn("jones.clientLaunch.resolveDefaults")(function* (
  input: Pick<ClientLaunchRequest, "modelSelection" | "runtimeMode"> & {
    readonly delegatedFrom?: unknown;
  },
  caller: Caller,
  projectId: ProjectId,
  modes: StartedModes,
  project: Effect.Effect<Parameters<typeof resolveProjectSettings>[2], OrchestratorMcpFailure>,
) {
  const defaults =
    caller.caller === undefined
      ? resolveProjectSettings(
          yield* ServerSettings.ServerSettingsService.pipe(
            Effect.flatMap((settings) => settings.getSettings),
            Effect.mapError(unavailable),
          ),
          projectId,
          yield* project,
        ).settings
      : undefined;
  const modelSelection =
    input.modelSelection ??
    caller.caller?.modelSelection ??
    defaults?.defaultModelSelection ??
    undefined;
  if (modelSelection === undefined)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "Pass modelSelection: the project has no default model. orchestrator_capabilities lists providers and models.",
    });
  const runtimeMode =
    defaults !== undefined &&
    input.runtimeMode === undefined &&
    !inheritsLinkedDelegationModes({
      delegatedFrom: input.delegatedFrom,
      linkOrigin: modes.linkOrigin,
      narrowedTo: caller.scope.client?.narrowedTo,
    })
      ? yield* resolveRuntimeMode(modes.runtimeMode, defaults.defaultRuntimeMode).pipe(
          Effect.orElseSucceed(() => modes.runtimeMode),
        )
      : modes.runtimeMode;
  return { modelSelection, runtimeMode };
});
