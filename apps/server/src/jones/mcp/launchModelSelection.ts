import {
  type ModelSelection,
  type ProjectId,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Project from "../../project/ProjectService.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import { checkAutoAccountRuntimeCeiling, makeMcpAutoAccountSelector } from "./accountRouting.ts";
import {
  agentModePickProblem,
  providerOptionDescriptors,
  refuseAgentModePick,
  resolveRuntimeMode,
} from "../../mcp/delegatedTaskModes.ts";
import {
  requireAdvertisedModel,
  validateModelOptions,
  withWorkspaceModels,
} from "../../mcp/OrchestratorMcpService.ts";
import type { Caller } from "../../mcp/threadAccess.ts";

const optionAuthority = (
  caller: Caller,
  provider: ServerProvider | undefined,
  selection: ModelSelection,
) => ({
  callerRuntimeMode: caller.limits.runtimeMode,
  picked: selection.options,
  descriptors: providerOptionDescriptors(provider),
  held:
    caller.caller?.modelSelection.instanceId === selection.instanceId
      ? caller.caller.modelSelection.options
      : undefined,
});

const validateProviderSelection = Effect.fn("mcp.validateProviderSelection")(function* (
  caller: Caller,
  provider: ServerProvider | undefined,
  selection: ModelSelection,
) {
  yield* refuseAgentModePick(optionAuthority(caller, provider, selection));
  if (provider === undefined) return selection;
  yield* requireAdvertisedModel(provider, selection.model);
  const options = yield* validateModelOptions(provider, selection.model, selection.options);
  return options === undefined || options === selection.options
    ? selection
    : { ...selection, options };
});

export const validateLaunchModel = Effect.fn("mcp.validateLaunchModel")(function* (
  caller: Caller,
  requestedSelection: ModelSelection,
  cwd: string | undefined,
) {
  const { instanceId } = requestedSelection;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const validateSelection = (providers: ReadonlyArray<ServerProvider>) =>
    validateProviderSelection(
      caller,
      withWorkspaceModels(providers, cwd).find((candidate) => candidate.instanceId === instanceId),
      requestedSelection,
    );
  const checked = validateSelection(yield* registry.getProviders);
  return yield* cwd === undefined
    ? checked
    : checked.pipe(
        Effect.catchIf(
          (error) => error.code === "model_unavailable",
          () =>
            Project.ProjectService.pipe(
              Effect.flatMap((projects) => projects.workspaceScanDirectory(cwd)),
              Effect.flatMap((scanCwd) =>
                registry.refreshWorkspaceSnapshot({ instanceId, cwd, scanCwd }),
              ),
              Effect.flatMap(validateSelection),
            ),
        ),
      );
});

export const resolveAutoLaunchModel = Effect.fn("mcp.resolveAutoLaunchModel")(function* (input: {
  readonly caller: Caller;
  readonly projectId: ProjectId;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly cwd: string;
}) {
  yield* checkAutoAccountRuntimeCeiling(input.runtimeMode, input.caller.scope.runtimeModeCeiling);
  const router = yield* makeMcpAutoAccountSelector;
  const { modelSelection, provider, binding } = yield* router.select({
    projectId: input.projectId,
    modelSelection: input.modelSelection,
    runtimeMode: input.runtimeMode,
    interactionMode: input.interactionMode,
    cwd: input.cwd,
    candidateAllowed: (candidate) =>
      agentModePickProblem(
        optionAuthority(input.caller, candidate, {
          ...input.modelSelection,
          instanceId: candidate.instanceId,
        }),
      ) === undefined,
  });
  yield* resolveRuntimeMode(input.runtimeMode, input.runtimeMode, {
    modelSelection,
    supportedRuntimeModes: provider.supportedRuntimeModes,
  });
  const selection = yield* validateProviderSelection(input.caller, provider, modelSelection);
  return { modelSelection: selection, binding };
});
