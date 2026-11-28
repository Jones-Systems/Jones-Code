import {
  AccountRouting,
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpFailure,
  type ProviderInstanceId,
  type RuntimeMode,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import {
  selectAutoAccount,
  unsupportedAccountRoutingWorkspace,
  type AccountAutoRoutingError,
} from "../accountRouting/selectAutoAccount.ts";
import { runtimeModeRank } from "../../mcp/delegatedTaskModes.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as ProviderInstances from "../../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";

export const DelegateTaskInput = Schema.Struct({
  ...OrchestratorMcpDelegateTaskInput.fields,
  accountRouting: Schema.optional(
    AccountRouting.annotate({
      description:
        "For a fresh local child, select an eligible account in the Codex or Claude family of target.providerInstanceId and target.model.",
    }),
  ),
});
export type DelegateTaskInput = typeof DelegateTaskInput.Type;

export const CapabilitiesResult = Schema.Struct({
  ...OrchestratorMcpCapabilitiesResult.fields,
  features: Schema.Struct({
    ...OrchestratorMcpCapabilitiesResult.fields.features.fields,
    accountAutoRouting: Schema.optionalKey(Schema.Literal(true)),
  }),
});
export type CapabilitiesResult = typeof CapabilitiesResult.Type;

export const localAccountRoutingRequired = () =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message:
      "Auto account routing is available for fresh launches in this environment. Omit environmentId or choose a concrete provider instance.",
  });

const fromAutoAccountError = (error: AccountAutoRoutingError) =>
  new OrchestratorMcpFailure({
    code:
      error.code === "unsupported_workspace" || error.code === "unsupported_provider"
        ? "invalid_request"
        : "provider_unavailable",
    message: error.message,
  });

export const existingAccountRoutingWorkspaceRequired = () =>
  fromAutoAccountError(unsupportedAccountRoutingWorkspace());

export const checkAutoAccountRuntimeCeiling = (runtimeMode: RuntimeMode, ceiling?: RuntimeMode) =>
  ceiling === undefined || runtimeModeRank(runtimeMode) <= runtimeModeRank(ceiling)
    ? Effect.void
    : Effect.fail(
        new OrchestratorMcpFailure({
          code: "runtime_mode_escalation_denied",
          message: `Launch runtime mode ${runtimeMode} exceeds this environment's ${ceiling} limit.`,
        }),
      );

export const makeMcpAutoAccountSelector = Effect.gen(function* () {
  const instances = yield* Effect.serviceOption(ProviderInstances.ProviderInstanceRegistry);
  const projects = yield* Effect.serviceOption(ProjectStore.ProjectStoreV2);
  const managedFolders = yield* Effect.serviceOption(ManagedProjectFolders.ManagedProjectFolders);
  const fileSystem = yield* Effect.serviceOption(FileSystem.FileSystem);
  const path = yield* Effect.serviceOption(Path.Path);
  const git = yield* Effect.serviceOption(GitVcsDriver.GitVcsDriver);
  const settings = yield* ServerSettings.ServerSettingsService;
  const dependencies = Option.all({ instances, projects, managedFolders, fileSystem, path, git });
  const unavailable = () =>
    new OrchestratorMcpFailure({
      code: "provider_unavailable",
      message: "Auto account routing is unavailable in this server context.",
    });
  return {
    available: Option.isSome(dependencies),
    select: Effect.fn("mcp.selectAutoAccount")(function* (
      input: Parameters<typeof selectAutoAccount>[0],
    ) {
      if (Option.isNone(dependencies)) return yield* unavailable();
      const services = dependencies.value;
      const candidates = new Map<ProviderInstanceId, ServerProvider>();
      const modelSelection = yield* selectAutoAccount({
        ...input,
        candidateAllowed: (provider) => {
          if (input.candidateAllowed?.(provider) === false) return false;
          candidates.set(provider.instanceId, provider);
          return true;
        },
      }).pipe(
        Effect.provideService(ProviderInstances.ProviderInstanceRegistry, services.instances),
        Effect.provideService(ProjectStore.ProjectStoreV2, services.projects),
        Effect.provideService(ManagedProjectFolders.ManagedProjectFolders, services.managedFolders),
        Effect.provideService(FileSystem.FileSystem, services.fileSystem),
        Effect.provideService(Path.Path, services.path),
        Effect.provideService(GitVcsDriver.GitVcsDriver, services.git),
        Effect.provideService(ServerSettings.ServerSettingsService, settings),
        Effect.mapError(fromAutoAccountError),
      );
      const provider = candidates.get(modelSelection.instanceId);
      if (provider === undefined) return yield* unavailable();
      return { provider, modelSelection };
    }),
  };
});
