import {
  resolveProjectProviderInstanceEnabled,
  textGeneratorForSelection,
  type ModelSelection,
  type ProjectId,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import { selectionAdmissionReason } from "../threadConfiguration/selectionAdmission.ts";
import { accountObserverFor } from "./accountObservation.ts";
import { chooseAccount } from "./policy.ts";

export class AccountAutoRoutingError extends Schema.TaggedError<AccountAutoRoutingError>()(
  "AccountAutoRoutingError",
  {
    code: Schema.Literals([
      "reuse_existing_thread",
      "unsupported_workspace",
      "unsupported_provider",
      "no_verified_account",
      "no_available_allowance",
      "verification_unavailable",
    ]),
    message: Schema.String,
    nextResetAt: Schema.optionalKey(Schema.String),
  },
) {}

export const unsupportedAccountRoutingWorkspace = () =>
  new AccountAutoRoutingError({
    code: "unsupported_workspace",
    message:
      "Auto account routing needs an existing execution directory. Choose the project root or an existing worktree, or select a specific account for a new workspace.",
  });

const verificationUnavailable = () =>
  new AccountAutoRoutingError({
    code: "verification_unavailable",
    message:
      "Auto account routing could not verify current launch settings. Try again or select a specific account.",
  });

const resetDisplay = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  timeZoneName: "short",
});

export interface SelectAutoAccountInput {
  readonly projectId: ProjectId;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
  /** Existing launch directory already resolved by the caller. Omitted uses the project root. */
  readonly cwd?: string;
  readonly candidateAllowed?: (provider: ServerProvider) => boolean;
}

/** Bind a fresh launch to an existing native instance without creating any launch state. */
export const selectAutoAccount = Effect.fn("AccountRouting.selectAutoAccount")(function* (
  input: SelectAutoAccountInput,
) {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const managedFolders = yield* ManagedProjectFolders.ManagedProjectFolders;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const project = yield* projects
    .get(input.projectId)
    .pipe(Effect.mapError(verificationUnavailable));
  if (Option.isNone(project)) return yield* unsupportedAccountRoutingWorkspace();
  const cwd = input.cwd ?? project.value.workspaceRoot;
  const cwdIdentity = (directory: string) =>
    fs.realPath(directory).pipe(
      Effect.flatMap((physical) =>
        fs
          .stat(physical)
          .pipe(Effect.map((stat) => (stat.type === "Directory" ? physical : undefined))),
      ),
      Effect.orElseSucceed(() => undefined),
    );
  if (!path.isAbsolute(cwd)) return yield* unsupportedAccountRoutingWorkspace();
  const physicalCwd = yield* cwdIdentity(cwd);
  if (physicalCwd === undefined) return yield* unsupportedAccountRoutingWorkspace();
  const rootCwd = yield* cwdIdentity(project.value.workspaceRoot);
  if (rootCwd === undefined) return yield* unsupportedAccountRoutingWorkspace();
  if (physicalCwd === rootCwd) {
    if (yield* managedFolders.isScratchProject(input.projectId))
      return yield* unsupportedAccountRoutingWorkspace();
  } else {
    const git = yield* Effect.serviceOption(GitVcsDriver.GitVcsDriver);
    if (Option.isNone(git)) return yield* unsupportedAccountRoutingWorkspace();
    const registered = yield* git.value
      .listWorktreePaths(project.value.workspaceRoot)
      .pipe(Effect.mapError(unsupportedAccountRoutingWorkspace));
    const found = yield* Effect.findFirst(registered, (candidate) =>
      cwdIdentity(candidate).pipe(Effect.map((physical) => physical === physicalCwd)),
    );
    if (Option.isNone(found)) return yield* unsupportedAccountRoutingWorkspace();
  }

  const representative = yield* registry.getInstance(input.modelSelection.instanceId);
  const driver =
    representative?.driverKind === "codex"
      ? "codex"
      : representative?.driverKind === "claudeAgent"
        ? "claudeAgent"
        : undefined;
  if (driver === undefined) {
    return yield* new AccountAutoRoutingError({
      code: "unsupported_provider",
      message: "Auto account routing requires a configured Codex or Claude provider instance.",
    });
  }

  const initialSettings = yield* settingsService.getSettings.pipe(
    Effect.mapError(verificationUnavailable),
  );
  const projectEnabled = (
    instanceId: ModelSelection["instanceId"],
    settings: typeof initialSettings,
  ) =>
    resolveProjectProviderInstanceEnabled(settings, input.projectId, instanceId) &&
    textGeneratorForSelection(settings, instanceId) === undefined;
  const candidates = (yield* registry.listInstances).filter(
    (instance) =>
      instance.driverKind === driver &&
      instance.enabled &&
      projectEnabled(instance.instanceId, initialSettings),
  );
  // One fresh observation per candidate and one deadline for the whole pool.
  // Scoped observations never consult cached usage merges or start a conversation.
  const observations = yield* Effect.forEach(
    candidates,
    Effect.fnUntraced(function* (instance) {
      const observe = accountObserverFor(instance);
      if (!observe) return undefined;
      const snapshot = yield* instance.snapshot.getSnapshot;
      if (
        snapshot.availability === "unavailable" ||
        snapshot.supportedRuntimeModes?.includes(input.runtimeMode) === false ||
        (input.interactionMode === "plan" && snapshot.showInteractionModeToggle === false)
      )
        return undefined;
      const observation = yield* observe(cwd, input.runtimeMode);
      if (observation === undefined) return undefined;
      const limits = yield* instance.runtimeModeLimitsForCwd?.(cwd) ?? Effect.succeed([]);
      if (limits.some((limit) => limit.mode === input.runtimeMode)) return undefined;
      return { instance, snapshot, observation };
    }),
    { concurrency: 4 },
  ).pipe(Effect.timeoutOption("45 seconds"));
  if (Option.isNone(observations)) return yield* verificationUnavailable();

  const latestProject = yield* projects
    .get(input.projectId)
    .pipe(Effect.mapError(verificationUnavailable));
  if (
    Option.isNone(latestProject) ||
    latestProject.value.workspaceRoot !== project.value.workspaceRoot ||
    (yield* cwdIdentity(project.value.workspaceRoot)) !== rootCwd ||
    (yield* cwdIdentity(cwd)) !== physicalCwd
  )
    return yield* unsupportedAccountRoutingWorkspace();
  const settings = yield* settingsService.getSettings.pipe(
    Effect.mapError(verificationUnavailable),
  );
  const now = yield* Clock.currentTimeMillis;
  const members = [];
  for (const entry of observations.value) {
    if (!entry) continue;
    const { instance, snapshot, observation } = entry;
    if (
      !projectEnabled(instance.instanceId, settings) ||
      !Object.is(registry.getCatalogSource(instance.instanceId), instance)
    )
      continue;
    const provider = {
      ...snapshot,
      installed: true, // The contextual native probe started this exact executable.
      models: observation.models.filter((model) => !model.isCustom),
    };
    const selection = { ...input.modelSelection, instanceId: instance.instanceId };
    if (
      selectionAdmissionReason({
        providers: [provider],
        selection,
        settings,
        projectId: input.projectId,
      }) ||
      input.candidateAllowed?.(provider) === false
    )
      continue;
    const allowance = observation.allowance(selection.model, now);
    if (allowance !== undefined) members.push({ instanceId: instance.instanceId, allowance });
  }

  const winner = chooseAccount(driver, members, now);
  if ("instanceId" in winner) return { ...input.modelSelection, instanceId: winner.instanceId };
  const nextResetAt =
    winner.nextReset === null
      ? undefined
      : DateTime.formatIso(DateTime.makeUnsafe(winner.nextReset));
  return yield* new AccountAutoRoutingError({
    code: members.length === 0 ? "no_verified_account" : "no_available_allowance",
    message:
      members.length === 0
        ? "No native subscription account could be verified for this model and launch mode. Check provider sign-in and project settings, or select an account explicitly."
        : winner.nextReset === null
          ? "No verified account has enough ordinary subscription allowance. Try after your provider limits reset or select an account explicitly."
          : `No verified account has enough ordinary subscription allowance. The next known eligible reset is ${resetDisplay.format(winner.nextReset)}. Try after it resets or select an account explicitly.`,
    ...(nextResetAt === undefined ? {} : { nextResetAt }),
  });
});
