// @effect-diagnostics nodeBuiltinImport:off
// Native activation receipt and cache paths share the detached launcher protocol.
import {
  ThreadId,
  EnvironmentId,
  type JonesUpdateState,
  type JonesUpdateDownloadInput,
  type JonesUpdateInstallInput,
  ServerSelfUpdateError,
} from "@t3tools/contracts";

import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import packageJson from "../../../package.json" with { type: "json" };
import {
  retainStagedSelection,
  restoreStagedSelection,
  retireStagedSelection,
} from "./stagedSelection.ts";
import { prepareNativeContinuationReceipt } from "./nativePreparation.ts";
import { isJonesRuntime, isPreviewRuntime } from "./qualification.ts";
import { publishJonesUpdateCapabilityReceipt } from "./capabilityReceipt.ts";
import { qualifiedServerCapability } from "./serverCapability.ts";
import {
  isUpdateOperationId,
  assertNoUnreconciledUpdateOperations,
  operationBinding,
  reconcileUpdateOperation,
  type OperationReconciliation,
} from "./launcherOperation.ts";
import { parseServiceState } from "../../cloud/serviceProtocol.ts";
import { readQualifiedBackupReceipt } from "../cloud/qualifiedBackup.ts";
import { JonesUpdater, type JonesStagedRetirementInput } from "./JonesUpdater.ts";
import { readQualifiedRuntimeReceipt } from "../cloud/qualifiedRuntime.ts";
import * as ServerConfig from "../../config.ts";
import * as SelfUpdate from "../../cloud/selfUpdate.ts";
import * as Launcher from "../../cloud/serviceLauncherClient.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as DesktopReceiver from "../../resourceTelemetry/DesktopTelemetryReceiver.ts";

export class JonesUpdates extends Context.Service<
  JonesUpdates,
  {
    readonly fleetOperationsSupported: boolean;
    readonly state: (after?: number) => Effect.Effect<JonesUpdateState | null>;
    readonly check: Effect.Effect<JonesUpdateState>;
    readonly prepareNative: (input: JonesUpdateInstallInput) => Effect.Effect<JonesUpdateState>;
    readonly download: (input: JonesUpdateDownloadInput) => Effect.Effect<JonesUpdateState>;
    readonly install: (input: JonesUpdateInstallInput) => Effect.Effect<JonesUpdateState>;
    readonly stageExact: (input: {
      readonly targetSource: string;
      readonly operationId: string;
    }) => Effect.Effect<JonesUpdateState>;
    readonly installForOperation: (
      input: JonesUpdateInstallInput & {
        readonly operationId: string;
        readonly targetSource: string;
        readonly expectedInstalledSource: string;
      },
    ) => Effect.Effect<JonesUpdateState>;
    readonly retireStagedOperation: (
      input: JonesStagedRetirementInput,
    ) => Effect.Effect<{ readonly retired: boolean; readonly reason?: string }>;
    readonly reconcileOperation: (operationId: string) => Effect.Effect<OperationReconciliation>;
  }
>()("t3/jones/updates/service/JonesUpdates") {}

const blocked = (message: string): JonesUpdateState => ({
  source: "jones-actions",
  channel: "jones-main",
  phase: "blocked",
  message,
  capability: { check: false, download: false, install: false, reason: "bootstrap-required" },
});

export const layer = Layer.effect(
  JonesUpdates,
  Effect.gen(function* () {
    const platform = yield* HostProcessPlatform;
    const architecture = yield* HostProcessArchitecture;
    const config = yield* ServerConfig.ServerConfig;
    const selfUpdate = yield* SelfUpdate.ServerSelfUpdate;
    const launcher = yield* Launcher.ServiceLauncherClient;
    const startup = yield* Startup.ServerRuntimeStartup;
    const context = yield* Effect.context<never>();
    const run = Effect.runPromiseWith(context);
    const version = launcher.currentVersion ?? packageJson.version;
    const runtimeReceipt = isPreviewRuntime(version)
      ? yield* Effect.tryPromise(() =>
          readQualifiedRuntimeReceipt(config.baseDir, version, { platform, architecture }),
        ).pipe(Effect.match({ onFailure: () => undefined, onSuccess: (receipt) => receipt }))
      : undefined;
    const nativeReceipt = runtimeReceipt !== undefined;
    const unsupportedOperations = {
      retireStagedOperation: () =>
        Effect.succeed({
          retired: false,
          reason: "Caller-bound native updates are unavailable on this host.",
        }),
      fleetOperationsSupported: false,
      stageExact: () =>
        Effect.succeed(blocked("Caller-bound native updates are unavailable on this host.")),
      installForOperation: () =>
        Effect.succeed(blocked("Caller-bound native updates are unavailable on this host.")),
      reconcileOperation: (operationId: string) =>
        Effect.succeed({
          state: "blocked" as const,
          operationId,
          reason: "Caller-bound native updates are unavailable on this host.",
        }),
    };
    if (
      !isJonesRuntime({
        version,
        buildMetadata: packageJson,
        qualifiedRuntimeReceipt: nativeReceipt,
      })
    ) {
      const unavailable = Effect.succeed(blocked("This host uses Release updates."));
      return JonesUpdates.of({
        ...unsupportedOperations,
        state: () => Effect.succeed(null),
        check: unavailable,
        prepareNative: () => unavailable,
        download: () => unavailable,
        install: () => unavailable,
      });
    }
    const reconcileOperation = (operationId: string) =>
      Effect.promise(async (): Promise<OperationReconciliation> => {
        try {
          if (!launcher.managed || !isUpdateOperationId(operationId))
            return {
              state: "blocked",
              operationId,
              reason: "A managed launcher and UUID v4 operation ID are required.",
            };
          const baseDir = await NodeFSP.realpath(config.baseDir);
          const state = parseServiceState(
            await NodeFSP.readFile(NodePath.join(baseDir, "runtime", "service-state.json"), "utf8"),
          );
          if (state === undefined)
            return { state: "blocked", operationId, reason: "Native update state is invalid." };
          return await reconcileUpdateOperation(baseDir, operationId, state.update);
        } catch {
          return {
            state: "blocked",
            operationId,
            reason: "Native update state is unavailable for reconciliation.",
          };
        }
      });
    const qualifiedSelfUpdate = yield* SelfUpdate.withRunningThreadContinuation({
      mode: config.mode,
      selfUpdate,
      reconcileQualifiedOperation: reconcileOperation,
      prepare: startup.markRunningProviderSessionsForContinuation.pipe(
        Effect.mapError(
          (cause) =>
            new ServerSelfUpdateError({ reason: "Could not prepare native continuations.", cause }),
        ),
      ),
      clear: (ids) =>
        startup.clearProviderSessionContinuationMarkers(ids).pipe(
          Effect.mapError(
            (cause) =>
              new ServerSelfUpdateError({
                reason: "Could not clear native continuations.",
                cause,
              }),
          ),
        ),
    });

    if (config.mode === "desktop") {
      const receiver = yield* DesktopReceiver.DesktopTelemetryReceiver;
      let state = blocked("Waiting for the desktop host's Jones updater.");
      let revision = 0;
      const tokens = new Map<string, string>();
      let installPending = false;
      const { latest, changes } = yield* receiver.desktopUpdates;
      const apply = (next: JonesUpdateState | undefined) => {
        if (next !== undefined) {
          state = { ...next };
          revision += 1;
        }
      };
      if (Option.isSome(latest)) apply(latest.value.state.jones);
      yield* changes.pipe(
        Stream.runForEach((report) => Effect.sync(() => apply(report.state.jones))),
        Effect.forkScoped,
      );
      const snapshot = () => ({ ...state, revision });
      const action = (selection: {
        action: "check" | "download";
        artifactId?: number;
        sourceSha?: string;
      }) =>
        Effect.scoped(
          Effect.gen(function* () {
            const requestId = NodeCrypto.randomUUID();
            const { changes: reports } = yield* receiver.desktopUpdates;
            yield* receiver.requestDesktopUpdate(requestId, selection);
            const report = yield* reports.pipe(
              Stream.filter(
                (report) => report.requestId === requestId && report.outcome !== undefined,
              ),
              Stream.runHead,
              Effect.timeoutOption(Duration.minutes(20)),
            );
            if (Option.isNone(report) || Option.isNone(report.value))
              return blocked("The desktop updater did not complete this request.");
            const result = report.value.value;
            apply(result.state.jones);
            if (result.outcome === "ready-to-install" && state.stagedHandle !== undefined)
              tokens.set(state.stagedHandle, requestId);
            return snapshot();
          }),
        ).pipe(
          Effect.catch(() =>
            Effect.succeed(blocked("The desktop host could not accept this request.")),
          ),
        );
      const prepareNative = (input: JonesUpdateInstallInput) =>
        Effect.gen(function* () {
          if (
            !/^[a-f0-9]{64}$/.test(input.stagedHandle) ||
            state.stagedHandle !== input.stagedHandle ||
            !state.capability.install ||
            state.environmentId !== input.environmentId ||
            state.currentVersion !== input.currentVersion
          ) {
            return blocked("Native preparation does not match the staged desktop build.");
          }
          yield* Effect.tryPromise({
            try: () =>
              prepareNativeContinuationReceipt({
                home: config.baseDir,
                databasePath: config.dbPath,
                environmentId: input.environmentId,
                version: input.currentVersion,
                handle: input.stagedHandle,
                prepare: () => run(startup.markOptedInProviderSessionsForContinuation),
                clear: (ids) =>
                  run(
                    startup.clearProviderSessionContinuationMarkers(
                      ids.map((id) => ThreadId.make(id)),
                    ),
                  ),
              }),
            catch: (cause) =>
              new ServerSelfUpdateError({
                reason: "Could not prepare the native recovery pair.",
                cause,
              }),
          });
          return snapshot();
        }).pipe(
          Effect.catch(() =>
            Effect.succeed(
              blocked("Could not prepare native continuations; the current app remains active."),
            ),
          ),
        );
      return JonesUpdates.of({
        ...unsupportedOperations,
        prepareNative,
        state: (after) =>
          after !== revision
            ? Effect.sync(snapshot)
            : changes.pipe(
                Stream.filter((report) => report.state.jones !== undefined),
                Stream.runHead,
                Effect.timeoutOption("25 seconds"),
                Effect.asVoid,
                Effect.andThen(Effect.sync(snapshot)),
              ),
        check: action({ action: "check" }),
        download: (input) => action({ action: "download", ...input }),
        install: (input) =>
          Effect.suspend(() => {
            if (installPending)
              return Effect.succeed(blocked("A native installation is already reserved."));
            installPending = true;
            return Effect.gen(function* () {
              if (
                state.stagedHandle !== input.stagedHandle ||
                !state.capability.install ||
                state.environmentId !== input.environmentId ||
                state.currentVersion !== input.currentVersion ||
                state.phase === "preparing" ||
                state.phase === "installing"
              )
                return blocked("This desktop installation is not prepared for this environment.");
              // A host restart or expired preparation must rebind this same staged handle,
              // rather than forcing the user to download a different candidate.
              const provenance = state.provenance;
              if (provenance === undefined)
                return blocked("The staged build has no qualified provenance.");
              const prior = tokens.get(input.stagedHandle);
              if (prior !== undefined)
                yield* receiver.cancelDesktopUpdate(prior).pipe(Effect.orDie);
              tokens.delete(input.stagedHandle);
              const refreshed = yield* action({
                action: "download",
                artifactId: provenance.artifactId,
                sourceSha: provenance.sourceSha,
              });
              const token = tokens.get(input.stagedHandle);
              if (
                token === undefined ||
                refreshed.stagedHandle !== input.stagedHandle ||
                refreshed.phase !== "staged"
              )
                return blocked("The fixed desktop stage could not be prepared for installation.");
              const prepared = yield* prepareNative(input);
              if (prepared.phase === "blocked") return prepared;
              state = { ...state, phase: "installing" };
              revision += 1;
              yield* receiver.commitDesktopUpdate(token).pipe(Effect.orDie);
              tokens.delete(input.stagedHandle);
              return snapshot();
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  if (state.phase !== "installing") installPending = false;
                }),
              ),
            );
          }),
      });
    }

    const id = yield* Effect.promise(() =>
      NodeFSP.readFile(config.environmentIdPath, "utf8")
        .then((s) => s.trim())
        .catch(() => ""),
    );
    const environmentId = id.length === 0 ? undefined : EnvironmentId.make(id);
    const supported =
      (platform === "linux" && (architecture === "x64" || architecture === "arm64")) ||
      (platform === "darwin" && architecture === "arm64");
    const terminal = launcher.qualifiedStartupOutcome;
    let restoreFailure: string | undefined;
    const restored = yield* Effect.tryPromise({
      try: () =>
        restoreStagedSelection(config.baseDir, launcher.currentVersion ?? packageJson.version),
      catch: (cause) =>
        cause instanceof Error ? cause.message : "Native staging requires reconciliation.",
    }).pipe(
      Effect.match({
        onFailure: (message) => {
          restoreFailure = message;
          return undefined;
        },
        onSuccess: (selection) => selection,
      }),
    );
    const recovery =
      terminal === undefined
        ? undefined
        : yield* Effect.tryPromise(() =>
            readQualifiedBackupReceipt(config.baseDir, terminal.id),
          ).pipe(Effect.catch(() => Effect.succeed(undefined)));
    let capabilityWrites = Promise.resolve();
    const publishCapability = (state: JonesUpdateState) => {
      if (environmentId === undefined) return;
      capabilityWrites = capabilityWrites
        .catch(() => {})
        .then(() =>
          publishJonesUpdateCapabilityReceipt({
            schema: 1,
            baseDir: config.baseDir,
            environmentId,
            currentVersion: version,
            processId: process.pid,
            qualifiedLauncher: launcher.managed && launcher.qualifiedUpdates === true,
            capability: {
              install:
                state.capability.install && !["preparing", "installing"].includes(state.phase),
            },
          }),
        );
      void capabilityWrites.catch(() => {});
    };
    const updater = new JonesUpdater({
      stateChanged: publishCapability,
      startupOutcome: () =>
        restoreFailure === undefined ? launcher.qualifiedStartupOutcome : undefined,
      initialState: {
        ...blocked("A source-qualified runtime and launcher bootstrap is required."),
        ...(terminal === undefined
          ? {}
          : {
              phase: terminal.status === "failed" ? ("error" as const) : terminal.status,
              message:
                terminal.status === "committed"
                  ? "The selected Jones runtime was installed."
                  : terminal.status === "rolled-back"
                    ? "Installation rolled back to its retained binary and state pair."
                    : "Native installation requires reconciliation.",
            }),
        ...(restored === undefined
          ? {}
          : {
              phase: "staged" as const,
              stagedHandle: restored.stagedHandle,
              message: "Downloaded and verified. Install is a separate action.",
              provenance: {
                repository: restored.receipt.repository,
                sourceSha: restored.receipt.sourceSha,
                sourceTree: restored.receipt.sourceTree,
                workflow: restored.receipt.workflow,
                runId: restored.receipt.runId,
                runAttempt: restored.receipt.runAttempt,
                artifactId: restored.receipt.artifactId,
                artifactDigest: restored.receipt.artifactDigest,
                payloadSha256: restored.receipt.archiveSha256,
                version: restored.receipt.version,
                platform: restored.receipt.platform,
                architecture: restored.receipt.architecture,
              },
            }),
        ...(restoreFailure === undefined
          ? {}
          : { phase: "blocked" as const, message: restoreFailure }),
        ...(environmentId === undefined ? {} : { environmentId }),
        currentVersion: version,
        ...(runtimeReceipt === undefined ? {} : { installedSource: runtimeReceipt.sourceSha }),
        ...(launcher.qualifiedUpdateMigrationPlan === undefined
          ? {}
          : { migrationPlan: launcher.qualifiedUpdateMigrationPlan }),
        ...(recovery === undefined
          ? {}
          : {
              recovery: {
                method: recovery.method,
                bytes: recovery.bytes,
                completedAt: recovery.completedAt,
                durationMs: recovery.durationMs,
              },
            }),
        capability: qualifiedServerCapability({
          supported,
          qualifiedRuntime: nativeReceipt,
          launcherManaged: launcher.managed,
          qualifiedStaging: launcher.qualifiedStaging === true,
          qualifiedUpdates: launcher.qualifiedUpdates === true,
          hasCurrentVersion: launcher.currentVersion !== undefined,
          hasStage: selfUpdate.stageQualified !== undefined,
          hasInstall: qualifiedSelfUpdate.installQualified !== undefined,
          hasEnvironment: environmentId !== undefined,
          restoreFailed: restoreFailure !== undefined,
        }),
      },
      platform: platform === "darwin" ? "darwin" : "linux",
      architecture: architecture === "arm64" ? "arm64" : "x64",
      cacheRoot: NodePath.join(config.baseDir, "runtime", "jones-actions"),
      installedSource: async () =>
        (
          await readQualifiedRuntimeReceipt(
            config.baseDir,
            launcher.currentVersion ?? packageJson.version,
          )
        ).sourceSha,
      stage: async (artifact) => {
        if (selfUpdate.stageQualified === undefined)
          throw new Error("bootstrap-required: Qualified staging is unavailable.");
        const result = await run(selfUpdate.stageQualified(artifact));
        const { migrationPlan, ...selection } = result;
        await retainStagedSelection(selection);
        return {
          stagedHandle: result.stagedHandle,
          version: result.receipt.version,
          ...(migrationPlan === undefined ? {} : { migrationPlan }),
        };
      },
      retireStaged: async (input) => {
        const prior = await run(reconcileOperation(input.operationId));
        if (
          launcher.qualifiedOperations !== true ||
          prior.state !== "absent" ||
          input.currentVersion !== version ||
          input.expectedInstalledSource !== runtimeReceipt?.sourceSha
        )
          throw new Error(
            "The exact staged operation is not proven unaccepted on this running source.",
          );
        const nativeState = parseServiceState(
          await NodeFSP.readFile(
            NodePath.join(config.baseDir, "runtime", "service-state.json"),
            "utf8",
          ),
        );
        if (nativeState === undefined) throw new Error("Native operation state is unreadable.");
        await assertNoUnreconciledUpdateOperations(config.baseDir, nativeState.update);
        await retireStagedSelection(config.baseDir, version, input);
      },
      install: async (input) => {
        if (qualifiedSelfUpdate.installQualified === undefined)
          throw new Error("bootstrap-required: Qualified Install is unavailable.");
        const operationId =
          input.operationId ??
          (launcher.qualifiedOperations === true ? NodeCrypto.randomUUID() : undefined);
        const accepted = await run(
          qualifiedSelfUpdate.installQualified(
            { ...input, ...(operationId === undefined ? {} : { operationId }) },
            undefined,
            (staged) =>
              operationId === undefined
                ? Effect.void
                : startup.capturePlannedUpdate === undefined
                  ? Effect.fail(
                      new ServerSelfUpdateError({
                        reason: "Planned update continuity is not configured.",
                      }),
                    )
                  : startup
                      .capturePlannedUpdate({
                        operationId,
                        binding: operationBinding(staged),
                        continueRunningThreads: input.continueRunningThreads === true,
                      })
                      .pipe(
                        Effect.mapError(
                          (cause) =>
                            new ServerSelfUpdateError({
                              reason: "Could not capture planned update continuity.",
                              cause,
                            }),
                        ),
                      ),
          ),
        );
        return {
          ...(accepted.updateId === undefined ? {} : { updateId: accepted.updateId }),
          ...(launcher.qualifiedUpdateMigrationPlan === undefined
            ? {}
            : { migrationPlan: launcher.qualifiedUpdateMigrationPlan }),
        };
      },
    });
    publishCapability(updater.snapshot());
    yield* Effect.promise(() => capabilityWrites.catch(() => {}));
    yield* Effect.sleep("15 seconds").pipe(
      Effect.andThen(Effect.promise(() => updater.check())),
      Effect.forkScoped,
    );
    yield* Effect.sleep("4 minutes").pipe(
      Effect.andThen(Effect.promise(() => updater.check())),
      Effect.forever,
      Effect.forkScoped,
    );
    const installForOperation: JonesUpdates["Service"]["installForOperation"] = (input) =>
      Effect.gen(function* () {
        const prior = yield* reconcileOperation(input.operationId);
        if (prior.state !== "absent") {
          const binding = prior.binding;
          if (
            prior.state === "blocked" ||
            binding === undefined ||
            binding.environmentId !== input.environmentId ||
            binding.currentVersion !== input.currentVersion ||
            binding.expectedInstalledSource !== input.expectedInstalledSource ||
            binding.targetSource !== input.targetSource ||
            binding.stagedHandle !== input.stagedHandle
          )
            return blocked(
              "The operation is ambiguous or its immutable installation binding changed.",
            );
          const { outcome: _outcome, updateId: _updateId, ...current } = updater.snapshot();
          return {
            ...current,
            updateId: input.operationId,
            phase: prior.state === "pending" ? "installing" : prior.state,
            message: "The native launcher retained this exact installation operation.",
          };
        }
        if (
          launcher.qualifiedOperations !== true ||
          runtimeReceipt?.sourceSha !== input.expectedInstalledSource ||
          updater.snapshot().provenance?.sourceSha !== input.targetSource
        )
          return blocked(
            "The installed source, staged source, or operation-capable launcher changed.",
          );
        return yield* Effect.promise(() => updater.install(input));
      });
    return JonesUpdates.of({
      retireStagedOperation: (input) => Effect.promise(() => updater.retireStagedOperation(input)),
      fleetOperationsSupported:
        launcher.managed && launcher.qualifiedOperations === true && supported && nativeReceipt,
      stageExact: (input) =>
        isUpdateOperationId(input.operationId) && launcher.qualifiedOperations === true
          ? Effect.promise(() => updater.stageExact(input.targetSource))
          : Effect.succeed(
              blocked("A UUID v4 operation ID and an operation-capable launcher are required."),
            ),
      installForOperation,
      reconcileOperation,
      prepareNative: () =>
        Effect.succeed(blocked("Native desktop preparation is unavailable on this server.")),
      state: (after) =>
        Effect.promise(async (signal) => {
          const state = await updater.observe(after, signal);
          if (state.updateId === undefined || state.outcome === undefined) return state;
          const receipt = await readQualifiedBackupReceipt(config.baseDir, state.updateId).catch(
            () => undefined,
          );
          return receipt === undefined
            ? state
            : {
                ...state,
                recovery: {
                  method: receipt.method,
                  bytes: receipt.bytes,
                  completedAt: receipt.completedAt,
                  durationMs: receipt.durationMs,
                },
              };
        }),
      check: Effect.promise(() => updater.check()),
      download: (input) => Effect.promise(() => updater.download(input)),
      install: (input) => Effect.promise(() => updater.install(input)),
    });
  }),
);
