// @effect-diagnostics nodeBuiltinImport:off
// Qualified staging delegates to the Node-only launcher boundary and owns its scratch lifetime.
import {
  ServerSelfUpdateError,
  type ServerSelfUpdateCapability,
  type ServerSelfUpdateInput,
  type ServerSelfUpdateProgressStage,
  type ServerSelfUpdateResult,
  type ThreadId,
} from "@t3tools/contracts";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Cause from "effect/Cause";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as HashSet from "effect/HashSet";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { HttpClient } from "effect/http";

import { CLI_RELEASE_BASE_URL_ENV } from "@t3tools/shared/cliRelease";
import {
  validateJonesStagedArtifact,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  currentQualifiedRuntimeBinding,
  qualifiedRuntimeArtifactFromJonesStage,
  stageQualifiedRuntime,
  verifyStagedQualifiedRuntime,
  type StagedQualifiedRuntime,
} from "../jones/cloud/qualifiedRuntime.ts";
import { extractQualifiedLinuxArchive } from "../jones/cloud/qualifiedArchive.ts";
import { extractQualifiedDarwinRuntime } from "../jones/cloud/qualifiedDarwinRuntime.ts";

import * as ServerConfig from "../config.ts";
import * as DesktopAppUpdate from "../desktopUpdate/DesktopAppUpdate.ts";
import * as ProcessRunner from "../processRunner.ts";
import {
  ensurePinnedRuntimeInstalled,
  pinnedRuntimeCommand,
  PinnedRuntimeInstallError,
  PinnedRuntimePreflightBlockedError,
} from "./pinnedRuntime.ts";
import {
  decodeServicePreflightResult,
  qualifiedServicePreflightFailure,
} from "./servicePreflight.ts";
import * as ServiceLauncherClient from "./serviceLauncherClient.ts";
import { isExactServiceVersion, SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";

const PREFLIGHT_TIMEOUT = Duration.seconds(30);
const isServiceLauncherClientError = Schema.is(ServiceLauncherClient.ServiceLauncherClientError);

export function resolveServerSelfUpdateCapability(input: {
  readonly desktopManaged: boolean;
  readonly launcherManaged: boolean;
}): ServerSelfUpdateCapability | null {
  if (input.desktopManaged) return "desktop-managed" as const;
  return input.launcherManaged ? ("boot-service" as const) : null;
}

export class ServerSelfUpdate extends Context.Service<
  ServerSelfUpdate,
  {
    readonly update: (
      input: ServerSelfUpdateInput,
      reportProgress?: (
        stage: ServerSelfUpdateProgressStage,
      ) => Effect.Effect<void, ServerSelfUpdateError>,
      onHandoffAccepted?: () => Effect.Effect<void>,
    ) => Effect.Effect<ServerSelfUpdateResult, ServerSelfUpdateError>;
    readonly stageQualified?: (
      artifact: JonesStagedArtifact,
    ) => Effect.Effect<StagedQualifiedRuntime, ServerSelfUpdateError>;
    readonly installQualified?: (
      input: { readonly stagedHandle: string; readonly continueRunningThreads?: boolean },
      onHandoffAccepted?: () => Effect.Effect<void>,
    ) => Effect.Effect<ServerSelfUpdateResult, ServerSelfUpdateError>;
    readonly commitDesktopUpdate: (
      requestId: string,
      onHandoffAccepted?: () => Effect.Effect<void>,
    ) => Effect.Effect<never, ServerSelfUpdateError>;
  }
>()("t3/cloud/selfUpdate/ServerSelfUpdate") {}

export const withRunningThreadContinuation = Effect.fn(
  "cloud.server_self_update.withRunningThreadContinuation",
)(function* (input: {
  readonly mode: ServerConfig.RuntimeMode;
  readonly selfUpdate: ServerSelfUpdate["Service"];
  readonly prepare: Effect.Effect<ReadonlyArray<ThreadId>, ServerSelfUpdateError>;
  readonly clear: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<void, ServerSelfUpdateError>;
}) {
  const desktopContinuationTokens = yield* Ref.make(HashSet.empty<string>());
  const qualifiedInstallInFlight = yield* Ref.make(false);
  const uncertainQualifiedHandoff = (cause: Cause.Cause<ServerSelfUpdateError>) => {
    const error = Cause.findErrorOption(cause);
    return (
      Option.isSome(error) &&
      isServiceLauncherClientError(error.value.cause) &&
      ["send", "disconnect", "timeout"].includes(error.value.cause.operation)
    );
  };
  const clearOnError = <A>(
    effect: Effect.Effect<A, ServerSelfUpdateError>,
    threadIds: () => ReadonlyArray<ThreadId>,
    handoffAccepted: () => boolean,
    preserveUncertainHandoff = false,
  ): Effect.Effect<A, ServerSelfUpdateError> =>
    effect.pipe(
      Effect.catchCause((cause) =>
        ((handoffAccepted() && Cause.hasInterruptsOnly(cause)) ||
        (preserveUncertainHandoff && uncertainQualifiedHandoff(cause))
          ? Effect.void
          : input.clear(threadIds())
        ).pipe(Effect.andThen(Effect.failCause(cause))),
      ),
    );

  const update: ServerSelfUpdate["Service"]["update"] = (
    request,
    reportProgress = () => Effect.void,
  ) => {
    let prepared = false;
    let handoffAccepted = false;
    let continuationThreadIds: ReadonlyArray<ThreadId> = [];
    return clearOnError(
      input.selfUpdate
        .update(
          request,
          (stage) =>
            (request.continueRunningThreads === true &&
            input.mode !== "desktop" &&
            stage === "installing" &&
            !prepared
              ? input.prepare.pipe(
                  Effect.tap((threadIds) =>
                    Effect.sync(() => {
                      prepared = true;
                      continuationThreadIds = threadIds;
                    }),
                  ),
                  Effect.asVoid,
                )
              : Effect.void
            ).pipe(Effect.andThen(reportProgress(stage))),
          () =>
            Effect.sync(() => {
              handoffAccepted = true;
            }),
        )
        .pipe(
          Effect.tap((result) => {
            if (
              result.method === "desktop-app" &&
              result.desktopUpdateToken !== undefined &&
              request.continueRunningThreads === true
            ) {
              return Ref.update(desktopContinuationTokens, HashSet.add(result.desktopUpdateToken));
            }
            return Effect.void;
          }),
        ),
      () => continuationThreadIds,
      () => handoffAccepted,
    );
  };

  return ServerSelfUpdate.of({
    update,
    ...(input.selfUpdate.stageQualified === undefined
      ? {}
      : { stageQualified: input.selfUpdate.stageQualified }),
    ...(input.selfUpdate.installQualified === undefined
      ? {}
      : {
          installQualified: (
            request: { readonly stagedHandle: string; readonly continueRunningThreads?: boolean },
            onAccepted?: () => Effect.Effect<void>,
          ) => {
            let handoffAccepted = false;
            let continuationThreadIds: ReadonlyArray<ThreadId> = [];
            return Effect.gen(function* () {
              if (yield* Ref.getAndSet(qualifiedInstallInFlight, true))
                return yield* new ServerSelfUpdateError({
                  reason: "A qualified Install has already started or needs reconciliation.",
                });
              return yield* clearOnError(
                Effect.gen(function* () {
                  continuationThreadIds =
                    request.continueRunningThreads === true ? yield* input.prepare : [];
                  return yield* input.selfUpdate.installQualified!(request, () =>
                    Effect.sync(() => {
                      handoffAccepted = true;
                    }).pipe(Effect.andThen(onAccepted?.() ?? Effect.void)),
                  );
                }),
                () => continuationThreadIds,
                () => handoffAccepted,
                true,
              ).pipe(
                Effect.catchCause((cause) =>
                  (handoffAccepted || uncertainQualifiedHandoff(cause)
                    ? Effect.void
                    : Ref.set(qualifiedInstallInFlight, false)
                  ).pipe(Effect.andThen(Effect.failCause(cause))),
                ),
              );
            });
          },
        }),
    commitDesktopUpdate: (requestId) =>
      Effect.gen(function* () {
        const shouldContinue = yield* Ref.modify(desktopContinuationTokens, (tokens) => [
          HashSet.has(tokens, requestId),
          HashSet.remove(tokens, requestId),
        ]);
        let handoffAccepted = false;
        let continuationThreadIds: ReadonlyArray<ThreadId> = [];
        return yield* clearOnError(
          Effect.gen(function* () {
            continuationThreadIds = shouldContinue ? yield* input.prepare : [];
            return yield* input.selfUpdate.commitDesktopUpdate(requestId, () =>
              Effect.sync(() => {
                handoffAccepted = true;
              }),
            );
          }),
          () => continuationThreadIds,
          () => handoffAccepted,
        ).pipe(
          Effect.catchCause((cause) =>
            (shouldContinue && !handoffAccepted
              ? Ref.update(desktopContinuationTokens, HashSet.add(requestId))
              : Effect.void
            ).pipe(Effect.andThen(Effect.failCause(cause))),
          ),
        );
      }),
  });
});

export const make = Effect.fn("cloud.server_self_update.make")(function* () {
  const serverConfig = yield* ServerConfig.ServerConfig;
  const desktopAppUpdate = yield* DesktopAppUpdate.DesktopAppUpdate;
  const launcher = yield* ServiceLauncherClient.ServiceLauncherClient;
  const runner = yield* ProcessRunner.ProcessRunner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcessPlatform;
  const arch = yield* HostProcessArchitecture;
  // Archive-distributed targets download from GitHub Releases. The client is
  // optional so callers without one (tests, npm-only hosts) still construct.
  const httpClient = yield* HttpClient.HttpClient;
  const releaseBaseUrl = Option.getOrUndefined(
    yield* Config.String(CLI_RELEASE_BASE_URL_ENV).pipe(Config.option),
  );
  const inFlight = yield* Ref.make(false);
  const runtimeContext = yield* Effect.context<never>();
  const runPromise = Effect.runPromiseWith(runtimeContext);

  const capability: ServerSelfUpdateCapability | null =
    serverConfig.mode === "desktop" ? "desktop-managed" : launcher.managed ? "boot-service" : null;
  const failWith = (reason: string, cause?: unknown) =>
    cause === undefined
      ? new ServerSelfUpdateError({ reason })
      : new ServerSelfUpdateError({ reason, cause });

  const update: ServerSelfUpdate["Service"]["update"] = Effect.fn(
    "cloud.server_self_update.update",
  )(function* (input, reportProgress = () => Effect.void, onHandoffAccepted = () => Effect.void) {
    if (capability === "desktop-managed") {
      // input.targetVersion is meaningless here: the desktop app's own
      // update feed decides what it downloads, and the result carries what
      // it actually got.
      if (desktopAppUpdate.available) {
        return yield* desktopAppUpdate.run(reportProgress);
      }
      return yield* failWith(
        "This server is managed by the T3 Code desktop app on its machine; update the desktop app to update it.",
      );
    }
    if (capability === null) {
      return yield* failWith(
        "Remote updates require the T3 Code background service. Run `t3 service install` on the server machine.",
      );
    }

    const targetVersion = input.targetVersion.trim();
    if (targetVersion.includes("-preview.") || launcher.currentVersion?.includes("-preview.")) {
      return yield* failWith(
        "Jones main previews require qualified Download staging and a separate explicit Install action.",
      );
    }
    if (!isExactServiceVersion(targetVersion)) {
      return yield* failWith(`'${targetVersion}' is not an exact t3 version.`);
    }
    if (yield* Ref.getAndSet(inFlight, true)) {
      return yield* failWith("A server update is already in progress.");
    }

    return yield* Effect.gen(function* () {
      yield* reportProgress("downloading");
      const paths = yield* ensurePinnedRuntimeInstalled({
        baseDir: serverConfig.baseDir,
        version: targetVersion,
        fs,
        path,
        runner,
        httpClient,
        platform,
        arch,
        releaseBaseUrl,
        validate: (runtime) =>
          runner
            .run({
              command: pinnedRuntimeCommand(runtime).command,
              args: [
                ...pinnedRuntimeCommand(runtime).args,
                "__service-preflight",
                "--database-path",
                serverConfig.dbPath,
                "--launcher-protocol",
                String(SERVICE_LAUNCHER_PROTOCOL),
              ],
              timeout: PREFLIGHT_TIMEOUT,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new PinnedRuntimeInstallError({
                    step: "running the staged service preflight",
                    cause,
                  }),
              ),
              Effect.flatMap(
                (
                  result,
                ): Effect.Effect<
                  void,
                  PinnedRuntimeInstallError | PinnedRuntimePreflightBlockedError
                > => {
                  if (result.code !== 0) {
                    return Effect.fail(
                      new PinnedRuntimeInstallError({
                        step: "running the staged service preflight",
                        exitCode: Number(result.code),
                        stdoutLength: result.stdout.length,
                        stderrLength: result.stderr.length,
                      }),
                    );
                  }
                  let parsed: unknown;
                  try {
                    parsed = JSON.parse(result.stdout.trim());
                  } catch (cause) {
                    return Effect.fail(
                      new PinnedRuntimeInstallError({
                        step: "decoding the staged service preflight",
                        cause,
                      }),
                    );
                  }
                  const preflight = decodeServicePreflightResult(parsed);
                  if (preflight === undefined || preflight.version !== targetVersion) {
                    return Effect.fail(
                      new PinnedRuntimeInstallError({
                        step: "verifying the staged service preflight",
                      }),
                    );
                  }
                  return preflight.status === "ready"
                    ? Effect.void
                    : Effect.fail(
                        new PinnedRuntimePreflightBlockedError({
                          version: targetVersion,
                          reason: preflight.reason,
                        }),
                      );
                },
              ),
            ),
      }).pipe(
        Effect.mapError((error) =>
          error._tag === "PinnedRuntimePreflightBlockedError"
            ? failWith(error.reason, error)
            : error._tag === "JonesRuntimePolicyError"
              ? failWith(error.message, error)
              : failWith(`Could not prepare t3@${targetVersion}.`, error),
        ),
      );

      yield* reportProgress("installing");
      const updateId = yield* Effect.uninterruptible(
        launcher.requestUpdate({ targetVersion, dbPath: serverConfig.dbPath }).pipe(
          Effect.mapError((error) =>
            failWith(
              error._tag === "ServiceLauncherRejectedError"
                ? error.reason
                : "Could not ask the service launcher to activate the prepared update.",
              error,
            ),
          ),
          Effect.tap(() => onHandoffAccepted()),
        ),
      );

      yield* Effect.logInfo("Server update prepared; handing off to the service launcher.", {
        updateId,
        targetVersion,
        runtimePath: paths.entryPath,
      });
      return { targetVersion, method: "boot-service" as const, updateId };
    }).pipe(Effect.onError(() => Ref.set(inFlight, false)));
  });

  const requireQualifiedLauncher = (install = false) => {
    if (
      capability !== "boot-service" ||
      (install ? launcher.qualifiedUpdates !== true : launcher.qualifiedStaging !== true) ||
      launcher.currentVersion === undefined
    ) {
      throw new Error(
        "bootstrap-required: This host needs a qualified Jones runtime and launcher before installing Actions artifacts.",
      );
    }
    return launcher.currentVersion;
  };
  const validateQualifiedCandidate = (entryPath: string, databasePath: string, version: string) =>
    Effect.gen(function* () {
      const result = yield* runner
        .run({
          command: entryPath,
          args: [
            "__service-preflight",
            "--database-path",
            databasePath,
            "--launcher-protocol",
            String(SERVICE_LAUNCHER_PROTOCOL),
          ],
          timeout: PREFLIGHT_TIMEOUT,
        })
        .pipe(
          Effect.mapError((cause) =>
            failWith(
              "startup-gate-unavailable: Could not preflight the qualified candidate.",
              cause,
            ),
          ),
        );
      if (result.timedOut || result.stdoutTruncated || result.stdoutInvalidUtf8)
        return yield* failWith(
          "startup-gate-unavailable: The candidate preflight response was incomplete.",
        );
      const reason = qualifiedServicePreflightFailure({
        code: result.code,
        stdout: result.stdout,
        version,
      });
      if (reason !== undefined) return yield* failWith(reason);
    });
  const stageQualified = (artifact: JonesStagedArtifact) =>
    Effect.tryPromise({
      try: async () => {
        const activeVersion = requireQualifiedLauncher();
        const staged = await validateJonesStagedArtifact(
          NodePath.dirname(artifact.payloadPath),
          artifact.candidate,
        );
        if (
          staged.candidate.platform !== platform ||
          staged.candidate.architecture !== arch ||
          (platform !== "linux" && platform !== "darwin")
        )
          throw new Error(
            "This service requires an Actions artifact matching its native platform.",
          );
        const binding = await currentQualifiedRuntimeBinding(serverConfig.baseDir, activeVersion, {
          platform,
          architecture: arch,
        });
        if (binding.dbPath !== (await NodeFSP.realpath(serverConfig.dbPath)))
          throw new Error("This qualified staging binding does not match the running database.");
        await NodeFSP.mkdir(NodePath.join(binding.baseDir, "runtime"), {
          recursive: true,
          mode: 0o700,
        });
        const scratch = await NodeFSP.mkdtemp(
          NodePath.join(binding.baseDir, "runtime", ".jones-extract-"),
        );
        try {
          if (staged.candidate.platform === "darwin") {
            await extractQualifiedDarwinRuntime({
              artifact: staged,
              destination: scratch,
              baseDir: binding.baseDir,
              run: async (command) => {
                const result = await runPromise(
                  runner.run({ ...command, timeout: Duration.minutes(5) }),
                );
                return { code: result.code ?? 1, stdout: result.stdout };
              },
            });
          } else await extractQualifiedLinuxArchive(staged.payloadPath, scratch);
          await validateJonesStagedArtifact(NodePath.dirname(staged.payloadPath), staged.candidate);
          return await stageQualifiedRuntime({
            artifact: qualifiedRuntimeArtifactFromJonesStage(staged, scratch),
            binding,
            host: { platform, architecture: arch },
            validate: async (entryPath) => {
              // Darwin Download inspects app metadata and ASAR; it never launches
              // an unsigned candidate against the owner's native service home.
              if (staged.candidate.platform === "darwin") return;
              await runPromise(
                validateQualifiedCandidate(entryPath, binding.dbPath, staged.receipt.version),
              );
            },
          });
        } finally {
          await NodeFSP.rm(scratch, { recursive: true, force: true });
        }
      },
      catch: (cause) =>
        failWith(
          cause instanceof Error ? cause.message : "Could not stage the qualified Jones runtime.",
          cause,
        ),
    });
  const installQualified: NonNullable<ServerSelfUpdate["Service"]["installQualified"]> = (
    input,
    onHandoffAccepted = () => Effect.void,
  ) =>
    Effect.gen(function* () {
      const staged = yield* Effect.tryPromise({
        try: () =>
          verifyStagedQualifiedRuntime(
            serverConfig.baseDir,
            requireQualifiedLauncher(true),
            input.stagedHandle,
            { platform, architecture: arch },
          ),
        catch: (cause) =>
          failWith(
            cause instanceof Error ? cause.message : "The staged update binding is invalid.",
            cause,
          ),
      });
      const actualDatabase = yield* Effect.tryPromise({
        try: () => NodeFSP.realpath(serverConfig.dbPath),
        catch: (cause) => failWith("Could not bind the running database.", cause),
      });
      if (staged.binding.dbPath !== actualDatabase)
        return yield* failWith("The staged update belongs to a different running database.");
      yield* validateQualifiedCandidate(
        NodePath.join(staged.binding.baseDir, "runtime", "versions", staged.receipt.version, "t3"),
        staged.binding.dbPath,
        staged.receipt.version,
      );
      const updateId = yield* Effect.uninterruptible(
        launcher
          .requestUpdate({
            targetVersion: staged.receipt.version,
            dbPath: staged.binding.dbPath,
            stagedHandle: staged.stagedHandle,
          })
          .pipe(
            Effect.mapError((cause) => failWith(cause.message, cause)),
            Effect.tap(() => onHandoffAccepted()),
          ),
      );
      return { targetVersion: staged.receipt.version, method: "boot-service" as const, updateId };
    });

  return ServerSelfUpdate.of({
    update,
    stageQualified,
    installQualified,
    commitDesktopUpdate: (requestId, onHandoffAccepted) =>
      desktopAppUpdate.commit(requestId, onHandoffAccepted),
  });
});

export const layer = Layer.effect(ServerSelfUpdate, make()).pipe(
  Layer.provide(ProcessRunner.layer),
);
