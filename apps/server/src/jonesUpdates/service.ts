// @effect-diagnostics nodeBuiltinImport:off
// Native activation receipt and cache paths share the detached launcher protocol.
import {
  type ThreadId,
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
import packageJson from "../../package.json" with { type: "json" };
import { retainStagedSelection, restoreStagedSelection } from "./stagedSelection.ts";
import { prepareNativeContinuationReceipt } from "./nativePreparation.ts";
import { isJonesRuntime, isPreviewRuntime } from "./qualification.ts";
import { JonesUpdater } from "./JonesUpdater.ts";
import { readQualifiedRuntimeReceipt } from "../cloud/qualifiedRuntime.ts";
import * as ServerConfig from "../config.ts";
import * as SelfUpdate from "../cloud/selfUpdate.ts";
import * as Launcher from "../cloud/serviceLauncherClient.ts";
import * as Startup from "../serverRuntimeStartup.ts";
import type { RestartContinuationMarkerV2 } from "../orchestration-v2/EventSink.ts";
import * as DesktopReceiver from "../resourceTelemetry/DesktopTelemetryReceiver.ts";

export class JonesUpdates extends Context.Service<
  JonesUpdates,
  {
    readonly state: (after?: number) => Effect.Effect<JonesUpdateState | null>;
    readonly check: Effect.Effect<JonesUpdateState>;
    readonly prepareNative: (input: JonesUpdateInstallInput) => Effect.Effect<JonesUpdateState>;
    readonly download: (input: JonesUpdateDownloadInput) => Effect.Effect<JonesUpdateState>;
    readonly install: (input: JonesUpdateInstallInput) => Effect.Effect<JonesUpdateState>;
  }
>()("t3/jonesUpdates/service/JonesUpdates") {}

const blocked = (message: string): JonesUpdateState => ({
  source: "jones-actions",
  channel: "jones-main",
  phase: "blocked",
  message,
  capability: { check: false, download: false, install: false, reason: "bootstrap-required" },
});

export const captureServerUpdateContinuations = <E>(input: {
  readonly prepare: Effect.Effect<ReadonlyArray<RestartContinuationMarkerV2>, E>;
  readonly clear: (markers: ReadonlyArray<RestartContinuationMarkerV2>) => Effect.Effect<void, E>;
}) => {
  const batches = new WeakMap<ReadonlyArray<string>, ReadonlyArray<RestartContinuationMarkerV2>>();
  return {
    prepare: input.prepare.pipe(
      Effect.map((markers): ReadonlyArray<ThreadId> => {
        const ids = Object.freeze(markers.map((marker) => marker.threadId));
        batches.set(ids, markers);
        return ids;
      }),
    ),
    clear: (ids: ReadonlyArray<string>): Effect.Effect<void, E | ServerSelfUpdateError> =>
      Effect.suspend((): Effect.Effect<void, E | ServerSelfUpdateError> => {
        const markers = batches.get(ids);
        if (!markers) return Effect.fail(new ServerSelfUpdateError({
          reason: "Continuation batch identity is unavailable or already consumed.",
        }));
        // Consume before clearing: a failed or interrupted clear must not authorize a blind retry.
        batches.delete(ids);
        return input.clear(markers);
      }),
  };
};

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
    const nativeReceipt = isPreviewRuntime(version)
      ? yield* Effect.tryPromise(() =>
          readQualifiedRuntimeReceipt(config.baseDir, version, { platform, architecture }),
        ).pipe(Effect.match({ onFailure: () => false, onSuccess: () => true }))
      : false;
    if (
      !isJonesRuntime({
        version,
        buildMetadata: packageJson,
        qualifiedRuntimeReceipt: nativeReceipt,
      })
    ) {
      const unavailable = Effect.succeed(blocked("This host uses Release updates."));
      return JonesUpdates.of({
        state: () => Effect.succeed(null),
        check: unavailable,
        prepareNative: () => unavailable,
        download: () => unavailable,
        install: () => unavailable,
      });
    }
    const continuations = captureServerUpdateContinuations({
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

    const qualifiedSelfUpdate = yield* SelfUpdate.withRunningThreadContinuation({
      mode: config.mode,
      selfUpdate,
      prepare: continuations.prepare,
      clear: (ids) => ids.length === 0 ? Effect.void : continuations.clear(ids),
    });
    const desktopContinuations = captureServerUpdateContinuations({
      prepare: startup.markOptedInProviderSessionsForContinuation,
      clear: startup.clearProviderSessionContinuationMarkers,
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
                prepare: () => run(desktopContinuations.prepare),
                clear: (ids) => run(desktopContinuations.clear(ids)),
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
    const updater = new JonesUpdater({
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
        currentVersion: packageJson.version,
        capability: {
          check: supported,
          download: supported,
          install: supported && launcher.qualifiedUpdates === true && environmentId !== undefined,
          ...(supported && launcher.qualifiedUpdates === true && environmentId !== undefined
            ? {}
            : {
                reason: supported
                  ? ("bootstrap-required" as const)
                  : ("unsupported-platform" as const),
              }),
        },
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
        await retainStagedSelection(result);
        return { stagedHandle: result.stagedHandle, version: result.receipt.version };
      },
      install: async (input) => {
        if (qualifiedSelfUpdate.installQualified === undefined)
          throw new Error("bootstrap-required: Qualified Install is unavailable.");
        await run(qualifiedSelfUpdate.installQualified(input));
      },
    });
    yield* Effect.sleep("15 seconds").pipe(
      Effect.andThen(Effect.promise(() => updater.check())),
      Effect.forkScoped,
    );
    yield* Effect.sleep("4 minutes").pipe(
      Effect.andThen(Effect.promise(() => updater.check())),
      Effect.forever,
      Effect.forkScoped,
    );
    return JonesUpdates.of({
      prepareNative: () =>
        Effect.succeed(blocked("Native desktop preparation is unavailable on this server.")),
      state: (after) => Effect.promise((signal) => updater.observe(after, signal)),
      check: Effect.promise(() => updater.check()),
      download: (input) => Effect.promise(() => updater.download(input)),
      install: (input) => Effect.promise(() => updater.install(input)),
    });
  }),
);
