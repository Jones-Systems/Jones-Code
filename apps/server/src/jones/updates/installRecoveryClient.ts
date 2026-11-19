import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  type EnvironmentId,
} from "@t3tools/contracts";
import type {
  JonesOperationId,
  JonesOperationReceipt,
} from "@t3tools/contracts/jones/jonesOperations";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import { GlobalFlag } from "effect/cli";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import { resolveCliAuthConfig } from "../../cli/config.ts";
import * as ServerConfig from "../../config.ts";
import { readLiveServerRuntimeState } from "../../serverRuntimeState.ts";

export class InstallRecoveryError extends Schema.TaggedError<InstallRecoveryError>()(
  "InstallRecoveryError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

const isInstallRecoveryError = Schema.is(InstallRecoveryError);

type HostAction =
  | { readonly type: "status" }
  | {
      readonly type: "operation" | "refresh-outcome" | "inspect-cleanup";
      readonly environmentId: EnvironmentId;
      readonly operationId: JonesOperationId;
    }
  | {
      readonly type: "stop";
      readonly environmentId: EnvironmentId;
      readonly operationId: JonesOperationId;
      readonly stopId: JonesOperationId;
    }
  | {
      readonly type: "recover-cleanup";
      readonly environmentId: EnvironmentId;
      readonly operationId: JonesOperationId;
      readonly jobId: JonesOperationId;
    };

const withHost = Effect.fn("InstallRecovery.withHost")(function* <A, E, R>(
  flags: { readonly baseDir: Option.Option<string>; readonly json: boolean },
  operate: boolean,
  run: (origin: string, token: string) => Effect.Effect<A, E, R>,
) {
  const logLevel = yield* GlobalFlag.LogLevel;
  const config = yield* resolveCliAuthConfig(flags, logLevel, { quietLogs: flags.json });
  const minimumLogLevel = flags.json ? "Error" : config.logLevel;
  return yield* Effect.gen(function* () {
    const runtime = yield* readLiveServerRuntimeState(config.serverRuntimeStatePath);
    if (Option.isNone(runtime)) {
      return yield* new InstallRecoveryError({
        detail: `No running Jones Code server uses ${config.baseDir}. Run this command on the host that owns the update.`,
      });
    }
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    return yield* Effect.acquireUseRelease(
      auth.issueSession({
        label: "t3 jones updates cli",
        ttl: Duration.minutes(2),
        scopes: operate
          ? [AuthOrchestrationReadScope, AuthOrchestrationOperateScope]
          : [AuthOrchestrationReadScope],
      }),
      (session) => run(runtime.value.origin, session.token),
      (session) => auth.revokeSession(session.sessionId).pipe(Effect.ignore({ log: true })),
    );
  }).pipe(
    Effect.scoped,
    Effect.provideService(References.MinimumLogLevel, minimumLogLevel),
    Effect.provide(
      EnvironmentAuth.layerRuntime.pipe(
        Layer.provide(ServerConfig.layer(config)),
        Layer.provide(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)),
      ),
    ),
  );
});

function verifyReceipt(
  receipt: JonesOperationReceipt | null,
  environmentId: EnvironmentId,
  operationId: JonesOperationId,
) {
  if (
    receipt !== null &&
    (receipt.environmentId !== environmentId || receipt.operationId !== operationId)
  ) {
    return Effect.fail(
      new InstallRecoveryError({ detail: "The host returned a receipt for a different update." }),
    );
  }
  return Effect.succeed(receipt);
}

export const runHostRecovery = Effect.fn("InstallRecovery.runHost")(function* (
  flags: Parameters<typeof withHost>[0],
  action: HostAction,
) {
  const operate = action.type !== "status" && action.type !== "operation";
  return yield* withHost(flags, operate, (origin, token) =>
    Effect.gen(function* () {
      const client = (yield* HttpApiClient.make(EnvironmentHttpApi, { baseUrl: origin }))
        .jonesUpdates;
      const headers = { authorization: `Bearer ${token}` };
      const state = yield* client.state({ headers, query: {} });
      if (action.type === "status") return { state };
      if (state?.environmentId !== action.environmentId) {
        return yield* new InstallRecoveryError({
          detail: "The live server does not match the requested environment ID.",
        });
      }
      const { environmentId, operationId } = action;
      if (action.type === "stop") {
        const receipt = yield* client.operationStop({
          headers,
          payload: { environmentId, operationId, stopId: action.stopId },
        });
        return { receipt: yield* verifyReceipt(receipt, environmentId, operationId) };
      }
      const inspected = action.type === "inspect-cleanup" || action.type === "recover-cleanup";
      const read = yield* client.operationRead({
        headers,
        payload: {
          environmentId,
          operationIds: [operationId],
          ...(inspected ? { inspectCleanup: operationId } : {}),
          ...(action.type === "refresh-outcome" ? { refreshInstallOutcome: operationId } : {}),
        },
      });
      const receipt = yield* verifyReceipt(
        read.receipts.find((entry) => entry.operationId === operationId)?.receipt ?? null,
        environmentId,
        operationId,
      );
      const inspection = read.cleanupInspection;
      if (action.type !== "recover-cleanup") {
        return {
          receipt,
          ...(inspected
            ? { cleanup: { operationId, status: inspection?.status ?? "unproved" } }
            : {}),
        };
      }
      if (
        receipt?.jobId !== action.jobId ||
        inspection?.operationId !== operationId ||
        inspection.status !== "eligible" ||
        inspection.recoveryToken === undefined
      ) {
        return yield* new InstallRecoveryError({
          detail:
            "The requested job did not pass a fresh cleanup inspection. Read the operation again before retrying.",
        });
      }
      const result = yield* client.recoverCleanup({
        headers,
        payload: {
          environmentId,
          operationId,
          jobId: action.jobId,
          recoveryToken: inspection.recoveryToken,
          confirmed: true,
        },
      });
      if (result.recoveredCleanup === null) {
        return yield* new InstallRecoveryError({
          detail:
            "The host could not prove cleanup release. Read the operation again before retrying.",
        });
      }
      return { receipt: yield* verifyReceipt(result.recoveredCleanup, environmentId, operationId) };
    }).pipe(
      Effect.timeout(Duration.seconds(20)),
      Effect.mapError((error) =>
        isInstallRecoveryError(error)
          ? error
          : new InstallRecoveryError({
              detail: `The host did not return a usable ${action.type} response. Check the selected server and CLI version. Read the exact operation before repeating an action.`,
            }),
      ),
      Effect.provide(FetchHttpClient.layer),
    ),
  );
});
