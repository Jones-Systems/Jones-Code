import type { ServerSelfUpdateOutcome } from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import packageJson from "../../package.json" with { type: "json" };
import {
  assertQualifiedTrialBinding,
  assertQualifiedResumeUnreserved,
  makeQualifiedTrialReceipt,
  reserveQualifiedResume,
  sameQualifiedTrialIdentity,
  type QualifiedTrialReceipt,
  type QualifiedTrialRuntimeWitness,
} from "../jones/cloud/qualifiedStartup.ts";
import {
  decodeServiceLauncherContext,
  decodeServiceLauncherParentMessage,
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
  type ServiceLauncherChildMessage,
  type ServiceLauncherParentMessage,
} from "./serviceProtocol.ts";

export class ServiceLauncherClientError extends Schema.TaggedError<ServiceLauncherClientError>()(
  "ServiceLauncherClientError",
  {
    operation: Schema.Literals([
      "decode-context",
      "version-mismatch",
      "ipc-unavailable",
      "unmanaged",
      "send",
      "disconnect",
      "timeout",
      "qualified-proof",
      "qualified-replay",
      "qualified-reservation",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.operation) {
      case "decode-context":
        return "The service launcher supplied invalid startup context.";
      case "version-mismatch":
        return "The service launcher started a different t3 version.";
      case "ipc-unavailable":
        return "The service launcher IPC channel is unavailable.";
      case "unmanaged":
        return "This server is not managed by the launcher.";
      case "send":
        return "Could not send a request to the service launcher.";
      case "disconnect":
        return "The service launcher disconnected before acknowledging the request.";
      case "timeout":
        return "The service launcher did not respond within 30 seconds.";
      case "qualified-proof":
        return "The qualified startup identity or grant is missing, unsupported, or mismatched.";
      case "qualified-replay":
        return "Qualified startup already attempted a handoff; reconciliation is required.";
      case "qualified-reservation":
        return "Qualified startup resume reservation requires reconciliation.";
    }
  }
}

export class ServiceLauncherRejectedError extends Schema.TaggedError<ServiceLauncherRejectedError>()(
  "ServiceLauncherRejectedError",
  {
    targetVersion: Schema.String,
    reason: Schema.String,
  },
) {
  override get message(): string {
    return this.reason;
  }
}

interface ServiceLauncherProcess {
  readonly connected: boolean;
  readonly send: (
    message: ServiceLauncherChildMessage,
    callback?: (error: Error | null) => void,
  ) => boolean;
  readonly on: (
    event: "message" | "disconnect",
    listener: (...args: ReadonlyArray<unknown>) => void,
  ) => void;
  readonly off: (
    event: "message" | "disconnect",
    listener: (...args: ReadonlyArray<unknown>) => void,
  ) => void;
}

export const ServiceLauncherHostProcess = Context.Reference<ServiceLauncherProcess>(
  "t3/cloud/serviceLauncherHostProcess",
  {
    defaultValue: () => ({
      get connected() {
        return process.connected && process.send !== undefined;
      },
      send: (message, callback) => {
        if (process.send === undefined) return false;
        return callback === undefined ? process.send(message) : process.send(message, callback);
      },
      on: (event, listener) => {
        process.on(event, listener);
      },
      off: (event, listener) => {
        process.off(event, listener);
      },
    }),
  },
);

export class ServiceLauncherClient extends Context.Service<
  ServiceLauncherClient,
  {
    readonly managed: boolean;
    readonly requiresQualifiedTrialGate: boolean;
    readonly prepareQualifiedTrial: (
      witness: QualifiedTrialRuntimeWitness,
    ) => Effect.Effect<ServerSelfUpdateOutcome, ServiceLauncherClientError>;
    readonly qualifiedUpdates?: boolean;
    readonly qualifiedStaging?: boolean;
    readonly currentVersion?: string;
    /** Last durable terminal result; reading it never sends a prepared IPC message. */
    readonly qualifiedStartupOutcome?: ServerSelfUpdateOutcome | undefined;
    readonly requestUpdate: (input: {
      readonly targetVersion: string;
      readonly dbPath: string;
      readonly stagedHandle?: string;
    }) => Effect.Effect<string, ServiceLauncherClientError | ServiceLauncherRejectedError>;
    readonly prepareTrial: Effect.Effect<
      ServerSelfUpdateOutcome | undefined,
      ServiceLauncherClientError
    >;
  }
>()("t3/cloud/serviceLauncherClient") {}

export const QualifiedTrialOperations = Context.Reference<{
  readonly receipt: typeof makeQualifiedTrialReceipt;
  readonly assertUnreserved: typeof assertQualifiedResumeUnreserved;
  readonly reserve: typeof reserveQualifiedResume;
}>("t3/cloud/serviceLauncherQualifiedTrialOperations", {
  defaultValue: () => ({
    receipt: makeQualifiedTrialReceipt,
    assertUnreserved: assertQualifiedResumeUnreserved,
    reserve: reserveQualifiedResume,
  }),
});

// Cancellation drains native publication before the startup finalizer can touch paired state.
const qualifiedOperation = <A>(
  operation: (signal: AbortSignal) => Promise<A>,
  failure: "qualified-proof" | "qualified-reservation",
) =>
  Effect.callback<A, ServiceLauncherClientError>((resume) => {
    const controller = new AbortController();
    const completion = Promise.resolve()
      .then(() => operation(controller.signal))
      .then(
        (result) => resume(Effect.succeed(result)),
        (cause) =>
          resume(Effect.fail(new ServiceLauncherClientError({ operation: failure, cause }))),
      );
    return Effect.promise(async () => {
      controller.abort();
      await completion;
    });
  });

const resolveStartup = Effect.fn("cloud.service_launcher_client.resolve_startup")(
  function* (options?: { readonly currentVersion?: string }) {
    const host = yield* ServiceLauncherHostProcess;
    const environment = yield* HostProcessEnvironment;
    const currentVersion = options?.currentVersion ?? packageJson.version;
    const rawContext = environment[SERVICE_LAUNCHER_CONTEXT_ENV];
    const context = rawContext === undefined ? undefined : decodeServiceLauncherContext(rawContext);

    if (rawContext !== undefined && context === undefined) {
      return yield* new ServiceLauncherClientError({ operation: "decode-context" });
    }
    if (context !== undefined && context.childVersion !== currentVersion) {
      return yield* new ServiceLauncherClientError({ operation: "version-mismatch" });
    }

    const managed = context !== undefined && host.connected;
    if (context !== undefined && !managed) {
      return yield* new ServiceLauncherClientError({ operation: "ipc-unavailable" });
    }

    return { host, context, managed };
  },
);

export const resolveServiceLauncherMode = Effect.fn("cloud.service_launcher_client.resolve_mode")(
  function* () {
    const { managed } = yield* resolveStartup();
    return { managed };
  },
);

export const make = Effect.fn("cloud.service_launcher_client.make")(function* (options?: {
  readonly currentVersion?: string;
}) {
  const { host, context, managed } = yield* resolveStartup(options);
  const qualifiedOperations = yield* QualifiedTrialOperations;

  const exchange = (
    message: ServiceLauncherChildMessage,
    accept: (reply: ServiceLauncherParentMessage) => boolean,
    qualifiedReceipt?: QualifiedTrialReceipt,
  ) =>
    Effect.callback<ServiceLauncherParentMessage, ServiceLauncherClientError>((resume) => {
      if (!managed) {
        resume(Effect.fail(new ServiceLauncherClientError({ operation: "unmanaged" })));
        return;
      }

      let settled = false;
      const cleanup = () => {
        host.off("message", onMessage);
        host.off("disconnect", onDisconnect);
      };
      const settle = (
        effect: Effect.Effect<ServiceLauncherParentMessage, ServiceLauncherClientError>,
      ) => {
        if (settled) return;
        settled = true;
        cleanup();
        resume(effect);
      };
      const onMessage = (...args: ReadonlyArray<unknown>) => {
        const reply = decodeServiceLauncherParentMessage(args[0]);
        if (qualifiedReceipt !== undefined) {
          const raw = args[0];
          if (
            typeof raw === "object" &&
            raw !== null &&
            "type" in raw &&
            raw.type === "committed"
          ) {
            if (
              reply?.type !== "committed" ||
              reply.startupGateProtocol !== 1 ||
              reply.qualified === undefined ||
              reply.updateId !== qualifiedReceipt.updateId ||
              !sameQualifiedTrialIdentity(qualifiedReceipt, reply.qualified)
            ) {
              settle(Effect.fail(new ServiceLauncherClientError({ operation: "qualified-proof" })));
              return;
            }
          }
        }
        if (reply !== undefined && accept(reply)) settle(Effect.succeed(reply));
      };
      const onDisconnect = () =>
        settle(Effect.fail(new ServiceLauncherClientError({ operation: "disconnect" })));

      host.on("message", onMessage);
      host.on("disconnect", onDisconnect);
      try {
        const sent = host.send(message, (error) => {
          if (error !== null) {
            settle(
              Effect.fail(new ServiceLauncherClientError({ operation: "send", cause: error })),
            );
          }
        });
        if (!sent && qualifiedReceipt !== undefined)
          settle(Effect.fail(new ServiceLauncherClientError({ operation: "send" })));
      } catch (cause) {
        settle(Effect.fail(new ServiceLauncherClientError({ operation: "send", cause })));
      }

      return Effect.sync(cleanup);
    }).pipe(
      Effect.timeoutOrElse({
        duration: "30 seconds",
        orElse: () => Effect.fail(new ServiceLauncherClientError({ operation: "timeout" })),
      }),
    );

  const requestUpdate = (input: {
    readonly targetVersion: string;
    readonly dbPath: string;
    readonly stagedHandle?: string;
  }) =>
    input.stagedHandle !== undefined &&
    (context?.qualifiedUpdatesProtocol !== 1 || context.startupGateProtocol !== 1)
      ? Effect.fail(
          new ServiceLauncherRejectedError({
            targetVersion: input.targetVersion,
            reason:
              "bootstrap-required: The installed launcher cannot activate qualified Jones artifacts. Upgrade it on this host first.",
          }),
        )
      : context !== undefined && context.protocol !== SERVICE_LAUNCHER_PROTOCOL
        ? Effect.fail(
            new ServiceLauncherRejectedError({
              targetVersion: input.targetVersion,
              reason:
                "The installed service launcher must be upgraded before another remote update.",
            }),
          )
        : exchange(
            { type: "request-update", ...input },
            (reply) => reply.type === "update-accepted" || reply.type === "update-rejected",
          ).pipe(
            Effect.flatMap((reply) =>
              reply.type === "update-accepted"
                ? Effect.succeed(reply.updateId)
                : reply.type === "update-rejected"
                  ? Effect.fail(
                      new ServiceLauncherRejectedError({
                        targetVersion: input.targetVersion,
                        reason: reply.reason,
                      }),
                    )
                  : Effect.die("service launcher returned an impossible update response"),
            ),
          );

  const pending = context?.update?.status === "pending" ? context.update : undefined;
  let outcome: ServerSelfUpdateOutcome | undefined =
    context?.update === undefined || context.update.status === "pending"
      ? undefined
      : context.update;
  const qualifiedTransaction =
    context?.update !== undefined &&
    "qualified" in context.update &&
    context.update.qualified !== undefined;
  const requiresQualifiedTrialGate = pending !== undefined && pending.qualified !== undefined;
  let qualifiedAttempted = false;
  const prepareQualifiedTrial: ServiceLauncherClient["Service"]["prepareQualifiedTrial"] = (
    witness,
  ) =>
    Effect.suspend(() => {
      if (qualifiedAttempted)
        return Effect.fail(new ServiceLauncherClientError({ operation: "qualified-replay" }));
      qualifiedAttempted = true;
      if (
        !requiresQualifiedTrialGate ||
        pending?.qualified === undefined ||
        context?.protocol !== SERVICE_LAUNCHER_PROTOCOL ||
        context.qualifiedUpdatesProtocol !== 1 ||
        context.startupGateProtocol !== 1
      )
        return Effect.fail(new ServiceLauncherClientError({ operation: "qualified-proof" }));
      const qualified = pending.qualified;
      return Effect.gen(function* () {
        const receipt = yield* qualifiedOperation(async (signal) => {
          const receipt = await qualifiedOperations.receipt({
            updateId: pending.id,
            qualified,
            witness,
            signal,
          });
          assertQualifiedTrialBinding({ updateId: pending.id, qualified, receipt });
          await qualifiedOperations.assertUnreserved(receipt);
          return receipt;
        }, "qualified-proof");
        const reply = yield* exchange(
          { type: "prepared", updateId: pending.id, startupGateProtocol: 1, qualified: receipt },
          (reply) => reply.type === "committed",
          receipt,
        );
        if (reply.type !== "committed" || reply.qualified === undefined)
          return yield* new ServiceLauncherClientError({ operation: "qualified-proof" });
        yield* qualifiedOperation(
          (signal) => qualifiedOperations.reserve({ receipt, signal }),
          "qualified-reservation",
        );
        outcome = {
          id: pending.id,
          fromVersion: pending.fromVersion,
          targetVersion: pending.targetVersion,
          status: "committed",
        };
        return outcome;
      });
    });
  const ordinaryPrepareTrial = yield* Effect.cached(
    pending !== undefined && !requiresQualifiedTrialGate
      ? exchange(
          { type: "prepared", updateId: pending.id },
          (reply) => reply.type === "committed" && reply.updateId === pending.id,
        ).pipe(
          Effect.flatMap((reply) => {
            if (reply.type !== "committed") {
              return Effect.die("service launcher returned an impossible prepared response");
            }
            outcome = {
              id: pending.id,
              fromVersion: pending.fromVersion,
              targetVersion: pending.targetVersion,
              status: "committed" as const,
            };
            return Effect.succeed(outcome);
          }),
        )
      : Effect.succeed(outcome),
  );
  const prepareTrial = requiresQualifiedTrialGate
    ? Effect.suspend(() =>
        outcome === undefined
          ? Effect.fail(new ServiceLauncherClientError({ operation: "qualified-proof" }))
          : Effect.succeed(outcome),
      )
    : ordinaryPrepareTrial;

  return ServiceLauncherClient.of({
    managed,
    requiresQualifiedTrialGate,
    prepareQualifiedTrial,
    qualifiedUpdates: context?.qualifiedUpdatesProtocol === 1 && context.startupGateProtocol === 1,
    qualifiedStaging: context?.qualifiedUpdatesProtocol === 1,
    ...(context === undefined ? {} : { currentVersion: context.childVersion }),
    requestUpdate,
    get qualifiedStartupOutcome() {
      return qualifiedTransaction ? outcome : undefined;
    },
    prepareTrial,
  });
});

export const layer = Layer.effect(ServiceLauncherClient, make());
