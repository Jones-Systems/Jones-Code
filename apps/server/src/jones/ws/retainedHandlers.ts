import {
  CurrentRuntimeStopRequestError,
  EnvironmentAuthenticatedPrincipal,
  NativeBootstrapDispatchError,
  NativeCreationRejectionCode,
  ORCHESTRATION_V2_WS_METHODS,
  ProviderConversationReselectionError,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as LinkedThreads from "../linkedThreads/LinkedThreads.ts";
import * as ThreadReferenceExcerpts from "../orchestration/threadReferenceExcerpt.ts";
import * as RuntimeStop from "../runtime/RuntimeStop.ts";
import * as ProviderConversationReselection from "../runtime/ProviderConversationReselection.ts";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as ServerRuntimeStartup from "../../serverRuntimeStartup.ts";

const isProviderConversationReselectionError = Schema.is(ProviderConversationReselectionError);
const isNativeCreationRejectionCode = Schema.is(NativeCreationRejectionCode);

type RetainedMethod =
  | typeof ORCHESTRATION_V2_WS_METHODS.listLinkedThreads
  | typeof ORCHESTRATION_V2_WS_METHODS.linkThreads
  | typeof ORCHESTRATION_V2_WS_METHODS.unlinkThreads
  | typeof ORCHESTRATION_V2_WS_METHODS.acceptThreadHandoff
  | typeof ORCHESTRATION_V2_WS_METHODS.getThreadReferenceExcerpt
  | typeof ORCHESTRATION_V2_WS_METHODS.previewProviderConversationReselection
  | typeof ORCHESTRATION_V2_WS_METHODS.reselectProviderConversation
  | typeof ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime
  | typeof ORCHESTRATION_V2_WS_METHODS.readCurrentRuntimeStopTarget
  | typeof ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop
  | typeof ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap;
type RetainedHandlers = Pick<Parameters<typeof WsRpcGroup.of>[0], RetainedMethod>;

// Resolve owners from the socket's actual server graph; only the authenticated
// principal crosses the request boundary. Durable writes still pass startup admission.
export const make = Effect.fn("RetainedWsHandlers.make")(function* (
  runtimeStopPrincipal: EnvironmentAuthenticatedPrincipal["Service"],
) {
  const providerConversationReselection = yield* Effect.serviceOption(
    ProviderConversationReselection.ProviderConversationReselection,
  );
  const withProviderConversationReselection = <A, E, R>(
    action: (
      owner: ProviderConversationReselection.ProviderConversationReselection["Service"],
    ) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | ProviderConversationReselectionError, R> =>
    Option.isNone(providerConversationReselection)
      ? Effect.fail(new ProviderConversationReselectionError({ reason: "service_unavailable" }))
      : action(providerConversationReselection.value);
  const currentRuntimeStop = yield* Effect.serviceOption(RuntimeStop.CurrentRuntimeStop);
  const withCurrentRuntimeStop = <A, E, R>(
    action: (owner: RuntimeStop.CurrentRuntimeStop["Service"]) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | CurrentRuntimeStopRequestError, R> =>
    Option.isNone(currentRuntimeStop)
      ? Effect.fail(
          new CurrentRuntimeStopRequestError({ reason: "captured_runtime_stop_unavailable" }),
        )
      : action(currentRuntimeStop.value);

  const threadLaunch = yield* ThreadLaunchService.ThreadLaunchService;
  const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
  const sql = yield* SqlClient.SqlClient;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
  const threadReferenceExcerpts = yield* ThreadReferenceExcerpts.ThreadReferenceExcerpts.pipe(
    Effect.provide(ThreadReferenceExcerpts.layer),
  );
  const linkedThreads = LinkedThreads.LinkedThreads.pipe(
    Effect.provide(LinkedThreads.layer),
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.provideService(ThreadManagementService.ThreadManagementService, threadManagement),
    Effect.provideService(ServerEnvironment.ServerEnvironment, serverEnvironment),
  );
  return {
    [ORCHESTRATION_V2_WS_METHODS.listLinkedThreads]: (input) =>
      linkedThreads.pipe(Effect.flatMap((owner) => owner.list({ type: "user" }, input))),
    [ORCHESTRATION_V2_WS_METHODS.linkThreads]: (input) =>
      linkedThreads.pipe(
        Effect.flatMap((owner) => owner.link({ type: "user" }, { ...input, basis: "manual" })),
      ),
    [ORCHESTRATION_V2_WS_METHODS.unlinkThreads]: (input) =>
      linkedThreads.pipe(Effect.flatMap((owner) => owner.unlink({ type: "user" }, input))),
    [ORCHESTRATION_V2_WS_METHODS.acceptThreadHandoff]: (input) =>
      linkedThreads.pipe(Effect.flatMap((owner) => owner.accept({ type: "user" }, input))),
    [ORCHESTRATION_V2_WS_METHODS.getThreadReferenceExcerpt]: (input) =>
      threadReferenceExcerpts.capture(input),
    [ORCHESTRATION_V2_WS_METHODS.previewProviderConversationReselection]: (input) =>
      startup
        .enqueueCommand(
          withProviderConversationReselection((owner) =>
            owner.preview(input, runtimeStopPrincipal),
          ),
        )
        .pipe(
          Effect.mapError((error) =>
            isProviderConversationReselectionError(error)
              ? error
              : new ProviderConversationReselectionError({ reason: "startup_unavailable" }),
          ),
        ),
    [ORCHESTRATION_V2_WS_METHODS.reselectProviderConversation]: (input) =>
      startup
        .enqueueCommand(
          withProviderConversationReselection((owner) =>
            owner.reselect(input, runtimeStopPrincipal),
          ),
        )
        .pipe(
          Effect.mapError((error) =>
            isProviderConversationReselectionError(error)
              ? error
              : new ProviderConversationReselectionError({ reason: "startup_unavailable" }),
          ),
        ),
    [ORCHESTRATION_V2_WS_METHODS.stopCurrentThreadRuntime]: (input) =>
      startup
        .enqueueCommand(
          withCurrentRuntimeStop((owner) => owner.stop(input)).pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, runtimeStopPrincipal),
          ),
        )
        .pipe(
          Effect.mapError(
            () =>
              new CurrentRuntimeStopRequestError({
                reason: "captured_runtime_stop_rejected",
              }),
          ),
        ),
    [ORCHESTRATION_V2_WS_METHODS.readCurrentRuntimeStopTarget]: (input) =>
      withCurrentRuntimeStop((owner) => owner.readTarget(input.threadId)).pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, runtimeStopPrincipal),
        Effect.mapError((error) => new CurrentRuntimeStopRequestError({ reason: error.reason })),
      ),
    [ORCHESTRATION_V2_WS_METHODS.observeCurrentThreadRuntimeStop]: (input) =>
      withCurrentRuntimeStop((owner) => owner.observe(input)).pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, runtimeStopPrincipal),
        Effect.mapError((error) => new CurrentRuntimeStopRequestError({ reason: error.reason })),
      ),
    [ORCHESTRATION_V2_WS_METHODS.dispatchNativeBootstrap]: (submission) =>
      Effect.annotateCurrentSpan({ "rpc.aggregate": "orchestrationV2" }).pipe(
        Effect.andThen(
          threadLaunch.dispatchNativeBootstrap === undefined
            ? Effect.fail(
                new NativeBootstrapDispatchError({
                  code: "unsupported_authority",
                  message: "Native bootstrap is unavailable",
                }),
              )
            : startup
                .enqueueCommand(
                  threadLaunch
                    .dispatchNativeBootstrap(submission)
                    .pipe(
                      Effect.provideService(
                        EnvironmentAuthenticatedPrincipal,
                        runtimeStopPrincipal,
                      ),
                    ),
                )
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new NativeBootstrapDispatchError({
                        code:
                          "code" in cause &&
                          typeof cause.code === "string" &&
                          isNativeCreationRejectionCode(cause.code)
                            ? cause.code
                            : "unresolved_claim",
                        message: "Native bootstrap was rejected; observe the original claim",
                      }),
                  ),
                ),
        ),
      ),
  } satisfies RetainedHandlers;
});
