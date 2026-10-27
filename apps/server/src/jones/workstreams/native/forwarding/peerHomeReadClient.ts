import { EnvironmentId, ProviderInteractionMode, RuntimeMode } from "@t3tools/contracts";
import { WorkstreamsHomeReadResultRequest } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { WorkstreamsNativeHomeTarget } from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { callerLinkOrigin } from "../../../../mcp/linkOrigin.ts";
import { readCaller } from "../../../../mcp/threadAccess.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as Peer from "../../../../peer/PeerMcpClient.ts";
import { RegistryError } from "../registry/session.ts";
import * as Protocol from "./homeReadProtocol.ts";
import { checkOwnPlacementForwarding } from "../read/memberReadAdmission.ts";

/** Request-local SOURCE admission, never a default, cache or copied home grant.
 * Member enrollment and original-caller admission produce this only in an authenticated request.
 */
export class MemberHomeReadRouting extends Context.Service<
  MemberHomeReadRouting,
  {
    readonly current: () => Effect.Effect<
      {
        readonly home: WorkstreamsNativeHomeTarget;
        readonly link_generation: string;
        readonly limits: Peer.PeerCallLimits;
        readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
      },
      RegistryError
    >;
  }
>()("t3/jones/workstreams/native/forwarding/peerHomeReadClient/MemberHomeReadRouting") {}

const decodeHome = Schema.decodeUnknownEffect(WorkstreamsNativeHomeTarget);
const decodeEnvironment = Schema.decodeUnknownEffect(EnvironmentId);
const decodeRequest = Schema.decodeUnknownEffect(WorkstreamsHomeReadResultRequest);
const decodeLimits = Schema.decodeUnknownEffect(
  Schema.Struct({ runtimeMode: RuntimeMode, interactionMode: ProviderInteractionMode }),
);

// This check supplies no admission. Non-MCP callers still require the routing producer.
const refuseRecursiveCaller = Effect.fn("PeerHomeReadClient.refuseRecursiveCaller")(function* (
  home: WorkstreamsNativeHomeTarget,
  request: WorkstreamsHomeReadResultRequest,
) {
  const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
  if (Option.isNone(invocation)) return;
  const scope = invocation.value;
  if (scope.client?.linked === true || scope.client?.remoteOrigin !== undefined)
    return yield* new RegistryError({ code: "home_route_required" });
  if (scope.thread === undefined) return;
  const threads = yield* Effect.serviceOption(Threads.ThreadManagementService);
  if (Option.isNone(threads)) return yield* new RegistryError({ code: "caller_unavailable" });
  const caller = yield* readCaller().pipe(
    Effect.provideService(Mcp.McpInvocationContext, scope),
    Effect.provideService(Threads.ThreadManagementService, threads.value),
    Effect.mapError((cause) => new RegistryError({ code: "caller_unavailable", cause })),
  );
  // Generic linked reads stay refused; the private original-thread placement proof is request-local.
  if (callerLinkOrigin(caller) !== undefined)
    yield* checkOwnPlacementForwarding(home, request).pipe(
      Effect.mapError(() => new RegistryError({ code: "home_route_required" })),
    );
});

/** Fixed-tool invocation plus strict downstream decoding is invocation negotiation only.
 * There is no tools/list preflight, legacy fallback, retry or general forwarding bypass.
 */
export const layer = Layer.effect(
  Protocol.HomeReadResultClient,
  Effect.gen(function* () {
    const peer = yield* Peer.PeerMcpClient;
    const routing = yield* MemberHomeReadRouting;
    return Protocol.HomeReadResultClient.of({
      read: Effect.fn("PeerHomeReadClient.read")(function* (home, request) {
        yield* refuseRecursiveCaller(home, request);
        const admitted = yield* routing.current();
        const target = yield* decodeHome(admitted.home, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "registry_mismatch", cause })),
        );
        if (
          target.registry_instance_id !== home.registry_instance_id ||
          target.owner_id !== home.owner_id ||
          target.home_environment_id !== home.home_environment_id
        )
          return yield* new RegistryError({ code: "registry_mismatch" });
        if (target.server_generation !== home.server_generation)
          return yield* new RegistryError({ code: "generation_mismatch" });
        const environmentId = yield* decodeEnvironment(target.home_environment_id).pipe(
          Effect.mapError(
            (cause) => new RegistryError({ code: "invalid_home_environment", cause }),
          ),
        );
        const generation = admitted.link_generation;
        if (typeof generation !== "string" || generation.trim().length === 0)
          return yield* new RegistryError({ code: "link_generation_required" });
        yield* decodeLimits(admitted.limits, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "invalid_limits", cause })),
        );
        yield* decodeRequest(request, { onExcessProperty: "error" }).pipe(
          Effect.mapError((cause) => new RegistryError({ code: "invalid_request", cause })),
        );
        yield* admitted.checkCurrent();
        const result = yield* peer
          .call({
            environmentId,
            tool: "jones_workstreams_home_read_result",
            arguments: request,
            limits: admitted.limits,
            expectedGeneration: generation,
            success: Schema.Unknown,
          })
          .pipe(Effect.mapError((cause) => new RegistryError({ code: cause.code, cause })));
        yield* admitted.checkCurrent();
        return result;
      }),
    });
  }),
);
