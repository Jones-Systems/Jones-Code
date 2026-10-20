import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Context from "effect/Context";
import { Tool } from "effect/ai";
import { resolveTarget } from "./target.ts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as McpInvocationContext from "../../mcp/McpInvocationContext.ts";
import * as RemoteAdmissionIntake from "./RemoteAdmissionIntake.ts";
import { screensRemoteOperation } from "./operations.ts";

export const wrap = <E>(tool: Tool.Any, handler: (params: object) => Effect.Effect<unknown, E>) =>
  Effect.gen(function* () {
    const intake = yield* Effect.serviceOption(RemoteAdmissionIntake.RemoteAdmissionIntake);
    if (Option.isNone(intake)) return handler;
    const services = yield* Effect.context<never>();
    intake.value.register(
      tool.name,
      (params, invocation) =>
        Schema.decodeUnknownEffect(tool.parametersSchema as Schema.Codec<unknown>)(params).pipe(
          Effect.flatMap((decoded) => handler(decoded as object)),
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provide(services),
          Effect.mapError((error) =>
            Schema.is(OrchestratorMcpFailure)(error)
              ? error
              : new RemoteAdmissionIntake.RemoteAdmissionIntakeError({ cause: error }),
          ),
        ),
      (params, invocation) =>
        resolveTarget(params, invocation).pipe(
          Effect.provide(services),
          Effect.mapError(
            (cause) => new RemoteAdmissionIntake.RemoteAdmissionIntakeError({ cause }),
          ),
        ),
    );
    return (params: object) =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext.McpInvocationContext;
        if (
          invocation.client === undefined ||
          !screensRemoteOperation(
            tool.name,
            params as Record<string, unknown>,
            Context.get(tool.annotations, Tool.Readonly),
          )
        )
          return yield* handler(params);
        return yield* intake.value.invoke(tool.name, params as Record<string, unknown>, invocation);
      });
  });
