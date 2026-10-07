import {
  AuthOrchestrationOperateScope,
  EnvironmentHttpApi,
  type EnvironmentInternalError,
  type EnvironmentRequestInvalidError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import {
  annotateEnvironmentRequest,
  requireEnvironmentScope,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
} from "../auth/http.ts";
import * as QueueCompatibility from "./QueueCompatibility.ts";

export const queueCompatibilityHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "queueDispatch",
  Effect.fnUntraced(function* (handlers) {
    const queue = yield* QueueCompatibility.QueueCompatibility;
    return handlers.handle(
      "dispatch",
      Effect.fn("queueDispatch.dispatch")(function* (args) {
        yield* annotateEnvironmentRequest(args.endpoint.name);
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        return yield* queue
          .dispatch(args.payload)
          .pipe(
            Effect.catchTag(
              "QueueCompatibilityError",
              (
                error,
              ): Effect.Effect<never, EnvironmentInternalError | EnvironmentRequestInvalidError> =>
                error.reason === "orchestration_dispatch_failed"
                  ? failEnvironmentInternal(error.reason, error.cause)
                  : failEnvironmentInvalidRequest(error.reason),
            ),
          );
      }),
    );
  }),
);
