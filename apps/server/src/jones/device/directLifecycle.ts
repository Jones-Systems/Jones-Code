import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/unstable/http/HttpClient";
import { DeviceHostError } from "../../device/DeviceHost.ts";

/** Registered before bootstrap so cancellation reconciles a partially started gateway. */
export const registerDirectRetirement = <E>(input: {
  readonly scope: Scope.Closeable;
  readonly direct: boolean;
  readonly retire: Effect.Effect<unknown, E>;
  readonly deactivate: () => void;
  readonly onFailure: (cause: E) => void;
}) =>
  Scope.addFinalizer(
    input.scope,
    Effect.gen(function* () {
      input.deactivate();
      if (input.direct)
        yield* input.retire.pipe(
          Effect.catch((cause) => Effect.sync(() => input.onFailure(cause))),
        );
    }),
  );

export const directGatewayHealth = (
  http: HttpClient.HttpClient,
  gatewayProbePort: number | undefined,
  owner: string,
  generation: string,
  hostId: string,
) =>
  gatewayProbePort !== undefined
    ? http.get(`http://127.0.0.1:${gatewayProbePort}/readyz`).pipe(
        Effect.flatMap((response) =>
          Effect.gen(function* () {
            if (response.status !== 200)
              return yield* new DeviceHostError({
                hostId,
                step: "checking direct gateway health",
                cause: new Error("Gateway health failed."),
              });
            return yield* response.json;
          }),
        ),
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.Struct({ owner: Schema.String, generation: Schema.String }),
          ),
        ),
        Effect.map((value) => value.owner === owner && value.generation === generation),
        Effect.timeout("5 seconds"),
        Effect.orElseSucceed(() => false),
      )
    : Effect.succeed(true);
