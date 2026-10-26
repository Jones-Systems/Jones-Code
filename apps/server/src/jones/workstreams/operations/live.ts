import { ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Random from "effect/Random";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as WorkstreamGateway from "../../../workstreams/WorkstreamGateway.ts";
import * as RegistrationContext from "../registrationContext/service.ts";
import { Activation, ExecutionContext, Ports } from "./ports.ts";
import * as Operations from "./service.ts";

const portsLayer = Layer.effect(
  Ports,
  Effect.gen(function* () {
    const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const gateway = yield* WorkstreamGateway.WorkstreamGateway;
    const registration = yield* RegistrationContext.WorkstreamsRegistrationContext;
    const activation = yield* Activation;
    const execution = yield* ExecutionContext;
    return Ports.of({
      ...(execution.mode === "native" ? { native: execution } : {}),
      environmentId: yield* environment.getEnvironmentId,
      activation: activation.status,
      registryId: activation.registryId,
      gateway,
      registration,
      threadExists: (id) =>
        store.getThreadShell(ThreadId.make(id)).pipe(
          Effect.map((thread) => thread !== null),
          Effect.mapError(() => "unavailable" as const),
        ),
      now: Clock.currentTimeMillis,
      sleep: (milliseconds) => Effect.sleep(milliseconds),
      jitter: Random.next,
    });
  }),
);

export const layer = Operations.layer.pipe(Layer.provide(portsLayer));
