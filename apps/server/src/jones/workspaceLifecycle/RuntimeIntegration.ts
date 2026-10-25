import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ServerConfig from "../../config.ts";
import { ServerEnvironmentIdentity } from "../../environment/ServerEnvironment.ts";
import * as Targets from "../ordinaryCheckout/CheckoutTarget.ts";
import * as TargetNode from "../ordinaryCheckout/CheckoutTargetNode.ts";
import { Configuration } from "./Configuration.ts";
import * as Sources from "./CheckoutSource.ts";
import * as Native from "./NativeSource.ts";
import { LifecycleError } from "./Protocol.ts";
import * as Registry from "./RegistryCli.ts";
import * as Store from "./TransitionStore.ts";
import * as Lifecycle from "./WorkspaceLifecycle.ts";
import * as NativeOperation from "./NativeOperation.ts";

const decode = Schema.decodeUnknownEffect(Configuration, { onExcessProperty: "error" });
const observations = Sources.layer.pipe(
  Layer.provide(Targets.layer),
  Layer.provideMerge(TargetNode.layer),
);
const source = Native.layer.pipe(Layer.provide(observations), Layer.provideMerge(Store.layer));

// Server-owned opt-in only. Construction performs no external registry command or store migration.
export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    if (config.workspaceLifecycle === undefined) return Layer.empty;
    const selected = yield* decode(config.workspaceLifecycle).pipe(
      Effect.mapError((cause) => new LifecycleError({ reason: "identity", cause })),
    );
    const identity = yield* Effect.serviceOption(ServerEnvironmentIdentity);
    if (Option.isNone(identity)) return yield* new LifecycleError({ reason: "unavailable" });
    if ((yield* identity.value.getEnvironmentId) !== selected.environmentId)
      return yield* new LifecycleError({ reason: "identity" });
    const configuration = Layer.succeed(Native.LifecycleConfiguration, {
      environmentId: selected.environmentId,
      projects: new Map(selected.projects.map(({ projectId, ...project }) => [projectId, project])),
    });
    return Lifecycle.layer.pipe(
      Layer.provide(source.pipe(Layer.provide(NativeOperation.layer))),
      Layer.provide(Registry.layer(selected.registry)),
      Layer.provideMerge(configuration),
    );
  }),
);
