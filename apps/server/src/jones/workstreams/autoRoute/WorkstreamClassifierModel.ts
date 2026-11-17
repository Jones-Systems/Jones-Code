import type { ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ProviderInstances from "../../../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as TextGenerators from "../../../textGeneration/TextGenerators.ts";
import { resolveRemoteEvaluationModel } from "../../../textGeneration/jones/remoteEvaluationModel.ts";
import { AUTO_ROUTE_BOUNDS } from "./policy.ts";

export class WorkstreamClassifierModelError extends Schema.TaggedError<WorkstreamClassifierModelError>()(
  "WorkstreamClassifierModelError",
  {
    category: Schema.Literals(["unavailable", "timeout"]),
  },
) {}

interface RequestedModel {
  readonly instanceId: string;
  readonly model: string;
}

export class WorkstreamClassifierModel extends Context.Service<
  WorkstreamClassifierModel,
  {
    readonly run: (input: {
      readonly prompt: string;
      readonly projectId: ProjectId;
      readonly onRequested: (requested: RequestedModel) => Effect.Effect<void>;
    }) => Effect.Effect<string, WorkstreamClassifierModelError>;
  }
>()("t3/jones/workstreams/autoRoute/WorkstreamClassifierModel") {}

const unavailable = () => new WorkstreamClassifierModelError({ category: "unavailable" });

const make = Effect.gen(function* () {
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const generators = yield* TextGenerators.TextGenerators;
  const registry = yield* ProviderInstances.ProviderInstanceRegistry;
  return WorkstreamClassifierModel.of({
    run: Effect.fn("WorkstreamClassifierModel.run")(function* (input) {
      const readConfigured = settingsService.getConfiguredTextGeneration;
      if (readConfigured === undefined) return yield* unavailable();
      const configured = yield* readConfigured(input.projectId).pipe(Effect.mapError(unavailable));
      if (configured === null) return yield* unavailable();

      const { selection } = configured;
      const request = { prompt: input.prompt, modelSelection: selection };
      const generator = configured.transport === "generator";
      const transport = generator
        ? generators.generatePromptOnlyText
        : (yield* resolveRemoteEvaluationModel(selection).pipe(
            Effect.provideService(ProviderInstances.ProviderInstanceRegistry, registry),
            Effect.mapError(unavailable),
          )).generatePromptOnlyText;
      if (transport === undefined || (generator && selection.options?.length))
        return yield* unavailable();

      yield* input.onRequested({ instanceId: selection.instanceId, model: selection.model });
      return yield* transport(request).pipe(
        Effect.mapError(unavailable),
        Effect.timeoutOrElse({
          duration: AUTO_ROUTE_BOUNDS.modelTimeoutMs,
          orElse: () => Effect.fail(new WorkstreamClassifierModelError({ category: "timeout" })),
        }),
      );
    }),
  });
});

export const layer = Layer.effect(WorkstreamClassifierModel, make);
