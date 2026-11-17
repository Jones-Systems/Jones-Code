import type { ProjectId } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ServerSettings from "../../../serverSettings.ts";
import * as TextGeneration from "../../../textGeneration/TextGeneration.ts";
import { AUTO_ROUTE_BOUNDS } from "./policy.ts";

export class WorkstreamClassifierModelError extends Schema.TaggedError<WorkstreamClassifierModelError>()(
  "WorkstreamClassifierModelError",
  {
    category: Schema.Literals(["unavailable", "timeout"]),
    /** Requested selection when it was resolved; not the provider's observed identity. */
    requested: Schema.NullOr(Schema.Struct({ instanceId: Schema.String, model: Schema.String })),
    cause: Schema.Defect(),
  },
) {}

export interface ClassifierModelResult {
  readonly text: string;
  readonly requested: { readonly instanceId: string; readonly model: string };
}

/**
 * Runs one classification prompt on the environment's text-generation model. It reuses
 * `generateTranscriptionPostProcessing`, the only provider-agnostic prompt-in/string-out operation;
 * spans and provider errors are therefore labeled as transcription post-processing.
 */
export class WorkstreamClassifierModel extends Context.Service<
  WorkstreamClassifierModel,
  {
    readonly run: (input: {
      readonly prompt: string;
      readonly projectId: ProjectId;
      /** Called with the resolved selection before the provider is invoked. */
      readonly onRequested?: (requested: ClassifierModelResult["requested"]) => Effect.Effect<void>;
    }) => Effect.Effect<ClassifierModelResult, WorkstreamClassifierModelError>;
  }
>()("t3/jones/workstreams/autoRoute/WorkstreamClassifierModel") {}

const make = Effect.gen(function* () {
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const fileSystem = yield* FileSystem.FileSystem;
  const textGeneration = yield* TextGeneration.TextGeneration;
  return WorkstreamClassifierModel.of({
    run: Effect.fn("WorkstreamClassifierModel.run")(function* (input) {
      const settings = yield* settingsService.getSettings.pipe(
        Effect.mapError(
          (cause) =>
            new WorkstreamClassifierModelError({ category: "unavailable", requested: null, cause }),
        ),
      );
      // Same selection and trust boundary as automatic thread titles.
      const selection = resolveProjectSettings(settings, input.projectId).settings
        .textGenerationModelSelection;
      const requested = { instanceId: String(selection.instanceId), model: selection.model };
      if (input.onRequested) yield* input.onRequested(requested);
      return yield* Effect.gen(function* () {
        // Providers run in a working directory; classification must not read a checkout.
        const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-workstream-route-" });
        const generated = yield* textGeneration.generateTranscriptionPostProcessing({
          cwd,
          prompt: input.prompt,
          modelSelection: selection,
        });
        return { text: generated.transcription, requested };
      }).pipe(
        Effect.scoped,
        Effect.mapError(
          (cause) =>
            new WorkstreamClassifierModelError({ category: "unavailable", requested, cause }),
        ),
        Effect.timeoutOrElse({
          duration: AUTO_ROUTE_BOUNDS.modelTimeoutMs,
          orElse: () =>
            Effect.fail(
              new WorkstreamClassifierModelError({
                category: "timeout",
                requested,
                cause: "timeout",
              }),
            ),
        }),
      );
    }),
  });
});

export const layer = Layer.effect(WorkstreamClassifierModel, make);
