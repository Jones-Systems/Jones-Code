import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderInstanceId,
  TextGenerationError,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as ProviderInstances from "../../../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as TextGenerators from "../../../textGeneration/TextGenerators.ts";
import * as Model from "./WorkstreamClassifierModel.ts";

const projectId = ProjectId.make("synthetic-project");
const selection = DEFAULT_SERVER_SETTINGS.textGenerationModelSelection;
const textGeneratorId = ProviderInstanceId.make("synthetic-text-generator");
const input = { projectId, prompt: "classify this", onRequested: () => Effect.void };

const provider = (
  generatePromptOnlyText?: ProviderInstance["generatePromptOnlyText"],
  model = selection.model,
) =>
  ({
    instanceId: selection.instanceId,
    enabled: true,
    snapshot: {
      getSnapshot: Effect.succeed({
        enabled: true,
        supportsRemoteRequestEvaluation: true,
        models: [
          {
            slug: model,
            capabilities: {
              optionDescriptors: [
                {
                  id: "reasoningEffort",
                  type: "select",
                  options: (selection.options ?? []).map((option) => ({ id: option.value })),
                },
              ],
            },
          },
        ],
      }),
    },
    generatePromptOnlyText,
  }) as unknown as ProviderInstance;

const modelLayer = (
  options: {
    instance?: ProviderInstance;
    generate?: TextGenerators.TextGenerators["Service"]["generatePromptOnlyText"];
    settings?: Parameters<typeof ServerSettings.layerTest>[0];
  } = {},
) =>
  Model.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ServerSettings.layerTest(
          options.settings ??
            (options.generate === undefined
              ? {}
              : {
                  textGenerators: {
                    [textGeneratorId]: {
                      kind: "openaiCompatible",
                      baseUrl: "https://generator.invalid/v1",
                    },
                  },
                  projectSettingsOverrides: {
                    [projectId]: {
                      textGenerationModelSelection: {
                        instanceId: textGeneratorId,
                        model: "exact-model",
                        options: [],
                      },
                    },
                  },
                }),
        ),
        Layer.mock(ProviderInstances.ProviderInstanceRegistry)({
          getInstance: (id) =>
            Effect.succeed(id === selection.instanceId ? options.instance : undefined),
          getCatalogSource: (id) => (id === selection.instanceId ? options.instance : undefined),
        }),
        Layer.mock(TextGenerators.TextGenerators)(
          options.generate === undefined ? {} : { generatePromptOnlyText: options.generate },
        ),
      ),
    ),
  );

it.effect("uses only the exact configured managed model and records its requested selection", () =>
  Effect.gen(function* () {
    const seen: unknown[] = [];
    const requested: unknown[] = [];
    const model = yield* Model.WorkstreamClassifierModel.pipe(
      Effect.provide(
        modelLayer({
          instance: provider((request) =>
            Effect.sync(() => {
              seen.push(request);
              return '{"choice":null}';
            }),
          ),
        }),
      ),
    );
    const result = yield* model.run({
      ...input,
      onRequested: (value) => Effect.sync(() => void requested.push(value)),
    });
    assert.strictEqual(result, '{"choice":null}');
    assert.deepStrictEqual(seen, [{ modelSelection: selection, prompt: input.prompt }]);
    assert.deepStrictEqual(requested, [
      { instanceId: selection.instanceId, model: selection.model },
    ]);
  }),
);

it.effect("honors a project-specific direct generator without an alternate provider", () =>
  Effect.gen(function* () {
    const model = yield* Model.WorkstreamClassifierModel.pipe(
      Effect.provide(
        modelLayer({
          generate: (request) =>
            Effect.sync(() => {
              assert.deepStrictEqual(request, {
                modelSelection: { instanceId: textGeneratorId, model: "exact-model", options: [] },
                prompt: input.prompt,
              });
              return "reply";
            }),
        }),
      ),
    );
    assert.strictEqual(yield* model.run(input), "reply");
  }),
);

it.effect(
  "never calls an agent or substitutes another model when the selection is unsupported",
  () =>
    Effect.gen(function* () {
      for (const instance of [
        undefined,
        provider(),
        provider(() => Effect.die("wrong model"), "different-model"),
      ]) {
        const model = yield* Model.WorkstreamClassifierModel.pipe(
          Effect.provide(modelLayer(instance ? { instance } : {})),
        );
        const result = yield* model
          .run({
            ...input,
            onRequested: () => Effect.die("an unsupported model cannot be invoked"),
          })
          .pipe(Effect.flip);
        assert.strictEqual(result.category, "unavailable");
      }
    }),
);

it.effect.each(["project", "environment"] as const)(
  "a disabled %s selection never falls back to the enabled managed model",
  (scope) =>
    Effect.gen(function* () {
      const disabledSelection = { instanceId: textGeneratorId, model: "disabled-model" };
      const model = yield* Model.WorkstreamClassifierModel.pipe(
        Effect.provide(
          modelLayer({
            instance: provider(() => Effect.die("must not invoke the fallback model")),
            settings: {
              textGenerators: {
                [textGeneratorId]: {
                  kind: "openaiCompatible",
                  baseUrl: "https://generator.invalid/v1",
                  enabled: false,
                },
              },
              ...(scope === "environment"
                ? { textGenerationModelSelection: disabledSelection }
                : {
                    projectSettingsOverrides: {
                      [projectId]: { textGenerationModelSelection: disabledSelection },
                    },
                  }),
            },
          }),
        ),
      );
      const failure = yield* model
        .run({ ...input, onRequested: () => Effect.die("must not record a model request") })
        .pipe(Effect.flip);
      assert.strictEqual(failure.category, "unavailable");
    }),
);

it.effect("reports provider failure without retaining its untrusted error text and times out", () =>
  Effect.gen(function* () {
    const model = yield* Model.WorkstreamClassifierModel.pipe(
      Effect.provide(
        modelLayer({
          instance: provider(() =>
            Effect.fail(
              new TextGenerationError({
                operation: "generate",
                detail: "private provider text",
              }),
            ),
          ),
        }),
      ),
    );
    const failure = yield* model.run(input).pipe(Effect.flip);
    assert.strictEqual(failure.category, "unavailable");
    assert.notInclude(JSON.stringify(failure), "private provider text");
    const slow = yield* Model.WorkstreamClassifierModel.pipe(
      Effect.flatMap((service) => service.run(input)),
      Effect.flip,
      Effect.forkChild,
      Effect.provide(modelLayer({ instance: provider(() => Effect.never) })),
    );
    yield* TestClock.adjust("46 seconds");
    assert.strictEqual((yield* Fiber.join(slow)).category, "timeout");
  }),
);
