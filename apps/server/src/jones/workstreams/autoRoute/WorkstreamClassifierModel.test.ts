import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProjectId, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as ServerSettings from "../../../serverSettings.ts";
import * as TextGeneration from "../../../textGeneration/TextGeneration.ts";
import * as Model from "./WorkstreamClassifierModel.ts";

const projectId = ProjectId.make("synthetic-project");

const fixture = (
  generate: TextGeneration.TextGeneration["Service"]["generateTranscriptionPostProcessing"],
) =>
  Model.layer.pipe(
    Layer.provide(
      Layer.mock(TextGeneration.TextGeneration)({ generateTranscriptionPostProcessing: generate }),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provideMerge(NodeServices.layer),
  );

it.effect(
  "sends the prompt verbatim from an empty temporary directory with the title model",
  () => {
    const seen: { cwd: string; prompt: string; model: string; empty: boolean }[] = [];
    return Effect.gen(function* () {
      const settings = yield* (yield* ServerSettings.ServerSettingsService).getSettings;
      const model = yield* Model.WorkstreamClassifierModel;
      const requested: unknown[] = [];
      const result = yield* model.run({
        prompt: "classify this",
        projectId,
        onRequested: (selection) => Effect.sync(() => void requested.push(selection)),
      });
      assert.strictEqual(result.text, '{"choice":null}');
      assert.deepStrictEqual(
        seen.map((s) => [s.prompt, s.model, s.empty]),
        [["classify this", settings.textGenerationModelSelection.model, true]],
      );
      assert.deepStrictEqual(requested, [result.requested]);
      const fs = yield* FileSystem.FileSystem;
      assert.isFalse(yield* fs.exists(seen[0]!.cwd));
    }).pipe(
      Effect.provide(
        Layer.merge(
          fixture((input) =>
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem;
              seen.push({
                cwd: input.cwd,
                prompt: input.prompt,
                model: input.modelSelection.model,
                empty: (yield* fs.readDirectory(input.cwd)).length === 0,
              });
              return { transcription: '{"choice":null}' };
            }).pipe(Effect.provide(NodeServices.layer), Effect.orDie),
          ),
          ServerSettings.layerTest(),
        ),
      ),
    );
  },
);

it.effect("reports provider failures as unavailable and slow replies as timeouts", () =>
  Effect.gen(function* () {
    const failing = yield* Model.WorkstreamClassifierModel.pipe(
      Effect.flatMap((m) => m.run({ prompt: "p", projectId })),
      Effect.flip,
      Effect.provide(
        fixture(() =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateTranscriptionPostProcessing",
              detail: "off",
            }),
          ),
        ),
      ),
    );
    assert.strictEqual(failing.category, "unavailable");
    const slow = yield* Model.WorkstreamClassifierModel.pipe(
      Effect.flatMap((m) => m.run({ prompt: "p", projectId })),
      Effect.flip,
      Effect.forkChild,
      Effect.provide(fixture(() => Effect.never)),
    );
    yield* TestClock.adjust("46 seconds");
    assert.strictEqual((yield* Fiber.join(slow)).category, "timeout");
  }),
);
